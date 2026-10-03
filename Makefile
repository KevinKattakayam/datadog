# ============================================================
#  Enterprise Observability Pipeline — Makefile
# ============================================================
.PHONY: dev down test lint load-test bench logs clean topics build help \
       infra fmt fire integration-test sdk-test \
       helm-template helm-lint helm-sync-rules helm-check-rules \
       helm-install helm-uninstall rules-test \
       terraform-plan terraform-apply \
       chaos chaos-kill9 chaos-clickhouse chaos-replay bench-throughput


# Colors
GREEN  := \033[0;32m
YELLOW := \033[0;33m
CYAN   := \033[0;36m
RESET  := \033[0m

# Go & Rust paths. Do not export GOROOT: the go binary knows its own root,
# and forcing one breaks every machine where Go is installed elsewhere.
export PATH := $(HOME)/go/bin:$(HOME)/.cargo/bin:$(PATH)

help: ## Show this help
	@echo "$(CYAN)Enterprise Observability Pipeline$(RESET)"
	@echo "$(YELLOW)─────────────────────────────────$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'

dev: ## Start full stack (docker compose up + build)
	@echo "$(CYAN)▸ Starting observability pipeline...$(RESET)"
	docker compose up -d --build
	@echo "$(GREEN)✓ Stack is running$(RESET)"
	@echo "  Grafana:      http://localhost:3000  (admin/admin)"
	@echo "  Prometheus:   http://localhost:9090"
	@echo "  Ingestor:     http://localhost:8080"
	@echo "  ClickHouse:   http://localhost:8123"

infra: ## Start only infrastructure (Kafka, CH, Prometheus, Grafana)
	@echo "$(CYAN)▸ Starting infrastructure...$(RESET)"
	docker compose up -d kafka kafka-init kafka-exporter clickhouse prometheus alertmanager grafana
	@echo "$(GREEN)✓ Infrastructure running$(RESET)"

down: ## Stop all services
	docker compose down

build: ## Build Go and Rust services
	@echo "$(CYAN)▸ Building Go ingestor...$(RESET)"
	cd ingestor && go build -o ../bin/ingestor ./cmd/server/
	@echo "$(CYAN)▸ Building Rust processor...$(RESET)"
	cd processor && cargo build --release
	@echo "$(GREEN)✓ Build complete$(RESET)"

test: ## Run all tests
	@echo "$(CYAN)▸ Running Go tests...$(RESET)"
	cd ingestor && go test -v -race -coverprofile=coverage.out ./...
	@echo "$(CYAN)▸ Running Rust tests...$(RESET)"
	cd processor && cargo test
	@echo "$(GREEN)✓ All tests passed$(RESET)"

lint: ## Run linters (golangci-lint + cargo clippy)
	@echo "$(CYAN)▸ Linting Go...$(RESET)"
	cd ingestor && go vet ./...
	@echo "$(CYAN)▸ Linting Rust...$(RESET)"
	cd processor && cargo clippy -- -D warnings
	@echo "$(GREEN)✓ Lint clean$(RESET)"

fmt: ## Format all code
	cd ingestor && go fmt ./...
	cd processor && cargo fmt

load-test: ## Run k6 load test (requires k6 installed)
	@echo "$(CYAN)▸ Running load test...$(RESET)"
	k6 run tests/load/k6_script.js

fire: ## Fire test metrics at the ingestor
	@echo "$(CYAN)▸ Firing 1000 metrics/sec for 60s...$(RESET)"
	go run tests/load/fire_metrics.go --rate=1000 --duration=60s

bench: ## Run Rust benchmarks
	cd processor && cargo bench

bench-throughput: ## Run sustained throughput test (5 min, 10k/sec)
	@echo "$(CYAN)▸ Running throughput benchmark...$(RESET)"
	go run tests/load/fire_metrics.go --rate=10000 --duration=300s
	@echo ""
	@echo "$(CYAN)▸ End-to-end freshness:$(RESET)"
	@curl -s "http://localhost:8123/?query=SELECT+now()-max(ts)+AS+lag+FROM+observability.metrics"
	@echo ""

logs: ## Follow all container logs
	docker compose logs -f

logs-ingestor: ## Follow ingestor logs
	docker compose logs -f ingestor

logs-processor: ## Follow processor logs
	docker compose logs -f processor

topics: ## Create Kafka topics manually
	bash infra/kafka/topics.sh

status: ## Show service status
	@echo "$(CYAN)▸ Service Status$(RESET)"
	@docker compose ps
	@echo ""
	@echo "$(CYAN)▸ Kafka Topics$(RESET)"
	@docker exec obs-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || true
	@echo ""
	@echo "$(CYAN)▸ Kafka Offsets (metrics.raw)$(RESET)"
	@docker exec obs-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic metrics.raw 2>/dev/null || true
	@echo ""
	@echo "$(CYAN)▸ Consumer Lag$(RESET)"
	@docker exec obs-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group processor-group-local 2>/dev/null || true

integration-test: ## Run end-to-end integration tests
	@echo "$(CYAN)▸ Running integration tests...$(RESET)"
	bash tests/integration/test_pipeline.sh
	@echo "$(GREEN)✓ Integration tests complete$(RESET)"

sdk-test: ## Run SDK unit tests
	@echo "$(CYAN)▸ Running SDK tests...$(RESET)"
	cd sdk/go && go test -v ./...
	@echo "$(GREEN)✓ SDK tests passed$(RESET)"

# ── Chaos Tests ───────────────────────────────────────────────

chaos: chaos-kill9 chaos-clickhouse ## Run all chaos tests

chaos-kill9: ## Chaos: kill -9 processor, assert zero data loss
	@echo "$(CYAN)▸ Running kill -9 chaos test...$(RESET)"
	bash bench/chaos/kill9_no_loss.sh

chaos-clickhouse: ## Chaos: pause ClickHouse, assert recovery and zero loss
	@echo "$(CYAN)▸ Running ClickHouse outage chaos test...$(RESET)"
	bash bench/chaos/clickhouse_outage.sh

chaos-replay: ## Chaos: crash after ClickHouse write and verify FINAL deduplication
	bash bench/chaos/replay_final_no_loss.sh

# ── Helm ──────────────────────────────────────────────────────

CHART := helm/observability-pipeline
RULE_SOURCES := infra/prometheus/alerts/pipeline.yml infra/prometheus/alerts/slo.yml infra/prometheus/rules/recording.yml

helm-sync-rules: ## Copy Prometheus rules into the chart (PrometheusRule source)
	@mkdir -p $(CHART)/files/rules
	cp $(RULE_SOURCES) $(CHART)/files/rules/

helm-check-rules: ## Fail if chart rule copies drifted from infra/prometheus
	@for f in $(RULE_SOURCES); do diff -q $$f $(CHART)/files/rules/$$(basename $$f) || { echo "run: make helm-sync-rules"; exit 1; }; done

helm-template: ## Render the chart (no subcharts; nothing to download)
	helm template obs-pipeline $(CHART) --namespace observability

helm-lint: helm-check-rules ## Lint the chart, including the monitoring CRD resources
	helm lint $(CHART)
	helm lint $(CHART) --set serviceMonitor.enabled=true --set prometheusRule.enabled=true --set processor.kedaScaling.enabled=true

helm-install: ## Deploy to Kubernetes (create the required Secrets first; see README)
	helm upgrade --install obs-pipeline $(CHART) --namespace observability --create-namespace

helm-uninstall: ## Uninstall Helm release
	helm uninstall obs-pipeline --namespace observability

# ── Prometheus rules ──────────────────────────────────────────

rules-test: ## Validate and unit-test Prometheus rules (requires promtool)
	promtool check rules $(RULE_SOURCES)
	promtool test rules tests/prometheus/rules_test.yml

terraform-plan: ## Run Terraform plan for AWS EKS
	@echo "$(CYAN)▸ Planning infrastructure...$(RESET)"
	cd terraform && terraform init && terraform plan
	@echo "$(GREEN)✓ Plan complete$(RESET)"

terraform-apply: ## Apply Terraform (provision AWS EKS)
	@echo "$(CYAN)▸ Provisioning infrastructure...$(RESET)"
	cd terraform && terraform apply
	@echo "$(GREEN)✓ Infrastructure provisioned$(RESET)"

clean: ## Stop containers and remove all volumes
	docker compose down -v --remove-orphans
	rm -rf bin/
	cd processor && cargo clean 2>/dev/null || true
	@echo "$(GREEN)✓ Cleaned$(RESET)"
