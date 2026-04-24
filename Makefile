# ============================================================
#  Enterprise Observability Pipeline — Makefile
# ============================================================
.PHONY: dev down test lint load-test bench logs clean topics build help \
       infra fmt fire integration-test sdk-test \
       helm-template helm-install helm-uninstall \
       schema-register connect-deploy grpc-gen \
       terraform-plan terraform-apply


# Colors
GREEN  := \033[0;32m
YELLOW := \033[0;33m
CYAN   := \033[0;36m
RESET  := \033[0m

# Go & Rust paths
export PATH := $(HOME)/.local/go/bin:$(HOME)/go/bin:$(HOME)/.cargo/bin:$(PATH)
export GOROOT := $(HOME)/.local/go

help: ## Show this help
	@echo "$(CYAN)Enterprise Observability Pipeline$(RESET)"
	@echo "$(YELLOW)─────────────────────────────────$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-15s$(RESET) %s\n", $$1, $$2}'

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

integration-test: ## Run end-to-end integration tests
	@echo "$(CYAN)▸ Running integration tests...$(RESET)"
	bash tests/integration/test_pipeline.sh
	@echo "$(GREEN)✓ Integration tests complete$(RESET)"

sdk-test: ## Run SDK unit tests
	@echo "$(CYAN)▸ Running SDK tests...$(RESET)"
	cd sdk/go && go test -v ./...
	@echo "$(GREEN)✓ SDK tests passed$(RESET)"

helm-template: ## Render Helm chart templates (dry-run)
	@echo "$(CYAN)▸ Rendering Helm templates...$(RESET)"
	helm template obs-pipeline helm/observability-pipeline/ --namespace observability
	@echo "$(GREEN)✓ Templates rendered$(RESET)"

helm-install: ## Deploy pipeline to Kubernetes via Helm
	@echo "$(CYAN)▸ Installing Helm chart...$(RESET)"
	helm upgrade --install obs-pipeline helm/observability-pipeline/ \
		--namespace observability --create-namespace
	@echo "$(GREEN)✓ Helm chart deployed$(RESET)"

helm-uninstall: ## Uninstall Helm release
	helm uninstall obs-pipeline --namespace observability

schema-register: ## Register Avro schemas with Confluent Schema Registry
	@echo "$(CYAN)▸ Registering Avro schemas...$(RESET)"
	bash infra/schema-registry/register-schemas.sh
	@echo "$(GREEN)✓ Schemas registered$(RESET)"

connect-deploy: ## Deploy Kafka Connect ClickHouse sink connector
	@echo "$(CYAN)▸ Deploying ClickHouse sink connector...$(RESET)"
	bash infra/kafka-connect/deploy-connector.sh
	@echo "$(GREEN)✓ Connector deployed$(RESET)"

grpc-gen: ## Generate Go code from protobuf definitions (requires protoc)
	@echo "$(CYAN)▸ Generating gRPC code from proto...$(RESET)"
	protoc --go_out=. --go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		ingestor/proto/v1/metric.proto
	@echo "$(GREEN)✓ Proto generated$(RESET)"

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


