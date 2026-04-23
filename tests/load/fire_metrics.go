// Load test script — Fires fake metrics at the ingestor.
// Usage: go run fire_metrics.go --rate=1000 --duration=60s

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Metric struct {
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Unit      string            `json:"unit"`
	Tags      map[string]string `json:"tags"`
	Timestamp int64             `json:"timestamp"`
	Host      string            `json:"host"`
}

var (
	rate     = flag.Int("rate", 1000, "metrics per second")
	duration = flag.Duration("duration", 60*time.Second, "test duration")
	url      = flag.String("url", "http://localhost:8080/ingest/batch", "ingestor URL")
	batch    = flag.Int("batch", 100, "batch size")
)

var services = []string{"checkout", "payments", "inventory", "search", "auth", "gateway"}
var endpoints = []string{"/api/v1/order", "/api/v1/pay", "/api/v1/search", "/health", "/api/v1/cart"}
var hosts = []string{"prod-api-01", "prod-api-02", "prod-api-03", "prod-api-04", "prod-api-05"}
var metricNames = []string{
	"api.request.duration_ms",
	"api.request.count",
	"api.error.count",
	"db.query.duration_ms",
	"cache.hit.ratio",
	"cpu.usage.percent",
	"memory.usage.bytes",
	"disk.io.read_bytes",
	"network.rx.bytes",
	"queue.depth.count",
}

func main() {
	flag.Parse()

	fmt.Printf("🚀 Load test: %d metrics/sec for %s → %s\n", *rate, *duration, *url)

	var sent atomic.Int64
	var errors atomic.Int64
	client := &http.Client{Timeout: 10 * time.Second}

	ticker := time.NewTicker(time.Second / time.Duration(*rate / *batch))
	defer ticker.Stop()

	deadline := time.After(*duration)
	startTime := time.Now()
	var wg sync.WaitGroup

	for {
		select {
		case <-deadline:
			wg.Wait()
			elapsed := time.Since(startTime)
			totalSent := sent.Load()
			totalErrors := errors.Load()
			fmt.Printf("\n📊 Results:\n")
			fmt.Printf("   Duration:    %s\n", elapsed.Round(time.Millisecond))
			fmt.Printf("   Sent:        %d metrics\n", totalSent)
			fmt.Printf("   Errors:      %d\n", totalErrors)
			fmt.Printf("   Throughput:  %.0f metrics/sec\n", float64(totalSent)/elapsed.Seconds())
			os.Exit(0)
		case <-ticker.C:
			wg.Add(1)
			go func() {
				defer wg.Done()
				metrics := generateBatch(*batch)
				if err := sendBatch(client, metrics); err != nil {
					errors.Add(1)
				} else {
					sent.Add(int64(len(metrics)))
				}
			}()
		}
	}
}

func generateBatch(size int) []Metric {
	metrics := make([]Metric, size)
	now := time.Now().Unix()

	for i := 0; i < size; i++ {
		name := metricNames[rand.Intn(len(metricNames))]
		host := hosts[rand.Intn(len(hosts))]
		svc := services[rand.Intn(len(services))]
		ep := endpoints[rand.Intn(len(endpoints))]

		var value float64
		var unit string
		switch name {
		case "api.request.duration_ms":
			value = 50 + rand.Float64()*200
			unit = "ms"
			// Inject occasional anomaly (5% chance)
			if rand.Float64() < 0.05 {
				value = 2000 + rand.Float64()*5000
			}
		case "cpu.usage.percent":
			value = 20 + rand.Float64()*60
			unit = "percent"
		case "memory.usage.bytes":
			value = 1e9 + rand.Float64()*3e9
			unit = "bytes"
		case "cache.hit.ratio":
			value = 0.8 + rand.Float64()*0.2
			unit = "percent"
		default:
			value = math.Abs(rand.NormFloat64()*100 + 500)
			unit = "count"
		}

		metrics[i] = Metric{
			Name:      name,
			Value:     value,
			Unit:      unit,
			Tags:      map[string]string{"service": svc, "endpoint": ep, "region": "us-east-1"},
			Timestamp: now,
			Host:      host,
		}
	}
	return metrics
}

func sendBatch(client *http.Client, metrics []Metric) error {
	body := map[string]interface{}{"metrics": metrics}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := client.Post(*url, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}
