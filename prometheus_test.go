package durex_test

import (
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/prometheus/client_golang/prometheus"
)

func TestPrometheusMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := durex.NewPrometheusMetrics(registry)

	// Test CommandStarted
	metrics.CommandStarted("test_command")
	metrics.CommandStarted("test_command")
	metrics.CommandStarted("other_command")

	// Verify metrics are registered
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	if len(metricFamilies) == 0 {
		t.Fatal("expected metrics to be registered")
	}

	// Find commands_started_total metric
	var startedTotal float64
	for _, mf := range metricFamilies {
		if mf.GetName() == "durex_commands_started_total" {
			for _, m := range mf.GetMetric() {
				startedTotal += m.GetCounter().GetValue()
			}
		}
	}

	if startedTotal != 3 {
		t.Errorf("expected 3 commands started, got %v", startedTotal)
	}

	// Test CommandCompleted
	metrics.CommandCompleted("test_command", 100*time.Millisecond)
	metrics.CommandCompleted("test_command", 200*time.Millisecond)

	metricFamilies, _ = registry.Gather()
	var completedTotal float64
	for _, mf := range metricFamilies {
		if mf.GetName() == "durex_commands_completed_total" {
			for _, m := range mf.GetMetric() {
				completedTotal += m.GetCounter().GetValue()
			}
		}
	}

	if completedTotal != 2 {
		t.Errorf("expected 2 commands completed, got %v", completedTotal)
	}

	// Test CommandFailed
	metrics.CommandFailed("test_command", nil)

	metricFamilies, _ = registry.Gather()
	var failedTotal float64
	for _, mf := range metricFamilies {
		if mf.GetName() == "durex_commands_failed_total" {
			for _, m := range mf.GetMetric() {
				failedTotal += m.GetCounter().GetValue()
			}
		}
	}

	if failedTotal != 1 {
		t.Errorf("expected 1 command failed, got %v", failedTotal)
	}

	// Test CommandRetried
	metrics.CommandRetried("test_command", 1)
	metrics.CommandRetried("test_command", 2)

	metricFamilies, _ = registry.Gather()
	var retriedTotal float64
	for _, mf := range metricFamilies {
		if mf.GetName() == "durex_commands_retried_total" {
			for _, m := range mf.GetMetric() {
				retriedTotal += m.GetCounter().GetValue()
			}
		}
	}

	if retriedTotal != 2 {
		t.Errorf("expected 2 retries, got %v", retriedTotal)
	}

	// Test QueueSize
	metrics.QueueSize(42)

	metricFamilies, _ = registry.Gather()
	var queueSize float64
	for _, mf := range metricFamilies {
		if mf.GetName() == "durex_queue_size" {
			for _, m := range mf.GetMetric() {
				queueSize = m.GetGauge().GetValue()
			}
		}
	}

	if queueSize != 42 {
		t.Errorf("expected queue size 42, got %v", queueSize)
	}
}

func TestPrometheusMetricsWithOptions(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := durex.NewPrometheusMetrics(
		registry,
		durex.WithPrometheusNamespace("myapp"),
		durex.WithPrometheusSubsystem("jobs"),
		durex.WithPrometheusBuckets([]float64{0.1, 0.5, 1.0, 5.0}),
	)

	metrics.CommandStarted("test")
	metrics.CommandCompleted("test", 500*time.Millisecond)

	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	// Check that custom namespace/subsystem is applied
	foundStarted := false
	foundDuration := false
	for _, mf := range metricFamilies {
		if mf.GetName() == "myapp_jobs_commands_started_total" {
			foundStarted = true
		}
		if mf.GetName() == "myapp_jobs_command_duration_seconds" {
			foundDuration = true
		}
	}

	if !foundStarted {
		t.Error("expected metric with custom namespace/subsystem")
	}
	if !foundDuration {
		t.Error("expected duration metric with custom namespace/subsystem")
	}
}

func TestPrometheusMetricsDefaultRegistry(t *testing.T) {
	// Create with nil registry (uses default)
	// Note: This may fail if other tests have already registered
	// the same metrics with the default registry
	defer func() {
		if r := recover(); r != nil {
			// Expected if metrics already registered
			t.Skip("skipping: metrics already registered with default registry")
		}
	}()

	metrics := durex.NewPrometheusMetrics(
		nil,
		durex.WithPrometheusNamespace("test_default"),
	)
	metrics.CommandStarted("test")
}

func TestPrometheusMetricsCollector(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := durex.NewPrometheusMetrics(registry)

	// Verify it can be used as a prometheus.Collector
	var _ prometheus.Collector = metrics

	// Test Describe
	descCh := make(chan *prometheus.Desc, 100)
	metrics.Describe(descCh)
	close(descCh)

	descCount := 0
	for range descCh {
		descCount++
	}
	if descCount == 0 {
		t.Error("expected Describe to send descriptors")
	}

	// Test Collect
	metricCh := make(chan prometheus.Metric, 100)
	metrics.CommandStarted("test")
	metrics.Collect(metricCh)
	close(metricCh)

	metricCount := 0
	for range metricCh {
		metricCount++
	}
	if metricCount == 0 {
		t.Error("expected Collect to send metrics")
	}
}

func TestPrometheusMetricsCollectors(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := durex.NewPrometheusMetrics(registry)

	collectors := metrics.Collectors()
	if len(collectors) != 6 {
		t.Errorf("expected 6 collectors, got %d", len(collectors))
	}
}
