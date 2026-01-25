package durex

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusMetrics implements MetricsCollector using Prometheus metrics.
type PrometheusMetrics struct {
	commandsStarted   *prometheus.CounterVec
	commandsCompleted *prometheus.CounterVec
	commandsFailed    *prometheus.CounterVec
	commandsRetried   *prometheus.CounterVec
	commandDuration   *prometheus.HistogramVec
	queueSize         prometheus.Gauge
}

// PrometheusOption configures PrometheusMetrics.
type PrometheusOption func(*prometheusConfig)

type prometheusConfig struct {
	namespace string
	subsystem string
	buckets   []float64
}

// WithPrometheusNamespace sets the namespace for all metrics.
func WithPrometheusNamespace(namespace string) PrometheusOption {
	return func(c *prometheusConfig) {
		c.namespace = namespace
	}
}

// WithPrometheusSubsystem sets the subsystem for all metrics.
func WithPrometheusSubsystem(subsystem string) PrometheusOption {
	return func(c *prometheusConfig) {
		c.subsystem = subsystem
	}
}

// WithPrometheusBuckets sets custom histogram buckets for duration metrics.
func WithPrometheusBuckets(buckets []float64) PrometheusOption {
	return func(c *prometheusConfig) {
		c.buckets = buckets
	}
}

// NewPrometheusMetrics creates a new PrometheusMetrics collector.
// The metrics are automatically registered with the provided registry.
// If registry is nil, prometheus.DefaultRegisterer is used.
func NewPrometheusMetrics(
	registry prometheus.Registerer,
	opts ...PrometheusOption,
) *PrometheusMetrics {
	cfg := &prometheusConfig{
		namespace: "durex",
		subsystem: "",
		buckets:   prometheus.DefBuckets,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	if registry == nil {
		registry = prometheus.DefaultRegisterer
	}

	m := &PrometheusMetrics{
		commandsStarted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: cfg.namespace,
				Subsystem: cfg.subsystem,
				Name:      "commands_started_total",
				Help:      "Total number of commands started",
			},
			[]string{"command"},
		),
		commandsCompleted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: cfg.namespace,
				Subsystem: cfg.subsystem,
				Name:      "commands_completed_total",
				Help:      "Total number of commands completed successfully",
			},
			[]string{"command"},
		),
		commandsFailed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: cfg.namespace,
				Subsystem: cfg.subsystem,
				Name:      "commands_failed_total",
				Help:      "Total number of commands that failed",
			},
			[]string{"command"},
		),
		commandsRetried: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: cfg.namespace,
				Subsystem: cfg.subsystem,
				Name:      "commands_retried_total",
				Help:      "Total number of command retries",
			},
			[]string{"command"},
		),
		commandDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: cfg.namespace,
				Subsystem: cfg.subsystem,
				Name:      "command_duration_seconds",
				Help:      "Duration of command execution in seconds",
				Buckets:   cfg.buckets,
			},
			[]string{"command"},
		),
		queueSize: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: cfg.namespace,
				Subsystem: cfg.subsystem,
				Name:      "queue_size",
				Help:      "Current number of commands in the queue",
			},
		),
	}

	// Register all metrics
	registry.MustRegister(
		m.commandsStarted,
		m.commandsCompleted,
		m.commandsFailed,
		m.commandsRetried,
		m.commandDuration,
		m.queueSize,
	)

	return m
}

// CommandStarted implements MetricsCollector.
func (m *PrometheusMetrics) CommandStarted(name string) {
	m.commandsStarted.WithLabelValues(name).Inc()
}

// CommandCompleted implements MetricsCollector.
func (m *PrometheusMetrics) CommandCompleted(name string, duration time.Duration) {
	m.commandsCompleted.WithLabelValues(name).Inc()
	m.commandDuration.WithLabelValues(name).Observe(duration.Seconds())
}

// CommandFailed implements MetricsCollector.
func (m *PrometheusMetrics) CommandFailed(name string, _ error) {
	m.commandsFailed.WithLabelValues(name).Inc()
}

// CommandRetried implements MetricsCollector.
func (m *PrometheusMetrics) CommandRetried(name string, _ int) {
	m.commandsRetried.WithLabelValues(name).Inc()
}

// QueueSize implements MetricsCollector.
func (m *PrometheusMetrics) QueueSize(size int) {
	m.queueSize.Set(float64(size))
}

// Collectors returns all Prometheus collectors for manual registration.
// Use this if you need more control over registration.
func (m *PrometheusMetrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.commandsStarted,
		m.commandsCompleted,
		m.commandsFailed,
		m.commandsRetried,
		m.commandDuration,
		m.queueSize,
	}
}

// Describe implements prometheus.Collector.
func (m *PrometheusMetrics) Describe(ch chan<- *prometheus.Desc) {
	m.commandsStarted.Describe(ch)
	m.commandsCompleted.Describe(ch)
	m.commandsFailed.Describe(ch)
	m.commandsRetried.Describe(ch)
	m.commandDuration.Describe(ch)
	m.queueSize.Describe(ch)
}

// Collect implements prometheus.Collector.
func (m *PrometheusMetrics) Collect(ch chan<- prometheus.Metric) {
	m.commandsStarted.Collect(ch)
	m.commandsCompleted.Collect(ch)
	m.commandsFailed.Collect(ch)
	m.commandsRetried.Collect(ch)
	m.commandDuration.Collect(ch)
	m.queueSize.Collect(ch)
}
