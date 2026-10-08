package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry          *prometheus.Registry
	requestsTotal     *prometheus.CounterVec
	inFlightRequests  *prometheus.GaugeVec
	serverUtilization *prometheus.GaugeVec
	rollingErrorRate  *prometheus.GaugeVec
}

func New() *Metrics {
	registry := prometheus.NewRegistry()

	return &Metrics{
		registry: registry,
		requestsTotal: promauto.With(registry).NewCounterVec(
			prometheus.CounterOpts{
				Name: "balancer_requests_total",
				Help: "Total number of requests routed by the balancer",
			},
			[]string{"backend", "backend_group"},
		),
		inFlightRequests: promauto.With(registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "balancer_server_inflight_requests",
				Help: "Number of in-flight requests to each backend server",
			},
			[]string{"backend", "backend_group"},
		),
		serverUtilization: promauto.With(registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "balancer_server_utilization",
				Help: "Utilization of each backend server",
			},
			[]string{"backend", "backend_group"},
		),
		rollingErrorRate: promauto.With(registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "balancer_server_rolling_error_rate",
				Help: "Rolling error rate (0-1) of each backend server over 1m window",
			},
			[]string{"backend", "backend_group"},
		),
	}
}

var Default = New()

func (m *Metrics) RecordRequestTotal(backend, backendGroup string) {
	m.requestsTotal.WithLabelValues(backend, backendGroup).Inc()
}

func (m *Metrics) RecordInFlight(backend, backendGroup string, n float64) {
	m.inFlightRequests.WithLabelValues(backend, backendGroup).Set(n)
}

func (m *Metrics) RecordUtilization(backend, backendGroup string, utilization float64) {
	m.serverUtilization.WithLabelValues(backend, backendGroup).Set(utilization)
}

func (m *Metrics) RecordRollingErrorRate(backend, backendGroup string, rate float64) {
	m.rollingErrorRate.WithLabelValues(backend, backendGroup).Set(rate)
}

func (m *Metrics) RecordServerRemoved(backend, backendGroup string) {
	m.inFlightRequests.DeleteLabelValues(backend, backendGroup)
	m.serverUtilization.DeleteLabelValues(backend, backendGroup)
	m.rollingErrorRate.DeleteLabelValues(backend, backendGroup)
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(
		m.registry,
		promhttp.HandlerOpts{},
	)
}
