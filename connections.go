package main

import (
	"net"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// httpConnectionMetrics tracks connections owned by the HTTP server.
// Closed and hijacked connections are removed, so memory scales with open connections.
type httpConnectionMetrics struct {
	mu          sync.Mutex
	states      map[net.Conn]http.ConnState
	connections *prometheus.GaugeVec
}

func newHTTPConnectionMetrics() *httpConnectionMetrics {
	m := &httpConnectionMetrics{
		states: make(map[net.Conn]http.ConnState),
		connections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "http_server_connections",
			Help: "Current connections owned by the HTTP server by state (new, active, idle), including metrics and health checks.",
		}, []string{"state"}),
	}
	for _, state := range []http.ConnState{http.StateNew, http.StateActive, http.StateIdle} {
		m.connections.WithLabelValues(state.String()).Set(0)
	}
	return m
}

func (m *httpConnectionMetrics) connState(conn net.Conn, state http.ConnState) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if previous, ok := m.states[conn]; ok {
		m.connections.WithLabelValues(previous.String()).Dec()
		delete(m.states, conn)
	}
	switch state {
	case http.StateNew, http.StateActive, http.StateIdle:
		m.states[conn] = state
		m.connections.WithLabelValues(state.String()).Inc()
	}
}
