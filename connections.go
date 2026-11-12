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
	states      map[net.Conn]connectionState
	clients     map[string]int
	connections *prometheus.GaugeVec
}

type connectionState struct {
	state http.ConnState
	ip    string
}

func newHTTPConnectionMetrics() *httpConnectionMetrics {
	m := &httpConnectionMetrics{
		states:  make(map[net.Conn]connectionState),
		clients: make(map[string]int),
		connections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "http_server_connections",
			Help: "Current connections owned by the business HTTP server by state (new, active, idle) and TCP peer IP, excluding the separate metrics and health server.",
		}, []string{"state", "client_ip"}),
	}
	return m
}

func (m *httpConnectionMetrics) connState(conn net.Conn, state http.ConnState) {
	m.mu.Lock()
	defer m.mu.Unlock()

	previous, exists := m.states[conn]
	ip := previous.ip
	if exists {
		m.connections.WithLabelValues(previous.state.String(), ip).Dec()
	}
	switch state {
	case http.StateNew, http.StateActive, http.StateIdle:
		if !exists {
			ip = conn.RemoteAddr().String()
			if host, _, err := net.SplitHostPort(ip); err == nil {
				ip = host
			}
			if m.clients[ip] == 0 {
				for _, initial := range []http.ConnState{http.StateNew, http.StateActive, http.StateIdle} {
					m.connections.WithLabelValues(initial.String(), ip).Set(0)
				}
			}
			m.clients[ip]++
		}
		m.states[conn] = connectionState{state: state, ip: ip}
		m.connections.WithLabelValues(state.String(), ip).Inc()
	default:
		if exists {
			delete(m.states, conn)
			m.clients[ip]--
			if m.clients[ip] == 0 {
				delete(m.clients, ip)
				for _, previousState := range []http.ConnState{http.StateNew, http.StateActive, http.StateIdle} {
					m.connections.DeleteLabelValues(previousState.String(), ip)
				}
			}
		}
	}
}
