package main

import (
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// httpConnectionMetrics tracks connections owned by the HTTP server.
// Closed and hijacked connection objects are removed. Event metric series are
// retained per client IP for the process lifetime, even when no connections remain.
type httpConnectionMetrics struct {
	mu          sync.Mutex
	states      map[net.Conn]connectionState
	clients     map[string]int
	connections *prometheus.GaugeVec
	opened      *prometheus.CounterVec
	closed      *prometheus.CounterVec
	lastOpened  *prometheus.GaugeVec
	lastClosed  *prometheus.GaugeVec
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
		opened: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_server_connections_opened_total",
			Help: "Total business TCP connections accepted per client IP since process start; keep-alive reuse is excluded. Retained after connections close.",
		}, []string{"client_ip"}),
		closed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_server_connections_closed_total",
			Help: "Total tracked business TCP connections closed per client IP since process start; hijacked connections are excluded. Retained after connections close.",
		}, []string{"client_ip"}),
		lastOpened: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "http_server_connection_last_opened_timestamp_seconds",
			Help: "Unix time in seconds of the most recent accepted business TCP connection per client IP, or 0 if no event has occurred. Retained after connections close.",
		}, []string{"client_ip"}),
		lastClosed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "http_server_connection_last_closed_timestamp_seconds",
			Help: "Unix time in seconds of the most recent tracked business TCP connection close per client IP, or 0 if no event has occurred. Hijacked connections are excluded. Retained after connections close.",
		}, []string{"client_ip"}),
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
			if state == http.StateNew {
				m.opened.WithLabelValues(ip).Inc()
				m.lastOpened.WithLabelValues(ip).Set(float64(time.Now().UnixNano()) / 1e9)
				// Publish a zero baseline for close metrics before the first close.
				// These series deliberately outlive the current-connection Gauge.
				m.closed.WithLabelValues(ip).Add(0)
				m.lastClosed.WithLabelValues(ip)
			}
		}
		m.states[conn] = connectionState{state: state, ip: ip}
		m.connections.WithLabelValues(state.String(), ip).Inc()
	default:
		if exists {
			if state == http.StateClosed {
				m.closed.WithLabelValues(ip).Inc()
				m.lastClosed.WithLabelValues(ip).Set(float64(time.Now().UnixNano()) / 1e9)
			}
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
