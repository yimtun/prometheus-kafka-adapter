package main

import (
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func eventRegistry(m *httpConnectionMetrics) *prometheus.Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(m.connections, m.opened, m.closed, m.lastOpened, m.lastClosed)
	return r
}

func eventValues(t *testing.T, r *prometheus.Registry, ip string) map[string]float64 {
	t.Helper()
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[string]float64)
	for _, family := range families {
		if family.GetName() == "http_server_connections" {
			continue
		}
		for _, metric := range family.Metric {
			if len(metric.Label) != 1 || metric.Label[0].GetName() != "client_ip" {
				t.Fatalf("unexpected event labels: %v", metric.Label)
			}
			if metric.Label[0].GetValue() != ip {
				continue
			}
			if metric.Counter != nil {
				values[family.GetName()] = metric.Counter.GetValue()
			} else {
				values[family.GetName()] = metric.Gauge.GetValue()
			}
		}
	}
	return values
}

func checkEvents(t *testing.T, r *prometheus.Registry, ip string, opened, closed float64) map[string]float64 {
	t.Helper()
	values := eventValues(t, r, ip)
	if len(values) != 4 || values["http_server_connections_opened_total"] != opened || values["http_server_connections_closed_total"] != closed {
		t.Fatalf("unexpected connection event values: %v", values)
	}
	return values
}

func TestConnectionEventsRetained(t *testing.T) {
	for _, ip := range []string{"10.27.19.55", "2001:db8::1"} {
		t.Run(ip, func(t *testing.T) {
			m := newHTTPConnectionMetrics()
			r := eventRegistry(m)
			a := &clientConn{addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 1234}}
			b := &clientConn{addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 5678}}
			before := float64(time.Now().UnixNano()) / 1e9
			m.connState(a, http.StateNew)
			initial := checkEvents(t, r, ip, 1, 0)
			openedAt := initial["http_server_connection_last_opened_timestamp_seconds"]
			if openedAt < before || openedAt > float64(time.Now().UnixNano())/1e9 || initial["http_server_connection_last_closed_timestamp_seconds"] != 0 {
				t.Fatalf("unexpected initial timestamps: %v", initial)
			}
			m.connState(a, http.StateNew) // Duplicate callback is not a new accept.
			for i := 0; i < 3; i++ {
				m.connState(a, http.StateActive)
				m.connState(a, http.StateIdle)
			}
			if values := checkEvents(t, r, ip, 1, 0); values["http_server_connection_last_opened_timestamp_seconds"] != openedAt {
				t.Fatal("keep-alive reuse changed last opened time")
			}
			m.connState(b, http.StateNew)
			m.connState(b, http.StateClosed) // No request was sent.
			checkEvents(t, r, ip, 2, 1)
			m.connState(a, http.StateClosed)
			closedValues := checkEvents(t, r, ip, 2, 2)
			closedAt := closedValues["http_server_connection_last_closed_timestamp_seconds"]
			if closedAt < openedAt || closedAt > float64(time.Now().UnixNano())/1e9 {
				t.Fatalf("unexpected last closed time: %v", closedAt)
			}
			m.connState(a, http.StateClosed)
			m.connState(b, http.StateClosed)
			if values := checkEvents(t, r, ip, 2, 2); values["http_server_connection_last_closed_timestamp_seconds"] != closedAt {
				t.Fatal("duplicate terminal callback changed last closed time")
			}
			if len(m.states) != 0 || len(m.clients) != 0 {
				t.Fatal("closed connection objects retained")
			}
			families, err := r.Gather()
			if err != nil || len(families) != 4 {
				t.Fatalf("expected only four persistent event families: %v, %v", families, err)
			}
			// Reconnecting after the last connection closes must not reset history.
			m.connState(a, http.StateNew)
			checkEvents(t, r, ip, 3, 2)
			m.connState(a, http.StateHijacked)
			m.connState(a, http.StateHijacked)
			m.connState(a, http.StateClosed) // Hijacked is not a tracked TCP close.
			if values := checkEvents(t, r, ip, 3, 2); values["http_server_connection_last_closed_timestamp_seconds"] != closedAt {
				t.Fatal("hijack changed last closed time")
			}
			if len(m.states) != 0 || len(m.clients) != 0 {
				t.Fatal("hijacked connection objects retained")
			}
			if values := eventValues(t, eventRegistry(newHTTPConnectionMetrics()), ip); len(values) != 0 {
				t.Fatal("new process metrics inherited history")
			}
		})
	}
}

func TestConnectionEventsConcurrent(t *testing.T) {
	m := newHTTPConnectionMetrics()
	r := eventRegistry(m)
	const clients = 64
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(port int) {
			defer wg.Done()
			conn := &clientConn{addr: &net.TCPAddr{IP: net.ParseIP("10.27.19.55"), Port: port}}
			m.connState(conn, http.StateNew)
			m.connState(conn, http.StateActive)
			m.connState(conn, http.StateIdle)
			m.connState(conn, http.StateClosed)
			m.connState(conn, http.StateClosed)
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < clients; i++ {
			if _, err := r.Gather(); err != nil {
				t.Error(err)
			}
		}
	}()
	wg.Wait()
	checkEvents(t, r, "10.27.19.55", clients, clients)
	if len(m.states) != 0 || len(m.clients) != 0 {
		t.Fatal("connection objects retained after concurrent closes")
	}
}
