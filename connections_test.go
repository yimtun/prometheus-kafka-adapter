package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestHTTPConnectionStates(t *testing.T) {
	m := newHTTPConnectionMetrics()
	registry := prometheus.NewRegistry()
	registry.MustRegister(m.connections)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	check := func(newCount, active, idle float64) {
		t.Helper()
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]float64{"new": newCount, "active": active, "idle": idle}
		got := map[string]float64{}
		for _, family := range families {
			for _, metric := range family.Metric {
				var state string
				for _, label := range metric.Label {
					if label.GetName() == "state" {
						state = label.GetValue()
					}
				}
				got[state] += metric.Gauge.GetValue()
			}
		}
		for state, count := range want {
			if got[state] != count {
				t.Fatalf("state %s: got %v, want %v", state, got[state], count)
			}
		}
	}
	check(0, 0, 0)
	m.connState(a, http.StateNew)
	check(1, 0, 0)
	m.connState(a, http.StateActive)
	m.connState(b, http.StateNew)
	check(1, 1, 0)
	m.connState(a, http.StateIdle)
	check(1, 0, 1)
	m.connState(a, http.StateActive) // Keep-alive reuse must not add a connection.
	check(1, 1, 0)
	m.connState(a, http.StateClosed)
	m.connState(b, http.StateClosed) // A connection can close before its first request.
	check(0, 0, 0)
	m.connState(a, http.StateNew)
	m.connState(a, http.StateActive)
	m.connState(a, http.StateHijacked)
	check(0, 0, 0)
	if len(m.states) != 0 {
		t.Fatal("terminal connections retained")
	}
}

// Exercise the hook with real TCP requests and keep-alive reuse.
func TestHTTPConnectionLifecycle(t *testing.T) {
	m := newHTTPConnectionMetrics()
	states := make(chan http.ConnState, 16)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.Config.ConnState = func(c net.Conn, state http.ConnState) {
		m.connState(c, state)
		states <- state
	}
	server.Start()
	defer server.Close()
	conn, err := net.DialTimeout("tcp", server.Listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	wait := func(want http.ConnState) {
		t.Helper()
		select {
		case got := <-states:
			if got != want {
				t.Fatalf("got state %v, want %v", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %v", want)
		}
	}
	wait(http.StateNew)
	reader := bufio.NewReader(conn)
	for i := 0; i < 2; i++ {
		if _, err := fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		wait(http.StateActive)
		wait(http.StateIdle)
		m.mu.Lock()
		count := len(m.states)
		m.mu.Unlock()
		if count != 1 {
			t.Fatalf("keep-alive connection count = %d, want 1", count)
		}
	}
	conn.Close()
	wait(http.StateClosed)
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.states) != 0 {
		t.Fatal("closed connection retained")
	}
}

type clientConn struct {
	net.Conn
	addr net.Addr
}

func (c *clientConn) RemoteAddr() net.Addr { return c.addr }

func TestHTTPConnectionClientIPs(t *testing.T) {
	m := newHTTPConnectionMetrics()
	registry := prometheus.NewRegistry()
	registry.MustRegister(m.connections)
	a := &clientConn{addr: &net.TCPAddr{IP: net.ParseIP("10.27.19.55"), Port: 1234}}
	b := &clientConn{addr: &net.TCPAddr{IP: net.ParseIP("10.27.19.55"), Port: 5678}}
	c := &clientConn{addr: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 9999}}
	m.connState(a, http.StateNew)
	m.connState(a, http.StateIdle)
	m.connState(b, http.StateNew)
	m.connState(b, http.StateActive)
	m.connState(c, http.StateNew)
	m.connState(a, http.StateClosed)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, metric := range families[0].Metric {
		labels := map[string]string{}
		for _, label := range metric.Label {
			labels[label.GetName()] = label.GetValue()
		}
		values[labels["client_ip"]+"/"+labels["state"]] = metric.Gauge.GetValue()
	}
	if len(values) != 6 || values["10.27.19.55/active"] != 1 || values["2001:db8::1/new"] != 1 {
		t.Fatalf("unexpected per-IP counts: %v", values)
	}
	m.connState(b, http.StateClosed)
	m.connState(c, http.StateHijacked)
	families, err = registry.Gather()
	if err != nil || len(families) != 0 || len(m.clients) != 0 || len(m.states) != 0 {
		t.Fatalf("client series were not cleaned up: %v, %v", families, err)
	}
}
