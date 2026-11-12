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
		for _, metric := range families[0].Metric {
			state := metric.Label[0].GetValue()
			if got := metric.Gauge.GetValue(); got != want[state] {
				t.Fatalf("state %s: got %v, want %v", state, got, want[state])
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
