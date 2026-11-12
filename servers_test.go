package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestSeparateMonitoringServer(t *testing.T) {
	for _, tc := range []struct{ port, metricsPort, businessAddr, monitoringAddr string }{
		{"", "", ":8080", ":9090"},
		{"8081", "9091", ":8081", ":9091"},
	} {
		t.Run(tc.businessAddr, func(t *testing.T) {
			t.Setenv("PORT", tc.port)
			t.Setenv("METRICS_PORT", tc.metricsPort)
			business := gin.New()
			business.POST("/receive", func(c *gin.Context) { c.Status(http.StatusNoContent) })
			connections := newHTTPConnectionMetrics()
			server, monitoring := newHTTPServers(business.Handler(), connections)
			if server.Addr != tc.businessAddr || monitoring.Addr != tc.monitoringAddr {
				t.Fatalf("unexpected addresses: %s, %s", server.Addr, monitoring.Addr)
			}
			if server.ConnState == nil || monitoring.ConnState != nil {
				t.Fatal("connection tracking must only be enabled for business listener")
			}
			for _, route := range []struct {
				handler      http.Handler
				method, path string
				status       int
			}{
				{server.Handler, "POST", "/receive", 204},
				{server.Handler, "GET", "/metrics", 404},
				{server.Handler, "GET", "/healthz", 404},
				{monitoring.Handler, "POST", "/receive", 404},
				{monitoring.Handler, "GET", "/healthz", 200},
				{monitoring.Handler, "GET", "/metrics", 200},
			} {
				r := httptest.NewRecorder()
				route.handler.ServeHTTP(r, httptest.NewRequest(route.method, route.path, nil))
				if r.Code != route.status {
					t.Fatalf("%s %s: got %d, want %d", route.method, route.path, r.Code, route.status)
				}
				if route.path == "/metrics" && route.status == 200 {
					body, _ := io.ReadAll(r.Result().Body)
					if !strings.Contains(string(body), "go_goroutines") {
						t.Fatal("monitoring endpoint is missing registered metrics")
					}
				}
			}
		})
	}
}
