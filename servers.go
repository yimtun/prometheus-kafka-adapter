package main

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Only the business listener contributes to the connection metrics.
func newHTTPServers(business http.Handler, connections *httpConnectionMetrics) (*http.Server, *http.Server) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9090"
	}
	monitoring := gin.New()
	monitoring.Use(gin.Recovery())
	monitoring.GET("/metrics", gin.WrapH(promhttp.Handler()))
	monitoring.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP"})
	})
	return &http.Server{
			Addr:      ":" + port,
			Handler:   business,
			ConnState: connections.connState,
		}, &http.Server{
			Addr:    ":" + metricsPort,
			Handler: monitoring.Handler(),
		}
}
