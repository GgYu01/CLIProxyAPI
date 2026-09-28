package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/api/middleware"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func requestAdmissionRuntimeConfig(cfg config.RequestAdmissionConfig) middleware.RequestAdmissionConfig {
	modelRPM := make(map[string]int, len(cfg.ModelRPM))
	for model, limit := range cfg.ModelRPM {
		modelRPM[model] = limit
	}
	return middleware.RequestAdmissionConfig{
		Enabled:                 cfg.Enabled,
		MaxInFlight:             cfg.MaxInFlight,
		MaxWaiting:              cfg.MaxWaiting,
		WaitTimeout:             time.Duration(cfg.WaitSeconds) * time.Second,
		ImageMaxInFlight:        cfg.ImageMaxInFlight,
		ImageMaxWaiting:         cfg.ImageMaxWaiting,
		ImageWaitTimeout:        time.Duration(cfg.ImageWaitSeconds) * time.Second,
		MaxBodyBytes:            cfg.MaxBodyBytes,
		BodyBudgetBytes:         cfg.BodyBudgetBytes,
		BodyBudgetWaitTimeout:   time.Duration(cfg.BodyBudgetWaitSeconds) * time.Second,
		LargeBodyThresholdBytes: cfg.LargeBodyThresholdBytes,
		LargeBodyMaxInFlight:    cfg.LargeBodyMaxInFlight,
		LargeBodyWaitTimeout:    time.Duration(cfg.LargeBodyWaitSeconds) * time.Second,
		ModelRPM:                modelRPM,
		ModelRPMWindow:          time.Duration(cfg.ModelRPMWindowSeconds) * time.Second,
		ModelRPMMaxWaiting:      cfg.ModelRPMMaxWaiting,
		FirstByteTimeout:        time.Duration(cfg.FirstByteTimeoutSeconds) * time.Second,
		FirstByteRetryAfter:     time.Duration(cfg.FirstByteRetryAfterSeconds) * time.Second,
	}
}

func (s *Server) registerRequestAdmissionCompatibilityRoutes() {
	s.engine.GET("/__queue/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "native": true})
	})
	s.engine.GET("/__queue/stats", func(c *gin.Context) {
		if s.requestAdmission == nil {
			c.JSON(http.StatusOK, middleware.AdmissionStats{})
			return
		}
		c.JSON(http.StatusOK, s.requestAdmission.Stats())
	})
}
