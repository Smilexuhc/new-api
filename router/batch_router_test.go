package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/controller"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/pkg/jsplugin"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchRoutesDoNotShadowPluginRoutes(t *testing.T) {
	setupRelayRouterTestDB(t)
	engine := gin.New()
	SetRelayRouter(engine)
	builder := newPluginGenerationBuilder(engine.Routes(), nil, productionPluginRouteHandlers)
	for _, route := range []jsplugin.Route{
		{Method: http.MethodPost, Path: "/v1/batches"},
		{Method: http.MethodGet, Path: "/v1/batches"},
		{Method: http.MethodGet, Path: "/v1/batches/:id"},
		{Method: http.MethodPost, Path: "/v1/batches/:id/cancel"},
		{Method: http.MethodDelete, Path: "/v1/batches/:id"},
	} {
		assert.False(t, builder.routeAnsweredByHost(route), "%s %s", route.Method, route.Path)
	}
}

func TestDisabledBatchRoutesAnswerLikeUnknownRoutes(t *testing.T) {
	setupRelayRouterTestDB(t)
	require.NoError(t, i18n.Init())
	setting := operation_setting.GetBatchSetting()
	enabled := setting.Enabled
	setting.Enabled = false
	t.Cleanup(func() { setting.Enabled = enabled })

	engine := gin.New()
	SetRelayRouter(engine)
	engine.NoRoute(controller.RelayNotFound)
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/v1/batches"},
		{http.MethodGet, "/v1/batches/"},
		{http.MethodPost, "/v1/batches/"},
		{http.MethodGet, "/v1/batches/b1/"},
		{http.MethodPost, "/v1/batches/b1/cancel/"},
	} {
		recorder := httptest.NewRecorder()
		engine.ServeHTTP(recorder, httptest.NewRequest(tc.method, tc.path, nil))
		assert.Equal(t, http.StatusNotFound, recorder.Code, "%s %s", tc.method, tc.path)
		assert.Empty(t, recorder.Header().Get("Location"), "%s %s", tc.method, tc.path)
	}
}
