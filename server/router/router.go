// Copyright 2018 Drone.IO Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package router

import (
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	swaggo_files "github.com/swaggo/files"
	swaggo_gin_swagger "github.com/swaggo/gin-swagger"

	"go.woodpecker-ci.org/woodpecker/v3/cmd/server/openapi"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/api"
	"go.woodpecker-ci.org/woodpecker/v3/server/api/metrics"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/header"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/token"
	"go.woodpecker-ci.org/woodpecker/v3/server/web"
)

// Load loads the router.
func Load(noRouteHandler http.HandlerFunc, middleware ...gin.HandlerFunc) http.Handler {
	e, _ := load(noRouteHandler, middleware...)
	return e
}

// load loads the router, and tells the routes open to bot tokens.
func load(noRouteHandler http.HandlerFunc, middleware ...gin.HandlerFunc) (*gin.Engine, *session.BotRoutes) {
	e := gin.New()
	e.UseRawPath = true
	// the client's address comes from X-Forwarded-For only behind these
	if err := e.SetTrustedProxies(server.Config.Server.TrustedProxies); err != nil {
		log.Error().Err(err).Msg("trusted proxies: not trusting any")
		_ = e.SetTrustedProxies(nil)
	}
	e.Use(gin.Recovery())

	e.Use(func(c *gin.Context) {
		log.Trace().Msgf("[%s] %s", c.Request.Method, c.Request.URL.Path)
		c.Next()
	})

	e.Use(header.NoCache)
	e.Use(header.Options)
	e.Use(header.Secure)
	e.Use(middleware...)
	e.Use(session.SetUser())
	// the routes open to bot tokens, as apiRoutes registers them
	bots := session.NewBotRoutes(server.Config.Server.RootPath)
	e.Use(session.AuditBot(bots))
	e.Use(session.GuardBot(bots))
	e.Use(token.Refresh)

	e.NoRoute(gin.WrapF(noRouteHandler))

	base := e.Group(server.Config.Server.RootPath)
	{
		base.GET("/web-config.js", web.Config)

		base.GET("/logout", api.GetLogout)
		auth := base.Group("/authorize")
		{
			auth.GET("", api.HandleAuth)
			auth.POST("", api.HandleAuth)
		}

		base.GET("/metrics", metrics.PromHandler())
		// TODO: remove in v4.0.0, use the ones in apiRoutes
		base.GET("/version", api.Version)
		base.GET("/healthz", api.Health)
	}

	apiRoutes(base, bots)
	if server.Config.WebUI.EnableSwagger {
		setupSwaggerConfigAndRoutes(e)
	}

	return e, bots
}

func setupSwaggerConfigAndRoutes(e *gin.Engine) {
	openapi.SwaggerInfo.Host = getHost(server.Config.Server.Host)
	openapi.SwaggerInfo.BasePath = server.Config.Server.RootPath + "/api"
	e.GET(server.Config.Server.RootPath+"/swagger/*any", swaggo_gin_swagger.WrapHandler(swaggo_files.Handler))
}

func getHost(s string) string {
	parse, _ := url.Parse(s)
	return parse.Host
}
