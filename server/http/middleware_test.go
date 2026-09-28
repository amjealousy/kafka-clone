package http

import (
	"strings"
	"testing"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

func corsServer(origins ...string) (*HttpServer, *router.Router) {
	server := newClusterServer(&clusterAdminStub{overview: testOverview()})
	server.SetAllowedOrigins(origins)
	r := router.New()
	server.SetupRouter(r)
	return server, r
}

func request(r *router.Router, method, uri, origin string) *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(method)
	ctx.Request.SetRequestURI(uri)
	if origin != "" {
		ctx.Request.Header.Set("Origin", origin)
	}
	r.Handler(ctx)
	return ctx
}

func TestCORSAllowsConfiguredOrigin(t *testing.T) {
	_, r := corsServer("http://127.0.0.1:8091")

	ctx := request(r, fasthttp.MethodGet, "/cluster/v1/overview", "http://127.0.0.1:8091")

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, body = %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if got := string(ctx.Response.Header.Peek("Access-Control-Allow-Origin")); got != "http://127.0.0.1:8091" {
		t.Fatalf("Allow-Origin = %q", got)
	}
	// Без Vary разделяемый кэш отдал бы ответ с чужим Allow-Origin.
	if !strings.Contains(string(ctx.Response.Header.Peek("Vary")), "Origin") {
		t.Fatalf("Vary = %q, want Origin", ctx.Response.Header.Peek("Vary"))
	}
}

func TestCORSRejectsUnknownOrigin(t *testing.T) {
	_, r := corsServer("http://127.0.0.1:8091")

	ctx := request(r, fasthttp.MethodGet, "/cluster/v1/overview", "http://evil.example")

	if ctx.Response.Header.Peek("Access-Control-Allow-Origin") != nil {
		t.Fatalf("Allow-Origin must be absent for unknown origin: %q", ctx.Response.Header.Peek("Access-Control-Allow-Origin"))
	}
}

// Wildcard должен разворачиваться в конкретный origin: с credentials звёздочка
// запрещена спецификацией, и ответ перестал бы работать после включения auth.
func TestCORSWildcardEchoesConcreteOrigin(t *testing.T) {
	_, r := corsServer("*")

	ctx := request(r, fasthttp.MethodGet, "/cluster/v1/overview", "http://127.0.0.1:9999")

	if got := string(ctx.Response.Header.Peek("Access-Control-Allow-Origin")); got != "http://127.0.0.1:9999" {
		t.Fatalf("Allow-Origin = %q, want concrete origin", got)
	}
	if got := string(ctx.Response.Header.Peek("Access-Control-Allow-Credentials")); got != "true" {
		t.Fatalf("Allow-Credentials = %q", got)
	}
}

func TestCORSDisabledByDefault(t *testing.T) {
	_, r := corsServer()

	ctx := request(r, fasthttp.MethodGet, "/cluster/v1/overview", "http://127.0.0.1:8091")

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d", ctx.Response.StatusCode())
	}
	if ctx.Response.Header.Peek("Access-Control-Allow-Origin") != nil {
		t.Fatal("CORS headers must be absent when no origins are configured")
	}
}

func TestCORSPreflightAnswersOptions(t *testing.T) {
	_, r := corsServer("http://127.0.0.1:8091")

	for _, path := range []string{"/topic/v1/create", "/broker/v1/produce"} {
		t.Run(path, func(t *testing.T) {
			ctx := request(r, fasthttp.MethodOptions, path, "http://127.0.0.1:8091")

			if ctx.Response.StatusCode() != fasthttp.StatusNoContent {
				t.Fatalf("status = %d, want 204", ctx.Response.StatusCode())
			}
			if got := string(ctx.Response.Header.Peek("Access-Control-Allow-Methods")); !strings.Contains(got, "POST") {
				t.Fatalf("Allow-Methods = %q", got)
			}
			if got := string(ctx.Response.Header.Peek("Access-Control-Allow-Headers")); !strings.Contains(got, "Content-Type") {
				t.Fatalf("Allow-Headers = %q", got)
			}
		})
	}
}

func TestCORSPreflightRejectsUnknownOrigin(t *testing.T) {
	_, r := corsServer("http://127.0.0.1:8091")

	ctx := request(r, fasthttp.MethodOptions, "/topic/v1/create", "http://evil.example")

	if ctx.Response.StatusCode() != fasthttp.StatusForbidden {
		t.Fatalf("status = %d, want 403", ctx.Response.StatusCode())
	}
}

// Пробы потребляет оркестратор, а не браузер: авторизации и CORS на них нет.
func TestProbesAreRegisteredWithoutAuth(t *testing.T) {
	_, r := corsServer()

	live := request(r, fasthttp.MethodGet, "/healthz", "")
	if live.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("/healthz status = %d", live.Response.StatusCode())
	}

	ready := request(r, fasthttp.MethodGet, "/readyz", "")
	if ready.Response.StatusCode() != fasthttp.StatusServiceUnavailable {
		t.Fatalf("/readyz status = %d, want 503 for not-ready stub", ready.Response.StatusCode())
	}
}
