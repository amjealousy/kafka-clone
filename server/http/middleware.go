package http

import (
	"strings"

	"github.com/valyala/fasthttp"
)

func AuthMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {

		next(ctx)
	}
}

func topicAccessMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {

		next(ctx)
	}
}

const (
	corsAllowMethods = "GET, POST, OPTIONS"
	corsAllowHeaders = "Content-Type, Authorization, Last-Event-ID"
	corsMaxAge       = "600"
)

// SetAllowedOrigins задаёт список origin'ов, которым разрешены cross-origin
// запросы к control-plane API. Пустой список полностью выключает CORS.
//
// Зачем это вообще нужно: когда нода, с которой загрузилась админка, умирает,
// браузер обязан перейти на другую ноду из списка discovery. Даже локально это
// cross-origin — 127.0.0.1:8090 и 127.0.0.1:8091 разные origin'ы, — поэтому без
// CORS клиентский failover невозможен в принципе, fetch упадёт до сети.
func (s *HttpServer) SetAllowedOrigins(origins []string) {
	allowed := make([]string, 0, len(origins))
	for _, origin := range origins {
		if trimmed := strings.TrimSpace(origin); trimmed != "" {
			allowed = append(allowed, trimmed)
		}
	}
	s.allowedOrigins = allowed
}

// originAllowed возвращает значение для Access-Control-Allow-Origin или пустую
// строку, если запрос не разрешён.
//
// Для "*" возвращается сам origin, а не звёздочка: так ответ остаётся валидным
// и после включения аутентификации с credentials, где wildcard запрещён
// спецификацией.
func (s *HttpServer) originAllowed(origin string) string {
	if origin == "" {
		return ""
	}
	for _, allowed := range s.allowedOrigins {
		if allowed == "*" || strings.EqualFold(allowed, origin) {
			return origin
		}
	}
	return ""
}

// applyCORS проставляет заголовки ответа. Vary: Origin обязателен — иначе
// разделяемый кэш отдаст ответ с чужим Allow-Origin другому клиенту.
func (s *HttpServer) applyCORS(ctx *fasthttp.RequestCtx) {
	origin := string(ctx.Request.Header.Peek("Origin"))
	ctx.Response.Header.Add("Vary", "Origin")

	allowed := s.originAllowed(origin)
	if allowed == "" {
		return
	}
	ctx.Response.Header.Set("Access-Control-Allow-Origin", allowed)
	ctx.Response.Header.Set("Access-Control-Allow-Credentials", "true")
}

// CORSMiddleware добавляет заголовки к обычным ответам. Preflight (OPTIONS)
// обрабатывается отдельно в corsPreflightHandler через router.GlobalOPTIONS.
func (s *HttpServer) CORSMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		s.applyCORS(ctx)
		next(ctx)
	}
}

// corsPreflightHandler отвечает на OPTIONS. Роутер уже проставил заголовок
// Allow для известного пути, поэтому здесь остаётся только CORS-часть.
func (s *HttpServer) corsPreflightHandler(ctx *fasthttp.RequestCtx) {
	s.applyCORS(ctx)

	if ctx.Response.Header.Peek("Access-Control-Allow-Origin") == nil {
		// Origin не разрешён: заголовков нет, браузер сам заблокирует запрос.
		ctx.SetStatusCode(fasthttp.StatusForbidden)
		return
	}

	ctx.Response.Header.Set("Access-Control-Allow-Methods", corsAllowMethods)
	ctx.Response.Header.Set("Access-Control-Allow-Headers", corsAllowHeaders)
	ctx.Response.Header.Set("Access-Control-Max-Age", corsMaxAge)
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}
