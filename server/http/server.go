package http

import (
	"embed"
	"encoding/json/v2"
	"io/fs"
	"kafka-clone/server/datatypes"
	brokerAPI "kafka-clone/server/datatypes/broker"
	"kafka-clone/server/persistent/db"
	"log/slog"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

//go:embed static
var embeddedFiles embed.FS

type HttpServer struct {
	mc          *db.MongoClient
	logger      *slog.Logger
	controller  datatypes.IClusterAdmin
	router      *router.Router
	broker      brokerAPI.APIBroker
	templatesFS fs.FS
	// allowedOrigins — origin'ы, которым разрешён cross-origin доступ; пустой
	// список выключает CORS (см. SetAllowedOrigins).
	allowedOrigins []string
}

func NewHttpServer(mc *db.MongoClient, logger *slog.Logger, controller datatypes.IClusterAdmin, broker brokerAPI.APIBroker) *HttpServer {
	logger = logger.With("component", "WEB")
	h := new(HttpServer)
	h.mc = mc
	h.logger = logger
	h.controller = controller
	h.broker = broker
	webFiles, err := fs.Sub(embeddedFiles, "static")
	if err != nil {
		panic(err)
	}
	h.templatesFS = webFiles
	return h
}

func (s *HttpServer) writeHttpError(ctx *fasthttp.RequestCtx, status int, code, description string) {
	s.writeJSON(ctx, status, map[string]string{"error": code, "error_description": description})
}

func (s *HttpServer) writeJSON(ctx *fasthttp.RequestCtx, status int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}
	ctx.SetContentType("application/json")
	ctx.Response.Header.Set("Cache-Control", "no-store")
	ctx.Response.Header.Set("Pragma", "no-cache")
	ctx.SetStatusCode(status)
	_, _ = ctx.Write(data)
}

func (s *HttpServer) SetupRouter(router *router.Router) {
	s.router = router
	// Preflight для всех известных путей: роутер сам проставит Allow, а
	// corsPreflightHandler добавит CORS-заголовки.
	router.GlobalOPTIONS = s.corsPreflightHandler

	brokerGroup := router.Group("/broker/v1")
	brokerHandler := s.CORSMiddleware(AuthMiddleware(s.BrokeringRouter))
	// ANY регистрирует в том числе OPTIONS и не даёт GlobalOPTIONS обработать
	// preflight перед cross-origin produce.
	brokerGroup.GET("/{action}", brokerHandler)
	brokerGroup.POST("/{action}", brokerHandler)

	topicGroup := router.Group("/topic/v1")
	topicGroup.GET("/describe/{name}", s.CORSMiddleware(topicAccessMiddleware(AuthMiddleware(s.DescribeTopicHandler))))
	topicGroup.POST("/create", s.CORSMiddleware(topicAccessMiddleware(AuthMiddleware(s.CreateTopicHandler))))

	// Discovery для админки: один согласованный снимок кластера из etcd.
	clusterGroup := router.Group("/cluster/v1")
	clusterGroup.GET("/overview", s.CORSMiddleware(AuthMiddleware(s.ClusterOverviewHandler)))
	clusterGroup.GET("/nodes", s.CORSMiddleware(AuthMiddleware(s.ClusterNodesHandler)))
	clusterGroup.GET("/topics", s.CORSMiddleware(AuthMiddleware(s.ClusterTopicsHandler)))

	// Пробы намеренно без авторизации и без CORS: их потребитель —
	// оркестратор и балансировщик.
	router.GET("/healthz", s.LivenessHandler)
	router.GET("/readyz", s.ReadinessHandler)

	router.GET("/", s.indexHandler)
	router.ServeFS("/static/{filepath:*}", s.templatesFS)
}
