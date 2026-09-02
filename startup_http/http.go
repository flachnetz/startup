package startup_http

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	tracing "github.com/flachnetz/startup/v2/startup_tracing"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/flachnetz/go-admin"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/goji/httpauth"
	"github.com/gorilla/handlers"
)

type Config struct {
	// name for the service. Will be used in the admin panel
	Name string

	// Routing configuration. You can use the supplied router or
	// return your own handler.
	Routing func(*http.ServeMux) http.Handler

	// Extra admin handlers to register on the admin page
	AdminHandlers []admin.RouteConfig

	// Registers a shutdown handler for the http server. If not set,
	// a default signal handler for clean shutdown on SIGINT and SIGTERM is used.
	RegisterSignalHandlerForServer func(*http.Server) <-chan struct{}

	// Wrap the http server with this middleware in the end. A good example would
	// be to use a tracing middleware at this point.
	UseMiddleware HttpMiddleware
}

type HttpMiddleware = func(http.Handler) http.Handler

type HTTPOptions struct {
	Address string `long:"http-address" env:"HTTP_ADDRESS" default:":3080" description:"Address to listen on."`

	TLSKeyFile  string `long:"http-tls-key" env:"HTTP_TLS_KEY" description:"Private key file to enable SSL support."`
	TLSCertFile string `long:"http-tls-cert" env:"HTTP_TLS_CERT" description:"Certificate file to enable SSL support."`

	DisableAdminRedirect bool   `long:"http-disable-admin-redirect" env:"HTTP_DISABLE_ADMIN_REDIRECT" description:"Disable admin redirect on /"`
	DisableAuth          bool   `long:"http-disable-admin-auth" env:"HTTP_DISABLE_ADMIN_AUTH" description:"Disable basic auth"`
	BasicAuthUsername    string `long:"http-admin-username" env:"HTTP_ADMIN_USERNAME" default:"admin" description:"Basic auth username for admin panel."`
	BasicAuthPassword    string `long:"http-admin-password" env:"HTTP_ADMIN_PASSWORD" default:"bingo" description:"Basic auth password for admin panel."`

	AccessLog            string `long:"http-access-log" env:"HTTP_ACCESS_LOG" description:"Write http access log to a file. Defaults to stdout."`
	AccessLogAdminRoute  bool   `long:"http-access-log-admin-route" env:"HTTP_ACCESS_LOG_ADMIN_ROUTE" description:"If enabled, admin route requests will also be logged."`
	AdminPageShowEnvVars bool   `long:"http-admin-show-env-vars" env:"HTTP_ADMIN_SHOW_ENV_VARS" description:"Show environment variables on the admin page."`

	inputs        startup_base.Inputs
	hasTracing    bool
	cancelContext context.Context
}

func (opts *HTTPOptions) Initialize(ctx context.Context, base startup_base.BaseOptions, tracingOpts *tracing.TracingOptions) {
	opts.inputs = base.Inputs
	opts.hasTracing = tracingOpts != nil
	opts.cancelContext = ctx
}

func (opts *HTTPOptions) ServeHandler(handler http.Handler) {
	opts.Serve(Config{
		Routing:       func(mux *http.ServeMux) http.Handler { return handler },
		UseMiddleware: tracing.Tracing(opts.inputs.ServiceName, "serve"),
	})
}

func (opts *HTTPOptions) Serve(config Config) {
	if config.UseMiddleware == nil && opts.hasTracing {
		slog.Info("Setup tracing http middleware")

		config.UseMiddleware = tracing.Tracing(opts.inputs.ServiceName, "serve")
	}

	server := &http.Server{
		Addr:              opts.Address,
		Handler:           opts.buildHandler(config),
		ReadHeaderTimeout: 1 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	registerSignalHandler := config.RegisterSignalHandlerForServer
	if registerSignalHandler == nil {
		registerSignalHandler = buildSignalHandlerForServer(opts.cancelContext)
	}

	waitCh := registerSignalHandler(server)

	err := opts.listenAndServe(server)

	if errors.Is(err, http.ErrServerClosed) {
		// wait for server to shutdown. ListenAndServe returns directly
		// if server.Shutdown() is called.
		<-waitCh
	} else if err != nil {
		startup_base.PanicOnError(err, "Could not start server")

		return
	}

	slog.Info("Server shutdown completed.")
}

// buildHandler assembles the full request handler: application routes, the
// admin panel, panic recovery, the access log and the configured middleware,
// in that order from the inside out.
func (opts *HTTPOptions) buildHandler(config Config) http.Handler {
	// guess the app name
	appName := config.Name
	if appName == "" {
		appName = opts.inputs.ServiceName
	}

	router := http.NewServeMux()

	// configure app routes
	var handler http.Handler = router
	if config.Routing != nil {
		handler = config.Routing(router)
	}

	if !opts.DisableAdminRedirect {
		// try to register / -> /admin redirect.
		tryRegisterAdminHandlerRedirect(router)
	}

	// Admin handler with a lot of admin-stuff
	adminHandler := requireAuth(opts.DisableAuth, opts.BasicAuthUsername, opts.BasicAuthPassword,
		admin.NewAdminHandler("/admin", appName, opts.adminRoutes(config)...))

	// merge handlers
	handler = mergeWithAdminHandler(adminHandler, handler)

	// don't let a panic crash the server.
	recoveryStack := handlers.RecoveryHandler(
		handlers.PrintRecoveryStack(true),
		handlers.RecoveryLogger(NewSlogRecoveryHandlerLogger()),
	)

	handler = opts.withAccessLog(recoveryStack(handler))

	if config.UseMiddleware != nil {
		handler = config.UseMiddleware(handler)
	}

	return handler
}

// adminRoutes returns the admin panel routes: the built-in ones, build info if
// this binary carries any, and whatever the caller added.
func (opts *HTTPOptions) adminRoutes(config Config) []admin.RouteConfig {
	routeConfigs := []admin.RouteConfig{
		admin.WithForceGC(),
		admin.WithPingPong(),
		admin.WithGCStats(),
		admin.WithPProfHandlers(),
		admin.WithHeapDump(),
		// admin.WithMetrics(metrics.DefaultRegistry),
		WithPrometheusMetrics("/metrics"),
		updateLogLevelHandler(),
	}

	if opts.AdminPageShowEnvVars {
		routeConfigs = append(routeConfigs, admin.WithEnvironmentVariables())
	}

	if startup_base.BuildGitHash != "" {
		var buildTime string
		if ts, err := strconv.Atoi(startup_base.BuildUnixTimestamp); err == nil {
			buildTime = time.Unix(int64(ts), 0).String()
		}

		routeConfigs = append(routeConfigs, admin.WithBuildInfo(admin.BuildInfo{
			Version:   startup_base.BuildVersion,
			GitHash:   startup_base.BuildGitHash,
			BuildTime: buildTime,
		}))
	}

	// add extra handlers from config
	return append(routeConfigs, config.AdminHandlers...)
}

// withAccessLog wraps handler in the configured access log: the slog logger by
// default, a plain file if one was configured, and nothing at all for
// /dev/null.
func (opts *HTTPOptions) withAccessLog(handler http.Handler) http.Handler {
	if opts.AccessLog == "" {
		// log all requests using slog logger
		return loggingHandler{handler: handler, log: opts.logAccessToSlog}
	}

	if opts.AccessLog == "/dev/null" {
		return handler
	}

	fp, err := startup_base.OpenWriter(opts.AccessLog)
	startup_base.PanicOnError(err, "Could not open log file")

	if fp == nil {
		return handler
	}

	// write events directly to log file
	return loggingHandler{
		handler: handler,
		log: func(_ context.Context, attrs []slog.Attr) {
			_, _ = io.WriteString(fp, accessLogLine(attrs))
		},
	}
}

// logAccessToSlog logs one request, skipping admin routes unless they were
// explicitly enabled.
func (opts *HTTPOptions) logAccessToSlog(ctx context.Context, attrs []slog.Attr) {
	if !opts.AccessLogAdminRoute && isAdminRoute(attrs) {
		return
	}

	access(ctx, attrs)
}

// isAdminRoute reports whether the logged request went to the admin panel.
func isAdminRoute(attrs []slog.Attr) bool {
	for _, a := range attrs {
		if a.Key == "uri" && strings.Contains(a.Value.String(), "/admin/") {
			return true
		}
	}

	return false
}

// accessLogLine renders one request as a single key=value line.
func accessLogLine(attrs []slog.Attr) string {
	var b strings.Builder

	for i, a := range attrs {
		if i > 0 {
			b.WriteByte(' ')
		}

		b.WriteString(a.Key)
		b.WriteByte('=')
		b.WriteString(a.Value.String())
	}

	b.WriteByte('\n')

	return b.String()
}

// listenAndServe starts the server, with TLS when both a certificate and a key
// were configured.
func (opts *HTTPOptions) listenAndServe(server *http.Server) error {
	if opts.TLSCertFile == "" && opts.TLSKeyFile == "" {
		slog.Info("Start http server", slog.String("address", server.Addr))

		return server.ListenAndServe()
	}

	slog.Info("Start https server",
		slog.String("address", opts.Address),
		slog.String("cert", opts.TLSCertFile),
		slog.String("key", opts.TLSKeyFile))

	return server.ListenAndServeTLS(opts.TLSCertFile, opts.TLSKeyFile)
}

func WithPrometheusMetrics(s string) admin.RouteConfig {
	return admin.Describe(
		"Prometheus metrics",
		admin.WithHandlerFunc("", s, func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			prometheusHandler := promhttp.Handler()
			prometheusHandler.ServeHTTP(w, req)
		}),
	)
}

func access(ctx context.Context, attrs []slog.Attr) {
	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}
	slog.DebugContext(ctx, "access", args...)
}

func buildSignalHandlerForServer(ctx context.Context) func(*http.Server) <-chan struct{} {
	return func(server *http.Server) <-chan struct{} {
		waitCh := make(chan struct{})

		signalCh := make(chan os.Signal, 1)

		go func() {
			signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
			defer signal.Stop(signalCh)

			select {
			// wait for signal
			case <-signalCh:

			// wait for context cancellation
			case <-ctx.Done():
			}

			slog.InfoContext(ctx, "Signal received, shutting down", slog.String("prefix", "httpd"))

			err := server.Shutdown(context.Background())
			if err != nil {
				slog.WarnContext(ctx, "Server shutdown", slog.String("prefix", "httpd"))
			}

			close(waitCh)
		}()

		return waitCh
	}
}

func tryRegisterAdminHandlerRedirect(router *http.ServeMux) {
	defer func() {
		if r := recover(); r != nil {
			slog.Debug("Admin handler redirect from / to /admin not possible")
		}
	}()

	router.Handle("GET /",
		http.RedirectHandler("/admin", http.StatusTemporaryRedirect))
}

func mergeWithAdminHandler(admin, rest http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		p := request.URL.Path
		if p == "/admin" || strings.HasPrefix(p, "/admin/") {
			admin.ServeHTTP(w, request)
		} else {
			rest.ServeHTTP(w, request)
		}
	}
}

func requireAuth(disableAuth bool, user, pass string, handler http.Handler) http.HandlerFunc {
	authed := httpauth.SimpleBasicAuth(user, pass)(handler)

	return func(writer http.ResponseWriter, request *http.Request) {
		if disableAuth || request.URL.Path == "/admin/ping" {
			handler.ServeHTTP(writer, request)
		} else {
			authed.ServeHTTP(writer, request)
		}
	}
}

func updateLogLevelHandler() admin.RouteConfig {
	return admin.Describe(
		"Configure logging by posting a log level like 'info', 'debug' or 'warn' to this endpoint.",
		admin.WithHandlerFunc("", "log/level", logLevelHandler()),
	)
}

// logLevelHandler reads the current log level on GET and sets it on POST.
func logLevelHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		switch strings.ToUpper(req.Method) {
		case http.MethodGet:
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(startup_base.LogLevel.Level().String()))

		case http.MethodPost:
			body, _ := io.ReadAll(req.Body)

			var level slog.Level
			if err := level.UnmarshalText(bytes.TrimSpace(body)); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)

				return
			}

			slog.With("prefix", "admin").Info("Set log level", "level", level)
			startup_base.LogLevel.Set(level)

		default:
			http.Error(w, "Method must be GET or POST", http.StatusMethodNotAllowed)
		}
	}
}
