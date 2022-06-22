package startup_http

import (
	"context"
	"github.com/flachnetz/go-admin"
	"github.com/flachnetz/startup/v2/startup_base"
	. "github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/goji/httpauth"
	"github.com/gorilla/handlers"
	"github.com/julienschmidt/httprouter"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Config struct {
	// name for the service. Will be used in the admin panel
	Name string

	// Routing configuration. You can use the supplied router or
	// return your own handler.
	Routing func(*httprouter.Router) http.Handler

	// Extra admin handlers to register on the admin page
	AdminHandlers []admin.RouteConfig

	// Registers a shutdown handler for the http server. If not set,
	// a default signal handler for clean shutdown on SIGINT and SIGTERM is used.
	RegisterSignalHandlerForServer func(*http.Server) <-chan struct{}

	// Wrap the http server with this middleware in the end. A good example would
	// be to use a tracing middleware at this point.
	UseMiddleware HttpMiddleware
}

type HTTPOptions struct {
	Address string `long:"http-address" default:":3080" description:"Address to listen on."`

	TLSKeyFile  string `long:"http-tls-key" description:"Private key file to enable SSL support."`
	TLSCertFile string `long:"http-tls-cert" description:"Certificate file to enable SSL support."`

	DisableAdminRedirect bool   `long:"http-disable-admin-redirect" description:"Disable admin redirect on /"`
	DisableAuth          bool   `long:"http-disable-admin-auth" description:"Disable basic auth"`
	BasicAuthUsername    string `long:"http-admin-username" default:"admin" description:"Basic auth username for admin panel."`
	BasicAuthPassword    string `long:"http-admin-password" default:"bingo" description:"Basic auth password for admin panel."`

	AccessLog           string `long:"http-access-log" description:"Write http access log to a file. Defaults to stdout."`
	AccessLogAdminRoute bool   `long:"http-access-log-admin-route" description:"If enabled, admin route requests will also be logged."`
}

func (opts HTTPOptions) Serve(config Config) {

	// guess the app name
	appName := config.Name
	if appName == "" {
		appName = path.Base(os.Args[0])
	}

	// add basic handlers
	routeConfigs := []admin.RouteConfig{
		admin.WithForceGC(),
		admin.WithDefaults(),
		admin.WithPProfHandlers(),
		admin.WithHeapDump(),
		admin.WithMetrics(metrics.DefaultRegistry),
		withUpdateLogLevel(),
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

	router := httprouter.New()

	// configure app routes
	var handler http.Handler = router
	if config.Routing != nil {
		handler = config.Routing(router)
	}

	if !opts.DisableAdminRedirect {
		// try to register / -> /admin redirect.
		tryRegisterAdminHandlerRedirect(router)
	}

	// add extra handlers from config
	routeConfigs = append(routeConfigs, config.AdminHandlers...)

	// Admin handler with a lot of admin-stuff
	adminHandler := requireAuth(opts.DisableAuth, opts.BasicAuthUsername, opts.BasicAuthPassword,
		admin.NewAdminHandler("/admin", appName, routeConfigs...))

	// merge handlers
	handler = mergeWithAdminHandler(adminHandler, handler)

	// don't let a panic crash the server.
	handler = handlers.RecoveryHandler(handlers.PrintRecoveryStack(true))(handler)

	if opts.AccessLog == "" {
		// log all requests using logrus logger
		handler = loggingHandler{
			handler: handler,
			log: func(ctx context.Context, line string) {
				if !opts.AccessLogAdminRoute && strings.Contains(line, "GET /admin/") {
					return
				}
				GetLogger(ctx, "httpd").Debug(line)
			},
		}

	} else if opts.AccessLog != "/dev/null" {
		fp, err := startup_base.OpenWriter(opts.AccessLog)
		startup_base.PanicOnError(err, "Could not open log file")

		// write events directly to log file
		handler = loggingHandler{
			handler: handler,
			log: func(ctx context.Context, line string) {
				_, _ = fp.Write([]byte(line + "\n"))
			},
		}
	}

	// Setup logger in context with tracing ids as fields
	handler = TracingLoggerMiddleWare(handler)

	if config.UseMiddleware != nil {
		handler = config.UseMiddleware(handler)
	}

	server := &http.Server{
		Addr:              opts.Address,
		Handler:           handler,
		ReadHeaderTimeout: 1 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	registerSignalHandler := config.RegisterSignalHandlerForServer
	if registerSignalHandler == nil {
		registerSignalHandler = RegisterSignalHandlerForServer
	}

	waitCh := registerSignalHandler(server)

	var err error
	if opts.TLSCertFile == "" && opts.TLSKeyFile == "" {
		log.Infof("Start http server on %s", server.Addr)
		err = server.ListenAndServe()
	} else {
		log.Infof("Start https server on %s with certificate %s and key %s",
			opts.Address, opts.TLSCertFile, opts.TLSKeyFile)

		err = server.ListenAndServeTLS(opts.TLSCertFile, opts.TLSKeyFile)
	}

	if err == http.ErrServerClosed {
		// wait for server to shutdown. ListenAndServe returns directly
		// if server.Shutdown() is called.
		<-waitCh

	} else if err != nil {
		startup_base.PanicOnError(err, "Could not start server")
		return
	}

	log.Info("Server shutdown completed.")
}

func RegisterSignalHandlerForServer(server *http.Server) <-chan struct{} {
	waitCh := make(chan struct{})

	signalCh := make(chan os.Signal, 1)

	go func() {
		signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(signalCh)

		// wait for signal
		<-signalCh

		log.WithField("prefix", "httpd").Info("Signal received, shutting down")

		err := server.Shutdown(context.Background())
		if err != nil {
			log.WithField("prefix", "httpd").Warnf("Server shutdown")
		}

		close(waitCh)
	}()

	return waitCh
}

func tryRegisterAdminHandlerRedirect(router *httprouter.Router) {
	defer func() {
		if r := recover(); r != nil {
			log.Debugf("Admin handler redirect from / to /admin not possible")
		}
	}()

	router.Handler("GET", "/",
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

func withUpdateLogLevel() admin.RouteConfig {
	return admin.Describe(
		"Configure logging by posting a log level like 'info', 'debug' or 'warn' to this endpoint.",
		admin.WithHandlerFunc("", "log/level", func(w http.ResponseWriter, req *http.Request) {
			if strings.ToUpper(req.Method) == "GET" {
				w.Header().Set("Content-Type", "text/plain")
				_, _ = w.Write([]byte(log.GetLevel().String()))
				return
			}

			if strings.ToUpper(req.Method) == "POST" {
				body, _ := ioutil.ReadAll(req.Body)
				level, err := log.ParseLevel(strings.TrimSpace(string(body)))
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}

				log.WithField("prefix", "admin").Infof("Set log level to %s", level)
				log.SetLevel(level)
				return
			}

			http.Error(w, "Method must be GET or POST", http.StatusMethodNotAllowed)
		}))
}
