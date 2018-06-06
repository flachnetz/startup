package startup_http

import (
	"context"
	"github.com/flachnetz/go-admin"
	"github.com/flachnetz/startup"
	"github.com/goji/httpauth"
	"github.com/gorilla/handlers"
	"github.com/julienschmidt/httprouter"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
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
}

type HTTPOptions struct {
	Address string `long:"http-address" default:":3080" description:"Address to listen on."`

	TLSKeyFile  string `long:"http-tls-key" description:"Private key file to enable SSL support."`
	TLSCertFile string `long:"http-tls-cert" description:"Certificate file to enable SSL support."`

	BasicAuthUsername string `long:"http-admin-username" default:"admin" description:"Basic auth username for admin panel."`
	BasicAuthPassword string `long:"http-admin-password" default:"bingo" description:"Basic auth password for admin panel."`

	AccessLog string `long:"http-access-log" description:"Write http access log to a file. Defaults to stdout."`
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
	}

	if startup.BuildGitHash != "" {
		var buildTime string
		if ts, err := strconv.Atoi(startup.BuildUnixTimestamp); err == nil {
			buildTime = time.Unix(int64(ts), 0).String()
		}

		routeConfigs = append(routeConfigs, admin.WithBuildInfo(admin.BuildInfo{
			Version:   startup.BuildVersion,
			GitHash:   startup.BuildGitHash,
			BuildTime: buildTime,
		}))
	}

	router := httprouter.New()

	// configure app routes
	var handler http.Handler = router
	if config.Routing != nil {
		handler = config.Routing(router)
	}

	// try to register / -> /admin redirect.
	tryRegisterAdminHandlerRedirect(router)

	// add extra handlers from config
	routeConfigs = append(routeConfigs, config.AdminHandlers...)

	// Admin handler with a lot of admin-stuff
	adminHandler := requireAuth(opts.BasicAuthUsername, opts.BasicAuthPassword,
		admin.NewAdminHandler("/admin", appName, routeConfigs...))

	// merge handlers
	handler = mergeWithAdminHandler(adminHandler, handler)

	if opts.AccessLog == "" {
		// log all requests using logrus logger
		handler = handlers.LoggingHandler(
			log.WithField("prefix", "httpd").WriterLevel(log.DebugLevel),
			handler)

	} else if opts.AccessLog != "/dev/null" {
		fp, err := startup.OpenWriter(opts.AccessLog)
		startup.PanicOnError(err, "Could not open log file")

		// write events directly to log file
		handler = handlers.LoggingHandler(fp, handler)
	}

	// don't let a panic crash the server.
	handler = handlers.RecoveryHandler(handlers.PrintRecoveryStack(true))(handler)

	server := &http.Server{
		Addr:              opts.Address,
		Handler:           handler,
		ReadHeaderTimeout: 1 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	waitCh := registerSignalHandlerForServer(server)

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
		startup.PanicOnError(err, "Could not start server")
		return
	}

	log.Info("Server shutdown completed.")
}

func registerSignalHandlerForServer(server *http.Server) <-chan struct{} {
	waitCh := make(chan struct{})

	signalCh := make(chan os.Signal)

	go func() {
		signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(signalCh)

		// wait for signal
		<-signalCh

		log.WithField("prefix", "httpd").Info("Signal received, shutting down")
		server.Shutdown(context.Background())

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

func requireAuth(user, pass string, handler http.Handler) http.HandlerFunc {
	authed := httpauth.SimpleBasicAuth(user, pass)(handler)

	return func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/admin/ping" {
			handler.ServeHTTP(writer, request)
		} else {
			authed.ServeHTTP(writer, request)
		}
	}
}
