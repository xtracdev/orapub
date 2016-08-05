package main

import (
	"github.com/alecthomas/kingpin"
	"os"
	"github.com/lox/httpcache"
	"net/http/httputil"
	"net/http"
	"log"
	"github.com/lox/httpcache/httplog"
)

var (
	app = kingpin.New("feedproxy", "Reverse caching proxy for event feed")
	listen = app.Flag("listen", "host:port to listen on").Required().String()
	origin = app.Flag("origin", "host:port of the origin").Required().String()
	dir = app.Flag("dir","cache directory path").Required().String()
	verbose = app.Flag("verbose", "verbose logging output").Bool()
)

func main() {
	kingpin.MustParse(app.Parse(os.Args[1:]))

	if *verbose {
		httpcache.DebugLogging = true
	}

	proxy := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = "http"
			r.URL.Host = *origin
		},
	}

	var cache httpcache.Cache

	log.Printf("storing cached resources in %s", *dir)
	if err := os.MkdirAll(*dir, 0700); err != nil {
		log.Fatal(err)
	}

	var err error
	cache, err = httpcache.NewDiskCache(*dir)
	if err != nil {
		log.Fatal(err)
	}

	handler := httpcache.NewHandler(cache, proxy)

	respLogger := httplog.NewResponseLogger(handler)


	log.Printf("listening on http://%s", *listen)
	log.Fatal(http.ListenAndServe(*listen, respLogger))
}
