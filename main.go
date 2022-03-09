package main

import (
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"

	metrics "github.com/slok/go-http-metrics/metrics/prometheus"
	"github.com/slok/go-http-metrics/middleware"
	"github.com/slok/go-http-metrics/middleware/std"
	"github.com/RedTimeDB/RedTimeProxy/backend"
	"github.com/RedTimeDB/RedTimeProxy/service"
	"github.com/RedTimeDB/RedTimeProxy/util"
)

var (
	configFile string
	version    bool
	//GitCommit is commit id
	GitCommit = "not build"
	//BuildTime is build time
	BuildTime = "not build"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Llongfile)
	log.SetOutput(os.Stdout)
	flag.StringVar(&configFile, "config", "proxy.yaml", "proxy config file")
	flag.BoolVar(&version, "version", false, "proxy version")
	flag.Parse()
}

func main() {
	if version {
		fmt.Printf("Version:    %s\n", backend.Version)
		fmt.Printf("Git commit: %s\n", GitCommit)
		fmt.Printf("Build time: %s\n", BuildTime)
		fmt.Printf("Go version: %s\n", runtime.Version())
		fmt.Printf("OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
		return
	}

	cfg, err := backend.NewConfigYaml(configFile) // Read config file and create config object.
	if err != nil {
		log.Printf("illegal config file: %s\n", err)
		return
	}
	log.Printf("version: %s, commit: %s, build: %s", backend.Version, GitCommit, BuildTime)
	cfg.PrintSummary()

	err = util.MakeDir(cfg.DataDir)
	if err != nil {
		log.Fatalln("create data dir error")
		return
	}
	//判断是够开启UDP-Server
	if cfg.UDPEnable {
		//开启UDP
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Println("UDP Server recover", r)
				}
			}()
			err := service.NewUDPService(cfg).ListenAndServe()
			if err != nil {
				log.Println(err)
			}
		}()
	}

	// Serve our metrics.
	go func() {
		log.Printf("metrics listening at %s", ":9009")
		if err := http.ListenAndServe(":9009", promhttp.Handler()); err != nil {
			log.Panicf("error while serving metrics: %s", err)
		}
	}()

	mdlw := middleware.New(middleware.Config{
		Recorder: metrics.NewRecorder(metrics.Config{}),
	})
	mux := http.NewServeMux()
	service.NewHTTPService(cfg).Register(mux)
	server := &http.Server{
		Addr:        cfg.ListenAddr,
		Handler:     mux,
		IdleTimeout: time.Duration(cfg.IdleTimeout) * time.Second,
	}
	h := std.Handler("", mdlw, mux)
	// Serve our handler.
	if cfg.HTTPSEnabled {
		log.Printf("https service start, listen on %s", server.Addr)
		err = http.ListenAndServeTLS(cfg.ListenAddr, cfg.HTTPSCert, cfg.HTTPSKey, h)
		//err = server.ListenAndServeTLS(cfg.HTTPSCert, cfg.HTTPSKey)
	} else {
		log.Printf("http service start, listen on %s", server.Addr)
		err = http.ListenAndServe(cfg.ListenAddr, h)
		//err = server.ListenAndServe()
	}
	if err != nil {
		log.Print(err)
		return
	}

}
