// Comando api: coordinador de EduForest. Expone la API REST y el WebSocket,
// reparte el entrenamiento entre los nodos por TCP y guarda los resultados
// en MongoDB y Redis.
//
// Variables de entorno (con sus valores por defecto):
//
//	API_ADDR=:8080  DATA_DIR=../data/siagie  MODELS_DIR=../models
//	NODES=localhost:9101,localhost:9102      lista de nodos del clúster
//	MONGO_URI= (vacío = base en memoria)     MONGO_DB=eduforest
//	REDIS_ADDR= (vacío = caché en memoria)
//	JWT_SECRET  ADMIN_USER=admin  ADMIN_PASSWORD=admin123  CORS_ORIGIN=*
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/ShoterBroXD/EduForest/internal/api"
	"github.com/ShoterBroXD/EduForest/internal/cluster"
	"github.com/ShoterBroXD/EduForest/internal/dataset"
	"github.com/ShoterBroXD/EduForest/internal/store"
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var nodes []string
	for _, n := range strings.Split(getEnv("NODES", "localhost:9101,localhost:9102"), ",") {
		if n = strings.TrimSpace(n); n != "" {
			nodes = append(nodes, n)
		}
	}

	var st store.Store
	var cache store.Cache
	if uri := os.Getenv("MONGO_URI"); uri != "" {
		m, err := store.NewMongo(ctx, uri, getEnv("MONGO_DB", "eduforest"))
		if err != nil {
			log.Fatalf("[api] no se pudo conectar a MongoDB: %v", err)
		}
		st = m
		log.Printf("[api] conectado a MongoDB")
	} else {
		st = store.NewMemory()
		log.Printf("[api] MONGO_URI vacío: se usa una base en memoria")
	}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		r, err := store.NewRedis(ctx, addr)
		if err != nil {
			log.Fatalf("[api] no se pudo conectar a Redis: %v", err)
		}
		cache = r
		log.Printf("[api] conectado a Redis en %s", addr)
	} else {
		cache = store.NewMemoryCache()
		log.Printf("[api] REDIS_ADDR vacío: se usa una caché en memoria")
	}

	t := time.Now()
	ds, rep, err := dataset.LoadDir(getEnv("DATA_DIR", "../data/siagie"), 3, runtime.NumCPU())
	if err != nil {
		log.Fatalf("[api] %v", err)
	}
	log.Printf("[api] dataset listo: %d muestras en %v", rep.Kept, time.Since(t).Round(time.Millisecond))

	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		secret = "eduforest-dev-secret"
		log.Printf("[api] JWT_SECRET vacío: se usa un secreto de desarrollo")
	}
	srv := api.New(api.Config{
		ModelsDir:  getEnv("MODELS_DIR", "../models"),
		JWTSecret:  []byte(secret),
		AdminUser:  getEnv("ADMIN_USER", "admin"),
		AdminPass:  getEnv("ADMIN_PASSWORD", "admin123"),
		CORSOrigin: getEnv("CORS_ORIGIN", "*"),
	}, st, cache, cluster.NewCoordinator(nodes), ds)
	srv.Start(ctx)

	httpSrv := &http.Server{Addr: getEnv("API_ADDR", ":8080"), Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Printf("[api] escuchando en %s | nodos: %v | Swagger en /swagger/", httpSrv.Addr, nodes)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Printf("[api] apagando...")
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(shutdown)
	st.Close(shutdown)
	cache.Close()
}
