// Comando node: recibe por TCP un fragmento del dataset y los árboles a construir.
//
//	go run ./cmd/node -addr :9100 -name nodo1
//
// Docker: NODE_ADDR, NODE_NAME, NODE_WORKERS.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/ShoterBroXD/EduForest/internal/cluster"
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	host, _ := os.Hostname()
	workersDef, _ := strconv.Atoi(getEnv("NODE_WORKERS", "0"))
	addr := flag.String("addr", getEnv("NODE_ADDR", "0.0.0.0:9100"), "dirección TCP donde escucha el nodo")
	name := flag.String("name", getEnv("NODE_NAME", host), "nombre del nodo en los logs y métricas")
	workers := flag.Int("workers", workersDef, "goroutines para construir árboles (0 = núcleos disponibles)")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("[%s] no se pudo abrir %s: %v", *name, *addr, err)
	}
	if err := cluster.NewNode(*name, *workers).Serve(ln); err != nil {
		log.Fatal(err)
	}
}
