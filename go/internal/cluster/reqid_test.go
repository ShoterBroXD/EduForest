package cluster

import (
	"context"
	"encoding/json"
	"net"
	"testing"
)

// El X-Request-ID guardado en el contexto debe llegar al nodo dentro de PREDICT.
func TestPredictEnviaRequestID(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	recibido := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var m Mensaje
		if err := json.NewDecoder(conn).Decode(&m); err != nil || m.Predict == nil {
			recibido <- ""
			return
		}
		recibido <- m.Predict.ReqID
		json.NewEncoder(conn).Encode(Mensaje{Tipo: TipoResult, Votes: &PredictVotes{Votes: [][]int{{1, 0, 0}}, Trees: 1}})
	}()

	ctx := WithRequestID(context.Background(), "abc123def456")
	addr := ln.Addr().String()
	if _, _, err := NewCoordinator([]string{addr}).Predict(ctx, []string{addr}, "m1", [][]float64{{0, 0, 0, 0}}, 3, 1); err != nil {
		t.Fatal(err)
	}
	if got := <-recibido; got != "abc123def456" {
		t.Fatalf("el nodo recibió req_id %q", got)
	}
}
