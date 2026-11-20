package store

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestMemoryRanking(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	m.GuardarColegios(ctx, "riesgo", []Colegio{
		{ID: "a", CodMod: 1, AlumnosAlta: 5},
		{ID: "b", CodMod: 2, AlumnosAlta: 50},
		{ID: "c", CodMod: 2, Anexo: 1, AlumnosAlta: 20},
	})
	r, _ := m.Ranking(ctx, "riesgo", 2)
	if len(r) != 2 || r[0].ID != "b" || r[1].ID != "c" {
		t.Fatalf("ranking incorrecto: %+v", r)
	}
	cs, _ := m.Colegios(ctx, "riesgo", 2)
	if len(cs) != 2 || cs[0].Anexo != 0 {
		t.Fatalf("colegios incorrectos: %+v", cs)
	}
	if _, err := m.Usuario(ctx, "nadie"); err != ErrNoEncontrado {
		t.Fatalf("se esperaba ErrNoEncontrado, hubo %v", err)
	}
}

func testCache(t *testing.T, c Cache) {
	ctx := context.Background()
	c.Set(ctx, "eduforest:test", []byte("hola"), time.Second)
	if v, ok := c.Get(ctx, "eduforest:test"); !ok || string(v) != "hola" {
		t.Fatalf("Get devolvió %q %v", v, ok)
	}
	if _, ok := c.Get(ctx, "eduforest:no-existe"); ok {
		t.Fatal("una clave inexistente no debería estar")
	}
	c.Incr(ctx, "eduforest:test-contadores", "GET /x")
	c.Incr(ctx, "eduforest:test-contadores", "GET /x")
	if n := c.Counters(ctx, "eduforest:test-contadores")["GET /x"]; n < 2 {
		t.Fatalf("contador = %d", n)
	}
}

func TestMemoryCache(t *testing.T) { testCache(t, NewMemoryCache()) }

// Corre solo si hay un Redis disponible: REDIS_TEST_ADDR=localhost:6379 go test ./internal/store
func TestRedis(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR no definido")
	}
	r, err := NewRedis(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	testCache(t, r)
}
