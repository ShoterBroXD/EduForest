package api

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ShoterBroXD/EduForest/internal/cluster"
	"github.com/ShoterBroXD/EduForest/internal/dataset"
	"github.com/ShoterBroXD/EduForest/internal/store"
)

// syntheticDataset arma un Dataset chico con las 15 variables: 2023 para
// entrenar y 2024 para probar, repartido en 40 colegios.
func syntheticDataset(n int) *dataset.Dataset {
	rng := rand.New(rand.NewSource(1))
	nf := len(dataset.FeatureNames)
	d := &dataset.Dataset{FeatureNames: dataset.FeatureNames}
	for i := 0; i < n; i++ {
		x := make([]float64, nf)
		for f := range x {
			x[f] = rng.Float64()
		}
		year := 2023
		if i%4 == 0 {
			year = 2024
		}
		x[0], x[2], x[4] = float64(year), float64(rng.Intn(3)), float64(5+rng.Intn(30))
		riesgo := 0
		switch {
		case x[14] > 0.9:
			riesgo = 2
		case x[10] > 0.6:
			riesgo = 1
		}
		d.X = append(d.X, x)
		d.Riesgo = append(d.Riesgo, riesgo)
		d.Logro = append(d.Logro, riesgo)
		d.Year = append(d.Year, year)
		d.CodMod = append(d.CodMod, 1000+i%40)
		d.Anexo = append(d.Anexo, 0)
	}
	return d
}

type testEnv struct {
	srv   *Server
	http  *httptest.Server
	admin string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	var nodes []string
	for i := 0; i < 3; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ln.Close() })
		go cluster.NewNode("nodo-test", 2).Serve(ln)
		nodes = append(nodes, ln.Addr().String())
	}
	mem := store.NewMemory()
	srv := New(Config{
		JWTSecret: []byte("test"), AdminUser: "admin", AdminPass: "admin123",
		MonitorInterval: 200 * time.Millisecond, Workers: 2,
	}, mem, store.NewMemoryCache(), cluster.NewCoordinator(nodes), syntheticDataset(4000))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	srv.Start(ctx)
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	env := &testEnv{srv: srv, http: hs}
	env.admin = env.login(t, "admin", "admin123")
	return env
}

func (e *testEnv) do(t *testing.T, method, path, token string, body any) (*http.Response, []byte) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, e.http.URL+path, &buf)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out bytes.Buffer
	out.ReadFrom(resp.Body)
	return resp, out.Bytes()
}

func (e *testEnv) login(t *testing.T, user, pass string) string {
	resp, body := e.do(t, "POST", "/api/auth/login", "", map[string]string{"usuario": user, "clave": pass})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login %s: %d %s", user, resp.StatusCode, body)
	}
	var tok struct{ Token string }
	json.Unmarshal(body, &tok)
	return tok.Token
}

func (e *testEnv) train(t *testing.T, body map[string]any) store.Entrenamiento {
	t.Helper()
	resp, raw := e.do(t, "POST", "/api/entrenamientos", e.admin, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("entrenar: %d %s", resp.StatusCode, raw)
	}
	var started struct{ ID string }
	json.Unmarshal(raw, &started)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, raw := e.do(t, "GET", "/api/entrenamientos/"+started.ID, e.admin, nil)
		var en store.Entrenamiento
		json.Unmarshal(raw, &en)
		if en.Estado != "en_curso" {
			return en
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("el entrenamiento no terminó a tiempo")
	return store.Entrenamiento{}
}

func TestAutenticacion(t *testing.T) {
	e := newTestEnv(t)
	if resp, _ := e.do(t, "POST", "/api/auth/login", "", map[string]string{"usuario": "admin", "clave": "mala"}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("clave incorrecta: se esperaba 401, hubo %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "GET", "/api/cluster", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("sin token: se esperaba 401, hubo %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "GET", "/api/cluster", "token-falso", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("token falso: se esperaba 401, hubo %d", resp.StatusCode)
	}
	resp, _ := e.do(t, "POST", "/api/usuarios", e.admin, map[string]string{"usuario": "angel", "clave": "clave123"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("crear usuario: %d", resp.StatusCode)
	}
	analista := e.login(t, "angel", "clave123")
	if resp, _ := e.do(t, "POST", "/api/entrenamientos", analista, nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("analista entrenando: se esperaba 403, hubo %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "GET", "/api/cluster", analista, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("analista viendo el clúster: %d", resp.StatusCode)
	}
}

func TestEntrenarPredecirYColegios(t *testing.T) {
	e := newTestEnv(t)

	ws, _, err := websocket.DefaultDialer.Dial(strings.Replace(e.http.URL, "http", "ws", 1)+"/ws?token="+e.admin, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	progreso := make(chan int, 1)
	go func() {
		n := 0
		for {
			var m map[string]any
			if ws.ReadJSON(&m) != nil {
				return
			}
			if m["tipo"] == "progreso" {
				n++
			}
			if m["tipo"] == "entrenamiento_fin" {
				progreso <- n
				return
			}
		}
	}()

	en := e.train(t, map[string]any{"objetivo": "riesgo", "modo": "sharding", "arboles": 9, "profundidad": 6})
	if en.Estado != "completado" {
		t.Fatalf("estado %s: %s", en.Estado, en.Error)
	}
	if len(en.Nodos) != 3 || en.Metricas == nil || en.Balance != 0.5 {
		t.Fatalf("entrenamiento incompleto: %+v", en)
	}
	select {
	case n := <-progreso:
		if n != 9 {
			t.Fatalf("el WebSocket recibió %d avisos de progreso, se esperaban 9", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no llegó entrenamiento_fin por el WebSocket")
	}

	// Predicción: la primera la votan los nodos, la segunda sale de la caché.
	x := map[string]float64{}
	for _, n := range dataset.FeatureNames {
		x[n] = 0.5
	}
	for i, want := range []string{"cluster", "cache"} {
		start := time.Now()
		resp, raw := e.do(t, "POST", "/api/predicciones", e.admin, map[string]any{"variables": x})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("predicción %d: %d %s", i, resp.StatusCode, raw)
		}
		var p predecirResp
		json.Unmarshal(raw, &p)
		if p.Fuente != want {
			t.Fatalf("predicción %d: fuente %s, se esperaba %s", i, p.Fuente, want)
		}
		if d := time.Since(start); d > 100*time.Millisecond {
			t.Fatalf("la predicción tardó %v (máximo 100 ms)", d)
		}
	}
	if resp, _ := e.do(t, "POST", "/api/predicciones", e.admin, map[string]any{"valores": []float64{1, 2}}); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("valores incompletos: se esperaba 400, hubo %d", resp.StatusCode)
	}

	// Ranking y consulta por colegio, con caché.
	_, raw := e.do(t, "GET", "/api/colegios?limite=5", e.admin, nil)
	var ranking []store.Colegio
	json.Unmarshal(raw, &ranking)
	if len(ranking) != 5 {
		t.Fatalf("ranking con %d colegios", len(ranking))
	}
	for i := 1; i < len(ranking); i++ {
		if ranking[i].AltaEsperados > ranking[i-1].AltaEsperados {
			t.Fatal("el ranking no está ordenado")
		}
	}
	// El ranking de 10 se precalculó en Redis al terminar el entrenamiento.
	if resp, _ := e.do(t, "GET", "/api/colegios?limite=10", e.admin, nil); resp.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("el ranking de 10 debería venir precalculado, X-Cache=%s", resp.Header.Get("X-Cache"))
	}
	path := "/api/colegios/" + itoa(ranking[0].CodMod)
	for _, want := range []string{"MISS", "HIT"} {
		resp, _ := e.do(t, "GET", path, e.admin, nil)
		if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Cache") != want {
			t.Fatalf("colegio: %d X-Cache=%s, se esperaba %s", resp.StatusCode, resp.Header.Get("X-Cache"), want)
		}
	}
	if resp, _ := e.do(t, "GET", "/api/colegios/99999999", e.admin, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("colegio inexistente: se esperaba 404, hubo %d", resp.StatusCode)
	}
}

func TestUnEntrenamientoALaVez(t *testing.T) {
	e := newTestEnv(t)
	e.srv.training.Store(true)
	if resp, _ := e.do(t, "POST", "/api/entrenamientos", e.admin, nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("se esperaba 409, hubo %d", resp.StatusCode)
	}
	e.srv.training.Store(false)
	if resp, _ := e.do(t, "POST", "/api/entrenamientos", e.admin, map[string]any{"modo": "otro"}); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("modo inválido: se esperaba 400, hubo %d", resp.StatusCode)
	}
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }
