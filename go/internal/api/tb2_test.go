package api

import (
	"net/http"
	"testing"

	"github.com/ShoterBroXD/EduForest/internal/store"
)

func TestComputeImpact(t *testing.T) {
	// Tres colegios: el modelo pone primero al que de verdad tiene más casos.
	cs := []store.Colegio{
		{Gestion: "Privada", Nivel: "Primaria", Alumnos: 500, AlumnosPorClase: []int{500, 0, 0}, RealesPorClase: []int{499, 0, 1}},
		{Gestion: "Pública de gestión directa", Nivel: "Secundaria", Alumnos: 100, AlumnosAlta: 30, AltaEsperados: 28, AlumnosPorClase: []int{60, 10, 30}, RealesPorClase: []int{70, 10, 20}},
		{Gestion: "Pública de gestión directa", Nivel: "Secundaria", Alumnos: 50, AlumnosAlta: 5, AltaEsperados: 6, AlumnosPorClase: []int{40, 5, 5}, RealesPorClase: []int{45, 1, 4}},
	}
	imp := computeImpact(cs, "riesgo", "m1", "Riesgo alto")
	if imp.Alumnos != 650 || imp.AltaReales != 25 || imp.AltaPredichos != 35 || imp.ColegiosConAlta != 2 {
		t.Fatalf("totales incorrectos: %+v", imp)
	}
	if imp.PorGestion[0].Grupo != "Privada" || imp.PorGestion[1].AltaReales != 24 {
		t.Fatalf("grupos por gestión incorrectos: %+v", imp.PorGestion)
	}
	// Visitando 1 de 3 colegios (34%): el modelo alcanza 20 de 25 (80%),
	// el más grande solo 1 de 25 (4%).
	var p *store.PuntoCurva
	for i := range imp.Curva {
		if imp.Curva[i].Colegios == 1 {
			p = &imp.Curva[i]
			break
		}
	}
	if p == nil || p.Modelo != 80 || p.PorTamano != 4 {
		t.Fatalf("curva incorrecta: %+v", imp.Curva)
	}
	last := imp.Curva[len(imp.Curva)-1]
	if last.Modelo != 100 || last.Azar != 100 || last.PorTamano != 100 {
		t.Fatalf("al visitar todos los colegios se debe alcanzar el 100%%: %+v", last)
	}
}

func TestPoliticasDeSeguridad(t *testing.T) {
	e := newTestEnv(t)
	e.srv.cfg.CORSOrigin = "http://localhost:3000"

	req, _ := http.NewRequest("GET", e.http.URL+"/api/salud", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	resp, _ := http.DefaultClient.Do(req)
	if resp.Header.Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatal("el origen del frontend debería estar permitido")
	}
	if resp.Header.Get("X-Request-ID") == "" || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("faltan cabeceras de seguridad")
	}
	req.Header.Set("Origin", "http://sitio-malicioso.com")
	resp, _ = http.DefaultClient.Do(req)
	if resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("un origen no permitido no debería recibir Access-Control-Allow-Origin")
	}

	// Límite de intentos: 10 por minuto por IP (ya se usó 1 al crear el entorno).
	var last int
	for i := 0; i < 12; i++ {
		resp, _ := e.do(t, "POST", "/api/auth/login", "", map[string]string{"usuario": "admin", "clave": "mala"})
		last = resp.StatusCode
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("se esperaba 429 al superar el límite, hubo %d", last)
	}
}

func TestBuscarEImpactoPorAPI(t *testing.T) {
	e := newTestEnv(t)
	e.train(t, map[string]any{"arboles": 6, "profundidad": 5})
	if resp, _ := e.do(t, "GET", "/api/impacto", e.admin, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("impacto: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "GET", "/api/colegios/buscar?q=ab", e.admin, nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("búsqueda corta: se esperaba 400, hubo %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "GET", "/api/colegios/buscar?q=1005", e.admin, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("búsqueda por código: %d", resp.StatusCode)
	}
	resp, body := e.do(t, "GET", "/api/variables", e.admin, nil)
	if resp.StatusCode != http.StatusOK || len(body) < 100 {
		t.Fatalf("variables: %d %s", resp.StatusCode, body)
	}
}
