package api

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Políticas de seguridad de la API (TB2):
//   - CORS: solo los orígenes de CORS_ORIGIN pueden llamar a la API desde un navegador.
//   - Cabeceras: el navegador no adivina tipos, no muestra la API dentro de un iframe
//     y no guarda en caché respuestas con datos privados.
//   - Límite de intentos de login por IP, para frenar ataques de fuerza bruta.
//   - Tamaño máximo del cuerpo de cada petición.
//   - Un identificador por petición (X-Request-ID) para seguirla en los logs.

const maxBody = 1 << 20 // 1 MB

func (s *Server) allowedOrigins() []string {
	var out []string
	for _, o := range strings.Split(s.cfg.CORSOrigin, ",") {
		if o = strings.TrimSpace(o); o != "" {
			out = append(out, o)
		}
	}
	return out
}

// originAllowed dice si un navegador con ese Origin puede usar la API. Sin
// lista configurada, o con "*", se acepta cualquiera (modo desarrollo).
func (s *Server) originAllowed(origin string) bool {
	list := s.allowedOrigins()
	if len(list) == 0 {
		return true
	}
	for _, o := range list {
		if o == "*" || o == origin {
			return true
		}
	}
	return false
}

func (s *Server) applySecurity(w http.ResponseWriter, r *http.Request) (reqID string) {
	h := w.Header()
	if origin := r.Header.Get("Origin"); origin != "" && s.originAllowed(origin) {
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-ID")
		h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		h.Set("Access-Control-Expose-Headers", "X-Request-ID, X-Cache")
		h.Add("Vary", "Origin")
	}
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
	if strings.HasPrefix(r.URL.Path, "/api/") {
		h.Set("Cache-Control", "no-store")
	}
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	}
	reqID = r.Header.Get("X-Request-ID")
	if !validReqID(reqID) {
		b := make([]byte, 6)
		rand.Read(b)
		reqID = hex.EncodeToString(b)
	}
	h.Set("X-Request-ID", reqID)
	return reqID
}

func validReqID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// clientIP usa la última dirección de X-Forwarded-For, que es la que agregó
// el servidor web propio al reenviar la petición; si no viene, usa la del socket.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[len(parts)-1])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// rateLimiter permite como máximo `limit` intentos por IP en cada ventana.
type rateLimiter struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	hits   map[string]*ventana
}

type ventana struct {
	inicio time.Time
	n      int
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{limit: limit, window: window, hits: map[string]*ventana{}}
}

// allow registra un intento y dice si se acepta; si no, cuántos segundos faltan.
func (l *rateLimiter) allow(ip string) (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	v := l.hits[ip]
	if v == nil || now.Sub(v.inicio) >= l.window {
		v = &ventana{inicio: now}
		l.hits[ip] = v
	}
	if len(l.hits) > 10000 { // limpieza simple para que el mapa no crezca sin fin
		for k, x := range l.hits {
			if now.Sub(x.inicio) >= l.window {
				delete(l.hits, k)
			}
		}
	}
	if v.n >= l.limit {
		return false, int(l.window.Seconds()-now.Sub(v.inicio).Seconds()) + 1
	}
	v.n++
	return true, 0
}

// limitLogin aplica el límite de intentos antes del handler de login.
func (s *Server) limitLogin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ok, wait := s.loginLimiter.allow(clientIP(r)); !ok {
			w.Header().Set("Retry-After", strconv.Itoa(wait))
			writeError(w, http.StatusTooManyRequests, "demasiados intentos de inicio de sesión, espera "+strconv.Itoa(wait)+" s")
			return
		}
		next(w, r)
	}
}
