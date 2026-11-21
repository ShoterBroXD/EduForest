package api

import (
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/ShoterBroXD/EduForest/internal/store"
)

const (
	RolAdmin    = "admin"
	RolAnalista = "analista"
)

type ctxKey struct{}

type Claims struct {
	Rol string `json:"rol"`
	jwt.RegisteredClaims
}

func hashPassword(pass string, salt []byte) (string, error) {
	key, err := pbkdf2.Key(sha256.New, pass, salt, 100_000, 32)
	return hex.EncodeToString(key), err
}

func newUser(nombre, pass, rol string) (*store.Usuario, error) {
	salt := make([]byte, 16)
	rand.Read(salt)
	h, err := hashPassword(pass, salt)
	if err != nil {
		return nil, err
	}
	return &store.Usuario{Nombre: nombre, Hash: h, Sal: hex.EncodeToString(salt), Rol: rol, Creado: time.Now()}, nil
}

func checkPassword(u *store.Usuario, pass string) bool {
	salt, err := hex.DecodeString(u.Sal)
	if err != nil {
		return false
	}
	h, err := hashPassword(pass, salt)
	return err == nil && subtle.ConstantTimeCompare([]byte(h), []byte(u.Hash)) == 1
}

func (s *Server) seedAdmin(ctx context.Context) error {
	if s.cfg.AdminUser == "" {
		return nil
	}
	if _, err := s.store.Usuario(ctx, s.cfg.AdminUser); err == nil {
		return nil
	}
	u, err := newUser(s.cfg.AdminUser, s.cfg.AdminPass, RolAdmin)
	if err != nil {
		return err
	}
	return s.store.GuardarUsuario(ctx, u)
}

func (s *Server) newToken(u *store.Usuario) (string, time.Time, error) {
	exp := time.Now().Add(s.cfg.TokenTTL)
	claims := Claims{Rol: u.Rol, RegisteredClaims: jwt.RegisteredClaims{
		Subject:   u.Nombre,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(exp),
		Issuer:    "eduforest",
	}}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.cfg.JWTSecret)
	return tok, exp, err
}

func (s *Server) parseToken(raw string) (*Claims, error) {
	var c Claims
	_, err := jwt.ParseWithClaims(raw, &c, func(t *jwt.Token) (any, error) {
		return s.cfg.JWTSecret, nil
	}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithIssuer("eduforest"))
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// auth exige un token válido y, si rol no está vacío, ese rol.
func (s *Server) auth(next http.HandlerFunc, rol string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			writeError(w, http.StatusUnauthorized, "falta el token: usa Authorization: Bearer <token>")
			return
		}
		c, err := s.parseToken(raw)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "token inválido o vencido")
			return
		}
		if rol != "" && c.Rol != rol {
			writeError(w, http.StatusForbidden, "se requiere el rol "+rol)
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, c)))
	}
}

func claimsFrom(r *http.Request) *Claims {
	c, _ := r.Context().Value(ctxKey{}).(*Claims)
	return c
}

type loginReq struct {
	Usuario string `json:"usuario"`
	Clave   string `json:"clave"`
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Usuario == "" {
		writeError(w, http.StatusBadRequest, "envía {\"usuario\": ..., \"clave\": ...}")
		return
	}
	u, err := s.store.Usuario(r.Context(), req.Usuario)
	if err != nil || !checkPassword(u, req.Clave) {
		writeError(w, http.StatusUnauthorized, "usuario o clave incorrectos")
		return
	}
	tok, exp, err := s.newToken(u)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"token": tok, "expira": exp, "usuario": u.Nombre, "rol": u.Rol})
}

func (s *Server) handleYo(w http.ResponseWriter, r *http.Request) {
	c := claimsFrom(r)
	writeJSON(w, http.StatusOK, map[string]any{"usuario": c.Subject, "rol": c.Rol, "expira": c.ExpiresAt.Time})
}

type crearUsuarioReq struct {
	Usuario string `json:"usuario"`
	Clave   string `json:"clave"`
	Rol     string `json:"rol"`
}

func (s *Server) handleCrearUsuario(w http.ResponseWriter, r *http.Request) {
	var req crearUsuarioReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "JSON inválido")
		return
	}
	if req.Rol == "" {
		req.Rol = RolAnalista
	}
	if err := validarUsuario(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := s.store.Usuario(r.Context(), req.Usuario); err == nil {
		writeError(w, http.StatusConflict, "el usuario ya existe")
		return
	}
	u, err := newUser(req.Usuario, req.Clave, req.Rol)
	if err == nil {
		err = s.store.GuardarUsuario(r.Context(), u)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, u)
}

func validarUsuario(req crearUsuarioReq) error {
	switch {
	case len(req.Usuario) < 3:
		return errors.New("el usuario debe tener al menos 3 caracteres")
	case len(req.Clave) < 6:
		return errors.New("la clave debe tener al menos 6 caracteres")
	case req.Rol != RolAdmin && req.Rol != RolAnalista:
		return errors.New("rol inválido: usa admin o analista")
	}
	return nil
}
