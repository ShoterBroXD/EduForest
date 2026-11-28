package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Hub difunde eventos (progreso, estado, logs) a los clientes WebSocket.
// Solo Run toca el mapa de clientes, por eso no hay mutex.
type Hub struct {
	register   chan *wsClient
	unregister chan *wsClient
	broadcast  chan []byte
}

type wsClient struct {
	conn *websocket.Conn
	send chan []byte
}

func NewHub() *Hub {
	return &Hub{
		register:   make(chan *wsClient),
		unregister: make(chan *wsClient),
		broadcast:  make(chan []byte, 256),
	}
}

func (h *Hub) Run(ctx context.Context) {
	clients := map[*wsClient]bool{}
	for {
		select {
		case c := <-h.register:
			clients[c] = true
		case c := <-h.unregister:
			if clients[c] {
				delete(clients, c)
				close(c.send)
			}
		case msg := <-h.broadcast:
			for c := range clients {
				select {
				case c.send <- msg:
				default: // cliente lento: se desconecta para no frenar a los demás
					delete(clients, c)
					close(c.send)
				}
			}
		case <-ctx.Done():
			for c := range clients {
				close(c.send)
			}
			return
		}
	}
}

// Broadcast no bloquea: si el canal está lleno, el evento se descarta.
func (h *Hub) Broadcast(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	select {
	case h.broadcast <- b:
	default:
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
}

// handleWS valida el token (en el parámetro ?token=, porque el navegador no
// puede mandar cabeceras al abrir un WebSocket) y registra al cliente.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	c, err := s.parseToken(r.URL.Query().Get("token"))
	if err != nil {
		writeError(w, http.StatusUnauthorized, "token inválido: conecta a /ws?token=<jwt>")
		return
	}
	// Solo los orígenes permitidos por CORS pueden abrir el WebSocket desde un
	// navegador. Clientes sin Origin (Postman, pruebas) entran con su token.
	up := upgrader
	up.CheckOrigin = func(r *http.Request) bool {
		o := r.Header.Get("Origin")
		return o == "" || s.originAllowed(o)
	}
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ws] upgrade falló: %v", err)
		return
	}
	client := &wsClient{conn: conn, send: make(chan []byte, 64)}
	s.hub.register <- client
	log.Printf("[ws] %s conectado", c.Subject)

	hello, _ := json.Marshal(map[string]any{"tipo": "bienvenida", "usuario": c.Subject, "modelos": s.modelList()})
	client.send <- hello

	go client.writePump()
	client.readPump(s.hub)
}

// writePump es la única goroutine que escribe en la conexión.
func (c *wsClient) writePump() {
	ping := time.NewTicker(30 * time.Second)
	defer func() {
		ping.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, nil)
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ping.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// readPump solo detecta cuándo el cliente se va (los clientes no envían datos).
func (c *wsClient) readPump(h *Hub) {
	defer func() { h.unregister <- c }()
	c.conn.SetReadLimit(4096)
	c.conn.SetReadDeadline(time.Now().Add(70 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(70 * time.Second))
		return nil
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}
