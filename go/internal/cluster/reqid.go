package cluster

import "context"

type reqIDKey struct{}

// WithRequestID guarda en el contexto el X-Request-ID de la petición HTTP.
// El coordinador lo envía a los nodos en PREDICT para que los logs de la API
// y de cada nodo se puedan unir por el mismo identificador.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, reqIDKey{}, id)
}

// RequestID devuelve el identificador guardado con WithRequestID, o "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(reqIDKey{}).(string)
	return id
}
