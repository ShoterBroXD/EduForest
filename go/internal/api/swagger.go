package api

import (
	_ "embed"
	"net/http"
)

//go:embed openapi.yaml
var openapiSpec []byte

const swaggerHTML = `<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>EduForest API</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
<script>
window.ui = SwaggerUIBundle({ url: "/swagger/openapi.yaml", dom_id: "#swagger-ui", persistAuthorization: true });
</script>
</body>
</html>`

func (s *Server) handleSwagger(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/swagger/openapi.yaml" {
		w.Header().Set("Content-Type", "application/yaml")
		w.Write(openapiSpec)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(swaggerHTML))
}
