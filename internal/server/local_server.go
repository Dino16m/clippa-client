package server

import (
	"context"
	"net/http"
	"strings"
)

type LocalServer struct {
	server  *http.Server
	cleanup func()
}

func NewLocalServer(server *http.Server, cleanup func()) *LocalServer {
	return &LocalServer{server: server, cleanup: cleanup}
}

func (l *LocalServer) Close() {
	l.server.Shutdown(context.Background())
	l.cleanup()
}

func (l *LocalServer) Port() string {
	parts := strings.Split(l.server.Addr, ":")
	return parts[len(parts)-1]
}
