package manager

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

type LocalServer struct {
	server  *http.Server
	cancel  context.CancelFunc
	ctx     context.Context
	cleanup func()
}

func (l *LocalServer) Close() {
	l.server.Shutdown(context.Background())
	cancelFunc := l.cancel
	cancelFunc()
}

func (l *LocalServer) Port() string {
	parts := strings.Split(l.server.Addr, ":")
	return parts[len(parts)-1]
}

func (l *LocalServer) Context() context.Context {
	return l.ctx
}

type LocalServerProvider struct {
	servers map[string]*LocalServer
}

func (s *LocalServerProvider) ProvideLocalServer(partyId string, tlsConfig *PartyTLS, ctx context.Context) (*LocalServer, error) {
	localServer, ok := s.servers[partyId]

	if ok {
		return localServer, nil
	}

	certPool := x509.NewCertPool()
	certPool.AddCert(tlsConfig.Certificate)
	listener, err := net.Listen("tcp", "0.0.0.0:0")

	if err != nil {
		return nil, err
	}
	serverCtx, cancelFunc := context.WithCancel(ctx)
	server := &http.Server{
		Addr: listener.Addr().String(),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{
				{
					Leaf:       tlsConfig.Certificate,
					PrivateKey: tlsConfig.PrivateKey,
				},
			},
			ClientCAs:  certPool,
			ClientAuth: tls.RequireAndVerifyClientCert,
		},
		BaseContext: func(l net.Listener) context.Context {
			return context.WithValue(serverCtx, "serverRequestId", uuid.NewString())
		},
	}

	go func() {
		if err := server.Serve(listener); err != nil {
			cancelFunc()
		}
	}()
	s.servers[partyId] = &LocalServer{
		server: server,
		cancel: cancelFunc,
		ctx:    serverCtx,
		cleanup: func() {
			delete(s.servers, partyId)
		},
	}

	return s.servers[partyId], nil
}
