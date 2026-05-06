package manager

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
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

type LocalServerProvider struct {
	servers map[string]*LocalServer
	mux     *http.ServeMux
	logger logrus.FieldLogger
}

func NewLocalServerProvider(mux *http.ServeMux, logger logrus.FieldLogger) *LocalServerProvider {
	return &LocalServerProvider{
		servers: make(map[string]*LocalServer),
		mux:     mux,
		logger: logger,
	}
}
func (s *LocalServerProvider) ProvideLocalServer( partyId string, tlsConfig *PartyTLS, ctx context.Context) (*LocalServer, error) {
	localServer, ok := s.servers[partyId]

	if ok {
		return localServer, nil
	}

	serverTls  := buildTLSConfig(tlsConfig)
	serverTls.ClientAuth = tls.RequireAndVerifyClientCert
	serverTls.ClientCAs = serverTls.RootCAs
	listener, err := net.Listen("tcp", "0.0.0.0:0")

	if err != nil {
		return nil, err
	}
	serverCtx, cancelFunc := context.WithCancel(ctx)

	server := &http.Server{
		Addr: listener.Addr().String(),
		TLSConfig: serverTls,
		Handler: s.mux,
		BaseContext: func(l net.Listener) context.Context {
			return context.WithValue(serverCtx, "serverRequestId", uuid.NewString())
		},
	}

	go func() {
		s.logger.Info("Running local TLS server")
		if err := server.ServeTLS(listener, tlsConfig.CertFile, tlsConfig.KeyFile); err != nil {
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
