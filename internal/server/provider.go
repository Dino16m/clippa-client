package server

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type LocalServerProvider struct {
	servers map[string]*LocalServer
	mux     *http.ServeMux
	logger  logrus.FieldLogger
	mutex   *sync.RWMutex
}

func NewLocalServerProvider(mux *http.ServeMux, logger logrus.FieldLogger) *LocalServerProvider {
	return &LocalServerProvider{
		servers: make(map[string]*LocalServer),
		mux:     mux,
		logger:  logger,
	}
}
func (s *LocalServerProvider) ProvideLocalServer(partyId string, serverTls *tls.Config, ctx context.Context) (*LocalServer, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	localServer, ok := s.servers[partyId]

	if ok {
		return localServer, nil
	}

	serverTls.ClientAuth = tls.RequireAndVerifyClientCert
	serverTls.ClientCAs = serverTls.RootCAs
	listener, err := net.Listen("tcp", "0.0.0.0:0")

	if err != nil {
		return nil, err
	}
	serverCtx, cancelFunc := context.WithCancel(ctx)

	server := &http.Server{
		Addr:      listener.Addr().String(),
		TLSConfig: serverTls,
		Handler:   s.mux,
		BaseContext: func(l net.Listener) context.Context {
			return context.WithValue(serverCtx, "serverRequestId", uuid.NewString())
		},
	}

	go func() {
		s.logger.Info("Running local TLS server")
		if err := server.ServeTLS(listener, "", ""); err != nil {
			cancelFunc()
		}
	}()
	s.servers[partyId] = &LocalServer{
		server: server,
		cleanup: func() {
			s.mutex.Lock()
			delete(s.servers, partyId)
			s.mutex.Unlock()
			cancelFunc()
		},
	}

	return s.servers[partyId], nil
}
