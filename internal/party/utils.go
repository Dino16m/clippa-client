package party

import (
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"net/http"
	"time"
)

func parseX509Cert(certPEM string) (*x509.Certificate, error) {
	certBytes, err := base64.StdEncoding.DecodeString(certPEM)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(certBytes)
	if block == nil {
		return nil, errors.New("Invalid certificate")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	return cert, nil
}

func parsePrivateKey(keyPEM string) (*ecdsa.PrivateKey, error) {
	keyBytes, err := base64.StdEncoding.DecodeString(keyPEM)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(keyBytes)
	if block == nil {
		return nil, errors.New("Invalid Key block")
	}

	privateKey, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func buildTLSConfig(config *PartyTLS) *tls.Config {

	tlsCert := tls.Certificate{
		PrivateKey:  config.PrivateKey,
		Leaf:        config.Certificate,
		Certificate: [][]byte{config.Certificate.Raw},
	}

	certPool := x509.NewCertPool()
	certPool.AddCert(config.Certificate)

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{tlsCert},
		InsecureSkipVerify: false,
		RootCAs:            certPool,
	}

	return tlsConfig
}

func provideHttpClient(config *PartyTLS) (*http.Client, error) {
	tlsConfig := buildTLSConfig(config)
	tlsConfig.InsecureSkipVerify = true
	tlsConfig.VerifyConnection = func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return errors.New("no peer certificates")
		}
		serverCert := cs.PeerCertificates[0]
		_, err := serverCert.Verify(x509.VerifyOptions{
			Roots: tlsConfig.RootCAs,
		})
		return err
	}
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
	}

	return client, nil
}
