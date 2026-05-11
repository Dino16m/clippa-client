package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/dino16m/clippa-client/internal"
	"github.com/dino16m/clippa-client/internal/party"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)


func configure() {
	viper.AddConfigPath(".")
	viper.SetConfigFile(".env")
	err := viper.ReadInConfig()
	viper.AutomaticEnv()

	// Handle errors
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	viper.SetDefault("Logger.Level", "info")
}

func loadTLSConfig() (*party.PartyTLS, error){
	tlsCert := viper.GetString("TLS_CERT")
	tlsKey := viper.GetString("TLS_KEY")

	if tlsCert == "" &&  tlsKey == "" {
		return nil, nil
	}

	if tlsCert == "" || tlsKey == "" {
		return nil, errors.New("TLS_CERT and TLS_KEY must be set")
	}


	keyPem, err := os.ReadFile(tlsKey)
	if err != nil {
		return nil, err
	}
	certPem, err := os.ReadFile(tlsCert)
	if err != nil {
		return &party.PartyTLS{}, err
	}

	certBlock, _ := pem.Decode(certPem)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" {
		return &party.PartyTLS{}, errors.New("failed to decode PEM block containing certficate")
	}
	certificate, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return &party.PartyTLS{}, err
	}

	keyBlock, _ := pem.Decode(keyPem)
	if keyBlock == nil || keyBlock.Type != "EC PRIVATE KEY" {
		return &party.PartyTLS{}, errors.New("failed to decode PEM block containing key")
	}
	privateKey, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return &party.PartyTLS{}, err
	}

	return &party.PartyTLS{
		Certificate: certificate,
		PrivateKey:  privateKey,
	}, nil
}

func getRequiredVar(key string) string {
	value := viper.GetString(key)
	if value == "" {
		panic(key + " is required")
	}
	return value
}

func main() {
	configure()

	partyId := getRequiredVar("PARTY_ID")
	memberId := getRequiredVar("MEMBER_ID")
	partySecret := getRequiredVar("PARTY_SECRET")
	baseURL := getRequiredVar("BASE_URL")

	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}

	tlsConfig, err := loadTLSConfig()
	if err != nil {
		panic(err)
	}

	networkInterfaceString := viper.GetString("NETWORK_INTERFACES")

	rawInterfaces := strings.Split(networkInterfaceString, ",")
	networkInterfaces := []string{}
	for _, iface := range rawInterfaces {
		trimmed := strings.TrimSpace(iface)
		if trimmed == "" {
			continue
		}
		networkInterfaces = append(networkInterfaces, trimmed)
	}

	logger := logrus.New()

	container := internal.ProvideContainer(
		logger,
		*parsedURL,
		partyId,
		memberId,
		partySecret,
		tlsConfig,
		networkInterfaces,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go container.ClipboardManager.Listen(ctx)
	logger.Info("Running party client")
	logger.Fatal(container.PartyClient.Join(ctx))
}
