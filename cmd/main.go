package cmd

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/url"
	"os"

	"github.com/dino16m/clippa-client/internal"
	"github.com/dino16m/clippa-client/internal/manager"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)


func configure() {
	viper.AutomaticEnv()
	viper.SetDefault("Logger.Level", "info")
}

func loadTLSConfig(tlsCert, tlsKey string) (manager.PartyTLS, error){
	 certPemFile, err := os.Open(tlsCert)
	if err != nil {
		return manager.PartyTLS{}, err
	}
	certPem := []byte{}
	_, err = certPemFile.Read(certPem)
	certPemFile.Close()
	if err != nil {
		return manager.PartyTLS{}, err
	}

	keyPemFile, err := os.Open(tlsKey)

	if err != nil {
		return manager.PartyTLS{}, err
	}
	keyPem := []byte{}
	_, err = keyPemFile.Read(keyPem)
	keyPemFile.Close()
	if err != nil {
		return manager.PartyTLS{}, err
	}

	certBlock, _ := pem.Decode(certPem)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" {
		return manager.PartyTLS{}, errors.New("failed to decode PEM block containing certficate")
	}
	certificate, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return manager.PartyTLS{}, err
	}

	keyBlock, _ := pem.Decode(keyPem)
	if keyBlock == nil || keyBlock.Type != "EC PRIVATE KEY" {
		return manager.PartyTLS{}, errors.New("failed to decode PEM block containing key")
	}
	privateKey, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return manager.PartyTLS{}, err
	}

	return manager.PartyTLS{
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
	tlsCert := getRequiredVar("TLS_CERT")
	tlsKey := getRequiredVar("TLS_KEY")
	baseURL := getRequiredVar("BASE_URL")

	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}

	tlsConfig, err := loadTLSConfig(tlsCert, tlsKey)
	if err != nil {
		panic(err)
	}

	networkInterfaces := viper.GetStringSlice("NETWORK_INTERFACES")

	container := internal.ProvideContainer(
		logrus.New(),
		*parsedURL,
		partyId,
		memberId,
		partySecret,
		&tlsConfig,
		networkInterfaces,
	)

	logrus.Info("Running party client")
	container.PartyClient.Join(context.Background())
}
