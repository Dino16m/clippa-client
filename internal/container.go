package internal

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"time"

	"github.com/dino16m/clippa-client/internal/clip"
	"github.com/dino16m/clippa-client/internal/controllers"
	"github.com/dino16m/clippa-client/internal/party"
	"github.com/dino16m/clippa-client/internal/server"
	"github.com/sirupsen/logrus"
)

type Container struct {
	ClipboardManager           *clip.ClipboardManager
	LocalPartyController       *controllers.LocalPartyController
	PartyClient                *party.PartyClient
	GlobalPartyManagerProvider *party.GlobalPartyManagerProvider
	LocalPartyHost             *party.LocalPartyHost
	LocalServerProvider        *server.LocalServerProvider
	LocalPartyManagerProvider  *party.LocalPartyManagerProvider
}

func ProvideContainer(logger *logrus.Logger, baseUrl url.URL, partyId string, memberId string, partySecret string, PartyTLS *party.PartyTLS, networkInterfaces []string) *Container {
	clipboardManager := clip.NewClipboardManager(logger)

	httpClient := &http.Client{
		Timeout: time.Duration(5) * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	localPartyHost := party.NewLocalPartyHost(logger, clipboardManager)
	localPartyCtrl := controllers.NewLocalPartyController(&baseUrl, httpClient, logger, localPartyHost, partyId)
	mux := http.NewServeMux()
	localPartyCtrl.RegisterRoutes(mux)
	localServerProvider := server.NewLocalServerProvider(mux, logger)

	globalPartyManagerProvider := party.NewGlobalPartyManagerProvider(logger, localServerProvider, func() []string { return networkInterfaces }, clipboardManager)
	localPartyManagerProvider := party.NewLocalPartyManagerProvider(logger, clipboardManager)
	partyClient := party.NewPartyClient(
		httpClient,
		&baseUrl,
		logger,
		partyId,
		partySecret,
		globalPartyManagerProvider,
		localPartyManagerProvider,
		memberId,
		localPartyHost, PartyTLS)

	return &Container{
		ClipboardManager:           clipboardManager,
		LocalPartyController:       localPartyCtrl,
		PartyClient:                partyClient,
		GlobalPartyManagerProvider: globalPartyManagerProvider,
		LocalPartyHost:             localPartyHost,
		LocalServerProvider:        localServerProvider,
		LocalPartyManagerProvider:  localPartyManagerProvider,
	}
}
