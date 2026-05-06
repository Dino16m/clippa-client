package internal

import (
	"net/http"
	"net/url"
	"time"

	"github.com/dino16m/clippa-client/internal/clip"
	"github.com/dino16m/clippa-client/internal/controllers"
	"github.com/dino16m/clippa-client/internal/manager"
	"github.com/sirupsen/logrus"
)

type Container struct {
	ClipboardManager           *clip.ClipboardManager
	LocalPartyController       *controllers.LocalPartyController
	PartyClient                *manager.PartyClient
	GlobalPartyManagerProvider *manager.GlobalPartyManagerProvider
	LocalPartyHost             *manager.LocalPartyHost
	LocalServerProvider        *manager.LocalServerProvider
	LocalPartyManagerProvider  *manager.LocalPartyManagerProvider
}

func ProvideContainer(logger *logrus.Logger, baseUrl url.URL, partyId string, memberId string, partySecret string, PartyTLS *manager.PartyTLS, networkInterfaces []string) *Container {
	clipboardManager := clip.NewClipboardManager(logger)

	httpClient := &http.Client{
		Timeout: time.Duration(5) * time.Second,
	}
	localPartyHost := manager.NewLocalPartyHost(logger, clipboardManager)
	localPartyCtrl := controllers.NewLocalPartyController(&baseUrl, httpClient, logger, localPartyHost, partyId)
	mux := http.NewServeMux()
	localPartyCtrl.RegisterRoutes(mux)
	localServerProvider := manager.NewLocalServerProvider(mux, logger)

	globalPartyManagerProvider := manager.NewGlobalPartyManagerProvider(logger, localServerProvider, func() []string { return networkInterfaces }, clipboardManager)
	localPartyManagerProvider := manager.NewLocalPartyManagerProvider(logger, clipboardManager)
	partyClient := manager.NewPartyClient(
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
