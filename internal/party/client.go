package party

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/coder/websocket"
	"github.com/sirupsen/logrus"
)

type PartyClient struct {
	client                     *http.Client
	baseUrl                    *url.URL
	baseWsUrl                  *url.URL
	logger                     *logrus.Logger
	partyId                    string
	partySecret                string
	globalPartyManagerProvider *GlobalPartyManagerProvider
	localPartyManagerProvider  *LocalPartyManagerProvider
	partyHost                  *LocalPartyHost
	memberId                   string
	partyTls                   *PartyTLS
	httpClientProvider         func(*PartyTLS) (*http.Client, error)
}

type PartyManager interface {
	HandleMessage(buf []byte) error
	Outbox() <-chan []byte
	CheckIn()
	Done() <-chan struct{}
}

func NewPartyClient(
	client *http.Client,
	baseUrl *url.URL,
	logger *logrus.Logger,
	partyId,
	partySecret string,
	globalPartyManagerProvider *GlobalPartyManagerProvider,
	localPartyManagerProvider *LocalPartyManagerProvider,
	memberId string,
	partyHost *LocalPartyHost,
	partyTls *PartyTLS,
) *PartyClient {
	baseWsUrl := &url.URL{Scheme: "ws", Host: baseUrl.Host}
	if baseUrl.Scheme == "https" {
		baseWsUrl.Scheme = "wss"
	}
	return &PartyClient{
		client:                     client,
		baseUrl:                    baseUrl,
		logger:                     logger,
		partyId:                    partyId,
		partySecret:                partySecret,
		baseWsUrl:                  baseWsUrl,
		globalPartyManagerProvider: globalPartyManagerProvider,
		localPartyManagerProvider:  localPartyManagerProvider,
		memberId:                   memberId,
		partyHost:                  partyHost,
		partyTls:                   partyTls,
		httpClientProvider:         provideHttpClient,
	}
}

func (c *PartyClient) resolveTLS(party Party) (*PartyTLS, error) {
	if c.partyTls != nil {
		return c.partyTls, nil
	}
	return party.TLSConfig()
}

func (c *PartyClient) getParty() (Party, error) {
	requestUrl := c.baseUrl.JoinPath("/api/parties/")

	query := requestUrl.Query()
	query.Set("id", c.partyId)
	requestUrl.RawQuery = query.Encode()

	req, err := http.NewRequest(http.MethodGet, requestUrl.String(), nil)
	if err != nil {
		return Party{}, err
	}
	req.Header.Set("X-Secret", c.partySecret)

	resp, err := c.client.Do(req)
	if err != nil {
		return Party{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Party{}, err
	}

	var party Party
	if err := json.NewDecoder(resp.Body).Decode(&party); err != nil {
		return Party{}, err
	}
	return party, nil
}

func (c *PartyClient) getAuth() (string, error) {
	requestUrl := c.baseUrl.JoinPath("/api/parties/auth")

	requestBody := map[string]any{
		"id":     c.partyId,
		"secret": c.partySecret,
	}
	requestJson, err := json.Marshal(requestBody)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest(http.MethodPost, requestUrl.String(), bytes.NewReader(requestJson))
	if err != nil {
		return "", err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", err
	}

	var auth struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&auth); err != nil {
		return "", err
	}
	return auth.Token, nil
}

func (c *PartyClient) runEavesdropper(ctx context.Context, eavesDropper *LocalPartyManager) error {
	connectCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	token, err := c.getAuth()
	if err != nil {
		return err
	}
	wsUrl := c.baseWsUrl.JoinPath("/api/parties/join")
	query := wsUrl.Query()
	query.Set("id", c.partyId)
	query.Set("token", token)
	query.Set("memberId", c.memberId)
	wsUrl.RawQuery = query.Encode()

	wsClient, _, err := websocket.Dial(connectCtx, wsUrl.String(), nil)
	if err != nil {
		return err
	}
	defer wsClient.CloseNow()

	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wsChannel := make(chan []byte)

	go func(ctx context.Context, conn *websocket.Conn, outbox chan<- []byte, cancel context.CancelFunc) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_, msg, err := conn.Read(ctx)
				if err != nil {
					cancel()
					return
				}
				outbox <- msg
			}
		}
	}(loopCtx, wsClient, wsChannel, cancel)

	for {
		select {
		case <-loopCtx.Done():
			return loopCtx.Err()
		case msg := <-wsChannel:
			eavesDropper.EavesDrop(msg)
		}
	}
}

func (c *PartyClient) runParty(ctx context.Context, manager PartyManager, wsClient *websocket.Conn) error {
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wsChannel := make(chan []byte)

	go func(ctx context.Context, conn *websocket.Conn, outbox chan<- []byte, cancel context.CancelFunc) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_, msg, err := conn.Read(ctx)
				if err != nil {
					cancel()
					return
				}
				outbox <- msg
			}
		}
	}(loopCtx, wsClient, wsChannel, cancel)
	sleepMinutes := rand.Intn(2) + 1
	sleepDuration := time.Duration(sleepMinutes) * time.Minute
	c.logger.Debug("Sleeping for ", sleepDuration)
	ticker := time.NewTicker(sleepDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go manager.CheckIn()
		case <-loopCtx.Done():
			return loopCtx.Err()
		case msg := <-wsChannel:
			go func(msg []byte) {
				err := manager.HandleMessage(msg)
				c.logger.WithError(err).Info("Handled Incoming WS")
				if err == nil {
					return
				}
				response := ErrorMessage(err.Error(), c.memberId)
				writeContext, timeoutCancel := context.WithTimeout(ctx, time.Millisecond*500)
				defer timeoutCancel()
				err = wsClient.Write(writeContext, websocket.MessageText, response)
				if err != nil {
					c.logger.WithError(err).Error("Web socket connection closed unexpectedly")
					cancel()
				}
			}(msg)
		case msg := <-manager.Outbox():
			c.logger.WithField("message", string(msg)).Debug("Sending message from outbox")
			writeContext, timeoutCancel := context.WithTimeout(ctx, time.Millisecond*500)
			defer timeoutCancel()
			err := wsClient.Write(writeContext, websocket.MessageText, msg)
			if err != nil {
				c.logger.WithError(err).Error("Web socket connection closed unexpectedly from outbox")
				return err
			}
		case <-manager.Done():
			return nil
		}
	}
}

func (c *PartyClient) joinGlobalParty(ctx context.Context, party Party) error {
	c.logger.Info("Joining global party")
	connectCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	token, err := c.getAuth()
	if err != nil {
		return err
	}
	wsUrl := c.baseWsUrl.JoinPath("/api/parties/join")
	query := wsUrl.Query()
	query.Set("id", c.partyId)
	query.Set("token", token)
	query.Set("memberId", c.memberId)
	wsUrl.RawQuery = query.Encode()

	wsClient, _, err := websocket.Dial(connectCtx, wsUrl.String(), nil)
	if err != nil {
		return err
	}
	defer wsClient.CloseNow()

	tlsConfig, err := c.resolveTLS(party)
	if err != nil {
		return err
	}
	manager := c.globalPartyManagerProvider.ProvideGlobalPartyManager(c.memberId, tlsConfig, c.partyId)
	return c.runParty(ctx, manager, wsClient)
}

func (c *PartyClient) joinSelfHostedParty(ctx context.Context) error {
	c.logger.Info("Joining self-hosted party")
	handle := c.partyHost.Join(c.memberId)
	manager := c.localPartyManagerProvider.ProvideLocalPartyManager(c.memberId)
	partyCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go c.runEavesdropper(partyCtx, manager)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case incoming := <-handle.Inbox():
			manager.HandleMessage(incoming)
		case <-manager.Done():
			return nil
		case msg := <-manager.Outbox():
			handle.HandleMessage(msg)
		}
	}
}

func (c *PartyClient) joinLocalParty(ctx context.Context, party Party) error {
	c.logger.WithField("Address", party.LeaderAddress).Info("Joining local party")
	connectCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	token, err := c.getAuth()
	if err != nil {
		return err
	}
	leaderURL, err := url.Parse(fmt.Sprintf("wss://%s", party.LeaderAddress))
	if err != nil {
		return err
	}
	wsUrl := leaderURL.JoinPath("/join/")
	query := wsUrl.Query()
	query.Set("id", c.partyId)
	query.Set("token", token)
	query.Set("memberId", c.memberId)
	wsUrl.RawQuery = query.Encode()

	tlsConfig, err := c.resolveTLS(party)
	if err != nil {
		return err
	}

	dialerClient, err := c.httpClientProvider(tlsConfig)
	if err != nil {
		return err
	}
	dialOptions := &websocket.DialOptions{
		HTTPClient: dialerClient,
	}
	wsClient, _, err := websocket.Dial(connectCtx, wsUrl.String(), dialOptions)
	if err != nil {
		return err
	}
	defer wsClient.CloseNow()

	manager := c.localPartyManagerProvider.ProvideLocalPartyManager(c.memberId)
	partyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	return c.runParty(partyCtx, manager, wsClient)
}

func (c *PartyClient) Join(ctx context.Context) error {
	_, err := c.getParty()
	if err != nil {
		return err
	}
	attempts := 0
	maxAttempts := 5
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			party, _ := c.getParty()
			if party.LeaderAddress != "" && c.globalPartyManagerProvider.IsInternalAddress(party.LeaderAddress) {
				if err := c.joinSelfHostedParty(ctx); err != nil {
					c.logger.WithError(err).Error("An error occurred in self-hosted party")
				}
			}
			if party.LeaderAddress != "" && !c.globalPartyManagerProvider.IsInternalAddress(party.LeaderAddress) {
				attempts++
				if err := c.joinLocalParty(ctx, party); err != nil {
					c.logger.WithError(err).Error("An error occurred in local party")
					if attempts <= maxAttempts {
						continue
					}
					attempts = 0
				}
			}
			party, _ = c.getParty()
			if err := c.joinGlobalParty(ctx, party); err != nil {
				c.logger.WithError(err).Error("An error occurred in main party")
				continue
			}

		}
	}
}
