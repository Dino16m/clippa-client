package manager

import (
	"context"
	"encoding/json"
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
	memberId                   string
}

func NewPartyClient(
	client *http.Client,
	baseUrl *url.URL,
	logger *logrus.Logger,
	partyId,
	partySecret string,
	globalPartyManagerProvider *GlobalPartyManagerProvider,
	memberId string,
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
		memberId:                   memberId,
	}
}

func (c *PartyClient) getParty() (Party, error) {
	requestUrl := c.baseUrl.JoinPath("/")

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
	requestUrl := c.baseUrl.JoinPath("/auth/")

	query := requestUrl.Query()
	query.Set("id", c.partyId)
	requestUrl.RawQuery = query.Encode()
	req, err := http.NewRequest(http.MethodGet, requestUrl.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Secret", c.partySecret)

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

func (c *PartyClient) joinMainParty(ctx context.Context) error {
	connectCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	token, err := c.getAuth()
	if err != nil {
		return err
	}
	wsUrl := c.baseWsUrl.JoinPath("/join/")
	query := wsUrl.Query()
	query.Set("id", c.partyId)
	query.Set("token", token)
	query.Set("memberId", c.memberId)
	wsUrl.RawQuery = query.Encode()

	party, err := c.getParty()
	if err != nil {
		return err
	}
	tlsConfig, err := party.TLSConfig()
	if err != nil {
		return err
	}

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
	sleepMinutes := rand.Intn(5-1) + 1
	ticker := time.NewTicker(time.Duration(sleepMinutes) * time.Minute)
	defer ticker.Stop()

	manager := c.globalPartyManagerProvider.ProvideGlobalPartyManager(c.memberId, tlsConfig, c.partyId)
	for {
		select {
		case <-ticker.C:
			manager.CheckIn()

		case <-ctx.Done():
			return ctx.Err()
		case msg := <-wsChannel:
			response, err := manager.HandleMessage(msg)
			if err == nil && response == nil {
				continue
			}
			if err != nil {
				response = ErrorMessage(err.Error(), c.memberId)
			}
			writeContext, timeoutCancel := context.WithTimeout(ctx, time.Millisecond*500)
			defer timeoutCancel()
			err = wsClient.Write(writeContext, websocket.MessageText, response)
			if err != nil {
				return err
			}
		case msg := <-manager.Outbox():
			writeContext, timeoutCancel := context.WithTimeout(ctx, time.Millisecond*500)
			defer timeoutCancel()
			err = wsClient.Write(writeContext, websocket.MessageText, msg)
			if err != nil {
				return err
			}
		case <-manager.Done():
			return nil
		}
	}
}

func (c *PartyClient) joinLocalParty(ctx context.Context) error {
	return nil
}

func (c *PartyClient) Join(ctx context.Context) error {
	_, err := c.getParty()
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			party, _ := c.getParty()
			if party.LeaderAddress != "" {
				if err := c.joinLocalParty(ctx); err != nil {
					c.logger.WithError(err).Error("An error occurred in local party")
					continue
				}
			}
			if err := c.joinMainParty(ctx); err != nil {
				c.logger.WithError(err).Error("An error occurred in main party")
				continue
			}

		}
	}
}
