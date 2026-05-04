package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/dino16m/clippa-client/internal/manager"
	"github.com/sirupsen/logrus"
)

type LocalPartyController struct {
	baseUrl   *url.URL
	client    *http.Client
	logger    logrus.FieldLogger
	partyHost *manager.LocalPartyHost
	partyId   string
}

func NewLocalPartyController(baseURL *url.URL, client *http.Client, logger logrus.FieldLogger, partyHost *manager.LocalPartyHost, partyId string) *LocalPartyController {
	return &LocalPartyController{
		baseUrl:   baseURL,
		client:    client,
		logger:    logger,
		partyHost: partyHost,
		partyId:   partyId,
	}
}

func (c *LocalPartyController) authorize(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	token := strings.TrimSpace(q.Get("token"))
	if token == "" {
		http.Error(w, "token is required", http.StatusBadRequest)
		return errors.New("token is required")
	}
	requestUrl := c.baseUrl.JoinPath("/parties/validate")

	requestBody := map[string]any{
		"id":    c.partyId,
		"token": token,
	}
	requestJson, err := json.Marshal(requestBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, requestUrl.String(), bytes.NewReader(requestJson))
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("unauthorized")
	}

	return nil
}

func (c *LocalPartyController) JoinParty(w http.ResponseWriter, r *http.Request) {
	err := c.authorize(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	memberId := strings.TrimSpace(r.URL.Query().Get("memberId"))
	if memberId == "" {
		c.logger.Error("Member id is required")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		c.logger.WithError(err).Error("websocket accept failed")
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	outbox := make(chan []byte)
	ctx := r.Context()
	ctx, cancel := context.WithCancel(ctx)

	go func(conn *websocket.Conn, outbox chan<- []byte, ctx context.Context, cancel context.CancelFunc) {
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
	}(conn, outbox, ctx, cancel)

	partyHandle := c.partyHost.Join(memberId)
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-partyHandle.Inbox():
			if !ok {
				return
			}
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			if err := conn.Write(ctxWithTimeout, websocket.MessageText, msg); err != nil {
				return
			}
		case msg, ok := <-outbox:
			if !ok {
				return
			}
			partyHandle.HandleMessage(msg)
		}
	}
}
func (c *LocalPartyController) Ping(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
func (c *LocalPartyController) RegisterRoutes(globalMux *http.ServeMux) {
	globalMux.HandleFunc("/join/", c.JoinParty)
	globalMux.HandleFunc("/ping/", c.Ping)
}
