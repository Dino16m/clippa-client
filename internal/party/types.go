package party

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"time"

	"github.com/dino16m/clippa-client/internal/server"
)

type UnitData struct{}
type ErrorData struct {
	Error string `json:"error"`
}
type ConclaveData struct {
	Addresses  []string `json:"addresses"`
	Generation string   `json:"generation"`
}
type InconclusiveData struct {
	Generation string `json:"generation"`
}
type Ballot struct {
	Address       string `json:"address"`
	Reachable     bool   `json:"reachable"`
	LatencyMillis int64  `json:"latency"`
}

type VoteData struct {
	Ballots    []Ballot `json:"ballots"`
	Generation string   `json:"generation"`
}

type SetLeaderData struct {
	Address    string `json:"address"`
	Generation string `json:"generation"`
}

type ClipboardData struct {
	Content string `json:"content"`
}

type Party struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	LeaderAddress string `json:"leaderAddress"`
	CertPEM       string `json:"certPem"`
	KeyPEM        string `json:"keyPem"`
}

func (p Party) TLSConfig() (*PartyTLS, error) {
	privateKey, err := parsePrivateKey(p.KeyPEM)
	if err != nil {
		return nil, err
	}
	cerificate, err := parseX509Cert(p.CertPEM)
	if err != nil {
		return nil, err
	}
	return &PartyTLS{
		Certificate: cerificate,
		PrivateKey:  privateKey,
	}, nil
}

type PartyTLS struct {
	Certificate *x509.Certificate
	PrivateKey  *ecdsa.PrivateKey
}

type MessageType string

const (
	Conclave          MessageType = "conclave"
	Inconclusive      MessageType = "inconclusive"
	Ping              MessageType = "ping"
	Pong              MessageType = "pong"
	Vote              MessageType = "vote"
	SetLeader         MessageType = "set-leader"
	LeaderElected     MessageType = "leader-elected"
	LeaderUnreachable MessageType = "leader-unreachable"
	Clipboard         MessageType = "clipboard"
	Joined            MessageType = "joined"
	Left              MessageType = "left"
	Error             MessageType = "error"
)

type Message[T any] struct {
	Data        T           `json:"data"`
	Sender      string      `json:"sender"`
	MessageType MessageType `json:"messageType"`
	CreatedAt   int64       `json:"createdAt"`
}

func ErrorMessage(msg string, senderId string) []byte {

	response := Message[ErrorData]{
		Data:        ErrorData{Error: msg},
		Sender:      senderId,
		MessageType: Error,
		CreatedAt:   time.Now().UTC().Unix(),
	}

	b, _ := json.Marshal(response)
	return b
}

func buildMessage[T any](sender string, msgType MessageType, data T) []byte {
	response := Message[T]{
		Data:        data,
		Sender:      sender,
		MessageType: msgType,
		CreatedAt:   time.Now().UTC().Unix(),
	}

	b, _ := json.Marshal(response)
	return b
}

func parseMessage[T any](raw []byte) (Message[T], error) {
	var msg Message[T]
	err := json.Unmarshal(raw, &msg)
	if err != nil {
		return Message[T]{}, err
	}
	return msg, nil
}

func getMessageType(raw []byte) (MessageType, error) {
	var msg Message[any]
	err := json.Unmarshal(raw, &msg)
	if err != nil {
		return "", err
	}
	return MessageType(msg.MessageType), nil
}

type ServerProvider interface {
	GetServer(partyId string) (*server.LocalServer, bool)
	GetOrCreateServer(partyId string, tlsConfig *tls.Config, ctx context.Context) (*server.LocalServer, error)
}

type ClipboardManager interface {
	Write(buf []byte)
	AddOutbox(outbox chan<- []byte, writer func([]byte) []byte)
	Listen(ctx context.Context)
}
