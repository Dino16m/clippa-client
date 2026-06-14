package party

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

type LocalPartyManagerProvider struct {
	logger logrus.FieldLogger
	clipboardManager ClipboardManager
}

func NewLocalPartyManagerProvider(logger logrus.FieldLogger, clipboardManager ClipboardManager) *LocalPartyManagerProvider {
	return &LocalPartyManagerProvider{logger: logger, clipboardManager: clipboardManager}
}

func (p *LocalPartyManagerProvider) ProvideLocalPartyManager(memberId string) *LocalPartyManager {
	return NewLocalPartyManager(memberId, p.logger, p.clipboardManager)
}

type LocalPartyManager struct {
	memberId string
	outbox   chan []byte
	done     chan struct{}
	members              map[string]struct{}
	logger               logrus.FieldLogger
	clipboardManager ClipboardManager
}

func NewLocalPartyManager(memberId string, logger logrus.FieldLogger, clipboardManager ClipboardManager) *LocalPartyManager {
	outbox := make(chan []byte)
	clipboardManager.AddOutbox(outbox, func(b []byte) []byte {
		return buildMessage(memberId, Clipboard, ClipboardData{Content: string(b)})
	})
	return &LocalPartyManager{
		memberId: memberId,
		outbox:   outbox,
		done:     make(chan struct{}),
		members:  make(map[string]struct{}),
		logger:   logger,
		clipboardManager: clipboardManager,
	}
}

func (m *LocalPartyManager) writeToOutbox(buf []byte) {
	m.outbox <- buf
}

func (m *LocalPartyManager) EavesDrop(buf []byte) {
	msgType, err := getMessageType(buf)
	if err != nil {
		return
	}
	m.logger.WithField("MSG", msgType).Info("Eavesdropping")
	if msgType == Conclave {
		m.writeToOutbox(buildMessage(m.memberId, Conclave, ConclaveData{Generation: fmt.Sprintf("%s:1", m.memberId)}))
		m.hangup()
	}
	if msgType == Ping {
		m.writeToOutbox(buildMessage(m.memberId, Conclave, ConclaveData{Generation: fmt.Sprintf("%s:1", m.memberId)}))
		m.hangup()
	}
}

func (m *LocalPartyManager) HandleMessage(buf []byte) error {
	msgType, err := getMessageType(buf)
	if err != nil {
		return  err
	}
	switch msgType {
	case Conclave:
		m.hangup()
		return nil
	case Clipboard:
		msg, _ := parseMessage[ClipboardData](buf)
		if msg.Sender == m.memberId {
			return  nil
		}
		m.clipboardManager.Write([]byte(msg.Data.Content))
		return  nil
	default:
		return nil
	}
}

func (m *LocalPartyManager)	Outbox() <-chan []byte {
	return m.outbox
}

func (m *LocalPartyManager)	CheckIn() {
	m.logger.Info("CheckIn called")
}

func (m *LocalPartyManager) hangup() {
	close(m.done)
}

func (m *LocalPartyManager)	Done() <-chan struct{} {
	return m.done
}