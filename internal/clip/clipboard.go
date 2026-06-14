package clip

import (
	"context"
	"slices"
	"time"

	"github.com/sirupsen/logrus"
	"golang.design/x/clipboard"
)

type outboxPair struct {
	writer func([]byte) []byte
	outbox chan<- []byte
}

type ClipboardManager struct {
	inbox    chan []byte
	outboxes []outboxPair
	logger   logrus.FieldLogger
}

func NewClipboardManager(logger logrus.FieldLogger) *ClipboardManager {
	return &ClipboardManager{
		inbox:    make(chan []byte),
		outboxes: []outboxPair{},
		logger:   logger,
	}
}

func (m *ClipboardManager) Write(buf []byte) {
	m.inbox <- buf
}

func (m *ClipboardManager) AddOutbox(outbox chan<- []byte, writer func([]byte) []byte) {
	m.outboxes = append(m.outboxes, outboxPair{writer: writer, outbox: outbox})
}

func (m *ClipboardManager) Outbox() <-chan []byte {
	return m.inbox
}

func (m *ClipboardManager) Listen(ctx context.Context) error {
	m.logger.Info("Initialising clipboard")
	err := clipboard.Init()
	if err != nil {
		return err
	}
	m.logger.Info("Clipboard initialised")
	var lastCopied string = ""
	ch := clipboard.Watch(ctx, clipboard.FmtText)
	m.logger.Info("Watching clipboard")
	for {
		select {
		case <-ctx.Done():
			return nil
		case outgoingClip := <-ch:
			m.logger.Info("Received outgoing Clip")
			if lastCopied == string(outgoingClip) {
				m.logger.Info("Skipping outgoing Clip")
				continue
			}
			m.logger.Info("Forwarding outgoing Clip")

			timer := time.NewTimer(time.Millisecond * 500)
			defer timer.Stop()
			closedOutboxes := []int{}

			for index, outbox := range m.outboxes {

				timer.Reset(time.Millisecond * 500)
				select {
				case outbox.outbox <- outbox.writer(outgoingClip):
				case <-timer.C:
					closedOutboxes = append(closedOutboxes, index)
					continue
				}
			}
			for idx, outbox := range m.outboxes {
				if !slices.Contains(closedOutboxes, idx) {
					m.outboxes = append(m.outboxes, outbox)
				}
			}

		case incomingClip := <-m.inbox:
			m.logger.Info("Received incoming Clip")
			if lastCopied == string(incomingClip) {
				continue
			}
			lastCopied = string(incomingClip)
			clipboard.Write(clipboard.FmtText, incomingClip)
		}
	}
}
