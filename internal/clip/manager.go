package clip

import (
	"context"
	"slices"
	"time"

	"golang.design/x/clipboard"
)


type ClipboardManager struct {
	inbox    chan []byte
	outboxes []chan<- []byte
}

func NewClipboardManager() *ClipboardManager {
	return &ClipboardManager{
		inbox:    make(chan []byte),
		outboxes: []chan<- []byte{},
	}
}

func (m *ClipboardManager) Write(buf []byte) {
	m.inbox <- buf
}

func (m *ClipboardManager) AddOutbox(outbox chan<- []byte) {
	m.outboxes = append(m.outboxes, outbox)
}

func (m *ClipboardManager) Outbox() <-chan []byte {
	return m.inbox
}


func (m *ClipboardManager) Listen(ctx context.Context) {
	clipboard.Init()
	var lastCopied string = ""
	ch := clipboard.Watch(ctx, clipboard.FmtText)
	for {
		select {
		case <-ctx.Done():
			return
		case outgoingClip := <-ch:
			if lastCopied == string(outgoingClip) {
				lastCopied = ""
				continue
			}

			timer := time.NewTimer(time.Millisecond * 500)
			defer timer.Stop()
			closedOutboxes := []int{}

			for index, outbox := range m.outboxes {
				timer.Reset(time.Millisecond * 500)
				select {
				case outbox <- outgoingClip:
				case <-timer.C:
					closedOutboxes = append(closedOutboxes, index)
					continue
				}
			}
			for idx := range closedOutboxes {
				m.outboxes = slices.Delete(m.outboxes, idx, idx+1)
			}

		case outgoingClip := <-m.inbox:
			lastCopied = string(outgoingClip)
			clipboard.Write(clipboard.FmtText, outgoingClip)
		}
	}
}
