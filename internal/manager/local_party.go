package manager

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type LocalPartyHandle struct {
	logger    logrus.FieldLogger
	inbox     <-chan []byte
	memberId  string
	broadcast func(string, []byte)
}

func (p *LocalPartyHandle) Inbox() <-chan []byte {
	return p.inbox
}

func (p *LocalPartyHandle) HandleMessage(msg []byte) {
	p.logger.Info("Received message")
	p.broadcast(p.memberId, msg)
}

type LocalPartyHost struct {
	logger   logrus.FieldLogger
	outboxes map[string]chan []byte
	mutex    *sync.RWMutex
}

func NewLocalPartyHost(logger logrus.FieldLogger) *LocalPartyHost {
	return &LocalPartyHost{logger: logger, outboxes: make(map[string]chan []byte), mutex: &sync.RWMutex{}}
}

func (p *LocalPartyHost) Join(memberId string) LocalPartyHandle {
	outbox := make(chan []byte)
	p.outboxes[memberId] = outbox
	return LocalPartyHandle{logger: p.logger, inbox: outbox, memberId: memberId, broadcast: p.sendMessage}
}

func (p *LocalPartyHost) sendMessage(senderId string, msg []byte) {
	p.logger.Info("forwarding")
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	p.logger.Infof("sending message to %d outboxes", len(p.outboxes)-1)
	closedOutboxes := []string{}
	for id, outbox := range p.outboxes {
		if id == senderId {
			continue
		}
		p.logger.Infof("forwarding message to %s", id)
		timer := time.NewTimer(time.Millisecond * 100)
		select {
		case outbox <- msg:
			p.logger.Infof("forwarded message to %s", id)
		case <-timer.C:
			p.logger.Infof("timed out forwarding message to %s", id)
			closedOutboxes = append(closedOutboxes, id)
			close(outbox)
		}
	}

	for _, id := range closedOutboxes {
		delete(p.outboxes, id)
	}
}
