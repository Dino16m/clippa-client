package manager

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)




type GlobalPartyManagerProvider struct {
	logger               *logrus.Logger
	serverProvider       *LocalServerProvider
	netInterfaceProvider func() []string
	clipboardManager     ClipboardManager
}

func NewGlobalPartyManagerProvider(
	logger *logrus.Logger,
	serverProvider *LocalServerProvider,
	netInterfaceProvider func() []string,
	clipboardManager ClipboardManager,
) *GlobalPartyManagerProvider {
	return &GlobalPartyManagerProvider{
		logger:               logger,
		serverProvider:       serverProvider,
		netInterfaceProvider: netInterfaceProvider,
		clipboardManager:     clipboardManager,
	}
}

func (m *GlobalPartyManagerProvider) ProvideGlobalPartyManager(
	memberId string, tlsConfig *PartyTLS, partyId string) *GlobalPartyManager {
	return NewGlobalPartyManager(
		memberId,
		m.logger.WithField("memberId", memberId).WithField("partyId", partyId),
		m.serverProvider,
		m.netInterfaceProvider,
		tlsConfig,
		partyId,
		m.clipboardManager)
}

func (m *GlobalPartyManagerProvider) IsInternalAddress(address string) bool {
	ip := strings.Split(address, ":")[0]
	ip = strings.TrimSpace(ip)
	return slices.Contains(m.netInterfaceProvider(), ip)
}


type ServerProvider interface {
	ProvideLocalServer(partyId string, tlsConfig *PartyTLS, ctx context.Context) (*LocalServer, error)
}

type GlobalPartyManager struct {
	clipboardManager     ClipboardManager
	hangup               chan struct{}
	memberId             string
	members              map[string]struct{}
	logger               *logrus.Entry
	outbox               chan []byte
	serverProvider       ServerProvider
	netInterfaceProvider func() []string
	conclaveManagers     map[string]*conclaveManager
	tlsConfig            *PartyTLS
	partyId              string
	httpClientProvider   func(*PartyTLS) (*http.Client, error)

	conclaveMutex *sync.RWMutex
}

func NewGlobalPartyManager(
	memberId string,
	logger *logrus.Entry,
	serverProvider ServerProvider,
	netInterfaceProvider func() []string,
	tlsConfig *PartyTLS,
	partyId string,
	clipboardManager ClipboardManager,
) *GlobalPartyManager {
	outbox := make(chan []byte)
	clipboardManager.AddOutbox(outbox)
	return &GlobalPartyManager{
		clipboardManager:     clipboardManager,
		memberId:             memberId,
		members:              make(map[string]struct{}),
		logger:               logger,
		outbox:               outbox,
		serverProvider:       serverProvider,
		netInterfaceProvider: netInterfaceProvider,
		conclaveManagers:     make(map[string]*conclaveManager),
		conclaveMutex:        &sync.RWMutex{},
		tlsConfig:            tlsConfig,
		partyId:              partyId,
		httpClientProvider:   provideHttpClient,
		hangup: make(chan struct{}),
	}
}

func (m *GlobalPartyManager) addMember(memberId string) {
	if memberId == m.memberId {
		return
	}

	m.members[memberId] = struct{}{}
}

func (m *GlobalPartyManager) clearMembers() {
	m.members = make(map[string]struct{})
}

func (m *GlobalPartyManager) removeMember(memberId string) {
	if memberId == m.memberId {
		return
	}
	delete(m.members, memberId)
}

func (m *GlobalPartyManager) removeMemberConclaves(memberId string) {
	conclaveIds := []string{}
	m.conclaveMutex.RLock()
	for generationId := range m.conclaveManagers {
		if m.isConclaveAdmin(memberId, generationId) {
			conclaveIds = append(conclaveIds, generationId)
		}
	}
	m.conclaveMutex.RUnlock()
	for _, generationId := range conclaveIds {
		m.endConclave(generationId)
	}
}

func (m *GlobalPartyManager) pruneConclaves() {
	m.conclaveMutex.RLock()
	endedConclaves := []string{}
	for generationId, conclave := range m.conclaveManagers {
		if time.Since(conclave.createdAt) > 5*time.Minute {
			endedConclaves = append(endedConclaves, generationId)
		}
	}
	m.conclaveMutex.RUnlock()

	for _, generationId := range endedConclaves {
		m.endConclave(generationId)
	}
}

func (m *GlobalPartyManager) conclaveInProgress() bool {
	m.conclaveMutex.RLock()
	defer m.conclaveMutex.RUnlock()
	return len(m.conclaveManagers) > 0
}

func (m *GlobalPartyManager) startConclave(generationId string) {
	m.conclaveMutex.RLock()
	_, ok := m.conclaveManagers[generationId]
	m.conclaveMutex.RUnlock()
	if ok {
		return
	}

	localServer, _ := m.serverProvider.ProvideLocalServer(m.partyId, m.tlsConfig, context.Background())
	port := localServer.Port()
	interfaces := m.netInterfaceProvider()
	addresses := make([]string, 0, len(interfaces))
	for _, iface := range interfaces {
		addresses = append(addresses, fmt.Sprintf("%s:%s", iface, port))
	}
	m.conclaveMutex.Lock()
	conclave := &conclaveManager{
		generationId:       generationId,
		votes:              make(map[string]map[string]int),
		mutex:              &sync.RWMutex{},
		internalAddresses:  addresses,
		candidateAddresses: []string{},
		createdAt: time.Now(),
	}
	m.conclaveManagers[generationId] = conclave
	m.conclaveMutex.Unlock()
	conclave.addCandidates(addresses)
	for _, address := range addresses {
		conclave.addVote(m.memberId, address, true)
	}

	response := buildMessage(m.memberId, Conclave, ConclaveData{Addresses: addresses, Generation: generationId})
	m.writeToOutbox(response)
}

func (m *GlobalPartyManager) handleConclave(msg Message[ConclaveData]) {

	m.conclaveMutex.RLock()
	_, ok := m.conclaveManagers[msg.Data.Generation]
	m.conclaveMutex.RUnlock()
	if !ok && len(m.conclaveManagers) > 0 {
		m.writeToOutbox(ErrorMessage(fmt.Sprintf("%s:%s", "ERR_DUPLICATE_CONCLAVE", msg.Data.Generation), m.memberId))
		m.logger.Error("Duplicate Conclave")
		return
	}

	m.startConclave(msg.Data.Generation)

	go func() {
		client, err := m.httpClientProvider(m.tlsConfig)
		if err != nil {
			m.logger.WithError(err).Error("Failed to build Http Client")
			return
		}
		ballots := make([]Ballot, len(msg.Data.Addresses))
		m.conclaveMutex.RLock()
		mgr := m.conclaveManagers[msg.Data.Generation]
		m.conclaveMutex.RUnlock()

		mgr.addCandidates(msg.Data.Addresses)

		for idx, address := range msg.Data.Addresses {
			now := time.Now().UnixMilli()
			requestUrl := &url.URL{Scheme: "https", Host: address}
			requestUrl = requestUrl.JoinPath("/ping/")
			response, err := client.Get(requestUrl.String())
			reachable := err == nil && response.StatusCode == http.StatusOK
			latency := time.Now().UnixMilli() - now
			ballots[idx] = Ballot{Address: address, Reachable: reachable, LatencyMillis: latency}

			mgr.addVote(m.memberId, address, reachable)
		}

		response := buildMessage(m.memberId, Vote, VoteData{Ballots: ballots, Generation: msg.Data.Generation})

		m.writeToOutbox(response)
	}()

}

func (m *GlobalPartyManager) writeToOutbox(buf []byte) {
	m.outbox <- buf
}

func (m *GlobalPartyManager) handleVote(msg Message[VoteData]) {
	m.logger.Info("handleVote called")
	m.conclaveMutex.RLock()
	mgr := m.conclaveManagers[msg.Data.Generation]
	m.conclaveMutex.RUnlock()
	for _, ballot := range msg.Data.Ballots {
		mgr.addVote(msg.Sender, ballot.Address, ballot.Reachable)
	}
	if mgr.votesComplete(m.members) {
		m.writeToOutbox(
			buildMessage(m.memberId, LeaderElected, SetLeaderData{Address: mgr.getLeader(), Generation: msg.Data.Generation}),
		)
	}
}

func (m *GlobalPartyManager) isConclaveAdmin(memberId, generationId string) bool {
	parts := strings.Split(generationId, ":")
	if len(parts) != 2 {
		return false
	}
	return parts[0] == memberId
}

func (m *GlobalPartyManager) endConclave(generationId string) {
	m.conclaveMutex.Lock()
	delete(m.conclaveManagers, generationId)
	m.conclaveMutex.Unlock()
}

func (m *GlobalPartyManager) setLeader(msg Message[SetLeaderData]) {
	defer m.endConclave(msg.Data.Generation)
	m.conclaveMutex.RLock()
	conclave := m.conclaveManagers[msg.Data.Generation]
	m.conclaveMutex.RUnlock()

	if slices.Contains(conclave.internalAddresses, msg.Data.Address) {
		return
	}
	server, err := m.serverProvider.ProvideLocalServer(m.partyId, m.tlsConfig, context.Background())
	if err != nil {
		return
	}
	server.Close()
}

func (m *GlobalPartyManager) hasConsensus(msg Message[SetLeaderData]) bool {
	m.conclaveMutex.RLock()
	mgr := m.conclaveManagers[msg.Data.Generation]
	m.conclaveMutex.RUnlock()
	leader := mgr.getLeader()
	return leader == msg.Data.Address
}

func (m *GlobalPartyManager) buildGenerationId() string {
	return fmt.Sprintf("%s:%s", m.memberId, uuid.NewString())
}

func (m *GlobalPartyManager) HandleMessage(buf []byte) (error) {
	msgType, err := getMessageType(buf)
	if err != nil {
		return  err
	}
	m.logger.WithField("msgType", msgType).Info("Got message type")
	switch msgType {
	case Conclave:
		msg, _ := parseMessage[ConclaveData](buf)
		m.logger.WithField("addresses", msg.Data.Addresses).Info("Conclave message received")
		go m.handleConclave(msg)
		return nil
	case Ping:
		m.logger.Info("Ping received")
		m.clearMembers()
		m.writeToOutbox(buildMessage(m.memberId, Pong, UnitData{}))
		return nil
	case Pong:
		m.logger.Info("Pong received")
		msg, _ := parseMessage[UnitData](buf)
		m.addMember(msg.Sender)
		return  nil
	case Vote:
		msg, _ := parseMessage[VoteData](buf)
		m.logger.WithField("ballots", len(msg.Data.Ballots)).Info("Vote message received")
		m.handleVote(msg)
		return  nil
	case SetLeader:
		msg, _ := parseMessage[SetLeaderData](buf)
		m.logger.WithField("leaderAddress", msg.Data.Address).Info("SetLeader message received")
		if m.hasConsensus(msg) {
			m.setLeader(msg)
			close(m.hangup)
		}
		return  nil
	case Inconclusive:
		msg, _ := parseMessage[InconclusiveData](buf)
		m.endConclave(msg.Data.Generation)
		m.logger.WithField("generation", msg.Data.Generation).Info("Conclave Inconclusive message received")
		return  nil
	case LeaderElected:
		msg, _ := parseMessage[SetLeaderData](buf)
		m.logger.WithField("leaderAddress", msg.Data.Address).Info("LeaderElected message received")
		if !m.hasConsensus(msg) && m.isConclaveAdmin(m.memberId, msg.Data.Generation) {
			m.endConclave(msg.Data.Generation)
			m.writeToOutbox(buildMessage(m.memberId, Inconclusive, InconclusiveData{Generation: msg.Data.Generation}))
			m.clearMembers()
			m.writeToOutbox(buildMessage(m.memberId, Ping, UnitData{}))
		}
		return  nil
	case Clipboard:
		msg, _ := parseMessage[ClipboardData](buf)
		if msg.Sender == m.memberId {
			return  nil
		}
		m.clipboardManager.Write([]byte(msg.Data.Content))
		return  nil
	case Joined:
		msg, _ := parseMessage[UnitData](buf)
		m.addMember(msg.Sender)
		m.writeToOutbox(buildMessage(m.memberId, Pong, UnitData{}))
		return  nil
	case Left:
		msg, _ := parseMessage[UnitData](buf)
		m.removeMember(msg.Sender)
		m.removeMemberConclaves(msg.Sender)
		m.logger.WithField("memberId", msg.Sender).Info("Member left")
		return  nil
	case Error:
		msg, _ := parseMessage[ErrorData](buf)
		m.logger.WithField("error", msg.Data.Error).Warn("Error message received")
		return nil
	default:
		return nil
	}
}

func (m *GlobalPartyManager) Outbox() <-chan []byte {
	return m.outbox
}

func (m *GlobalPartyManager) Done() <-chan struct{} {
	return m.hangup
}

func (m *GlobalPartyManager) canStartConclave() bool {
	if len(m.members) < 1 {
		return false
	}
	members := []string{}
	for  memberId := range m.members {
		members = append(members, memberId)
	}
	slices.Sort(members)
	return members[0] == m.memberId
}

func (m *GlobalPartyManager) CheckIn() {
	m.pruneConclaves()
	if len(m.members) == 0 {
		m.writeToOutbox(buildMessage(m.memberId, Ping, UnitData{}))
	}
	if m.conclaveInProgress() || len(m.members) == 0 {
		return
	}

	if m.canStartConclave() {
		m.startConclave(m.buildGenerationId())
	}
}
