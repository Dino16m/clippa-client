package party

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type GlobalPartyManagerProvider struct {
	logger               *logrus.Logger
	serverProvider       ServerProvider
	netInterfaceProvider func() []string
	clipboardManager     ClipboardManager
}

func NewGlobalPartyManagerProvider(
	logger *logrus.Logger,
	serverProvider ServerProvider,
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
		m.clipboardManager, NewConclaveManager())
}

func (m *GlobalPartyManagerProvider) IsInternalAddress(address string) bool {
	ip := strings.Split(address, ":")[0]
	ip = strings.TrimSpace(ip)
	return slices.Contains(m.netInterfaceProvider(), ip)
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
	tlsConfig            *PartyTLS
	partyId              string
	httpClientProvider   func(*PartyTLS) (*http.Client, error)

	conclaveManager *ConclaveManager
}

func NewGlobalPartyManager(
	memberId string,
	logger *logrus.Entry,
	serverProvider ServerProvider,
	netInterfaceProvider func() []string,
	tlsConfig *PartyTLS,
	partyId string,
	clipboardManager ClipboardManager,
	conclaveManager *ConclaveManager,
) *GlobalPartyManager {
	outbox := make(chan []byte)
	clipboardManager.AddOutbox(outbox, func(b []byte) []byte {
		return buildMessage(memberId, Clipboard, ClipboardData{Content: string(b)})
	})
	return &GlobalPartyManager{
		clipboardManager:     clipboardManager,
		memberId:             memberId,
		members:              make(map[string]struct{}),
		logger:               logger,
		outbox:               outbox,
		serverProvider:       serverProvider,
		netInterfaceProvider: netInterfaceProvider,
		tlsConfig:            tlsConfig,
		partyId:              partyId,
		httpClientProvider:   provideHttpClient,
		hangup:               make(chan struct{}),
		conclaveManager: conclaveManager,
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

func (m *GlobalPartyManager) getInternalAddresses() ([]string, error) {
	interfaces := m.netInterfaceProvider()
	if len(interfaces) == 0 {
		return []string{}, nil
	}

	localServer, err := m.serverProvider.GetOrCreateServer(m.partyId, buildTLSConfig(m.tlsConfig), context.Background())
	if err != nil {
		return nil, err
	}
	port := localServer.Port()
	addresses := make([]string, len(interfaces))
	for idx, iface := range interfaces {
		addresses[idx] = fmt.Sprintf("%s:%s", iface, port)
	}
	return addresses, nil
}

func (m *GlobalPartyManager) startConclave(generationId string) error {
	_, ok := m.conclaveManager.GetConclave(generationId)

	if ok {
		return nil
	}

	m.conclaveManager.AbortActiveConclaves()

	addresses, err := m.getInternalAddresses()
	if err != nil {
		return err
	}

	conclave := m.conclaveManager.StartConclave(generationId, addresses)
	conclave.addCandidates(addresses)
	for _, address := range addresses {
		conclave.addVote(m.memberId, address, true)
	}
	response := buildMessage(m.memberId, Conclave, ConclaveData{Addresses: addresses, Generation: generationId})
	m.writeToOutbox(response)
	return nil
}

func (m *GlobalPartyManager) isNewGeneration(generationId string) bool {
	conclaves := m.conclaveManager.ListConclaves()
	epochId := m.epochId(generationId)
	for _, conclave := range conclaves {
		if m.epochId(conclave) > epochId {
			return false
		}
	}
	return true
}

func (m *GlobalPartyManager) handleConclave(msg Message[ConclaveData]) {
	_, ok := m.conclaveManager.GetConclave(msg.Data.Generation)
	if !ok && len(m.conclaveManager.Conclaves()) > 0 && !m.isNewGeneration(msg.Data.Generation) {
		m.writeToOutbox(ErrorMessage(fmt.Sprintf("%s:%s", "ERR_DUPLICATE_CONCLAVE", msg.Data.Generation), m.memberId))
		m.logger.Error("Duplicate Conclave")
		return
	}

	err := m.startConclave(msg.Data.Generation)
	if err != nil {
		m.logger.WithError(err).Error("Failed to start conclave")
		return
	}

	client, err := m.httpClientProvider(m.tlsConfig)
	if err != nil {
		m.logger.WithError(err).Error("Failed to build Http Client")
		return
	}

	conclave, _ := m.conclaveManager.GetConclave(msg.Data.Generation)

	conclave.addCandidates(msg.Data.Addresses)
	ballots := make([]Ballot, len(msg.Data.Addresses))

	for idx, address := range msg.Data.Addresses {
		now := time.Now().UnixMilli()
		requestUrl := &url.URL{Scheme: "https", Host: address}
		requestUrl = requestUrl.JoinPath("/ping/")
		response, err := client.Get(requestUrl.String())
		if err == nil {
			defer response.Body.Close()
		}
		m.logger.WithError(err).WithField("address", address).WithField("response", response).Info("pinged address")
		reachable := err == nil && response != nil && response.StatusCode == http.StatusOK
		latency := time.Now().UnixMilli() - now
		ballots[idx] = Ballot{Address: address, Reachable: reachable, LatencyMillis: latency}

		conclave.addVote(m.memberId, address, reachable)
		conclave.addVote(msg.Sender, address, true) // Sender always votes for itself
	}

	response := buildMessage(m.memberId, Vote, VoteData{Ballots: ballots, Generation: msg.Data.Generation})

	m.writeToOutbox(response)
}

func (m *GlobalPartyManager) writeToOutbox(buf []byte) {
	m.outbox <- buf
}

func (m *GlobalPartyManager) completeConclave() {
	for _, conclave := range m.conclaveManager.Conclaves() {
		if !conclave.votesComplete(m.members) {
			continue
		}
		if conclave.getLeader() != "" {
			m.writeToOutbox(
				buildMessage(m.memberId, LeaderElected, SetLeaderData{Address: conclave.getLeader(), Generation: conclave.generationId}),
			)
		} else {
			m.writeToOutbox(
				buildMessage(m.memberId, Inconclusive, InconclusiveData{Generation: conclave.generationId}),
			)
			m.conclaveManager.EndConclave(conclave.generationId)
		}
	}
}

func (m *GlobalPartyManager) handleVote(msg Message[VoteData]) {
	for _, ballot := range msg.Data.Ballots {
		m.conclaveManager.AddVote(msg.Data.Generation, msg.Sender, ballot.Address, ballot.Reachable)
	}
	m.completeConclave()
}



func (m *GlobalPartyManager) cleanupServer() {
	server, ok := m.serverProvider.GetServer(m.partyId)
	if !ok {
		return
	}
	server.Close()
}

func (m *GlobalPartyManager) cleanup(msg Message[SetLeaderData]) {
	conclave, ok := m.conclaveManager.GetConclave(msg.Data.Generation)
	if !ok {
		return
	}
	defer m.conclaveManager.EndConclave(msg.Data.Generation)

	if slices.Contains(conclave.internalAddresses, msg.Data.Address) {
		return
	}
	m.cleanupServer()
}

func (m *GlobalPartyManager) hasConsensus(msg Message[SetLeaderData]) bool {
	conclave, ok := m.conclaveManager.GetConclave(msg.Data.Generation)
	if !ok {
		return false
	}
	if !conclave.votesComplete(m.members) {
		return false
	}
	return conclave.getLeader() == msg.Data.Address
}

func (m *GlobalPartyManager) epochId(generationId string) string {
	return strings.Split(generationId, ":")[1]
}

func (m *GlobalPartyManager) buildGenerationId() string {
	id, _ := uuid.NewV7()
	return fmt.Sprintf("%s:%s", m.memberId, id.String())
}

func (m *GlobalPartyManager) handleLeaderElected(msg Message[SetLeaderData]) {
	if !m.hasConsensus(msg) && m.conclaveManager.IsConclaveAdmin(m.memberId, msg.Data.Generation) {
		m.conclaveManager.EndConclave(msg.Data.Generation)
		m.writeToOutbox(buildMessage(m.memberId, Inconclusive, InconclusiveData{Generation: msg.Data.Generation}))
		m.clearMembers()
		m.writeToOutbox(buildMessage(m.memberId, Ping, UnitData{}))
	}
	if m.hasConsensus(msg) {
		m.writeToOutbox(buildMessage(m.memberId, SetLeader, SetLeaderData{Address: msg.Data.Address, Generation: msg.Data.Generation}))
		m.cleanup(msg)
		time.AfterFunc(time.Second*2, func() {
			close(m.hangup)
		})
	}
}

func (m *GlobalPartyManager) handleMemberLeft(msg Message[UnitData]) {
	m.removeMember(msg.Sender)
	m.logger.WithField("senderId", msg.Sender).Info("Member left")
	m.conclaveManager.AbortActiveConclaves()
}

func (m *GlobalPartyManager) HandleMessage(buf []byte) error {
	msgType, err := getMessageType(buf)
	if err != nil {
		return err
	}
	m.logger.WithField("msgType", msgType).Info("Got message")
	switch msgType {
	case Conclave:
		msg, _ := parseMessage[ConclaveData](buf)
		m.logger.WithField("addresses", msg.Data.Addresses).Info("Conclave message received")
		m.handleConclave(msg)
		return nil
	case Ping:
		msg, _ := parseMessage[UnitData](buf)
		m.logger.Info("Ping received from ", msg.Sender)
		m.clearMembers()
		m.addMember(msg.Sender)
		m.writeToOutbox(buildMessage(m.memberId, Pong, UnitData{}))
		return nil
	case Pong:
		msg, _ := parseMessage[UnitData](buf)
		m.logger.Info("Pong received from ", msg.Sender)
		m.addMember(msg.Sender)
		return nil
	case Vote:
		msg, _ := parseMessage[VoteData](buf)
		m.logger.WithField("ballots", len(msg.Data.Ballots)).Info("Vote message received")
		m.handleVote(msg)
		return nil
	case SetLeader:
		msg, _ := parseMessage[SetLeaderData](buf)
		m.logger.WithField("leaderAddress", msg.Data.Address).Info("SetLeader message received")
		if m.hasConsensus(msg) {
			m.cleanup(msg)
			close(m.hangup)
		}
		return nil
	case Inconclusive:
		msg, _ := parseMessage[InconclusiveData](buf)
		m.conclaveManager.EndConclave(msg.Data.Generation)
		m.cleanupServer()
		m.logger.WithField("generation", msg.Data.Generation).Info("Conclave Inconclusive message received")
		return nil
	case LeaderElected:
		msg, _ := parseMessage[SetLeaderData](buf)
		m.logger.
			WithField("leaderAddress", msg.Data.Address).
			WithField("HasConsensus", m.hasConsensus(msg)).
			WithField("IsAdmin", m.conclaveManager.IsConclaveAdmin(m.memberId, msg.Data.Generation)).
			Info("LeaderElected message received")
		m.handleLeaderElected(msg)
		return nil
	case Clipboard:
		msg, _ := parseMessage[ClipboardData](buf)
		if msg.Sender == m.memberId {
			return nil
		}
		m.clipboardManager.Write([]byte(msg.Data.Content))
		return nil
	case Joined:
		msg, _ := parseMessage[UnitData](buf)
		m.addMember(msg.Sender)
		m.writeToOutbox(buildMessage(m.memberId, Ping, UnitData{}))
		return nil
	case Left:
		msg, _ := parseMessage[UnitData](buf)
		m.handleMemberLeft(msg)
		return nil
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
	members := []string{
		m.memberId,
	}
	for memberId := range m.members {
		members = append(members, memberId)
	}

	slices.Sort(members)
	return members[0] == m.memberId
}

func (m *GlobalPartyManager) pruneConclaves() {
	pruned := m.conclaveManager.PruneConclaves()
	for _, generationId := range pruned {
		if m.conclaveManager.IsConclaveAdmin(m.memberId, generationId) {
			m.writeToOutbox(buildMessage(m.memberId, Inconclusive, InconclusiveData{Generation: generationId}))
		}
	}
}

func (m *GlobalPartyManager) CheckIn() {
	m.logger.WithField("MemberCount", len(m.members)).WithField("CanStartConclave", m.canStartConclave()).Debug("Checking in")
	m.pruneConclaves()
	if len(m.members) == 0 || !m.conclaveManager.ConclaveInProgress() {
		m.writeToOutbox(buildMessage(m.memberId, Ping, UnitData{}))
	}
	m.logger.Info("Conclave in progress ", m.conclaveManager.ConclaveInProgress())
	if m.conclaveManager.ConclaveInProgress() {
		m.completeConclave()
	}
	if m.conclaveManager.ConclaveInProgress() || len(m.members) == 0 {
		return
	}

	if m.canStartConclave() {
		err := m.startConclave(m.buildGenerationId())
		if err != nil {
			m.logger.WithError(err).Error("Failed to start conclave")
		}
	}
}
