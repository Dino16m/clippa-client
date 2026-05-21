package party

import (
	"strings"
	"sync"
	"time"
)

type conclaveVote struct {
	Address  string
	Reachable bool
	MemberId string
	GenerationId string
}

type ConclaveManager struct {
	conclaves map[string]*conclave
	mutex     *sync.RWMutex
	delayedBallots map[string][]conclaveVote
}

func NewConclaveManager() *ConclaveManager {
	return &ConclaveManager{
		conclaves: make(map[string]*conclave),
		mutex:     &sync.RWMutex{},
		delayedBallots: make(map[string][]conclaveVote),
	}
}

func (m *ConclaveManager) PruneConclaves() []string {
	m.mutex.RLock()
	endedConclaves := []string{}
	for generationId, conclave := range m.conclaves {
		if time.Since(conclave.createdAt) > 5*time.Minute {
			endedConclaves = append(endedConclaves, generationId)
		}
	}
	m.mutex.RUnlock()

	for _, generationId := range endedConclaves {
		m.EndConclave(generationId)
	}
	return endedConclaves
}

func (m *ConclaveManager) ListConclaves() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	conclaveIds := []string{}
	for generationId := range m.conclaves {
		conclaveIds = append(conclaveIds, generationId)
	}
	return conclaveIds
}

func (m *ConclaveManager) GetConclave(generationId string) (*conclave, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	conclave, ok := m.conclaves[generationId]
	return conclave, ok
}

func (m *ConclaveManager) StartConclave(generationId string, addresses []string) *conclave {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	conclave := newConclave(generationId, addresses)
	m.conclaves[generationId] = conclave
	m.applyDelayedBallots(conclave)
	return conclave
}

func (m *ConclaveManager) Conclaves() []*conclave {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	conclaves := []*conclave{}
	for _, conclave := range m.conclaves {
		conclaves = append(conclaves, conclave)
	}
	return conclaves
}

func (m *ConclaveManager) ConclaveInProgress() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.conclaves) > 0
}

func (m *ConclaveManager) AbortActiveConclaves() []string {
	conclaveIds := []string{}
	for generationId := range m.conclaves {
		conclaveIds = append(conclaveIds, generationId)
		m.EndConclave(generationId)
	}
	return conclaveIds
}

func (m *ConclaveManager) RemoveMemberConclaves(memberId string) {
	conclaveIds := []string{}
	m.mutex.RLock()
	for generationId := range m.conclaves {
		if m.IsConclaveAdmin(memberId, generationId) {
			conclaveIds = append(conclaveIds, generationId)
		}
	}
	m.mutex.RUnlock()
	for _, generationId := range conclaveIds {
		m.EndConclave(generationId)
	}
}

func (m *ConclaveManager) IsConclaveAdmin(memberId, generationId string) bool {
	parts := strings.Split(generationId, ":")
	if len(parts) != 2 {
		return false
	}
	return parts[0] == memberId
}

func (m *ConclaveManager) EndConclave(generationId string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.conclaves, generationId)
}

func (m *ConclaveManager) AddVote(generationId, memberId, address string, reachable bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	conclave, ok := m.conclaves[generationId]
	if !ok {
		m.delayedBallots[generationId] = append(m.delayedBallots[generationId], conclaveVote{Address: address, Reachable: reachable, MemberId: memberId, GenerationId: generationId})
		return
	}

	conclave.addVote(memberId, address, reachable)

	m.applyDelayedBallots(conclave)
}


// Applies ballots cast before the conclave started.
// This function expects that a lock is acquired before it's called.
func (m *ConclaveManager) applyDelayedBallots(conclave *conclave) {
	delayedBallots := m.delayedBallots[conclave.generationId]
	for _, delayedBallot := range delayedBallots {
		conclave.addVote(delayedBallot.MemberId, delayedBallot.Address, delayedBallot.Reachable)
	}
	delete(m.delayedBallots, conclave.generationId)
}