package manager

import (
	"slices"
	"sync"
)

type conclaveManager struct {
	generationId       string
	internalAddresses  []string
	candidateAddresses []string
	votes              map[string]map[string]int
	mutex              *sync.RWMutex
}

func (m *conclaveManager) getLeader() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	addressVotes := make(map[string]int)
	for _, votes := range m.votes {
		for address, vote := range votes {
			addressVotes[address] += vote
		}
	}
	leader := ""
	maxVotes := 0
	for address, votes := range addressVotes {
		if votes > maxVotes {
			maxVotes = votes
			leader = address
		}
	}
	return leader
}

func (m *conclaveManager) addCandidates(addresses []string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, address := range addresses {
		if !slices.Contains(m.candidateAddresses, address) {
			m.candidateAddresses = append(m.candidateAddresses, address)
		}
	}
}

func (m *conclaveManager) addVote(memberId, address string, reachable bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	memberVotes, ok := m.votes[memberId]
	if !ok {
		memberVotes = make(map[string]int)
		m.votes[memberId] = memberVotes
	}
	if reachable {
		m.votes[memberId][address]++
	}
}

func (m *conclaveManager) votesComplete() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	addresses := []string{}
	for _, votes := range m.votes {
		for address := range votes {
			addresses = append(addresses, address)
		}
	}
	return len(addresses) == len(m.candidateAddresses)
}
