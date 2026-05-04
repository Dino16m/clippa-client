package manager

import (
	"slices"
	"sync"
	"time"
)

type conclaveManager struct {
	generationId       string
	internalAddresses  []string
	candidateAddresses []string
	votes              map[string]map[string]int
	mutex              *sync.RWMutex
	createdAt time.Time
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
	maxVotes := 0
	voteAddress := make(map[int][]string)
	for address, votes := range addressVotes {
		voteAddress[votes] = append(voteAddress[votes], address)
		if votes > maxVotes {
			maxVotes = votes
		}
	}
	leaders := voteAddress[maxVotes]
	if len(leaders) > 1 {
		slices.Sort(leaders)
	}
	return leaders[0]
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
		memberVotes[address] = 0
		m.votes[memberId] = memberVotes
	}
	if reachable {
		m.votes[memberId][address]++
	}
}

func (m *conclaveManager) votesComplete(members map[string]struct{}) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if len(m.votes) != len(members) {
		return false
	}
	votesComplete := false
	for _, votes := range m.votes {
		votesComplete =  len(votes) == len(m.candidateAddresses)
		if !votesComplete {
			break
		}
	}
	return votesComplete
}
