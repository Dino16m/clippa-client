package party

import (
	"slices"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type conclave struct {
	generationId       string
	internalAddresses  []string
	candidateAddresses []string
	votes              map[string]map[string]int
	mutex              *sync.RWMutex
	createdAt time.Time
	consensusCount int
}

func newConclave(generationId string, internalAddresses []string) *conclave {
	return &conclave{
		generationId:       generationId,
		votes:              make(map[string]map[string]int),
		mutex:              &sync.RWMutex{},
		internalAddresses:  internalAddresses,
		candidateAddresses: []string{},
		createdAt:          time.Now(),
	}
}

func (m *conclave) getLeader() string {
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
	if maxVotes != len(m.votes) { // all parties must agree on at least one leader
		logrus.WithField("generation", m.generationId).WithField("votes", addressVotes).Warn("Inconclusive conclave")
		return ""
	}
	leaders := voteAddress[maxVotes]
	slices.Sort(leaders)
	return leaders[0]
}


func (m *conclave) addCandidates(addresses []string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, address := range addresses {
		if !slices.Contains(m.candidateAddresses, address) {
			m.candidateAddresses = append(m.candidateAddresses, address)
		}
	}
}


func (m *conclave) addVote(memberId, address string, reachable bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	memberVotes, ok := m.votes[memberId]
	if !ok {
		memberVotes = make(map[string]int)
		m.votes[memberId] = memberVotes
	}
	_, addressExists := memberVotes[address]
	if !addressExists {
		m.votes[memberId][address] = 0
	}
	if reachable {
		m.votes[memberId][address]++
	}
}

func (m *conclave) votesComplete(members map[string]struct{}) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if len(m.votes) != len(members) + 1 { // include yourself
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
