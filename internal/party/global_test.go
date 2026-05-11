package party

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dino16m/clippa-client/internal/clip"
	"github.com/dino16m/clippa-client/internal/server"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

// mockServerProvider mocks the LocalServerProvider for testing purposes.
type mockServerProvider struct {
	server *http.Server
}

func (p *mockServerProvider) ProvideLocalServer(partyId string, tlsConfig *tls.Config, ctx context.Context) (*server.LocalServer, error) {
	return server.NewLocalServer(p.server, func() {}), nil
}

// GlobalPartyManagerTestSuite is the test suite for the GlobalPartyManager.
type GlobalPartyManagerTestSuite struct {
	suite.Suite
	manager          *GlobalPartyManager
	memberId         string
	clipboardManager *clip.ClipboardManager
	testServer       *httptest.Server
}

// SetupTest runs before each test in the suite.
func (s *GlobalPartyManagerTestSuite) SetupTest() {
	logger := logrus.New()
	logger.SetOutput(io.Discard) // Suppress logs during tests

	s.clipboardManager = clip.NewClipboardManager(logger)
	mockNetProvider := func() []string {
		return []string{"127.0.0.1"}
	}

	s.testServer = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	mockServerProvider := &mockServerProvider{
		server: &http.Server{
			Addr: s.testServer.Listener.Addr().String()},
	}

	mockHttpClientProvider := func(tlsConfig *PartyTLS) (*http.Client, error) {
		return s.testServer.Client(), nil
	}

	s.memberId = "test-member-1"
	s.manager = NewGlobalPartyManager(
		s.memberId,
		logrus.NewEntry(logger),
		mockServerProvider,
		mockNetProvider,
		&PartyTLS{},
		"test-party",
		s.clipboardManager,
	)
	s.manager.httpClientProvider = mockHttpClientProvider
}

// TearDownTest runs after each test in the suite.
func (s *GlobalPartyManagerTestSuite) TearDownTest() {
	s.testServer.Close()
}

// TestGlobalPartyManagerTestSuite runs the entire test suite.
func TestGlobalPartyManagerTestSuite(t *testing.T) {
	suite.Run(t, new(GlobalPartyManagerTestSuite))
}

func (s *GlobalPartyManagerTestSuite) TestPing() {
	pingMessage := buildMessage("some-other-member", Ping, UnitData{})
	go s.manager.HandleMessage(pingMessage)

	select {
	case response := <-s.manager.Outbox():
		msg, err := parseMessage[UnitData](response)
		s.NoError(err)
		s.Equal(s.memberId, msg.Sender)
	case <-time.After(1 * time.Second):
		s.T().Fatal("timed out waiting for pong")
	}
}

func (s *GlobalPartyManagerTestSuite) TestClipboard() {
	// Create an incoming "Clipboard" message
	clipboardData := ClipboardData{Content: "hello world"}
	clipboardMessage := buildMessage("some-other-member", Clipboard, clipboardData)

	// Handle the message in a goroutine to avoid blocking
	go s.manager.HandleMessage(clipboardMessage)

	// Assert the outcome by listening to the clipboard manager's outbox
	select {
	case receivedContent := <-s.clipboardManager.Outbox():
		s.Equal("hello world", string(receivedContent))
	case <-time.After(1 * time.Second):
		s.T().Fatal("timed out waiting for clipboard content")
	}
}

func (s *GlobalPartyManagerTestSuite) TestPong() {
	// Create an incoming "Pong" message
	pongMessage := buildMessage("some-other-member", Pong, UnitData{})

	// Handle the message
	err := s.manager.HandleMessage(pongMessage)
	s.NoError(err)

	// Assert that the member was added
	_, ok := s.manager.members["some-other-member"]
	s.True(ok, "member should have been added")
}

func (s *GlobalPartyManagerTestSuite) TestJoined() {
	// Create an incoming "Joined" message
	joinedMessage := buildMessage("some-other-member", Joined, UnitData{})

	// Handle the message
	err := s.manager.HandleMessage(joinedMessage)
	s.NoError(err)

	// Assert that the member was added
	_, ok := s.manager.members["some-other-member"]
	s.True(ok, "member should have been added")
}

func (s *GlobalPartyManagerTestSuite) TestLeft() {
	// Add a member to the manager
	s.manager.addMember("some-other-member")

	// Create an incoming "Left" message
	leftMessage := buildMessage("some-other-member", Left, UnitData{})

	// Handle the message
	err := s.manager.HandleMessage(leftMessage)
	s.NoError(err)

	// Assert that the member was removed
	_, ok := s.manager.members["some-other-member"]
	s.False(ok, "member should have been removed")
}

func (s *GlobalPartyManagerTestSuite) TestError() {
	// Create an incoming "Error" message
	errorMessage := buildMessage("some-other-member", Error, ErrorData{Error: "test error"})

	// Handle the message
	err := s.manager.HandleMessage(errorMessage)
	s.NoError(err)
}

func (s *GlobalPartyManagerTestSuite) TestConclaveAndVoteMessageFlow() {
	// Configure the test server to handle ping requests
	s.testServer.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Equal("/ping/", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	})
	// Create an incoming "Conclave" message
	conclaveMessage := buildMessage("some-other-member", Conclave, ConclaveData{
		Addresses:  []string{s.testServer.Listener.Addr().String()},
		Generation: "some-other-member:gen-1",
	})

	// Handle the message
	go s.manager.HandleMessage(conclaveMessage)

	// The first message will be a conclave message from the manager starting the conclave.
	// We read and discard it.
	<-s.manager.Outbox()

	// Assert that a "Vote" message is sent to the outbox
	select {
	case voteMessage := <-s.manager.Outbox():
		msgType, _ := getMessageType(voteMessage)
		s.Equal(Vote, msgType)
		msg, _ := parseMessage[VoteData](voteMessage)
		s.Equal("some-other-member:gen-1", msg.Data.Generation)
		s.Len(msg.Data.Ballots, 1)
		s.True(msg.Data.Ballots[0].Reachable)
		s.Equal(s.testServer.Listener.Addr().String(), msg.Data.Ballots[0].Address)
	case <-time.After(2 * time.Second):
		s.T().Fatal("timed out waiting for vote message")
	}
}

func (s *GlobalPartyManagerTestSuite) TestSetLeaderMessage() {
	generationId := s.manager.buildGenerationId()

	// Drain the outbox to prevent deadlock when startConclave writes to it
	go func() {
		for range s.manager.Outbox() {
		}
	}()

	s.manager.startConclave(generationId)

	// Get the internal address (which is the leader after startConclave self-votes)
	internalAddress := s.manager.conclaveManagers[generationId].internalAddresses[0]

	// Send a SetLeader message that matches the consensus leader
	setLeaderMessage := buildMessage("leader-member", SetLeader, SetLeaderData{
		Address:    internalAddress,
		Generation: generationId,
	})

	err := s.manager.HandleMessage(setLeaderMessage)
	s.NoError(err)

	// Assert the conclave was terminated
	_, ok := s.manager.conclaveManagers[generationId]
	s.False(ok, "conclave should have been terminated")
}

func (s *GlobalPartyManagerTestSuite) TestLeaderNotElectedWithDisagreement() {
	generationId := s.manager.buildGenerationId()

	// startConclave writes a Conclave message to the outbox, so run it in a goroutine
	go s.manager.startConclave(generationId)

	// Drain the Conclave message from startConclave
	select {
	case <-s.manager.Outbox():
	case <-time.After(1 * time.Second):
		s.T().Fatal("timed out waiting for conclave message")
	}

	// Send a LeaderElected message where the leader does NOT match the consensus
	leaderElectedMessage := buildMessage("other-member", LeaderElected, SetLeaderData{
		Address:    "192.168.1.100:9090",
		Generation: generationId,
	})

	go s.manager.HandleMessage(leaderElectedMessage)

	// Verify Inconclusive message is sent
	select {
	case msg := <-s.manager.Outbox():
		msgType, _ := getMessageType(msg)
		s.Equal(Inconclusive, msgType)
		inconclusiveMsg, _ := parseMessage[InconclusiveData](msg)
		s.Equal(generationId, inconclusiveMsg.Data.Generation)
	case <-time.After(1 * time.Second):
		s.T().Fatal("timed out waiting for inconclusive message")
	}

	// Verify Ping message is sent (beginning of new discovery cycle)
	select {
	case msg := <-s.manager.Outbox():
		msgType, _ := getMessageType(msg)
		s.Equal(Ping, msgType)
	case <-time.After(1 * time.Second):
		s.T().Fatal("timed out waiting for ping message")
	}

	// Verify the old conclave is terminated
	_, ok := s.manager.conclaveManagers[generationId]
	s.False(ok, "conclave should have been terminated")
}

func (s *GlobalPartyManagerTestSuite) TestVoteAggregationAndLeaderElection() {
	// Add members before starting the conclave
	s.manager.addMember("member-2")
	s.manager.addMember("member-3")
	ctx, cancel := context.WithCancel(context.Background())
	// Start a conclave manually
	generationId := s.manager.buildGenerationId()
	go func() {
		for {
			select {
			case msg := <-s.manager.Outbox():
				msgType, _ := getMessageType(msg)
				if msgType != LeaderElected {
					continue
				}
				leaderElectedMsg, _ := parseMessage[SetLeaderData](msg)
				s.Equal(generationId, leaderElectedMsg.Data.Generation)
				cancel()
				return
			case <-ctx.Done():
				s.T().Fatal("timed out waiting for leader elected message")
			}
		}
	}()

	s.manager.startConclave(generationId)

	// Simulate votes from other members
	vote1 := buildMessage("member-2", Vote, VoteData{
		Generation: generationId,
		Ballots:    []Ballot{{Address: s.manager.conclaveManagers[generationId].internalAddresses[0], Reachable: true}},
	})
	vote2 := buildMessage("member-3", Vote, VoteData{
		Generation: generationId,
		Ballots:    []Ballot{{Address: s.manager.conclaveManagers[generationId].internalAddresses[0], Reachable: true}},
	})

	s.manager.HandleMessage(vote1)
	s.manager.HandleMessage(vote2)

	// Assert that a "LeaderElected" message is sent to the outbox
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
		cancel()
		s.T().Fatal("timed out waiting for leader elected message")
	}
}

func (s *GlobalPartyManagerTestSuite) TestInconclusiveMessage() {
	generationId := s.manager.buildGenerationId()

	// Add members to verify they get cleared
	s.manager.addMember("some-other-member")
	s.manager.addMember("yet-another-member")

	// startConclave writes a Conclave message to the outbox, so run it in a goroutine
	go s.manager.startConclave(generationId)

	// Drain the Conclave message from startConclave
	select {
	case <-s.manager.Outbox():
	case <-time.After(1 * time.Second):
		s.T().Fatal("timed out waiting for conclave message")
	}

	// Send an Inconclusive message for that generation
	inconclusiveMessage := buildMessage("some-other-member", Inconclusive, InconclusiveData{
		Generation: generationId,
	})

	s.manager.HandleMessage(inconclusiveMessage)

	_, ok := s.manager.conclaveManagers[generationId]
	s.False(ok, "conclave should have been terminated")
}
