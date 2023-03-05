package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type topology map[string][]string

type topologyRequest struct {
	Topology topology `json:"topology"`
}

type topologyStore struct {
	topology topology
	mutex    sync.RWMutex
}

type broadcastRequest struct {
	Message int `json:"message"`
}

type messageToBeSent struct {
	Message           int
	DestinationNodeId string
	ReadRecipient     string
}

func (ts *topologyStore) Set(topology topology) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	ts.topology = topology
}

func (ts *topologyStore) Get(node string) []string {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	value, present := ts.topology[node]
	if present {
		return value
	}
	return make([]string, 0)
}

type messageStore struct {
	messages map[int]bool
	mutex    sync.RWMutex
}

func NewMessageStore() *messageStore {
	var m messageStore
	m.messages = make(map[int]bool)
	return &m
}

func (v *messageStore) Add(value int) bool {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	_, found := v.messages[value]
	if found {
		return false
	}

	v.messages[value] = true
	return true
}

func (v *messageStore) GetAll() []int {
	v.mutex.RLock()
	defer v.mutex.RUnlock()

	messages := make([]int, len(v.messages))

	i := 0
	for message := range v.messages {
		messages[i] = message
		i++
	}
	return messages
}

func broadcast(ctx context.Context,
	wg *sync.WaitGroup,
	broadcastChannel chan *messageToBeSent,
	n *maelstrom.Node) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case messageToBeSent := <-broadcastChannel:
			ctxT, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
			defer cancel()

			_, err := n.SyncRPC(
				ctxT,
				messageToBeSent.DestinationNodeId,
				map[string]any{
					"type":    "broadcast",
					"message": messageToBeSent.Message,
				},
			)

			if err != nil {
				if !errors.Is(err, context.Canceled) {
					broadcastChannel <- messageToBeSent
				}
			}
		}
	}
}

func main() {
	var topologyStore topologyStore
	messages := NewMessageStore()
	broadcastChannel := make(chan *messageToBeSent, 200000)
	n := maelstrom.NewNode()
	numGoRoutines := 50
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < numGoRoutines; i++ {
		wg.Add(1)
		go broadcast(ctx, &wg, broadcastChannel, n)
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		isNew := messages.Add(body.Message)

		if isNew {
			neighbours := topologyStore.Get(n.ID())

			for _, destinationNodeId := range neighbours {
				broadcastChannel <- &messageToBeSent{
					DestinationNodeId: destinationNodeId,
					Message:           body.Message,
				}
			}
		}

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(
			msg,
			map[string]any{"type": "read_ok", "messages": messages.GetAll()},
		)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var topologyRequest topologyRequest
		if err := json.Unmarshal(msg.Body, &topologyRequest); err != nil {
			return err
		}

		topologyStore.Set(topologyRequest.Topology)

		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	err := n.Run()
	cancel()
	wg.Wait()

	if err != nil {
		log.Fatal(err)
	}
}
