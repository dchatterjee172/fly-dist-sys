package main

import (
	"encoding/json"
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
	return nil
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

func broadcast(messages *messageStore, topologyStore *topologyStore, n *maelstrom.Node) {
	connectedNodes := topologyStore.Get(n.ID())

	if connectedNodes == nil {
		return
	}

	for _, message := range messages.GetAll() {
		for _, destinationNode := range connectedNodes {
			n.RPC(
				destinationNode,
				map[string]any{"type": "broadcast", "message": message},
				func(msg maelstrom.Message) error { return nil },
			)
		}
	}
}

func main() {
	var topologyStore topologyStore
	messages := NewMessageStore()
	done := make(chan bool)
	ticker := time.NewTicker(200 * time.Millisecond)
	n := maelstrom.NewNode()

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				broadcast(messages, &topologyStore, n)
			}
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages.Add(body.Message)
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
	ticker.Stop()
	done <- true

	if err != nil {
		log.Fatal(err)
	}
}
