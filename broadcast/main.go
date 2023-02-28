package main

import (
	"encoding/json"
	"errors"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type topology map[string][]string

type topologyRequest struct {
	Topology topology `json:"topology"`
}

type topologyStore struct {
	topology topology
	mutex    sync.Mutex
}

func (ts *topologyStore) Set(topology topology) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	ts.topology = topology
}

func (ts *topologyStore) Get(node string) []string {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	value, present := ts.topology[node]
	if present {
		return value
	}
	return nil
}

type messages struct {
	messages []any
	mutex    sync.Mutex
}

func (v *messages) Add(value any) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	v.messages = append(v.messages, value)
}

func (v *messages) GetAll() []any {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	return v.messages
}

func main() {
	var topologyStore topologyStore
	var messages messages

	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message, present := body["message"]
		if !present {
			return errors.New("`message` not present in body")
		}

		messages.Add(message)

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

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
