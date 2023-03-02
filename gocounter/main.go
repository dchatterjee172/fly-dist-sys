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

type addRequest struct {
	Delta int `json:"delta"`
}

type broadcastRequest struct {
	Value int `json:"value"`
}

type valueStore struct {
	value int
	mutex sync.RWMutex
}

func newValueStore() *valueStore {
	var valueStore valueStore
	valueStore.value = 0
	return &valueStore
}

func (vs *valueStore) Add(value int) int {
	vs.mutex.Lock()
	defer vs.mutex.Unlock()

	vs.value += value
	return vs.value
}

func (vs *valueStore) Sub(value int) {
	vs.mutex.Lock()
	defer vs.mutex.Unlock()

	vs.value -= value
	if vs.value < 0 {
		panic("value is negative")
	}
}

func (vs *valueStore) Get() int {
	vs.mutex.RLock()
	defer vs.mutex.RUnlock()

	return vs.value
}

func (vs *valueStore) SetIfGreater(value int) int {
	vs.mutex.Lock()
	defer vs.mutex.Unlock()

	if value > vs.value {
        vs.value = value
	}
	return vs.value
}

func readFromKVWithDefaultZero(ctx context.Context, kv *maelstrom.KV, key string) (int, error) {
	currentValue, err := kv.ReadInt(ctx, key)
	if err != nil {
		rpcError := &maelstrom.RPCError{}
		if errors.As(err, &rpcError) {
			if rpcError.Code == 20 {
				return 0, nil
			}
		}
		return 0, err
	}
	return currentValue, nil
}

func compareAnvsetInKV(ctx context.Context, kv *maelstrom.KV, key string, from int, to int) (bool, error) {
	err := kv.CompareAndSwap(ctx, key, from, to, true)
	if err != nil {
		rpcError := &maelstrom.RPCError{}
		if errors.As(err, &rpcError) {
			if rpcError.Code == 22 {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

func syncDeltaToKV(ctx context.Context, kv *maelstrom.KV, deltaStore *valueStore, key string, valueStore *valueStore) error {
	currentDelta := deltaStore.Get()

	if currentDelta < 1 {
		return nil
	}

	currentValue, err := readFromKVWithDefaultZero(ctx, kv, key)
	if err != nil {
		return err
	}

	nextValue := currentValue + currentDelta

	wasSet, err := compareAnvsetInKV(ctx, kv, key, currentValue, nextValue)

	if err != nil {
		return err
	}

	if wasSet {
		deltaStore.Sub(currentDelta)
        valueStore.SetIfGreater(nextValue)
	}

	return nil
}

func broadcast(n *maelstrom.Node, vs *valueStore) {
	for _, destinationNode := range n.NodeIDs() {
        if n.ID() == destinationNode {
            continue
        }
		n.Send(
			destinationNode,
			map[string]any{"type": "broadcast", "value": vs.Get()},
		)
	}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	deltaStore := newValueStore()
    valueStore := newValueStore()
	ctx := context.TODO()
	key := "val"
	done := make(chan bool)
	ticker := time.NewTicker(200 * time.Millisecond)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				syncDeltaToKV(ctx, kv, deltaStore, key, valueStore)
                broadcast(n, valueStore)
			}
		}
	}()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body addRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := body.Delta
		deltaStore.Add(delta)

		return n.Reply(msg, map[string]string{"type": "add_ok"})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		value := body.Value
        valueStore.SetIfGreater(value)

        return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		currentValue, err := readFromKVWithDefaultZero(ctx, kv, key)
		if err != nil {
			return err
		}
		currentDelta := deltaStore.Get()
        value := currentDelta + currentValue

        value = valueStore.SetIfGreater(value)
		return n.Reply(
			msg,
			map[string]any{"type": "read_ok", "value": value},
		)
	})

	err := n.Run()
	ticker.Stop()
	done <- true

	if err != nil {
		log.Fatal(err)
	}
}
