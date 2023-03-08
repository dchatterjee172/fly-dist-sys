package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type sendRequest struct {
	Key     string `json:"key"`
	Message int    `json:"msg"`
}

type pollRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type commitOffsetsRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type listCommitedOffsetsRequest struct {
	Keys []string `json:"keys"`
}

type storage struct {
	offsets  []int
	messages []int
}

func NewStorage() *storage {
	var storage storage
	storage.offsets = make([]int, 0)
	storage.messages = make([]int, 0)
	return &storage
}

type keyStorage struct {
	data  map[string]*storage
	mutex sync.RWMutex
}

func NewKeyStorage() *keyStorage {
	var ks keyStorage
	ks.data = make(map[string]*storage)
	return &ks
}

func (ks *keyStorage) Set(key string, offset int, message int) {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	_, found := ks.data[key]
	if !found {
		ks.data[key] = NewStorage()
	}

	storage := ks.data[key]

	for index := 0; index < len(storage.offsets); index++ {
		if storage.offsets[index] > offset {
			storage.offsets = append(storage.offsets[:index+1], storage.offsets[index:]...)
			storage.offsets[index] = offset

			storage.messages = append(storage.messages[:index+1], storage.messages[index:]...)
			storage.messages[index] = message
			return
		}
	}

	storage.offsets = append(storage.offsets, offset)
	storage.messages = append(storage.messages, message)
}

func (ks *keyStorage) Get(key string, offsetStart int, limit int) [][2]int {
	if limit < 0 {
		panic("`limit` cannot be negative")
	}
	ks.mutex.RLock()
	defer ks.mutex.RUnlock()

	data := make([][2]int, 0)

	storage, found := ks.data[key]
	if !found {
		return data
	}

	for index := 0; index < len(storage.offsets); index++ {
		if storage.offsets[index] >= offsetStart {
			data = append(data, [2]int{storage.offsets[index], storage.messages[index]})
		}
		if len(data) == limit {
			break
		}
	}

	return data
}

func readFromKVWithDefault(ctx context.Context, kv *maelstrom.KV, key string, defaultValue int) (int, error) {
	currentValue, err := kv.ReadInt(ctx, key)
	if err != nil {
		rpcError := &maelstrom.RPCError{}
		if errors.As(err, &rpcError) {
			if rpcError.Code == 20 {
				return defaultValue, nil
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

func getOffSet(ctx context.Context, kv *maelstrom.KV, retries int) (int, error) {
	offsetKey := "offset"

	if retries < 0 {
		panic("`retries` cannot be negative")
	}

	for i := 0; i < retries; i++ {
		currentOffset, err := readFromKVWithDefault(ctx, kv, offsetKey, 0)
		if err != nil {
			return 0, err
		}
		nextOffset := currentOffset + 1
		isSet, err := compareAnvsetInKV(ctx, kv, offsetKey, currentOffset, nextOffset)
		if err != nil {
			return 0, err
		}
		if isSet {
			return nextOffset, nil
		}
	}
	return 0, errors.New("Could not generate offset")
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	ctx, cancel := context.WithCancel(context.Background())
	keyStorage := NewKeyStorage()

	n.Handle("send", func(msg maelstrom.Message) error {
		var body sendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offset, err := getOffSet(ctx, kv, 10)
		if err != nil {
			return err
		}

		keyStorage.Set(body.Key, offset, body.Message)

		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body pollRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := make(map[string][][2]int)

		for key, offset := range body.Offsets {
			msgs[key] = keyStorage.Get(key, offset, 2)
		}

		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body commitOffsetsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for key, offset := range body.Offsets {
			kv.Write(ctx, key, offset)
		}

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body listCommitedOffsetsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := make(map[string]int)

		for _, key := range body.Keys {
			offset, err := readFromKVWithDefault(ctx, kv, key, -1)
			if err != nil {
				return err
			}
			if offset < 0 {
				continue
			}
			offsets[key] = offset
		}

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	err := n.Run()
	cancel()
	if err != nil {
		log.Fatal(err)
	}
}