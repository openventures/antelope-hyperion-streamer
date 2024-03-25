package state

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
)

type State struct {
	HeadGlobalSequence uint64
}

func Store(ctx context.Context, path string, state State) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(state)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	err = os.WriteFile(path, buf.Bytes(), 0644)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}

func Load(ctx context.Context, path string) (State, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return State{}, nil
		}
		return State{}, fmt.Errorf("read: %w", err)
	}
	var state State
	err = gob.NewDecoder(bytes.NewReader(raw)).Decode(&state)
	if err != nil {
		return State{}, fmt.Errorf("decode: %w", err)
	}
	return state, nil
}
