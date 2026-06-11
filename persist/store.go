package persist

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
)

// Store is a nil-safe, generic JSON file store with atomic writes.
// A nil *Store is valid — Load returns the zero value of T and Save is a no-op.
type Store[T any] struct {
	path string
	mu   sync.Mutex
}

// New returns a Store backed by path. Returns nil if path is empty, which disables persistence.
func New[T any](path string) *Store[T] {
	if path == "" {
		return nil
	}
	return &Store[T]{path: path}
}

// Load reads and unmarshals the stored value from disk.
// Returns the zero value of T when the store is nil or the backing file does not yet exist.
func (s *Store[T]) Load() (T, error) {
	var zero T
	if s == nil {
		return zero, nil
	}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return zero, nil
	}
	if err != nil {
		return zero, err
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return zero, err
	}
	return v, nil
}

// Save atomically marshals v and writes it to disk using a tmp-then-rename strategy,
// which prevents a partial write from corrupting the file. No-op when the store is nil.
func (s *Store[T]) Save(v T) error {
	if s == nil {
		return nil
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
