package config

import (
	"errors"
	"sync"
)

type Registry interface {
	Lookup(name string) (interface{}, bool)
	Register(name string, entity interface{}) error
	Remove(name string) (interface{}, error)
}

type upstreamRegistry struct {
	store map[string]interface{}
	sync.RWMutex
}

func (u *upstreamRegistry) Register(name string, entity interface{}) error {
	u.Lock()
	defer u.Unlock()
	u.store[name] = entity
	return nil
}

func (u *upstreamRegistry) Lookup(name string) (interface{}, bool) {
	u.RLock()
	defer u.RUnlock()
	if entity, ok := u.store[name]; ok {
		return entity, true
	}

	return nil, false
}

func (u *upstreamRegistry) Remove(name string) (interface{}, error) {
	u.Lock()
	defer u.Unlock()
	if entity, ok := u.store[name]; ok {
		delete(u.store, name)
		return entity, nil
	}

	return nil, errors.New("NOT_FOUND")
}

func NewRegistry() Registry {
	return &upstreamRegistry{store: map[string]interface{}{}}
}
