package config

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRegistryForLookupNonExistingEntities(t *testing.T) {
	registry := NewRegistry()

	entity, ok := registry.Lookup("hello")
	assert.False(t, ok)
	assert.Nil(t, entity)
}

func TestRegistryForRemoveNonExistingEntities(t *testing.T) {
	registry := NewRegistry()

	entity, err := registry.Remove("hello")
	assert.Error(t, err, "NOT_FOUND")
	assert.Nil(t, entity)
}

func TestRegistryForRegisterAndLookupEntity(t *testing.T) {
	registry := NewRegistry()
	entity := uuid.New().String()
	err := registry.Register("hello", entity)
	assert.NoError(t, err)

	result, ok := registry.Lookup("hello")
	assert.True(t, ok)
	assert.Equal(t, entity, result)
}

func TestRegistryForRegisterAndDeleteEntity(t *testing.T) {
	registry := NewRegistry()
	entity := uuid.New().String()
	err := registry.Register("hello", entity)
	assert.NoError(t, err)

	result, err := registry.Remove("hello")
	assert.NoError(t, err)
	assert.Equal(t, entity, result)

	result, ok := registry.Lookup("hello")
	assert.False(t, ok)
	assert.Nil(t, result)
}
