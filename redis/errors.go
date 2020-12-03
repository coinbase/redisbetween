package redis

import "fmt"

// ConnectionError represents a connection error.
type ConnectionError struct {
	Address string
	ID      uint64
	Wrapped error
	Message string
}

// Error implements the error interface.
func (e ConnectionError) Error() string {
	if e.Wrapped != nil {
		return fmt.Sprintf("connection(%s[%d]) %s: %s", e.Address, e.ID, e.Message, e.Wrapped.Error())
	}
	return fmt.Sprintf("connection(%s[%d]) %s", e.Address, e.ID, e.Message)
}
