package handlers

import (
	"fmt"
	"testing"
)

var writeCommands = []struct {
	in  string
	out bool
}{
	{"SET", true},
	{"INCR", true},
	{"INCRBY", true},
	{"DEL", true},
	{"UNLINK", true},
	{"EVAL", true},
	{"EVALSHA", true},
}

var readCommands = []struct {
	in  string
	out bool
}{
	{"GET", true},
	{"MGET", true},
	{"HGET", true},
	{"KEYS", true},
	{"EVALSHA_RO", true},
}

func TestIsWriteCommand(t *testing.T) {
	for _, tt := range writeCommands {
		t.Run(fmt.Sprintf("%s is write", tt.in), func(t *testing.T) {
			out := isWriteCommand([]string{tt.in})

			if out != tt.out {
				t.Errorf("%s should be WRITE command", tt.in)
			}
		})
	}

	for _, tt := range readCommands {
		t.Run(fmt.Sprintf("%s is not write", tt.in), func(t *testing.T) {
			out := isWriteCommand([]string{tt.in})

			if out == true {
				t.Errorf("%s not should be WRITE command", tt.in)
			}
		})
	}

	for tt := range UnsupportedCommands {
		t.Run(fmt.Sprintf("%s is not write", tt), func(t *testing.T) {
			out := isWriteCommand([]string{tt})

			if out == true {
				t.Errorf("%s not should be WRITE command", tt)
			}
		})
	}

	for tt := range TransactionCommands {
		t.Run(fmt.Sprintf("%s is not write", tt), func(t *testing.T) {
			out := isWriteCommand([]string{tt})

			if out == true {
				t.Errorf("%s not should be WRITE command", tt)
			}
		})
	}
}

func TestIsReadCommand(t *testing.T) {
	for _, tt := range readCommands {
		t.Run(fmt.Sprintf("%s is read", tt.in), func(t *testing.T) {
			out := isReadCommand([]string{tt.in})

			if out != tt.out {
				t.Errorf("%s should be READ command", tt.in)
			}
		})
	}

	for _, tt := range writeCommands {
		t.Run(fmt.Sprintf("%s is not read", tt.in), func(t *testing.T) {
			out := isReadCommand([]string{tt.in})

			if out == true {
				t.Errorf("%s should not be READ command", tt.in)
			}
		})
	}

	for tt := range UnsupportedCommands {
		t.Run(fmt.Sprintf("%s is not read", tt), func(t *testing.T) {
			out := isReadCommand([]string{tt})

			if out == true {
				t.Errorf("%s not should be READ command", tt)
			}
		})
	}

	for tt := range TransactionCommands {
		t.Run(fmt.Sprintf("%s is not read", tt), func(t *testing.T) {
			out := isReadCommand([]string{tt})

			if out == true {
				t.Errorf("%s not should be READ command", tt)
			}
		})
	}
}
