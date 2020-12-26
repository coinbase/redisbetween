package redis

import (
	"fmt"
	"strings"
)

type MsgType byte

const (
	TypeString    MsgType = '+'
	TypeError     MsgType = '-'
	TypeInt       MsgType = ':'
	TypeBulkBytes MsgType = '$'
	TypeArray     MsgType = '*'
)

var NoKeyCommands = map[string]bool{
	"ASKING":       true,
	"AUTH":         true,
	"BGREWRITEAOF": true,
	"BGSAVE":       true,
	"CLIENT":       true,
	"CLUSTER":      true,
	"COMMAND":      true,
	"CONFIG":       true,
	"DBSIZE":       true,
	"DEBUG":        true,
	"DISCARD":      true,
	"ECHO":         true,
	"EVAL":         true,
	"EVALSHA":      true,
	"EXEC":         true,
	"FLUSHALL":     true,
	"FLUSHDB":      true,
	"INFO":         true,
	"KEYS":         true,
	"LASTSAVE":     true,
	"MIGRATE":      true,
	"MONITOR":      true,
	"MULTI":        true,
	"OBJECT":       true,
	"PING":         true,
	"QUIT":         true,
	"RANDOMKEY":    true,
	"READONLY":     true,
	"READWRITE":    true,
	"ROLE":         true,
	"SAVE":         true,
	"SCAN":         true,
	"SCRIPT":       true,
	"SELECT":       true,
	"SENTINEL":     true,
	"SHUTDOWN":     true,
	"SLAVEOF":      true,
	"SLOWLOG":      true,
	"SWAPDB":       true,
	"SYNC":         true,
	"TIME":         true,
	"UNWATCH":      true,
	"WAIT":         true,
	"WATCH":        true,
}

func (t MsgType) String() string {
	switch t {
	case TypeString:
		return "<string>"
	case TypeError:
		return "<error>"
	case TypeInt:
		return "<int>"
	case TypeBulkBytes:
		return "<bulkbytes>"
	case TypeArray:
		return "<array>"
	default:
		return fmt.Sprintf("<unknown-0x%02x>", byte(t))
	}
}

type Message struct {
	Type MsgType

	Value []byte
	Array []*Message
}

func (r *Message) IsString() bool {
	return r.Type == TypeString
}

func (r *Message) IsError() bool {
	return r.Type == TypeError
}

func (r *Message) IsInt() bool {
	return r.Type == TypeInt
}

func (r *Message) IsBulkBytes() bool {
	return r.Type == TypeBulkBytes
}

func (r *Message) IsArray() bool {
	return r.Type == TypeArray
}

// convenience function for testing only
func (r *Message) String() string {
	bytes, _ := EncodeToBytes(r)
	return strings.ReplaceAll(string(bytes), "\r\n", " \\r\\n ")
}

// returns all the keys touched by the command, in order. assumes this message is a command
func (r *Message) Keys() [][]byte {
	if !r.IsArray() {
		return nil
	}
	cmd := strings.ToUpper(string(r.Array[0].Value))
	args := r.Array[1:]
	if cmd == "BITOP" && len(args) > 1 {
		return values(args[1:])
	} else if NoKeyCommands[cmd] || len(args) == 0 {
		return nil
	}
	return values(args)
}

func values(mm []*Message) [][]byte {
	out := make([][]byte, len(mm))
	for i, m := range mm {
		out[i] = m.Value
	}
	return out
}

// convenience function for making commands, which are just flat arrays
func NewCommand(parts ...string) *Message {
	p := make([]*Message, len(parts))
	for i, pp := range parts {
		p[i] = NewBulkBytes([]byte(pp))
	}
	return NewArray(p)
}

func NewString(value []byte) *Message {
	r := &Message{}
	r.Type = TypeString
	r.Value = value
	return r
}

func NewError(value []byte) *Message {
	r := &Message{}
	r.Type = TypeError
	r.Value = value
	return r
}

func NewErrorf(format string, args ...interface{}) *Message {
	return NewError([]byte(fmt.Sprintf(format, args...)))
}

func NewInt(value []byte) *Message {
	r := &Message{}
	r.Type = TypeInt
	r.Value = value
	return r
}

func NewBulkBytes(value []byte) *Message {
	r := &Message{}
	r.Type = TypeBulkBytes
	r.Value = value
	return r
}

func NewArray(array []*Message) *Message {
	r := &Message{}
	r.Type = TypeArray
	r.Array = array
	return r
}
