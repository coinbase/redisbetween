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
