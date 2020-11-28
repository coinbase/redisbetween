package proxy

import (
	"bytes"
	"fmt"
	"strconv"
	//"github.com/CodisLabs/codis/pkg/utils/bufio2"
	//"github.com/CodisLabs/codis/pkg/utils/errors"
)

const (
	minItoa = -128
	maxItoa = 32768
)

var (
	itoaOffset [maxItoa - minItoa + 1]uint32
	itoaBuffer string
)

func init() {
	var b bytes.Buffer
	for i := range itoaOffset {
		itoaOffset[i] = uint32(b.Len())
		b.WriteString(strconv.Itoa(i + minItoa))
	}
	itoaBuffer = b.String()
}

func itoa(i int64) string {
	if i >= minItoa && i <= maxItoa {
		beg := itoaOffset[i-minItoa]
		if i == maxItoa {
			return itoaBuffer[beg:]
		}
		end := itoaOffset[i-minItoa+1]
		return itoaBuffer[beg:end]
	}
	return strconv.FormatInt(i, 10)
}

type ArgEncoder struct {
	Args []interface{}
	Err  error
}

func (e *ArgEncoder) Encode(r *Message) {
	if err := e.encodeResp(r); err != nil {
		e.Err = err
	}
}

func (e *ArgEncoder) encodeMultiBulk(multi []*Message) error {
	//if err := e.bw.WriteByte(byte(TypeArray)); err != nil {
	//	return err
	//}
	return e.encodeArray(multi)
}

func (e *ArgEncoder) encodeTextBytes(b []byte) error {
	// TODO instead of []byte, should we make this string() ?
	e.Args = append(e.Args, b)
	return nil
}

func (e *ArgEncoder) encodeTextString(s string) error {
	e.Args = append(e.Args, s)
	return nil
}

func (e *ArgEncoder) encodeInt(v int64) error {
	//e.Args = append(e.Args, v)
	return e.encodeTextString(itoa(v))
}

func (e *ArgEncoder) encodeBulkBytes(b []byte) error {
	e.Args = append(e.Args, b)
	fmt.Println("bulkbytes", e.Args)
	return nil
}

func (e *ArgEncoder) encodeArray(array []*Message) error {
	if array == nil {
		return e.encodeInt(-1)
	} else {
		//if err := e.encodeInt(int64(len(array))); err != nil {
		//	return err
		//}
		for _, r := range array {
			if err := e.encodeResp(r); err != nil {
				return err
			}
		}
		return nil
	}
}

func (e *ArgEncoder) encodeResp(r *Message) error {
	fmt.Println("encodeResp", r.Value, r.Array, r.Type.String())
	switch r.Type {
	default:
		return fmt.Errorf("bad resp type %s", r.Type)
	case TypeString, TypeError, TypeInt:
		return e.encodeTextBytes(r.Value)
	case TypeBulkBytes:
		return e.encodeBulkBytes(r.Value)
	case TypeArray:
		return e.encodeArray(r.Array)
	}
}
