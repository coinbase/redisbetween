// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"math"
	"strconv"
	"testing"
)

var tmap = make(map[int64][]byte)

func init() {
	var n = len(itoaOffset)*2 + 100000
	for i := -n; i <= n; i++ {
		tmap[int64(i)] = []byte(strconv.Itoa(int(i)))
	}
	for i := math.MinInt64; i != 0; i = int(float64(i) / 1.1) {
		tmap[int64(i)] = []byte(strconv.Itoa(int(i)))
	}
	for i := math.MaxInt64; i != 0; i = int(float64(i) / 1.1) {
		tmap[int64(i)] = []byte(strconv.Itoa(int(i)))
	}
}

func TestBtoi64(t *testing.T) {
	for i, b := range tmap {
		v, err := Btoi64(b)
		assert.Nil(t, err)
		assert.Equal(t, i, v)
	}
}

func TestDecodeInvalidRequests(t *testing.T) {
	test := []string{
		"*hello\r\n",
		"*-100\r\n",
		"*3\r\nhi",
		"*3\r\nhi\r\n",
		"*4\r\n$1",
		"*4\r\n$1\r",
		"*4\r\n$1\n",
		"*2\r\n$3\r\nget\r\n$what?\r\nx\r\n",
		"*4\r\n$3\r\nget\r\n$1\r\nx\r\n",
		"*2\r\n$3\r\nget\r\n$1\r\nx",
		"*2\r\n$3\r\nget\r\n$1\r\nx\r",
		"*2\r\n$3\r\nget\r\n$100\r\nx\r\n",
		"$6\r\nfoobar\r",
		"$0\rn\r\n",
		"$-1\n",
		"*0",
		"*2n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"*-\r\n",
		"+OK\n",
		"-Error message\r",
	}
	for _, s := range test {
		_, err := DecodeFromBytes([]byte(s))
		assert.NotNil(t, err)
	}
}

func TestDecodeSimpleRequest1(t *testing.T) {
	_, err := DecodeFromBytes([]byte("\r\n"))
	assert.NotNil(t, err)
}

func TestDecodeSimpleRequest2(t *testing.T) {
	test := []string{
		"hello world\r\n",
		"hello world    \r\n",
		"    hello world    \r\n",
		"    hello     world\r\n",
		"    hello     world    \r\n",
	}
	for _, s := range test {
		a, err := DecodeMultiBulkFromBytes([]byte(s))

		assert.NoError(t, err)
		assert.Len(t, a, 2)
		assert.True(t, bytes.Equal(a[0].Value, []byte("hello")))
		assert.True(t, bytes.Equal(a[1].Value, []byte("world")))
	}
}

func TestDecodeSimpleRequest3(t *testing.T) {
	test := []string{"\r", "\n", " \n"}
	for _, s := range test {
		_, err := DecodeFromBytes([]byte(s))
		assert.NotNil(t, err)
	}
}

func TestDecodeBulkBytes(t *testing.T) {
	test := "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"
	resp, err := DecodeFromBytes([]byte(test))
	assert.NoError(t, err)
	assert.Len(t, resp.Array, 2)
	s1 := resp.Array[0]
	assert.True(t, bytes.Equal(s1.Value, []byte("LLEN")))
	s2 := resp.Array[1]
	assert.True(t, bytes.Equal(s2.Value, []byte("mylist")))
}

func TestDecoder(t *testing.T) {
	test := []string{
		"$6\r\nfoobar\r\n",
		"$0\r\n\r\n",
		"$-1\r\n",
		"*0\r\n",
		"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"*3\r\n:1\r\n:2\r\n:3\r\n",
		"*-1\r\n",
		"+OK\r\n",
		"-Error message\r\n",
		"*2\r\n$1\r\n0\r\n*0\r\n",
		"*3\r\n$4\r\nEVAL\r\n$31\r\nreturn {1,2,{3,'Hello World!'}}\r\n$1\r\n0\r\n",
	}
	for _, s := range test {
		_, err := DecodeFromBytes([]byte(s))
		assert.NoError(t, err)
	}
}
