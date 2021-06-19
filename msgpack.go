package work

import (
	"bytes"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

func marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)
	enc.UseCompactFloats(true)
	enc.SetSortMapKeys(true)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type bufReader interface {
	io.Reader
	io.ByteScanner
}

func unmarshal(r bufReader, v interface{}) error {
	dec := msgpack.NewDecoder(r)
	dec.UseLooseInterfaceDecoding(true)
	return dec.Decode(v)
}
