package work

import (
	"bytes"
	"io"

	"github.com/vmihailenco/msgpack/v4"
)

func marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactEncoding(true)
	enc.SortMapKeys(true)
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
	dec.UseDecodeInterfaceLoose(true)
	return dec.Decode(v)
}
