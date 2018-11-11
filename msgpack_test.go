package work

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshal(t *testing.T) {
	job := NewJob()
	b, err := json.Marshal(job)
	require.NoError(t, err)
	t.Log("json", len(b))

	b, err = marshal(job)
	require.NoError(t, err)
	t.Log("msgpack", len(b))
}
