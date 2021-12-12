package http

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDuration(t *testing.T) {
	d := Duration(time.Minute)
	b, err := d.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "\"1m0s\"", string(b))

	var d2 Duration
	err = d2.UnmarshalJSON(b)
	require.NoError(t, err)
	require.Equal(t, time.Minute, time.Duration(d2))

	var d3 Duration
	err = d3.UnmarshalJSON([]byte("1"))
	require.Error(t, err)
	require.Equal(t, "invalid duration: 1", err.Error())

	var d4 Duration
	err = d4.UnmarshalJSON([]byte("\"bad\""))
	require.Error(t, err)
	require.Equal(t, "time: invalid duration \"bad\"", err.Error())
}
