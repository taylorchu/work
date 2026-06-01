package work

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestUUIDPayloadUsesExtEncoding guards against the regression where uuid.UUID
// fields were serialized as msgpack bin8 (code 0xc4) instead of the registered
// fixext16 ext type (code 0xd8). A 0xc4-encoded payload is undecodable by a
// process that has the ext decoder registered, and poisons the dequeuer.
func TestUUIDPayloadUsesExtEncoding(t *testing.T) {
	type payload struct {
		ID uuid.UUID `msgpack:"id"`
	}
	id := uuid.MustParse("f32a93af-3d28-4df9-bb16-81634cfcf71f")

	job := NewJob()
	require.NoError(t, job.MarshalPayload(payload{ID: id}))

	// fixext16 framing for ext type 3: 0xd8 0x03 followed by the 16 raw bytes.
	idBytes, _ := id.MarshalBinary()
	wantFraming := append([]byte{0xd8, uuidExtID}, idBytes...)
	require.True(t, bytes.Contains(job.Payload, wantFraming),
		"expected fixext16 (0xd8 0x03) UUID framing in payload, got % x", job.Payload)
	require.NotContains(t, string(job.Payload), string([]byte{0xc4, 0x10}),
		"payload must not contain bin8 (0xc4 0x10) UUID framing")

	// And it must round-trip back to the same UUID.
	var got payload
	require.NoError(t, job.UnmarshalPayload(&got))
	require.Equal(t, id, got.ID)
}
