package work

import (
	"reflect"

	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

// uuidExtID is the msgpack extension id used to encode uuid.UUID values inside
// job payloads. It is 3 for historical reasons: the polytomic application
// previously registered this codec from its own package, and jobs already
// persisted in Redis encode UUIDs as fixext16 with this type id. Changing it
// would make those queued jobs undecodable.
const uuidExtID = 3

// The msgpack ext codec for uuid.UUID is registered here, in the work package,
// because this is the package that owns (Un)MarshalPayload and the underlying
// marshal/unmarshal helpers. msgpack's ext registry is process-global, so the
// encoder and decoder must be registered before any payload containing a
// uuid.UUID is (de)serialized.
//
// Previously this registration lived only in the application's top-level `app`
// package as an init() side effect. Because the job enqueue paths
// (e.g. sync/executor) do not import `app`, any binary built without it -- most
// notably per-package test binaries -- had no encoder registered and serialized
// uuid.UUID ([16]byte) as msgpack bin8 (code c4) instead of the ext type
// (fixext16, code d8). The worker, which does register the ext decoder, then
// could not decode those payloads and failed with
// "invalid job payload: msgpack: invalid code=c4 decoding ext len", wedging the
// single-slot dequeuer on the undecodable job.
//
// Registering on the work package guarantees encode/decode symmetry: a payload
// cannot be (un)marshaled without importing this package, and importing it runs
// this init().
func uuidEncoder(e *msgpack.Encoder, v reflect.Value) ([]byte, error) {
	uu := v.Interface().(uuid.UUID)
	return uu.MarshalBinary()
}

func uuidDecoder(d *msgpack.Decoder, v reflect.Value, extLen int) error {
	b := make([]byte, extLen)
	if err := d.ReadFull(b); err != nil {
		return err
	}
	uu, err := uuid.FromBytes(b)
	if err != nil {
		return err
	}
	*v.Addr().Interface().(*uuid.UUID) = uu

	return nil
}

func init() {
	msgpack.RegisterExtDecoder(uuidExtID, uuid.UUID{}, uuidDecoder)
	msgpack.RegisterExtEncoder(uuidExtID, uuid.UUID{}, uuidEncoder)
}
