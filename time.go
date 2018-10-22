package work

import (
	"encoding/json"
	"time"
)

// Time marshals and unmarshals time.Time to unix timestamp.
type Time struct {
	time.Time
}

// NewTime creates a new Time set to time.Now.
func NewTime() Time {
	return Time{time.Now().Truncate(time.Second)}
}

// MarshalJSON marshals Time to unix timestamp.
func (t *Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Unix())
}

// UnmarshalJSON unmarshals Time from unix timestamp.
func (t *Time) UnmarshalJSON(b []byte) error {
	var ts int64
	err := json.Unmarshal(b, &ts)
	if err != nil {
		return err
	}
	t.Time = time.Unix(ts, 0)
	return nil
}
