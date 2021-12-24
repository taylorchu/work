package http

import (
	"encoding/json"
	"fmt"
	"time"
)

type duration time.Duration

func (d duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch v := v.(type) {
	case string:
		tmp, err := time.ParseDuration(v)
		if err != nil {
			return err
		}
		*d = duration(tmp)
		return nil
	default:
		return fmt.Errorf("invalid duration: %v", v)
	}
}
