package topic

import "time"

type Topic struct {
	Id          int
	Name        string
	Retention   time.Duration
	StartOffset uint64
}
type Message struct {
	Offset    uint64
	Timestamp int64
	Payload   []byte
}
