package topic

type Topic struct {
	Id         int
	Name       string
	Partitions []*Partition
}
type Message struct {
	Offset    uint64
	Timestamp int64
	Payload   []byte
}

type Offset struct {
	Value uint64
	Tag   Alias
}

type Alias string

const (
	FromBeginning Alias = "Beginning"
	TillEnd       Alias = "End" // also means Broker need to stream til client break a net connection
)
