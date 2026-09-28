package dto

import (
	"errors"
	"strings"
)

const MaxMessageSize = 64 * 1024

type ProduceRequest struct {
	TopicName   string `json:"topic_name"`
	PartitionID int64  `json:"partition_id"`
	Key         string `json:"key,omitempty"`
	Msg         []byte `json:"msg"`
}
type ProduceResponse struct {
	Status        string `json:"status"`
	StatusMessage string `json:"status_message,omitempty"`
}
type ConsumeRequest struct {
	TopicName   string `json:"topic_name"`
	PartitionID int64  `json:"partition_id"`

	StartOffset   *uint64 `json:"start_offset,omitempty,string"`
	FromBeginning bool    `json:"from_beginning,omitempty"`

	FinOffset *uint64 `json:"fin_offset,omitempty,string"`
	TillEnd   bool    `json:"till_end,omitempty"`
}
type ConsumeResponse struct {
	TopicName     string
	PartitionId   string
	StartPosition string
	EndPosition   string
}
type ConsumeResponseList struct {
	List  []ConsumeResponse `json:"list"`
	Error *string           `json:"error,omitempty"`
}

// CreateTopicRequest — запрос на создание топика через HTTP control-plane API.
type CreateTopicRequest struct {
	TopicName     string `json:"topic_name"`
	NumPartitions int    `json:"num_partitions"`
	// ReplicationFactor == 0 означает "по умолчанию" (лидер + 2 реплики),
	// решение принимает контроллер.
	ReplicationFactor int `json:"replication_factor,omitempty"`
}

const MaxTopicNameLength = 249

func (r CreateTopicRequest) Validate() error {
	if r.TopicName == "" {
		return errors.New("topic_name is required")
	}
	if len(r.TopicName) > MaxTopicNameLength {
		return errors.New("topic_name exceeds maximum length")
	}
	if strings.ContainsAny(r.TopicName, "/ \t\n") {
		// Имя топика становится частью ключа etcd (/kafka/topic/<name>),
		// поэтому разделитель внутри имени сломал бы разбор префикса.
		return errors.New("topic_name must not contain spaces or slashes")
	}
	if r.NumPartitions <= 0 {
		return errors.New("num_partitions must be greater than zero")
	}
	if r.ReplicationFactor < 0 {
		return errors.New("replication_factor must not be negative")
	}
	return nil
}

func (r ProduceRequest) Validate() error {
	if r.TopicName == "" {
		return errors.New("topic_name is required")
	}
	if r.PartitionID < 0 {
		return errors.New("partition_id must be greater than or equal to zero")
	}
	if r.Msg == nil {
		return errors.New("msg is required")
	}
	if len(r.Msg) > MaxMessageSize {
		return errors.New("msg exceeds maximum size")
	}
	return nil
}

func (r ConsumeRequest) Validate() error {
	if r.TopicName == "" {
		return errors.New("topic_name is required")
	}
	if r.PartitionID < 0 {
		return errors.New("partition_id must be greater than or equal to zero")
	}
	if (r.StartOffset == nil) == !r.FromBeginning {
		return errors.New("exactly one of start_offset or from_beginning is required")
	}
	if (r.FinOffset == nil) == !r.TillEnd {
		return errors.New("exactly one of fin_offset or till_end is required")
	}
	if r.StartOffset != nil && r.FinOffset != nil && *r.FinOffset < *r.StartOffset {
		return errors.New("fin_offset must be greater than or equal to start_offset")
	}
	return nil
}
