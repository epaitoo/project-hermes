package wal

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"time"

	"github.com/epaitoo/hermes/internal/models"
	"github.com/google/uuid"
)

type RecordType uint8

const (
	RecordCreated RecordType = iota
	RecordDone
	RecordFailed
	RecordMovedToDLQ
	RecordRedrive
	RecordDiscard
)

type Record struct {
	Type    RecordType
	Payload []byte
}

type JobCreatedPayload struct {
	QueueName string
	Job       models.Job
}

type JobFailedPayload struct {
	JobID      uuid.UUID
	RetryCount int
	NextRunAt  time.Time
}

type JobDonePayload struct {
	JobID     uuid.UUID
	QueueName string
}

type JobMovedToDLQPayload struct {
	JobID     uuid.UUID
	QueueName string
}

type JobRedrivePayload struct {
	JobID     uuid.UUID
	QueueName string
}

type JobDiscardPayload struct {
	JobID     uuid.UUID
	QueueName string
}

func (rec *Record) Encode() []byte {
	length := 1 + len(rec.Payload)
	buf := make([]byte, 0, 4+length)
	buf = binary.BigEndian.AppendUint32(buf, uint32(length))
	buf = append(buf, byte(rec.Type))
	buf = append(buf, rec.Payload...)
	return buf
}

func Decode(r io.Reader) (*Record, error) {
	lengthBuf := make([]byte, 4)
	_, err := io.ReadFull(r, lengthBuf)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lengthBuf)

	//  holds the record body
	buf := make([]byte, length)

	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	// extract type and payload from byte
	recordType := RecordType(buf[0])
	payload := buf[1:]

	return &Record{Type: recordType, Payload: payload}, nil

}

func NewRecord(t RecordType, payload any) (*Record, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &Record{Type: t, Payload: data}, nil
}
