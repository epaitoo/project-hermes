package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"time"

	"github.com/epaitoo/hermes/internal/models"
	"github.com/google/uuid"
)

var ErrChecksumMismatch = errors.New("wal: record checksum mismatch")

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

	var contents []byte
	contents = append(contents, byte(rec.Type))
	contents = append(contents, rec.Payload...)

	length := len(contents)
	checkSum := crc32.ChecksumIEEE(contents)

	buf := make([]byte, 0, 8+length)
	buf = binary.BigEndian.AppendUint32(buf, uint32(length))
	buf = binary.BigEndian.AppendUint32(buf, uint32(checkSum))
	buf = append(buf, contents...)
	return buf
}

func Decode(r io.Reader) (*Record, error) {
	lengthBuf := make([]byte, 8)
	_, err := io.ReadFull(r, lengthBuf)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lengthBuf[0:4])
	wantChecksum := binary.BigEndian.Uint32(lengthBuf[4:8])

	//  holds the record body
	content := make([]byte, length)

	_, err = io.ReadFull(r, content)
	if err != nil {
		return nil, err
	}

	// verify checksum
	got := crc32.ChecksumIEEE(content)
	if got != wantChecksum {
		return nil, ErrChecksumMismatch
	}

	// extract type and payload from byte
	recordType := RecordType(content[0])
	payload := content[1:]

	return &Record{Type: recordType, Payload: payload}, nil

}

func NewRecord(t RecordType, payload any) (*Record, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &Record{Type: t, Payload: data}, nil
}
