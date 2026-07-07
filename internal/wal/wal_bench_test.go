package wal

import (
	"path/filepath"
	"testing"

	"github.com/epaitoo/hermes/internal/models"
	"github.com/google/uuid"
)

func benchRecord(b *testing.B) *Record {
	b.Helper()
	rec, err := NewRecord(RecordCreated, JobCreatedPayload{
		QueueName: "email",
		Job:       models.Job{Id: uuid.New(), Name: "bench-job", Status: models.StatusPending},
	})
	if err != nil {
		b.Fatalf("new record: %v", err)
	}
	return rec
}

// BenchmarkWALAppendFsync measures a durable append: encode, write, and the
// file.Sync() that guarantees the record survives a crash. This is the real
// hot-path cost the broker pays on every submission.
func BenchmarkWALAppendFsync(b *testing.B) {
	w, err := Open(filepath.Join(b.TempDir(), "fsync.wal"))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { w.Close() })
	rec := benchRecord(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(rec); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALAppendNoFsync measures the same encode + write with the fsync
// removed. It is NOT durable; it exists only to isolate what the fsync costs.
// The gap between this and BenchmarkWALAppendFsync is the price of durability.
func BenchmarkWALAppendNoFsync(b *testing.B) {
	w, err := Open(filepath.Join(b.TempDir(), "nofsync.wal"))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { w.Close() })
	rec := benchRecord(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.file.Write(rec.Encode()); err != nil {
			b.Fatal(err)
		}
	}
}
