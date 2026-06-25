package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/epaitoo/hermes/internal/models"
	"github.com/google/uuid"
)

func TestReplayTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Write three good records.
	for i := 0; i < 3; i++ {
		rec, err := NewRecord(RecordCreated, JobCreatedPayload{
			QueueName: "email",
			Job:       models.Job{Id: uuid.New(), Name: "job", Status: models.StatusPending},
		})
		if err != nil {
			t.Fatalf("new record: %v", err)
		}
		if err := w.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	w.Close()

	// Simulate a crash mid-write: chop the last few bytes off the file,
	// leaving a half-written final record.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-5); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Replay should keep the intact records and silently drop the torn tail.
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	recs, err := w2.Replay()
	if err != nil {
		t.Fatalf("replay returned error on torn tail, want nil: %v", err)
	}

	// We wrote 3, corrupted the last one, so 2 should survive.
	if len(recs) != 2 {
		t.Errorf("got %d records, want 2", len(recs))
	}
}
