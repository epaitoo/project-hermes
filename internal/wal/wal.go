package wal

import (
	"errors"
	"io"
	"os"
)

type WAL struct {
	file *os.File
	path string
}

func Open(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		return nil, err
	}

	return &WAL{file: file, path: path}, nil
}

func (w *WAL) Append(rec *Record) error {
	buf := rec.Encode()
	_, err := w.file.Write(buf)

	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Replay() ([]*Record, error) {
	var r []*Record

	f, err := os.Open(w.path)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	for {
		rec, err := Decode(f)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrChecksumMismatch) {
			break
		}

		// a torn or corrupt record can only be the last one,
		// since we append and fsync each write
		// so stopping here and keeping prior records is safe

		if err != nil {
			return nil, err
		}

		r = append(r, rec)
	}

	return r, nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}
