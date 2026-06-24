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

	defer file.Close()

	return &WAL{file: file, path: path}, nil
}

func (w *WAL) Append(rec *Record) error {
	buf := rec.Encode()
	_, err := w.file.Write(buf)

	if err != nil {
		return err
	}

	defer w.file.Close()

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
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		r = append(r, rec)
	}

	return r, nil
}
