/*
Copyright 2021 The OpenEBS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package watchman

import (
	"bytes"
	"crypto/md5"
	"errors"
	"io/ioutil"
	"os"
	"time"
)

type WatchType int
type FileData []byte
type FileReader func(file *os.File) (FileData, error)
type File struct {
	fileName string
	fileData FileData
	reader   FileReader
	tag      string
}

type Event struct {
	files []*File
}
type Watchman struct {
	files        []*File
	ticker       *time.Ticker
	pollInterval time.Duration
	eventChan    chan Event
	errChan      chan error
	stopChan     chan struct{}
}

type NewOpt func(w *Watchman) error
type NewFileOpt func(f *File) error
type FileFilter func(f *File) bool

var (
	ErrInvalidFd       = errors.New("invalid file descriptor")
	ErrInvalidFile     = errors.New("invalid file")
	ErrInvalidDuration = errors.New("invalid duration")
)

func New(opts ...NewOpt) *Watchman {
	w := Watchman{}
	for _, opt := range opts {
		opt(&w)
	}

	// set defaults
	if w.pollInterval == 0 {
		w.pollInterval = 10 * time.Second
	}
	return &w
}

func NewFile(fileName string, opts ...NewFileOpt) (*File, error) {
	var err error
	f := File{}

	_, err = os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	f.fileName = fileName

	for _, opt := range opts {
		err = opt(&f)
		if err != nil {
			return nil, err
		}
	}

	// set defaults
	if f.reader == nil {
		f.reader = ReadFile
	}

	// read initial file data
	openFile, err := os.Open(f.fileName)
	if err != nil {
		return nil, err
	}
	data, err := f.reader(openFile)
	if err != nil {
		return nil, err
	}
	f.fileData = data

	return &f, nil
}

func WithPollInterval(duration time.Duration) NewOpt {
	return func(w *Watchman) error {
		if duration == 0 {
			return ErrInvalidDuration
		}
		w.pollInterval = duration
		return nil
	}
}

func WithReader(reader FileReader) NewFileOpt {
	return func(f *File) error {
		f.reader = reader
		return nil
	}
}

func WithTag(tag string) NewFileOpt {
	return func(f *File) error {
		f.tag = tag
		return nil
	}
}

func (w *Watchman) Start() (<-chan Event, <-chan error) {
	w.ticker = time.NewTicker(w.pollInterval)
	w.eventChan = make(chan Event)
	w.errChan = make(chan error)
	w.stopChan = make(chan struct{})
	go func() {
		for {
			select {
			case <-w.stopChan:
				return
			case <-w.ticker.C:
				w.publishChanges()
			}
		}
	}()
	return w.eventChan, w.errChan
}

func (w *Watchman) Stop() {
	close(w.stopChan)
	close(w.errChan)
	close(w.eventChan)
	w.ticker.Stop()
}

func (w *Watchman) publishChanges() {
	event := Event{}
	for _, f := range w.files {
		openFile, err := os.Open(f.fileName)
		if err != nil {
			w.errChan <- err
			continue
		}

		data, err := f.reader(openFile)
		if err != nil {
			w.errChan <- err
		} else if !bytes.Equal(data, f.fileData) {
			event.files = append(event.files, f)
			f.fileData = data
		}
		openFile.Close()
	}

	if len(event.files) > 0 {
		w.eventChan <- event
	}
}

func (w *Watchman) AddFile(file *File) error {
	w.files = append(w.files, file)
	return nil
}

func (w *Watchman) RemoveFile(file *File) {
	for idx, f := range w.files {
		if f == file {
			w.files[idx] = w.files[len(w.files)-1]
			w.files = w.files[:len(w.files)-1]
			return
		}
	}
}

func (w *Watchman) Events() <-chan Event {
	return w.eventChan
}

func (w *Watchman) Err() <-chan error {
	return w.errChan
}

func (w *Watchman) Find(filter FileFilter) []*File {
	var ret []*File
	for _, f := range w.files {
		if filter(f) {
			ret = append(ret, f)
		}
	}
	return ret
}

func MD5Checksum(f *os.File) (FileData, error) {
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	checksum := md5.Sum(data)
	return checksum[:], nil
}

func ReadFile(f *os.File) (FileData, error) {
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (f *File) SetTag(tag string) {
	f.tag = tag
}

func (f *File) GetTag() string {
	return f.tag
}

func (e Event) Files() []*File {
	return e.files
}
