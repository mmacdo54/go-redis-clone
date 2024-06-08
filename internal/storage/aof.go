package storage

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
)

type Store struct {
	file   *os.File
	reader *bufio.Reader
	mutex  sync.Mutex
}

func NewStore(fileName string) (*Store, error) {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)

	if err != nil {
		return nil, err
	}

	store := &Store{file: f, reader: bufio.NewReader(f)}

	go func() {
		for {
			store.mutex.Lock()
			store.file.Sync()
			store.mutex.Unlock()
			time.Sleep(time.Second)
		}
	}()

	return store, nil
}

func (s *Store) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.file.Close()
}

func (s *Store) Write(v resp.RespValue) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, err := s.file.Write(v.Marshall()); err != nil {
		return err
	}
	return nil
}

func (s *Store) Read(fn func(v resp.RespValue)) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.file.Seek(0, io.SeekStart)
	reader := resp.NewRespReader(s.file)

	for {
		val, err := reader.ReadResp()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		fn(val)
	}

	return nil
}
