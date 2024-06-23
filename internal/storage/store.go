package storage

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/lib/pq"
)

type Store interface {
	init() error
	Exists(KV) (bool, error)
	GetByKey(KV) (KV, bool, error)
	SetKV(KV) error
	DeleteByKey(KV) (int, error)
}

type JSONB map[string]interface{}

func (a JSONB) Value() (driver.Value, error) {
	return json.Marshal(a)
}

func (a *JSONB) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("type assertion to []byte failed")
	}
	return json.Unmarshal(b, &a)
}

type KV struct {
	Typ string `gorm:"not null"`
	Key string `gorm:"index:idx_name,unique;not null"`
	Str string
	Arr pq.StringArray `gorm:"type:varchar[]"`
	Set JSONB
	Exp int `gorm:"not null"`
}

func InitStore() (Store, error) {
	s := NewPostgresStore()

	if err := s.init(); err != nil {
		return &s, err
	}

	return &s, nil
}
