package storage

type Store interface {
	init() error
	Close() error
	Create(KV) error
	Exists(KV) (bool, error)
	GetByKey(KV) (KV, bool, error)
	SetKV(KV) error
	DeleteByKey(KV) (int, error)
}

type KV struct {
	Typ string
	Key string
	Str string
	Arr []string
	Exp int
}

func InitStore() (Store, error) {
	// TODO ADD OTHER STORE OPTIONS
	s := NewMongoStore()

	if err := s.init(); err != nil {
		return &s, err
	}

	return &s, nil
}
