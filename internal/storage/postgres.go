package storage

import (
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PostgresTransaction struct {
	tx *gorm.DB
}

func (t PostgresTransaction) Commit() error {
	if err := t.tx.Commit().Error; err != nil {
		return err
	}
	return nil
}

func (t PostgresTransaction) Abort() error {
	if err := t.tx.Rollback().Error; err != nil {
		return err
	}
	return nil
}

type PostgresStore struct {
	database *gorm.DB
}

func NewPostgresStore() PostgresStore {
	return PostgresStore{}
}

func (s *PostgresStore) init() error {
	dsn := "host=localhost user=redis password=redis dbname=redis port=5432"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}

	s.database = db
	if err := db.AutoMigrate(&KV{}); err != nil {
		return err
	}

	return nil
}

func (s *PostgresStore) Exists(kv KV) (bool, error) {
	res := s.database.Where("key = ?", kv.Key).First(&KV{})

	if res.Error != nil {
		return false, res.Error
	}

	return res.RowsAffected == 1, nil
}

func (s *PostgresStore) InitTransaction() (Transaction, error) {
	res := s.database.Begin()

	if res.Error != nil {
		return PostgresTransaction{}, res.Error
	}

	return PostgresTransaction{tx: res}, nil
}

func (s *PostgresStore) GetByKey(kv KV) (KV, bool, error) {
	keyValue := KV{}
	res := s.database.Where("key = ?", kv.Key).Limit(1).First(&keyValue)

	if res.Error != nil && res.Error != gorm.ErrRecordNotFound {
		return keyValue, false, res.Error
	}

	if res.RowsAffected == 0 {
		return keyValue, false, nil
	}

	now := int(time.Now().Unix()) * 1000
	if keyValue.Exp > 0 && keyValue.Exp < now {
		tx, err := s.InitTransaction()
		if err != nil {
			return KV{}, false, err
		}
		if _, err := s.DeleteByKey(KV{Key: kv.Key}, tx); err != nil {
			tx.Abort()
			return KV{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return KV{}, false, err
		}
		return KV{}, false, nil
	}

	return keyValue, true, nil
}

func (s *PostgresStore) SetKV(kv KV, t Transaction) error {
	if err := t.(PostgresTransaction).tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"typ", "arr", "set", "str", "exp"}),
	}).Create(&kv).Error; err != nil {
		t.Abort()
		return err
	}

	return nil
}

func (s *PostgresStore) DeleteByKey(kv KV, t Transaction) (int, error) {
	res := t.(PostgresTransaction).tx.Where("key = ?", kv.Key).Delete(&KV{})

	if res.Error != nil {
		t.Abort()
		return 0, res.Error
	}

	return int(res.RowsAffected), nil
}
