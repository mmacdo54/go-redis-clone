package storage

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

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

func (s *PostgresStore) GetByKey(kv KV) (KV, bool, error) {
	keyValue := KV{}
	res := s.database.Where("key = ?", kv.Key).Limit(1).First(&keyValue)

	if res.Error != nil && res.Error != gorm.ErrRecordNotFound {
		return keyValue, false, res.Error
	}

	if res.RowsAffected == 0 {
		return keyValue, false, nil
	}

	return keyValue, true, nil
}

func (s *PostgresStore) SetKV(kv KV) error {
	if err := s.database.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"arr", "set", "str", "exp"}),
	}).Create(&kv).Error; err != nil {
		return err
	}

	return nil
}

func (s *PostgresStore) DeleteByKey(kv KV) (int, error) {
	res := s.database.Where("key = ?", kv.Key).Delete(&KV{})

	if res.Error != nil {
		return 0, res.Error
	}

	return int(res.RowsAffected), nil
}
