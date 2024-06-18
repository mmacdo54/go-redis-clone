package storage

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBObject struct{}

type MongoStore struct {
	database *mongo.Collection
}

func NewMongoStore() MongoStore {
	return MongoStore{}
}

func (s *MongoStore) init() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://root:example@localhost:27017"))
	if err != nil {
		return err
	}
	s.database = client.Database("redis").Collection("key-value")
	return nil
}

func (s *MongoStore) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.database.Database().Client().Disconnect(ctx); err != nil {
		return err
	}
	return nil
}

func (s *MongoStore) Create(kv KV) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.database.InsertOne(ctx, bson.D{{"key", kv.Key}, {"str", kv.Str}})
	if err != nil {
		return err
	}
	return nil
}

func (s *MongoStore) Exists(kv KV) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.database.FindOne(ctx, bson.D{{"key", kv.Key}}).Err()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *MongoStore) GetByKey(kv KV) (KV, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res KV
	err := s.database.FindOne(ctx, bson.D{{"key", kv.Key}}).Decode(&res)

	if err != nil && err != mongo.ErrNoDocuments {
		return res, false, err
	}

	if err == mongo.ErrNoDocuments {
		return res, false, nil
	}

	return res, true, nil
}

func (s *MongoStore) SetKV(kv KV) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	upsert := true
	_, err := s.database.UpdateOne(ctx, bson.D{{"key", kv.Key}}, bson.D{{"$set", kv}}, &options.UpdateOptions{Upsert: &upsert})

	if err != nil {
		return err
	}

	return nil
}

func (s *MongoStore) DeleteByKey(kv KV) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := s.database.DeleteOne(ctx, bson.D{{"key", kv.Key}})

	if err != nil {
		return 0, err
	}

	return int(res.DeletedCount), nil
}
