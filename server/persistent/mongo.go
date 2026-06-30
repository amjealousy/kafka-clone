package persistent

import (
	"context"
	"fmt"

	"kafka-clone/server/topic"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoClient struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
}

// NewMongoClient создает подключение к Mongo
func NewMongoClient(ctx context.Context, uri, dbName, collName string) (*MongoClient, error) {
	clientOpts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongo: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping mongo: %w", err)
	}

	db := client.Database(dbName)
	coll := db.Collection(collName)

	return &MongoClient{
		client:     client,
		database:   db,
		collection: coll,
	}, nil
}
func MockNewMongoClient() (*MongoClient, error) {

	return &MongoClient{
		client:     nil,
		database:   nil,
		collection: nil,
	}, nil
}

// FetchAllTopics выкачивает все топики из коллекции
func (m *MongoClient) FetchAllTopics(ctx context.Context) ([]topic.Topic, error) {
	// bson.D{} означает "выбрать все документы" (без фильтрации)
	cursor, err := m.collection.Find(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to execute find query: %w", err)
	}
	defer cursor.Close(ctx)

	var topics []topic.Topic
	// Десериализуем все документы сразу в слайс структур
	if err := cursor.All(ctx, &topics); err != nil {
		return nil, fmt.Errorf("failed to decode topics: %w", err)
	}

	return topics, nil
}

// Close закрывает соединение с БД при остановке брокера
func (m *MongoClient) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
func (m *MongoClient) UpdateTopic(ctx context.Context, topic topic.Topic) error {
	filter := bson.M{"id": topic.Id} // Ищем по ID топика

	update := bson.M{
		"$set": bson.M{
			"name":         topic.Name,
			"retention":    topic.Retention,
			"start_offset": topic.StartOffset,
		},
	}

	// upsert: true позволяет создать запись, если её не было
	opts := options.Update().SetUpsert(true)

	_, err := m.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed to update topic %s: %w", topic.Name, err)
	}

	return nil
}
