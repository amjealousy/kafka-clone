package db

import (
	"context"
	"fmt"
	"time"

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

// localPartitionsCollection — имя коллекции с манифестом локально размещённых
// на нодах topic/partition (по одной записи на пару node+topic+partition).
const localPartitionsCollection = "local_partitions"

// LocalPartitionRecord — запись манифеста: "нода X когда-то создала у себя
// топик Topic, партицию PartitionID". Используется при рестарте ноды, чтобы
// заново поднять TopicProcessor/PartitionProcessor под уже существующие на
// диске сегменты, не дожидаясь событий из etcd.
type LocalPartitionRecord struct {
	NodeID      int       `bson:"node_id"`
	Topic       string    `bson:"topic"`
	PartitionID int       `bson:"partition_id"`
	RetentionNs int64     `bson:"retention_ns"`
	UpdatedAt   time.Time `bson:"updated_at"`
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
	if m.client == nil {
		return nil // mock-клиент без реального подключения
	}
	return m.client.Disconnect(ctx)
}

func (m *MongoClient) UpdateTopic(ctx context.Context, topic topic.Topic) error {
	if m.collection == nil {
		return nil // mock-клиент: no-op
	}
	filter := bson.M{"id": topic.Id} // Ищем по ID топика

	update := bson.M{
		"$set": bson.M{
			"name":       topic.Name,
			"partitions": topic.Partitions,
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

// UpsertLocalPartition сохраняет/обновляет запись манифеста о том, что данная
// нода локально разместила у себя указанный topic+partition. Вызывается один
// раз при фактическом создании партиции (см. Broker.EnsureLocalPartition).
func (m *MongoClient) UpsertLocalPartition(ctx context.Context, rec LocalPartitionRecord) error {
	if m.database == nil {
		return nil // mock-клиент: no-op
	}
	coll := m.database.Collection(localPartitionsCollection)

	filter := bson.M{
		"node_id":      rec.NodeID,
		"topic":        rec.Topic,
		"partition_id": rec.PartitionID,
	}
	update := bson.M{
		"$set": bson.M{
			"retention_ns": rec.RetentionNs,
			"updated_at":   time.Now(),
		},
	}
	opts := options.Update().SetUpsert(true)

	_, err := coll.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed to upsert local partition manifest (node=%d topic=%s partition=%d): %w",
			rec.NodeID, rec.Topic, rec.PartitionID, err)
	}
	return nil
}

// ListLocalPartitions возвращает все записи манифеста для указанной ноды —
// используется при старте, чтобы восстановить локально принадлежащие ей
// topic/partition (см. Broker.LoadLocalPartitionsFromMongo).
func (m *MongoClient) ListLocalPartitions(ctx context.Context, nodeID int) ([]LocalPartitionRecord, error) {
	if m.database == nil {
		return nil, nil // mock-клиент: пусто
	}
	coll := m.database.Collection(localPartitionsCollection)

	cursor, err := coll.Find(ctx, bson.M{"node_id": nodeID})
	if err != nil {
		return nil, fmt.Errorf("failed to query local partitions manifest for node %d: %w", nodeID, err)
	}
	defer cursor.Close(ctx)

	var records []LocalPartitionRecord
	if err := cursor.All(ctx, &records); err != nil {
		return nil, fmt.Errorf("failed to decode local partitions manifest: %w", err)
	}
	return records, nil
}
