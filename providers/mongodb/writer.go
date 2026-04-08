//go:build mongodb

package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// mongoDBWriter persists MigrationUnits to a MongoDB destination using bulk
// operations. It supports InsertMany for new documents and upsert for
// conflict handling.
type mongoDBWriter struct {
	database *mongo.Database
	opts     provider.WriteOptions
	written  int64
	failed   int64
	skipped  int64
	bytes    int64
	log      interface{ Info(msg string, args ...any) }
}

func newMongoDBWriter(database *mongo.Database, opts provider.WriteOptions) *mongoDBWriter {
	return &mongoDBWriter{
		database: database,
		opts:     opts,
		log:      logger.L().With("component", "mongodb-writer"),
	}
}

// Write persists a batch of MigrationUnits to the destination MongoDB.
// It groups documents by collection and uses bulk operations for performance.
func (w *mongoDBWriter) Write(ctx context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	start := time.Now()
	prevWritten, prevFailed, prevSkipped, prevBytes := w.written, w.failed, w.skipped, w.bytes

	if len(units) == 0 {
		return &provider.BatchResult{
			TotalUnits: 0,
			Duration:   time.Since(start),
		}, nil
	}

	// Group documents by collection
	collectionDocs := make(map[string][]mongoDocument)
	for i := range units {
		unit := &units[i]

		// Decode the document
		doc, err := decodeMongoDocument(unit.Data)
		if err != nil {
			w.failed++
			w.log.Info("failed to decode document", "key", unit.Key, "error", err)
			continue
		}

		collectionDocs[doc.Collection] = append(collectionDocs[doc.Collection], *doc)
	}

	var failedKeys []string
	var errors []error

	// Write each collection's documents
	for collection, docs := range collectionDocs {
		if err := w.writeCollection(ctx, collection, docs, &failedKeys, &errors); err != nil {
			w.log.Info("failed to write collection", "collection", collection, "error", err)
		}
	}

	return &provider.BatchResult{
		TotalUnits:   len(units),
		WrittenUnits: int(w.written - prevWritten),
		FailedUnits:  int(w.failed - prevFailed),
		SkippedUnits: int(w.skipped - prevSkipped),
		FailedKeys:   failedKeys,
		BytesWritten: w.bytes - prevBytes,
		Duration:     time.Since(start),
		Errors:       errors,
	}, nil
}

// Flush is a no-op for MongoDB since writes are immediate.
func (w *mongoDBWriter) Flush(_ context.Context) error {
	return nil
}

// writeCollection writes documents to a specific collection.
func (w *mongoDBWriter) writeCollection(ctx context.Context, collection string, docs []mongoDocument, failedKeys *[]string, errors *[]error) error {
	coll := w.database.Collection(collection)

	// Check conflict strategy
	if w.opts.OnConflict == provider.ConflictSkip {
		return w.writeWithSkip(ctx, coll, docs, failedKeys, errors)
	}

	// Default: overwrite (upsert)
	return w.writeWithUpsert(ctx, coll, docs, failedKeys, errors)
}

// writeWithUpsert uses upsert operations to insert or update documents.
func (w *mongoDBWriter) writeWithUpsert(ctx context.Context, coll *mongo.Collection, docs []mongoDocument, failedKeys *[]string, errors *[]error) error {
	// Prepare bulk write models
	var writeModels []mongo.WriteModel

	for _, doc := range docs {
		// Get document ID for upsert filter
		docID, err := extractDocumentID(doc.Data)
		if err != nil {
			w.failed++
			key := formatDocumentID(doc.DocumentID)
			*failedKeys = append(*failedKeys, key)
			*errors = append(*errors, fmt.Errorf("extract id: %w", err))
			continue
		}

		// Create upsert model
		filter := bson.M{"_id": docID}
		update := bson.M{"$set": doc.Data}
		model := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true)

		writeModels = append(writeModels, model)
		w.bytes += int64(len(doc.Data))
	}

	if len(writeModels) == 0 {
		return nil
	}

	// Execute bulk write
	bulkOpts := options.BulkWrite().SetOrdered(false)
	result, err := coll.BulkWrite(ctx, writeModels, bulkOpts)
	if err != nil {
		w.failed += int64(len(writeModels))
		w.log.Info("bulk write failed", "collection", coll.Name(), "error", err)
		return err
	}

	w.written += result.UpsertedCount + result.ModifiedCount
	w.skipped += int64(len(writeModels)) - result.UpsertedCount - result.ModifiedCount

	return nil
}

// writeWithSkip checks for existing documents and skips them.
func (w *mongoDBWriter) writeWithSkip(ctx context.Context, coll *mongo.Collection, docs []mongoDocument, failedKeys *[]string, errors *[]error) error {
	for _, doc := range docs {
		docID, err := extractDocumentID(doc.Data)
		if err != nil {
			w.failed++
			key := formatDocumentID(doc.DocumentID)
			*failedKeys = append(*failedKeys, key)
			*errors = append(*errors, fmt.Errorf("extract id: %w", err))
			continue
		}

		// Check if document exists
		count, err := coll.CountDocuments(ctx, bson.M{"_id": docID})
		if err != nil {
			w.failed++
			key := formatDocumentID(doc.DocumentID)
			*failedKeys = append(*failedKeys, key)
			*errors = append(*errors, fmt.Errorf("count check: %w", err))
			continue
		}

		if count > 0 {
			w.skipped++
			continue
		}

		// Insert the document
		_, err = coll.InsertOne(ctx, doc.Data)
		if err != nil {
			w.failed++
			key := formatDocumentID(doc.DocumentID)
			*failedKeys = append(*failedKeys, key)
			*errors = append(*errors, fmt.Errorf("insert: %w", err))
			continue
		}

		w.written++
		w.bytes += int64(len(doc.Data))
	}

	return nil
}
