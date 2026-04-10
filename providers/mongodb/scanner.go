//go:build mongodb

package mongodb

import (
	"context"
	"io"

	"github.com/bytedance/sonic"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// mongoDBScanner enumerates documents from a MongoDB database using cursor-based
// iteration. It first lists all collections, then scans each collection with
// a cursor, yielding batches of MigrationUnits.
type mongoDBScanner struct {
	database        *mongo.Database
	opts            provider.ScanOptions
	stats           provider.ScanStats
	collections     []string
	currentColl     int
	cursor          *mongo.Cursor
	done            bool
	tablesCompleted map[string]bool // collections to skip on resume
	log             interface {
		Debug(msg string, args ...any)
	}
}

func newMongoDBScanner(database *mongo.Database, opts provider.ScanOptions) *mongoDBScanner {
	s := &mongoDBScanner{
		database: database,
		opts:     opts,
		log:      logger.L().With("component", "mongodb-scanner"),
	}

	// Parse resume token to restore stats for logging. Collection skipping
	// is handled by TablesCompleted filtering below; we do NOT use
	// TablesDone for index-based skipping (conflicts with name filtering).
	if len(opts.ResumeToken) > 0 {
		if stats, err := provider.UnmarshalScanToken(opts.ResumeToken); err == nil {
			s.stats = stats
			s.log.Debug("resuming from checkpoint",
				"collections_done", stats.TablesDone,
				"collections_total", stats.TablesTotal,
				"docs_scanned", stats.TotalScanned,
			)
		}
	}

	// Track collections to skip on resume.
	if len(opts.TablesCompleted) > 0 {
		s.tablesCompleted = make(map[string]bool, len(opts.TablesCompleted))
		for _, t := range opts.TablesCompleted {
			s.tablesCompleted[t] = true
		}
	}

	return s
}

// Next returns the next batch of MigrationUnits. It returns io.EOF when
// all collections have been scanned.
func (s *mongoDBScanner) Next(ctx context.Context) ([]provider.MigrationUnit, error) {
	if s.done {
		return nil, io.EOF
	}

	batchSize := s.opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	// If we don't have collections listed yet, do that first
	if s.collections == nil {
		if err := s.listCollections(ctx); err != nil {
			return nil, err
		}
		if len(s.collections) == 0 {
			s.done = true
			return nil, io.EOF
		}
	}

	// Collect units until we have a full batch or run out of data
	units := make([]provider.MigrationUnit, 0, batchSize)

	for len(units) < batchSize && !s.done {
		// If we have a cursor, get next document
		if s.cursor != nil && s.cursor.Next(ctx) {
			unit, err := s.readDocument(s.collections[s.currentColl], s.cursor)
			if err != nil {
				s.log.Debug("failed to read document, skipping",
					"collection", s.collections[s.currentColl],
					"error", err)
				continue
			}
			units = append(units, *unit)
			s.stats.TotalScanned++
			s.stats.TotalBytes += unit.Size
			continue
		}

		// Check for cursor error
		if s.cursor != nil {
			if err := s.cursor.Err(); err != nil {
				s.log.Debug("cursor error", "collection", s.collections[s.currentColl], "error", err)
			}
			_ = s.cursor.Close(ctx)
			s.cursor = nil
			s.currentColl++
			s.stats.TablesDone++
		}

		// Move to next collection or finish
		if s.currentColl >= len(s.collections) {
			s.done = true
			break
		}

		// Open cursor for next collection
		collName := s.collections[s.currentColl]
		s.log.Debug("scanning collection", "collection", collName)

		// Build find options
		findOpts := options.Find().SetBatchSize(int32(batchSize))

		cursor, err := s.database.Collection(collName).Find(ctx, bson.M{}, findOpts)
		if err != nil {
			s.log.Debug("failed to open cursor for collection", "collection", collName, "error", err)
			s.currentColl++
			s.stats.TablesDone++
			continue
		}
		s.cursor = cursor
	}

	if len(units) == 0 {
		return nil, io.EOF
	}

	return units, nil
}

// Stats returns current scan statistics.
func (s *mongoDBScanner) Stats() provider.ScanStats {
	return s.stats
}

func (s *mongoDBScanner) Close() error {
	if s.cursor != nil {
		err := s.cursor.Close(context.Background())
		s.cursor = nil
		s.done = true
		return err
	}
	return nil
}

// listCollections enumerates all collections in the database.
func (s *mongoDBScanner) listCollections(ctx context.Context) error {
	cursor, err := s.database.ListCollections(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close(ctx) }()

	var collections []string
	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			continue
		}
		if name, ok := result["name"].(string); ok {
			// Skip system collections
			if len(name) > 0 && name[0] != '.' {
				collections = append(collections, name)
			}
		}
	}

	s.collections = collections
	s.stats.TablesTotal = len(collections)

	// Filter out completed collections on resume.
	if len(s.tablesCompleted) > 0 {
		filtered := collections[:0]
		for _, c := range collections {
			if !s.tablesCompleted[c] {
				filtered = append(filtered, c)
			}
		}
		s.collections = filtered
		s.stats.TablesTotal = len(filtered)
		if s.currentColl > len(s.collections) {
			s.currentColl = len(s.collections)
		}
	}

	s.log.Debug("found collections", "count", len(s.collections))

	return nil
}

// readDocument reads a single MongoDB document and returns it as a MigrationUnit.
func (s *mongoDBScanner) readDocument(collection string, cursor *mongo.Cursor) (*provider.MigrationUnit, error) {
	// Get raw BSON bytes - cursor.Current is already bson.Raw (a byte slice)
	rawBytes := cursor.Current

	// Extract document ID
	docID, err := extractDocumentID(rawBytes)
	if err != nil {
		return nil, err
	}

	// Create the document envelope
	doc := &mongoDocument{
		Collection: collection,
		DocumentID: docID,
		Data:       rawBytes,
	}

	// Encode to JSON for the MigrationUnit
	data, err := encodeMongoDocument(doc)
	if err != nil {
		return nil, err
	}

	// Create document key (collection:_id)
	key := collection + ":" + formatDocumentID(docID)

	return &provider.MigrationUnit{
		Key:      key,
		Table:    collection,
		DataType: provider.DataTypeDocument,
		Data:     data,
		Meta:     provider.UnitMeta{},
		Size:     int64(len(rawBytes)),
	}, nil
}

// formatDocumentID formats a document ID for use as a key component.
func formatDocumentID(id any) string {
	switch v := id.(type) {
	case bson.ObjectID:
		return v.Hex()
	case string:
		return v
	default:
		// For other types, use JSON representation
		b, _ := sonic.Marshal(v)
		return string(b)
	}
}
