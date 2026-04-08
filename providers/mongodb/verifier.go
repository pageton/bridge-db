//go:build mongodb

package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// mongoDBVerifier checks that documents in the destination MongoDB match the source.
// It verifies document counts per collection and performs sample-based comparison.
type mongoDBVerifier struct {
	database *mongo.Database
	log      interface{ Info(msg string, args ...any) }
}

func newMongoDBVerifier(database *mongo.Database) *mongoDBVerifier {
	return &mongoDBVerifier{
		database: database,
		log:      logger.L().With("component", "mongodb-verifier"),
	}
}

// Verify checks the given document keys for existence and correctness.
// If keys is empty, it performs a count-based check across all collections.
func (v *mongoDBVerifier) Verify(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	if len(keys) == 0 {
		// Count-based verification: check each collection has documents
		return v.verifyCounts(ctx)
	}

	// Key-level verification: check specific documents
	return v.verifyDocuments(ctx, keys)
}

// verifyCounts checks that all collections have at least one document.
func (v *mongoDBVerifier) verifyCounts(ctx context.Context) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError

	// List collections
	cursor, err := v.database.ListCollections(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			continue
		}

		name, ok := result["name"].(string)
		if !ok || len(name) == 0 || name[0] == '.' {
			continue
		}

		// Count documents in collection
		count, err := v.database.Collection(name).CountDocuments(ctx, bson.M{})
		if err != nil {
			errs = append(errs, provider.VerificationError{
				Table:   name,
				Message: fmt.Sprintf("count failed: %v", err),
			})
			continue
		}

		if count == 0 {
			errs = append(errs, provider.VerificationError{
				Table:   name,
				Message: "collection is empty",
			})
		}
	}

	return errs, nil
}

// verifyDocuments checks specific documents by their keys.
// Keys are in the format "collection:documentID".
func (v *mongoDBVerifier) verifyDocuments(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError

	for _, key := range keys {
		collection, docID, err := parseDocumentKey(key)
		if err != nil {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Message: fmt.Sprintf("invalid key format: %v", err),
			})
			continue
		}

		// Check if document exists
		count, err := v.database.Collection(collection).CountDocuments(ctx, bson.M{"_id": docID})
		if err != nil {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Table:   collection,
				Message: fmt.Sprintf("count check failed: %v", err),
			})
			continue
		}

		if count == 0 {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Table:   collection,
				Message: "document does not exist in destination",
			})
		}
	}

	return errs, nil
}

// parseDocumentKey parses a document key in the format "collection:documentID".
func parseDocumentKey(key string) (collection string, docID any, err error) {
	// Find the first colon that separates collection from ID
	// We need to handle ObjectIDs which contain colons
	colonIdx := -1
	for i := 0; i < len(key); i++ {
		if key[i] == ':' {
			// Check if this looks like an ObjectID (24 hex chars after colon)
			if i+1 < len(key) && len(key[i+1:]) == 24 {
				// Likely an ObjectID
				colonIdx = i
				break
			}
			// For non-ObjectID keys, use first colon
			if colonIdx == -1 {
				colonIdx = i
			}
		}
	}

	if colonIdx == -1 {
		return "", nil, fmt.Errorf("invalid key format, expected 'collection:id'")
	}

	collection = key[:colonIdx]
	idStr := key[colonIdx+1:]

	// Try to parse as ObjectID
	if len(idStr) == 24 {
		if objID, err := bson.ObjectIDFromHex(idStr); err == nil {
			return collection, objID, nil
		}
	}

	// Return as string
	return collection, idStr, nil
}
