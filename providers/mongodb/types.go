//go:build mongodb

package mongodb

import (
	"fmt"
	"github.com/bytedance/sonic"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// mongoDocument is the JSON envelope for a MongoDB MigrationUnit's Data field.
// It carries the document's data alongside metadata needed to reconstruct it
// on the destination.
type mongoDocument struct {
	// Collection is the source collection name.
	Collection string `json:"collection"`

	// DocumentID is the document's _id field (for upsert operations).
	DocumentID any `json:"document_id"`

	// Data holds the BSON-serialized document.
	Data bson.Raw `json:"data"`
}

// encodeMongoDocument serialises a mongoDocument into JSON bytes.
// We use JSON instead of raw BSON for the MigrationUnit.Data field to maintain
// consistency with other providers and enable debugging.
func encodeMongoDocument(doc *mongoDocument) ([]byte, error) {
	// Convert BSON to a map for JSON serialization
	var docMap map[string]any
	if err := bson.Unmarshal(doc.Data, &docMap); err != nil {
		return nil, fmt.Errorf("unmarshal bson: %w", err)
	}

	envelope := map[string]any{
		"collection":  doc.Collection,
		"document_id": doc.DocumentID,
		"document":    docMap,
	}

	return sonic.Marshal(envelope)
}

// decodeMongoDocument deserialises JSON bytes into a mongoDocument.
func decodeMongoDocument(data []byte) (*mongoDocument, error) {
	var envelope map[string]any
	if err := sonic.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("decode mongo document: %w", err)
	}

	collection, _ := envelope["collection"].(string)
	documentID := envelope["document_id"]

	// Re-encode the document to BSON
	// Re-encode the document to BSON
	docMap, ok := envelope["document"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid document format")
	}

	docBytes, err := bson.Marshal(docMap)
	if err != nil {
		return nil, fmt.Errorf("marshal document to bson: %w", err)
	}

	return &mongoDocument{
		Collection: collection,
		DocumentID: documentID,
		Data:       docBytes,
	}, nil
}

// extractDocumentID extracts the _id field from a BSON document.
func extractDocumentID(data bson.Raw) (any, error) {
	var doc map[string]any
	if err := bson.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	id, ok := doc["_id"]
	if !ok {
		return nil, fmt.Errorf("document missing _id field")
	}

	return id, nil
}
