//go:build mongodb

package mongodb

import (
	"testing"

	"github.com/bytedance/sonic"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestDecodeMongoDocument_RestoresObjectID(t *testing.T) {
	id := bson.NewObjectID()
	envelope := map[string]any{
		"collection":  "users",
		"document_id": id.Hex(),
		"document": map[string]any{
			"_id":  id.Hex(),
			"name": "alice",
		},
	}

	data, err := sonic.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	doc, err := decodeMongoDocument(data)
	if err != nil {
		t.Fatalf("decodeMongoDocument() error = %v", err)
	}

	if got, ok := doc.DocumentID.(bson.ObjectID); !ok || got != id {
		t.Fatalf("DocumentID = %#v, want ObjectID %s", doc.DocumentID, id.Hex())
	}

	decodedID, err := extractDocumentID(doc.Data)
	if err != nil {
		t.Fatalf("extractDocumentID() error = %v", err)
	}
	if got, ok := decodedID.(bson.ObjectID); !ok || got != id {
		t.Fatalf("decoded _id = %#v, want ObjectID %s", decodedID, id.Hex())
	}
}

func TestEncodeDecodeMongoDocument_PreservesRawBSONTypes(t *testing.T) {
	id := bson.NewObjectID()
	raw, err := bson.Marshal(bson.M{
		"_id":   id,
		"age":   int32(30),
		"name":  "alice",
		"price": 19.99,
	})
	if err != nil {
		t.Fatalf("marshal bson: %v", err)
	}

	encoded, err := encodeMongoDocument(&mongoDocument{
		Collection: "users",
		DocumentID: id,
		Data:       raw,
	})
	if err != nil {
		t.Fatalf("encodeMongoDocument() error = %v", err)
	}

	decoded, err := decodeMongoDocument(encoded)
	if err != nil {
		t.Fatalf("decodeMongoDocument() error = %v", err)
	}

	var doc bson.M
	if err := bson.Unmarshal(decoded.Data, &doc); err != nil {
		t.Fatalf("unmarshal decoded bson: %v", err)
	}
	if got, ok := doc["_id"].(bson.ObjectID); !ok || got != id {
		t.Fatalf("decoded _id = %#v, want ObjectID %s", doc["_id"], id.Hex())
	}
	if got, ok := doc["age"].(int32); !ok || got != 30 {
		t.Fatalf("decoded age = %#v, want int32(30)", doc["age"])
	}
}

func TestCanonicalizeMongoValue_DeterministicAcrossMapOrder(t *testing.T) {
	a := map[string]any{"name": "alice", "age": int32(30)}
	b := map[string]any{"age": int32(30), "name": "alice"}

	aj, err := sonic.Marshal(canonicalizeMongoValue(a))
	if err != nil {
		t.Fatalf("marshal a: %v", err)
	}
	bj, err := sonic.Marshal(canonicalizeMongoValue(b))
	if err != nil {
		t.Fatalf("marshal b: %v", err)
	}
	if string(aj) != string(bj) {
		t.Fatalf("canonicalized JSON differs:\n%s\n%s", aj, bj)
	}
}
