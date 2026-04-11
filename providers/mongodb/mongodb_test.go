//go:build mongodb

package mongodb

import (
	"strings"
	"testing"

	"github.com/bytedance/sonic"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/pageton/bridge-db/internal/config"
)

// ---------------------------------------------------------------------------
// encodeMongoDocument / decodeMongoDocument
// ---------------------------------------------------------------------------

func TestEncodeDecodeMongoDocument_RoundTrip(t *testing.T) {
	id := bson.NewObjectID()
	raw, err := bson.Marshal(bson.M{
		"_id":   id,
		"name":  "alice",
		"age":   int32(30),
		"tags":  bson.A{"admin", "user"},
		"score": 95.5,
	})
	if err != nil {
		t.Fatalf("marshal bson: %v", err)
	}

	original := &mongoDocument{
		Collection: "users",
		DocumentID: id,
		Data:       raw,
	}

	encoded, err := encodeMongoDocument(original)
	if err != nil {
		t.Fatalf("encodeMongoDocument() error = %v", err)
	}

	decoded, err := decodeMongoDocument(encoded)
	if err != nil {
		t.Fatalf("decodeMongoDocument() error = %v", err)
	}

	if decoded.Collection != "users" {
		t.Fatalf("Collection = %q, want %q", decoded.Collection, "users")
	}

	gotID, ok := decoded.DocumentID.(bson.ObjectID)
	if !ok || gotID != id {
		t.Fatalf("DocumentID = %#v, want ObjectID %s", decoded.DocumentID, id.Hex())
	}

	var doc bson.M
	if err := bson.Unmarshal(decoded.Data, &doc); err != nil {
		t.Fatalf("unmarshal decoded bson: %v", err)
	}
	if got, ok := doc["_id"].(bson.ObjectID); !ok || got != id {
		t.Fatalf("decoded _id = %#v, want ObjectID %s", doc["_id"], id.Hex())
	}
	if doc["name"] != "alice" {
		t.Fatalf("decoded name = %v, want %q", doc["name"], "alice")
	}
	if got, ok := doc["age"].(int32); !ok || got != 30 {
		t.Fatalf("decoded age = %v, want int32(30)", doc["age"])
	}
	tags, ok := doc["tags"].(bson.A)
	if !ok || len(tags) != 2 || tags[0] != "admin" || tags[1] != "user" {
		t.Fatalf("decoded tags = %v, want [admin, user]", doc["tags"])
	}
}

func TestDecodeMongoDocument_InvalidJSON(t *testing.T) {
	_, err := decodeMongoDocument([]byte("not-json{{{"))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestDecodeMongoDocument_MissingDocumentField(t *testing.T) {
	envelope := map[string]any{
		"collection":  "users",
		"document_id": "abc",
	}
	data, err := sonic.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	_, err = decodeMongoDocument(data)
	if err == nil {
		t.Fatal("expected error for missing document field, got nil")
	}
}

func TestDecodeMongoDocument_Base64BSONField(t *testing.T) {
	id := bson.NewObjectID()
	raw, err := bson.Marshal(bson.M{"_id": id, "val": "hello"})
	if err != nil {
		t.Fatalf("marshal bson: %v", err)
	}

	envelope := map[string]any{
		"collection":    "items",
		"document_id":   id.Hex(),
		"document_bson": raw,
	}
	data, err := sonic.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	doc, err := decodeMongoDocument(data)
	if err != nil {
		t.Fatalf("decodeMongoDocument() error = %v", err)
	}
	if doc.Collection != "items" {
		t.Fatalf("Collection = %q, want %q", doc.Collection, "items")
	}

	gotID, ok := doc.DocumentID.(bson.ObjectID)
	if !ok || gotID != id {
		t.Fatalf("DocumentID = %#v, want %s", doc.DocumentID, id.Hex())
	}

	var m bson.M
	if err := bson.Unmarshal(doc.Data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["val"] != "hello" {
		t.Fatalf("val = %v, want %q", m["val"], "hello")
	}
}

// ---------------------------------------------------------------------------
// normalizeMongoDocumentID
// ---------------------------------------------------------------------------

func TestNormalizeMongoDocumentID_ValidHex(t *testing.T) {
	hex := "507f1f77bcf86cd799439011"
	result := normalizeMongoDocumentID(hex)

	objID, ok := result.(bson.ObjectID)
	if !ok {
		t.Fatalf("expected bson.ObjectID, got %T", result)
	}
	if objID.Hex() != hex {
		t.Fatalf("Hex() = %q, want %q", objID.Hex(), hex)
	}
}

func TestNormalizeMongoDocumentID_NonHexString(t *testing.T) {
	input := "zzzzzzzzzzzzzzzzzzzzzzzz"
	result := normalizeMongoDocumentID(input)

	s, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if s != input {
		t.Fatalf("got %q, want %q", s, input)
	}
}

func TestNormalizeMongoDocumentID_ShortString(t *testing.T) {
	input := "abc"
	result := normalizeMongoDocumentID(input)

	s, ok := result.(string)
	if !ok {
		t.Fatalf("expected string, got %T", result)
	}
	if s != input {
		t.Fatalf("got %q, want %q", s, input)
	}
}

func TestNormalizeMongoDocumentID_NonStringType(t *testing.T) {
	input := 42
	result := normalizeMongoDocumentID(input)

	if result != input {
		t.Fatalf("got %v, want %v", result, input)
	}
}

func TestNormalizeMongoDocumentID_Nil(t *testing.T) {
	result := normalizeMongoDocumentID(nil)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// extractDocumentID
// ---------------------------------------------------------------------------

func TestExtractDocumentID_ValidDocument(t *testing.T) {
	id := bson.NewObjectID()
	raw, err := bson.Marshal(bson.M{"_id": id, "name": "test"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := extractDocumentID(raw)
	if err != nil {
		t.Fatalf("extractDocumentID() error = %v", err)
	}

	objID, ok := got.(bson.ObjectID)
	if !ok || objID != id {
		t.Fatalf("got %#v, want ObjectID %s", got, id.Hex())
	}
}

func TestExtractDocumentID_MissingID(t *testing.T) {
	raw, err := bson.Marshal(bson.M{"name": "no-id"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	_, err = extractDocumentID(raw)
	if err == nil {
		t.Fatal("expected error for missing _id, got nil")
	}
}

func TestExtractDocumentID_StringID(t *testing.T) {
	raw, err := bson.Marshal(bson.M{"_id": "custom-id"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := extractDocumentID(raw)
	if err != nil {
		t.Fatalf("extractDocumentID() error = %v", err)
	}

	s, ok := got.(string)
	if !ok || s != "custom-id" {
		t.Fatalf("got %v, want %q", got, "custom-id")
	}
}

// ---------------------------------------------------------------------------
// formatDocumentID
// ---------------------------------------------------------------------------

func TestFormatDocumentID_ObjectID(t *testing.T) {
	id := bson.NewObjectID()
	result := formatDocumentID(id)

	if result != id.Hex() {
		t.Fatalf("got %q, want %q", result, id.Hex())
	}
}

func TestFormatDocumentID_String(t *testing.T) {
	result := formatDocumentID("my-string-id")
	if result != "my-string-id" {
		t.Fatalf("got %q, want %q", result, "my-string-id")
	}
}

func TestFormatDocumentID_Int(t *testing.T) {
	result := formatDocumentID(42)

	var expected int
	if err := sonic.Unmarshal([]byte(result), &expected); err != nil {
		t.Fatalf("result %q is not valid JSON int: %v", result, err)
	}
	if expected != 42 {
		t.Fatalf("got %d, want 42", expected)
	}
}

func TestFormatDocumentID_Nil(t *testing.T) {
	result := formatDocumentID(nil)
	if result != "null" {
		t.Fatalf("got %q, want %q", result, "null")
	}
}

// ---------------------------------------------------------------------------
// parseDocumentKey
// ---------------------------------------------------------------------------

func TestParseDocumentKey_SimpleStringID(t *testing.T) {
	coll, id, err := parseDocumentKey("users:my-custom-id")
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if coll != "users" {
		t.Fatalf("collection = %q, want %q", coll, "users")
	}
	if id != "my-custom-id" {
		t.Fatalf("docID = %v, want %q", id, "my-custom-id")
	}
}

func TestParseDocumentKey_ObjectID(t *testing.T) {
	hex := "507f1f77bcf86cd799439011"
	coll, id, err := parseDocumentKey("orders:" + hex)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if coll != "orders" {
		t.Fatalf("collection = %q, want %q", coll, "orders")
	}
	objID, ok := id.(bson.ObjectID)
	if !ok {
		t.Fatalf("docID type = %T, want bson.ObjectID", id)
	}
	if objID.Hex() != hex {
		t.Fatalf("docID hex = %q, want %q", objID.Hex(), hex)
	}
}

func TestParseDocumentKey_WithColons(t *testing.T) {
	coll, id, err := parseDocumentKey("logs:id:with:colons")
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if coll != "logs" {
		t.Fatalf("collection = %q, want %q", coll, "logs")
	}
	if id != "id:with:colons" {
		t.Fatalf("docID = %v, want %q", id, "id:with:colons")
	}
}

func TestParseDocumentKey_NoColon(t *testing.T) {
	_, _, err := parseDocumentKey("nodelimiter")
	if err == nil {
		t.Fatal("expected error for key without colon, got nil")
	}
}

func TestParseDocumentKey_EmptyString(t *testing.T) {
	_, _, err := parseDocumentKey("")
	if err == nil {
		t.Fatal("expected error for empty key, got nil")
	}
}

// ---------------------------------------------------------------------------
// canonicalizeMongoValue
// ---------------------------------------------------------------------------

func TestCanonicalizeMongoValue_MapToSortedKVArray(t *testing.T) {
	input := map[string]any{
		"z": 1,
		"a": 2,
		"m": 3,
	}

	result := canonicalizeMongoValue(input)

	arr, ok := result.([]canonicalKV)
	if !ok {
		t.Fatalf("result type = %T, want []canonicalKV", result)
	}
	if len(arr) != 3 {
		t.Fatalf("len = %d, want 3", len(arr))
	}
	expected := []string{"a", "m", "z"}
	for i, key := range expected {
		if arr[i].Key != key {
			t.Fatalf("arr[%d].Key = %q, want %q", i, arr[i].Key, key)
		}
	}
}

func TestCanonicalizeMongoValue_ArrayRecurses(t *testing.T) {
	input := []any{
		map[string]any{"z": 1, "a": 2},
		"simple",
	}

	result := canonicalizeMongoValue(input)

	arr, ok := result.([]any)
	if !ok {
		t.Fatalf("result type = %T, want []any", result)
	}
	if len(arr) != 2 {
		t.Fatalf("len = %d, want 2", len(arr))
	}

	nested, ok := arr[0].([]canonicalKV)
	if !ok || len(nested) != 2 || nested[0].Key != "a" || nested[1].Key != "z" {
		t.Fatalf("nested[0] = %v, want sorted [{a 2} {z 1}]", arr[0])
	}
	if arr[1] != "simple" {
		t.Fatalf("arr[1] = %v, want %q", arr[1], "simple")
	}
}

func TestCanonicalizeMongoValue_ObjectIDToHex(t *testing.T) {
	id := bson.NewObjectID()
	result := canonicalizeMongoValue(id)

	s, ok := result.(string)
	if !ok {
		t.Fatalf("result type = %T, want string", result)
	}
	if s != id.Hex() {
		t.Fatalf("got %q, want %q", s, id.Hex())
	}
}

func TestCanonicalizeMongoValue_SimplePassthrough(t *testing.T) {
	if got := canonicalizeMongoValue("hello"); got != "hello" {
		t.Fatalf("string: got %v, want %q", got, "hello")
	}
	if got := canonicalizeMongoValue(42); got != 42 {
		t.Fatalf("int: got %v, want 42", got)
	}
	if got := canonicalizeMongoValue(3.14); got != 3.14 {
		t.Fatalf("float: got %v, want 3.14", got)
	}
	if got := canonicalizeMongoValue(true); got != true {
		t.Fatalf("bool: got %v, want true", got)
	}
}

func TestCanonicalizeMongoValue_BsonMConverted(t *testing.T) {
	input := bson.M{"b": 1, "a": 2}

	result := canonicalizeMongoValue(input)

	arr, ok := result.([]canonicalKV)
	if !ok {
		t.Fatalf("result type = %T, want []canonicalKV", result)
	}
	if arr[0].Key != "a" || arr[1].Key != "b" {
		t.Fatalf("keys not sorted: %v", arr)
	}
}

func TestCanonicalizeMongoValue_BsonARecurses(t *testing.T) {
	input := bson.A{bson.M{"z": 1, "a": 2}, 42}

	result := canonicalizeMongoValue(input)

	arr, ok := result.([]any)
	if !ok {
		t.Fatalf("result type = %T, want []any", result)
	}
	nested, ok := arr[0].([]canonicalKV)
	if !ok || nested[0].Key != "a" {
		t.Fatalf("nested[0] not sorted: %v", arr[0])
	}
	if arr[1] != 42 {
		t.Fatalf("arr[1] = %v, want 42", arr[1])
	}
}

func TestCanonicalizeMongoValue_NestedMap(t *testing.T) {
	input := map[string]any{
		"inner": map[string]any{"y": 20, "x": 10},
		"val":   "top",
	}

	result := canonicalizeMongoValue(input)

	arr, ok := result.([]canonicalKV)
	if !ok {
		t.Fatalf("result type = %T, want []canonicalKV", result)
	}
	if len(arr) != 2 {
		t.Fatalf("len = %d, want 2", len(arr))
	}
	if arr[0].Key != "inner" {
		t.Fatalf("arr[0].Key = %q, want %q", arr[0].Key, "inner")
	}
	inner, ok := arr[0].Value.([]canonicalKV)
	if !ok || len(inner) != 2 || inner[0].Key != "x" || inner[1].Key != "y" {
		t.Fatalf("inner not sorted: %v", arr[0].Value)
	}
	if arr[1].Key != "val" || arr[1].Value != "top" {
		t.Fatalf("arr[1] = %v, want {val top}", arr[1])
	}
}

// ---------------------------------------------------------------------------
// buildMongoDBURI
// ---------------------------------------------------------------------------

func TestBuildMongoDBURI_Basic(t *testing.T) {
	cfg := &config.MongoDBConfig{
		Host:     "localhost",
		Port:     27017,
		Database: "mydb",
	}

	uri := buildMongoDBURI(cfg)
	if uri != "mongodb://localhost:27017/mydb" {
		t.Fatalf("got %q", uri)
	}
}

func TestBuildMongoDBURI_WithAuth(t *testing.T) {
	cfg := &config.MongoDBConfig{
		Host:     "db.example.com",
		Port:     27017,
		Username: "admin",
		Password: "s3cret",
		Database: "app",
	}

	uri := buildMongoDBURI(cfg)
	if !strings.HasPrefix(uri, "mongodb://admin:s3cret@db.example.com:27017/app") {
		t.Fatalf("got %q", uri)
	}
}

func TestBuildMongoDBURI_NonAdminAuthSource(t *testing.T) {
	cfg := &config.MongoDBConfig{
		Host:       "localhost",
		Port:       27017,
		Database:   "testdb",
		AuthSource: "customAuth",
	}

	uri := buildMongoDBURI(cfg)
	if !strings.Contains(uri, "authSource=customAuth") {
		t.Fatalf("got %q, want authSource=customAuth", uri)
	}
}

func TestBuildMongoDBURI_AdminAuthSourceOmitted(t *testing.T) {
	cfg := &config.MongoDBConfig{
		Host:       "localhost",
		Port:       27017,
		Database:   "testdb",
		AuthSource: "admin",
	}

	uri := buildMongoDBURI(cfg)
	if strings.Contains(uri, "authSource") {
		t.Fatalf("admin authSource should be omitted, got %q", uri)
	}
	if uri != "mongodb://localhost:27017/testdb" {
		t.Fatalf("got %q", uri)
	}
}

func TestBuildMongoDBURI_WithTLS(t *testing.T) {
	cfg := &config.MongoDBConfig{
		Host:     "localhost",
		Port:     27017,
		Database: "mydb",
		TLS:      true,
	}

	uri := buildMongoDBURI(cfg)
	if !strings.Contains(uri, "tls=true") {
		t.Fatalf("got %q, want tls=true", uri)
	}
}

func TestBuildMongoDBURI_WithTLSAndAuthSource(t *testing.T) {
	cfg := &config.MongoDBConfig{
		Host:       "localhost",
		Port:       27017,
		Database:   "mydb",
		AuthSource: "custom",
		TLS:        true,
	}

	uri := buildMongoDBURI(cfg)
	if !strings.Contains(uri, "authSource=custom") {
		t.Fatalf("got %q, want authSource=custom", uri)
	}
	if !strings.Contains(uri, "tls=true") {
		t.Fatalf("got %q, want tls=true", uri)
	}
	if !strings.Contains(uri, "&") {
		t.Fatalf("expected & separator between params, got %q", uri)
	}
}

func TestBuildMongoDBURI_NoDatabase(t *testing.T) {
	cfg := &config.MongoDBConfig{
		Host: "localhost",
		Port: 27017,
	}

	uri := buildMongoDBURI(cfg)
	if uri != "mongodb://localhost:27017" {
		t.Fatalf("got %q", uri)
	}
}

func TestBuildMongoDBURI_PasswordWithSpecialChars(t *testing.T) {
	cfg := &config.MongoDBConfig{
		Host:     "localhost",
		Port:     27017,
		Username: "user",
		Password: "p@ss:w/rd",
		Database: "db",
	}

	uri := buildMongoDBURI(cfg)
	if !strings.HasPrefix(uri, "mongodb://user:p%40ss%3Aw%2Frd@localhost:27017/db") {
		t.Fatalf("got %q", uri)
	}
}
