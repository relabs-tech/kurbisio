package backend

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// Standalone test functions that don't require the full backend context
func TestCursorEncoding(t *testing.T) {
	// Test cursor encoding/decoding
	originalCursor := PaginationCursor{
		Timestamp: time.Date(2025, 6, 17, 10, 30, 0, 0, time.UTC),
		ID:        uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
	}

	// Encode cursor using the string format
	encoded := originalCursor.Encode()

	if encoded == "" {
		t.Error("Expected non-empty encoded cursor")
	}

	// Decode cursor
	decoded, err := DecodePaginationCursor(encoded)
	if err != nil {
		t.Fatalf("Failed to decode cursor: %v", err)
	}

	// Verify timestamp
	if !decoded.Timestamp.Equal(originalCursor.Timestamp) {
		t.Errorf("Expected timestamp %v, got %v", originalCursor.Timestamp, decoded.Timestamp)
	}

	// Verify ID
	if decoded.ID != originalCursor.ID {
		t.Errorf("Expected ID %v, got %v", originalCursor.ID, decoded.ID)
	}
}

func TestCursorMethods(t *testing.T) {
	// Test the actual methods once they're compiled with the full package
	cursor := PaginationCursor{
		Timestamp: time.Date(2025, 6, 17, 10, 30, 0, 0, time.UTC),
		ID:        uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
	}

	encoded := cursor.Encode()
	decoded, err := DecodePaginationCursor(encoded)

	if err != nil {
		t.Errorf("Failed to decode cursor: %v", err)
	}

	if !decoded.Timestamp.Equal(cursor.Timestamp) {
		t.Errorf("Timestamp mismatch: expected %v, got %v", cursor.Timestamp, decoded.Timestamp)
	}

	if decoded.ID != cursor.ID {
		t.Errorf("ID mismatch: expected %v, got %v", cursor.ID, decoded.ID)
	}
}

func TestCursorInvalidFormats(t *testing.T) {
	testCases := []string{
		"invalid_format",
		"123",
		"123.invalid_uuid",
		"invalid_timestamp.550e8400-e29b-41d4-a716-446655440000",
		"",
	}

	for _, tc := range testCases {
		_, err := DecodePaginationCursor(tc)
		if err == nil {
			t.Errorf("Expected error for invalid cursor format: %s", tc)
		}
	}
}
