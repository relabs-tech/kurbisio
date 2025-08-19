// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// PaginationCursor represents a cursor for pagination containing timestamp and ID
type PaginationCursor struct {
	Timestamp time.Time `json:"timestamp"`
	ID        uuid.UUID `json:"id"`
}

// Encode encodes the cursor to a base64 string format
func (c PaginationCursor) Encode() string {
	encoded := fmt.Sprintf("%d.%s", c.Timestamp.UnixNano(), c.ID.String())
	return base64.URLEncoding.EncodeToString([]byte(encoded))
}

// DecodePaginationCursor decodes a base64 cursor string back to PaginationCursor
func DecodePaginationCursor(encoded string) (PaginationCursor, error) {
	decoded, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		return PaginationCursor{}, fmt.Errorf("invalid cursor format: %v", err)
	}

	parts := strings.SplitN(string(decoded), ".", 2)
	if len(parts) != 2 {
		return PaginationCursor{}, fmt.Errorf("invalid cursor format: %s", encoded)
	}

	timestampNano, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return PaginationCursor{}, fmt.Errorf("invalid timestamp in cursor: %v", err)
	}

	id, err := uuid.Parse(parts[1])
	if err != nil {
		return PaginationCursor{}, fmt.Errorf("invalid ID in cursor: %v", err)
	}

	return PaginationCursor{
		Timestamp: time.Unix(0, timestampNano).UTC(),
		ID:        id,
	}, nil
}
