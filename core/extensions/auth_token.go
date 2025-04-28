package extensions

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/backend"
	"github.com/relabs-tech/kurbisio/core/client"
	"github.com/relabs-tech/kurbisio/core/logger"
)

// AuthToken is a Kurbisio extension which adds token-based authentication capabilities.
//
// Features:
// - Users can create tokens with custom expiration dates (defaults to 3 months)
// - Tokens are shown only once at creation time and cannot be retrieved later
// - Only metadata (description and expiration date) can be viewed subsequently
//
// Usage:
// - Add token to requests using header: "Authorization: AuthToken: <token>"
// - Token format: base64-encoded account ID followed by random string
//
// Requirements:
// - Requires the "account" collection in the configuration
// - Requires a shortcut to the account collection
//
// Added Resources:
// - Collection: "account/token_metadata" - stores token metadata
//   Users can list/read/update/delete their own tokens
// - Singleton: "account/token_metadata/token" - stores secure token data
//   (hashed token with salt, not the actual token)
//
// API Endpoints:
// - POST /_account/create_token - Creates a new token
//   Accepts JSON body matching TokenMetadataModel
//
// Security:
// - Tokens are never stored in plain text
// - Uses random 16-character salt for each token
// - Stores only salted hash of token in database
type AuthToken struct {
}

// GetName returns the name of the extension
func (a AuthToken) GetName() string {
	return "AuthToken"
}

type tokenAndMetadata struct {
	Token tokenModel `json:"token"`
	TokenMetadataModel
}

type tokenModel struct {
	TokenID       uuid.UUID `json:"token_id"`
	AccountID     uuid.UUID `json:"account_id"`
	TokenCheckSum string    `json:"token_checksum"`
	Salt          string    `json:"salt"` // Add salt for secure hashing
}

type TokenMetadataModel struct {
	Token       string    `json:"token,omitempty"` // The actual token, not stored in the database
	AccountID   uuid.UUID `json:"account_id"`
	TokenID     uuid.UUID `json:"token_id"`
	ExpireAt    time.Time `json:"expire_at"`
	Timestamp   time.Time `json:"timestamp"`
	Description string    `json:"description"` // User-provided description
}

const (
	// saltLength is the number of random bytes used for the salt
	// 16 bytes provides sufficient uniqueness while keeping the stored data compact
	saltLength = 16
)

// UpdateConfig update the kurbisio configuration, potentially adding collection, singletons...
func (a AuthToken) UpdateConfig(config backend.Configuration) (backend.Configuration, error) {
	// Add the account/token collection to the configuration
	found := false
	for _, s := range config.Shortcuts {
		if s.Target == "account" {
			found = true
			break
		}
	}
	if !found {
		return config, fmt.Errorf("the account shortcut is mandatory to be able to use the %s extension", a.GetName())
	}

	for _, c := range config.Collections {
		if c.Resource == "account" {
			config.Collections = append(config.Collections,
				backend.CollectionConfiguration{
					Resource:    "account/token_metadata",
					Description: "Metadata of authentication tokens for API access. user read/write",
					Permits: []access.Permit{
						{
							Role: "everybody",
							Operations: []core.Operation{
								core.OperationRead,
								core.OperationUpdate,
								core.OperationDelete,
								core.OperationList,
							},
							Selectors: []string{"account"},
						},
					},
				},
			)
			config.Singletons = append(config.Singletons,
				backend.SingletonConfiguration{
					Resource:    "account/token_metadata/token",
					Description: "Authentication tokens for API access. Not accessible to the users. It stores the checksum of the token",
				})
			return config, nil
		}
	}
	return config, fmt.Errorf("the account collection is mandatory to be able to use the %s extension", a.GetName())
}

// UpdateMux updates the mux router with the extension routes
func (a AuthToken) UpdateMux(router *mux.Router) error {

	router.HandleFunc("/_account/create_token",
		func(w http.ResponseWriter, r *http.Request) {
			rlog := logger.FromContext(r.Context())
			rlog.Infoln("Creating token")
			// Check if the user is authorized to create a token
			auth := access.AuthorizationFromContext(r.Context())
			if auth == nil {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			var request TokenMetadataModel
			// Parse the request body
			ioDecoder := json.NewDecoder(r.Body)
			if err := ioDecoder.Decode(&request); err != nil {
				rlog.Errorln("Failed to decode request body:", err)
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}

			if request.ExpireAt.IsZero() {
				request.ExpireAt = time.Now().AddDate(0, 3, 0)
			}

			token, metadata, err := generateToken(request.Description, request.ExpireAt)
			if err != nil {
				rlog.Errorln("Failed to generate token:", err)
				http.Error(w, "Failed to generate token", http.StatusInternalServerError)
				return
			}

			accountID, ok := auth.Selector("account_id")
			if !ok {
				http.Error(w, "Account ID not found in authorization", http.StatusUnauthorized)
				return
			}
			accountUUID, err := uuid.Parse(accountID)
			if err != nil {
				http.Error(w, "Invalid account ID", http.StatusInternalServerError)
				return
			}
			metadata.AccountID = accountUUID
			token.AccountID = accountUUID
			token.TokenID = metadata.TokenID

			adminClient := client.NewWithRouter(router).WithAdminAuthorization()
			withoutSecretMetadata := *metadata
			withoutSecretMetadata.Token = ""

			_, err = adminClient.
				Collection("account/token_metadata").Item(metadata.TokenID).Upsert(withoutSecretMetadata, nil)

			if err != nil {
				http.Error(w, "Failed to store token metadata", http.StatusInternalServerError)
				return
			}

			_, err = adminClient.
				Collection("account/token_metadata").Item(metadata.TokenID).Subcollection("token").Singleton().Upsert(token, nil)
			if err != nil {
				rlog.WithError(err).Errorln("Failed to store token")
				http.Error(w, "Failed to store token", http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"token": "%s"}`, metadata.Token)
		},
	).Methods("POST")

	// Add middleware to verify token authentication
	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rlog := logger.FromContext(r.Context())
			if token := r.Header.Get("Authorization"); strings.HasPrefix(token, "AuthToken: ") {
				// Verify token and set appropriate context
				tokenStr := strings.TrimPrefix(token, "AuthToken: ")

				split := strings.Split(tokenStr, ".")
				if len(split) != 2 {
					rlog.Errorln("Invalid token format, missing .")
					http.Error(w, "Invalid token format", http.StatusUnauthorized)
					return
				}
				base64TokenID := split[0]
				tokenIDBytes, err := base64.RawURLEncoding.DecodeString(base64TokenID)
				if err != nil {
					http.Error(w, "Invalid token format, missing header", http.StatusUnauthorized)
					return
				}
				id, err := uuid.ParseBytes(tokenIDBytes)
				if err != nil {
					rlog.WithError(err).Errorln("Invalid token format, invalid UUID")
					http.Error(w, "Invalid token format, invalid UUID", http.StatusUnauthorized)
					return
				}

				adminClient := client.NewWithRouter(router).WithAdminAuthorization()
				var token tokenAndMetadata
				_, err = adminClient.
					Collection("account/token_metadata").Item(id).Read(&token, "token")
				if err != nil {
					rlog.WithError(err).Errorln("Token not found")
					http.Error(w, "Token not found", http.StatusUnauthorized)
					return
				}

				if verifyToken(tokenStr, token) {
					auth := &access.Authorization{}
					_, err = adminClient.
						Collection("account").Item(token.AccountID).Read(&auth)
					if err != nil {
						http.Error(w, "Account not found", http.StatusUnauthorized)
						return
					}

					ctx := r.Context()
					ctx = access.ContextWithAuthorization(ctx, auth)
					r = r.WithContext(ctx)

				} else {
					http.Error(w, "Token expired or invalid", http.StatusUnauthorized)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	})
	return nil
}

// generateSalt creates a cryptographically secure random salt
// The salt does not need to be kept secret, but must be:
// 1. Unique for each token
// 2. Random (cryptographically secure)
// 3. Long enough to prevent collisions
func generateSalt() (string, error) {
	// Generate 16 bytes of random data for the salt
	salt := make([]byte, saltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("failed to generate salt: %w", err)
	}
	return base64.StdEncoding.EncodeToString(salt), nil
}

// hashTokenWithSalt combines the token and salt, then hashes them
func hashTokenWithSalt(token, salt string) string {
	// Combine token and salt
	salted := token + salt

	// Create SHA-256 hash
	hasher := sha256.New()
	hasher.Write([]byte(salted))

	// Convert to base64 string
	return base64.StdEncoding.EncodeToString(hasher.Sum(nil))
}

// generateToken creates a new token with specified parameters
// A token is made of the account ID and a random string
func generateToken(description string, expireAt time.Time) (*tokenModel, *TokenMetadataModel, error) {
	tokenID := uuid.New()

	base64UUID := base64.RawURLEncoding.EncodeToString([]byte(tokenID.String()))

	randomBytes := make([]byte, 32)
	if _, err := rand.Read(randomBytes); err != nil {
		return nil, nil, fmt.Errorf("failed to generate secure token: %v", err)
	}

	// Use URL-safe base64 encoding without padding
	token := base64.RawURLEncoding.EncodeToString(randomBytes)

	token = base64UUID + "." + token

	salt, err := generateSalt()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate token: %w", err)
	}

	checksum := hashTokenWithSalt(token, salt)

	tokenModel := &tokenModel{
		TokenCheckSum: checksum,
		Salt:          salt,
	}
	tokenMetadata :=
		&TokenMetadataModel{
			Token:       token,
			TokenID:     tokenID,
			ExpireAt:    expireAt,
			Description: description,
			Timestamp:   time.Now().UTC(),
		}

	// Return the actual token only here - it won't be stored and won't be retrievable again
	return tokenModel, tokenMetadata, nil
}

// verifyToken checks if a provided token is valid by comparing its hash
func verifyToken(providedToken string, tokenModel tokenAndMetadata) bool {
	// Validate salt format
	saltBytes, err := base64.StdEncoding.DecodeString(tokenModel.Token.Salt)
	if err != nil || len(saltBytes) != saltLength {
		return false // Invalid salt format or length
	}

	// Compute hash using the same salt and algorithm
	computedHash := hashTokenWithSalt(providedToken, tokenModel.Token.Salt)

	// Compare computed hash with stored hash
	return computedHash == tokenModel.Token.TokenCheckSum && tokenModel.ExpireAt.After(time.Now())
}
