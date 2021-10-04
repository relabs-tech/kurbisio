package kss

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core/logger"
)

// LocalFilesystem is the entity which provides local filesystem
type LocalFilesystem struct {
	router     *mux.Router
	baseFolder string
	publicURL  url.URL
	privateKey *rsa.PrivateKey
}

// New returns a new LocalFilesystem
func New(router *mux.Router, baseFolder string, publicURL url.URL, privateKey *rsa.PrivateKey) (*LocalFilesystem, error) {
	if privateKey == nil {
		logger.Default().Warn("No private key provided to sign URLs, a random one will be generated")
		logger.Default().Warn("This can only work when running in a single instance configuration")
		logger.Default().Warn("This cannot work when running in AWS Lambda")

		var err error
		privateKey, err = rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, err
		}
	}
	f := LocalFilesystem{router: router, baseFolder: baseFolder, publicURL: publicURL, privateKey: privateKey}
	f.configure()
	return &f, nil
}

func (f LocalFilesystem) configure() {
	logger.Default().Debugln("filesystem routes enabled")
	logger.Default().Debugln("  handle statistics route: /kuribisio/filesystem GET")

	f.router.Handle("/kurbisio/filesystem", http.HandlerFunc(f.handler)).Methods(http.MethodOptions, http.MethodGet, http.MethodPut, http.MethodPost)
}

func (f LocalFilesystem) handler(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	u := r.URL
	if u.Scheme == "" && u.Host == "" {
		u.Scheme = f.publicURL.Scheme
		u.Host = f.publicURL.Host
	}

	if !f.isValid(u.String()) {
		logger.Default().Errorf("invalid signature for %s", u.String())
		http.Error(w, "not authorized", http.StatusUnauthorized)
		return
	}

	key := v.Get("key")
	v.Get("expiry")
	method := v.Get("method")

	if r.Method != method {
		logger.Default().Errorf("Signature valid for %s, but was used for %s in %s", method, r.Method, r.URL.String())
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if strings.Contains(key, "..") {
		http.Error(w, ".. not authorized in keys", http.StatusBadRequest)
		return
	}
	filePath := filepath.Join(f.baseFolder, key, "file")

	logger.Default().Infof("Filesystem: [%s] key: '%s'", r.Method, key)
	if r.Method == http.MethodGet {

		// etag := "compute me"
		// w.Header().Set("Etag", etag)
		// if ifNoneMatchFound(r.Header.Get("If-None-Match"), etag) {
		// 	w.WriteHeader(http.StatusNotModified)
		// 	return
		// }
		http.ServeFile(w, r, filePath)
		return
	}
	if r.Method == http.MethodPost || r.Method == http.MethodPut {

		err := r.ParseMultipartForm(200 * 1024 * 1024)
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 1200: Could not call ParseMultipartForm %s key: '%s'", r.URL.String(), key)
			http.Error(w, "Error 1200", http.StatusInternalServerError)
		}

		file, _, err := r.FormFile(key)
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 1201: Could not read FormFile %s key: '%s'", r.URL.String(), key)
			http.Error(w, "Error 1201", http.StatusInternalServerError)
		}
		defer file.Close()

		err = os.MkdirAll(path.Join(f.baseFolder, key), 0700)
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 1202: Could not create `%s` key: '%s'", f.baseFolder+key, key)
			http.Error(w, "Error 1202", http.StatusInternalServerError)
		}

		dstFile, err := os.Create(path.Join(f.baseFolder, key, "file"))
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 1203: Could not create `%s` key: '%s'", f.baseFolder+key, key)
			http.Error(w, "Error 1203", http.StatusInternalServerError)
		}
		defer dstFile.Close()
		_, err = io.Copy(dstFile, file)
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 1204: Could not copy `%s` key: '%s'", f.baseFolder+key, key)
			http.Error(w, "Error 1204", http.StatusInternalServerError)
		}

		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
	return

}

// Delete deltes a the key file
func (f LocalFilesystem) Delete(key string) error {
	filePath := filepath.Join(f.baseFolder, key)
	return os.RemoveAll(filePath)

}

// GetPreSignedURL returns a pre-signed URL that can be used with the given method until expiry time is passed
// key must be a valid file name
func (f LocalFilesystem) GetPreSignedURL(method, key string, expiry time.Time) (URL string, err error) {
	v := url.Values{}
	v.Set("key", key)
	v.Set("expiry", expiry.Format(time.RFC3339))
	v.Set("method", method)
	if strings.Contains(key, "..") {
		err = fmt.Errorf("'..' is not allowed in a key")
		return
	}
	u := url.URL{
		Scheme:   f.publicURL.Scheme,
		Host:     f.publicURL.Host,
		Path:     f.publicURL.Path + "/kurbisio/filesystem",
		RawQuery: v.Encode(),
	}

	// crypto/rand.Reader is a good source of entropy for blinding the RSA
	// operation.
	rng := rand.Reader

	data, err := json.Marshal(u)
	if err != nil {
		return
	}
	hashed := sha256.Sum256(data)

	signature, err := rsa.SignPKCS1v15(rng, f.privateKey, crypto.SHA256, hashed[:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error from signing: %s\n", err)
		return
	}

	v.Set("signature", string(signature))
	u.RawQuery = v.Encode()
	URL = u.String()
	return
}

// isValid tells whether or not this url is valid
func (f LocalFilesystem) isValid(URL string) bool {
	u, err := url.Parse(URL)
	if err != nil {
		return false
	}
	v := u.Query()
	key := v.Get("key")
	if key == "" || strings.Contains(key, "..") {
		return false
	}
	timeStr := v.Get("expiry")
	if timeStr == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339, timeStr)
	if t.Before(time.Now()) {
		return false
	}

	signature := v.Get("signature")
	v.Del("signature")
	u.RawQuery = v.Encode()

	data, err := json.Marshal(u)
	if err != nil {
		return false
	}
	hashed := sha256.Sum256(data)
	err = rsa.VerifyPKCS1v15(&f.privateKey.PublicKey, crypto.SHA256, hashed[:], []byte(signature))
	if err != nil {

		return false
	}
	return true
}

// ifNoneMatchFound returns true if etag is found in ifNoneMatch. The format of ifNoneMatch is one
// of the following:
// If-None-Match: "<etag_value>"
// If-None-Match: "<etag_value>", "<etag_value>", …
// If-None-Match: *
func ifNoneMatchFound(ifNoneMatch, etag string) bool {
	ifNoneMatch = strings.Trim(ifNoneMatch, " ")
	if len(ifNoneMatch) == 0 {
		return false
	}
	if ifNoneMatch == "*" {
		return true
	}
	for _, s := range strings.Split(ifNoneMatch, ",") {
		s = strings.Trim(s, " \"")
		t := strings.Trim(etag, " \"")
		if s == t {
			return true
		}
	}
	return false
}
