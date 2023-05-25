package kss

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core/logger"
)

// LocalFilesystem is the entity which provides local filesystem
type LocalFilesystem struct {
	router     *mux.Router
	baseFolder string
	publicURL  url.URL
	privateKey *rsa.PrivateKey
	callback   FileUpdatedCallBack
}

// NewLocalFilesystem returns a new LocalFilesystem
func NewLocalFilesystem(router *mux.Router, config LocalConfiguration) (*LocalFilesystem, error) {
	if config.PrivateKey == nil {
		logger.Default().Warn("No private key provided to sign URLs, a random one will be generated")
		logger.Default().Warn("This can only work when running in a single instance configuration")
		logger.Default().Warn("This cannot work when running in AWS Lambda")

		var err error
		config.PrivateKey, err = rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, err
		}
	}
	publicURL, err := url.Parse((config.PublicURL))
	if err != nil {
		return nil, err
	}
	f := LocalFilesystem{
		router:     router,
		baseFolder: config.KeyPrefix,
		publicURL:  *publicURL,
		privateKey: config.PrivateKey,
	}

	f.configure()
	return &f, nil
}

// WithCallBack Replaces teh current callback with WithCallBack
func (f *LocalFilesystem) WithCallBack(callback FileUpdatedCallBack) {
	f.callback = callback
}

func (f *LocalFilesystem) configure() {
	logger.Default().Debugln("filesystem routes enabled")
	logger.Default().Debugln("  handle statistics route: /kuribisio/filesystem GET")

	f.router.Handle("/kurbisio/filesystem", http.HandlerFunc(f.handler)).Methods(http.MethodOptions, http.MethodGet, http.MethodPut, http.MethodPost)
}

func (f *LocalFilesystem) handler(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	u := r.URL
	if u.Scheme == "" && u.Host == "" {
		u.Scheme = f.publicURL.Scheme
		u.Host = f.publicURL.Host
	}

	if !f.isValid(u.String()) {
		logger.Default().Errorf("invalid signature for %s", u.String())
		http.Error(w, "not authorized", http.StatusForbidden)
		return
	}

	key := v.Get("key")
	v.Get("expiry")
	method := v.Get("method")

	if r.Method != method {
		logger.Default().Errorf("Signature valid for %s, but was used for %s in %s", method, r.Method, r.URL.String())
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if strings.Contains(key, "..") {
		http.Error(w, ".. not authorized in keys", http.StatusBadRequest)
		return
	}
	filePath := filepath.Join(f.baseFolder, key, "file")

	logger.Default().Infof("Filesystem: [%s] key: '%s'", r.Method, key)
	if r.Method == http.MethodGet {
		http.ServeFile(w, r, filePath)
		return
	}
	if r.Method == http.MethodPut {

		dirPath := filepath.Dir(filePath)
		err := os.MkdirAll(dirPath, 0700)
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 1202: Could not create `%s` key: '%s'", f.baseFolder+key, key)
			http.Error(w, "Error 1202", http.StatusInternalServerError)
			return
		}

		dstFile, err := os.Create(filePath)
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 1203: Could not create `%s` key: '%s'", f.baseFolder+key, key)
			http.Error(w, "Error 1203", http.StatusInternalServerError)
			return
		}
		defer dstFile.Close()
		defer r.Body.Close()
		_, err = io.Copy(dstFile, r.Body)
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 1204: Could not copy `%s` key: '%s'", f.baseFolder+key, key)
			http.Error(w, "Error 1204", http.StatusInternalServerError)
			return
		}

		if f.callback != nil {
			s, err := dstFile.Stat()
			size := s.Size()
			if err != nil {
				logger.Default().WithError(err).Errorf("Could not obtain stat for file %s", dstFile.Name())
			}

			go f.callback(FileUpdatedEvent{
				Etags: time.Now().Format(time.RFC1123),
				Key:   key,
				Type:  "uploaded",
				Size:  size,
			})
		}

		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
	return

}

// Delete deletes a the key file
func (f *LocalFilesystem) Delete(key string) error {
	logger.Default().Infoln("Deleting ", key)
	filePath := filepath.Join(f.baseFolder, key, "file")
	if err := os.RemoveAll(filePath); err != nil {
		return err
	}
	f.recurseDeleteParentIfEmpty(filepath.Join(f.baseFolder, key))
	return nil
}

// recurseDeleteParentIfEmpty the current dir if it is empty and delete the parent if it is also empty after deleting the current
func (f *LocalFilesystem) recurseDeleteParentIfEmpty(currentDir string) {
	absCurrentDir, err := filepath.Abs(currentDir)
	if err != nil {
		logger.Default().WithError(err).Error("Could not get abs path of ", currentDir)
		return
	}
	absBaseFolder, err := filepath.Abs(f.baseFolder)
	if err != nil {
		logger.Default().WithError(err).Error("Could not get abs path of basefolder ", f.baseFolder)
		return
	}
	if absBaseFolder == absCurrentDir {
		return
	}

	entries, err := os.ReadDir(currentDir)
	if err != nil {
		logger.Default().WithError(err).Error("Could not list ", currentDir)
		return
	}
	if len(entries) > 0 {
		return
	}
	if err := os.Remove(currentDir); err != nil {
		logger.Default().WithError(err).Error("Could not delete ", currentDir)
	}
	f.recurseDeleteParentIfEmpty(filepath.Dir(currentDir))
}

// DeleteAllWithPrefix all keys starting with
func (f *LocalFilesystem) DeleteAllWithPrefix(key string) error {
	filePath := filepath.Join(f.baseFolder, key)
	if err := os.RemoveAll(filePath); err != nil {
		return err
	}
	f.recurseDeleteParentIfEmpty(filepath.Join(f.baseFolder, key))
	return nil
}

// GetPreSignedURL returns a pre-signed URL that can be used with the given method until expiry time is passed
// key must be a valid file name
func (f *LocalFilesystem) GetPreSignedURL(method Method, key string, expireIn time.Duration) (URL string, err error) {
	v := url.Values{}
	v.Set("key", key)
	v.Set("expiry", time.Now().Add(expireIn).Format(time.RFC3339))
	v.Set("method", string(method))
	if strings.Contains(key, "..") {
		err = fmt.Errorf("'..' is not allowed in a key")
		return
	}
	if key == "" {
		err = fmt.Errorf("empty key is not allowed")
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

	hashed := sha256.Sum256([]byte(v.Encode()))

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
func (f *LocalFilesystem) isValid(URL string) bool {
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

	hashed := sha256.Sum256([]byte(v.Encode()))
	err = rsa.VerifyPKCS1v15(&f.privateKey.PublicKey, crypto.SHA256, hashed[:], []byte(signature))
	if err != nil {

		return false
	}
	return true
}

// UploadData uploads data into a new key object
func (f *LocalFilesystem) UploadData(key string, data []byte) error {
	logger.Default().Infoln("Writing ", key)
	filePath := filepath.Join(f.baseFolder, key, "file")
	dirPath := filepath.Dir(filePath)
	err := os.MkdirAll(dirPath, 0700)
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0666)
}

// DownloadData downloads data from key object
func (f *LocalFilesystem) DownloadData(key string) ([]byte, error) {
	logger.Default().Infoln("Reading ", key)
	filePath := filepath.Join(f.baseFolder, key, "file")
	return os.ReadFile(filePath)
}
