package backend

import (
	"context"
	"fmt"
	"strings"

	"github.com/goccy/go-json"

	"github.com/google/uuid"
	"github.com/relabs-tech/kurbisio/core"
	"github.com/relabs-tech/kurbisio/core/backend/kss"
	"github.com/relabs-tech/kurbisio/core/logger"
)

func (b *Backend) configureKSS(config kss.Configuration) error {
	var hasCompanionFileEnabled bool
	for _, c := range b.config.Collections {
		if c.WithCompanionFile {
			hasCompanionFileEnabled = true
			break
		}
	}
	if !hasCompanionFileEnabled {
		logger.Default().Info("KSS not in use")
		return nil
	}
	logger.Default().Info("KSS in use with driver", config.DriverType)

	if config.DriverType == kss.DriverTypeLocal {
		if config.LocalConfiguration == nil {
			return fmt.Errorf("kss expecting a configuration for local KSS, but got nothing")
		}

		if config.LocalConfiguration.PublicURL == "" {
			if b.publicURL != "" {
				logger.Default().Warnf("KSS uses %s as public URL since PublicURL is defined", b.publicURL)
				config.LocalConfiguration.PublicURL = b.publicURL
			} else {
				logger.Default().Warnf("KSS has no PublicURL defined, this can only work in test. KSS Public URL shall be set to the name address that is served by Kurbisio")
			}
		}
		drv, err := kss.NewLocalFilesystem(b.router, *config.LocalConfiguration)
		drv.WithCallBack(b.fileUploadedCallBack)
		if err != nil {
			return fmt.Errorf("cannot create new Local KSS driver %s %w", b.publicURL, err)
		}
		b.KssDriver = drv
	} else if config.DriverType == kss.DriverTypeAWSS3 {
		if config.S3Configuration == nil {
			return fmt.Errorf("kss expecting a configuration for S3 KSS, but got nothing")
		}

		drv, err := kss.NewS3(*config.S3Configuration)

		if err != nil {
			return fmt.Errorf("cannot create new S3 KSS driver %s %w", b.publicURL, err)
		}
		drv.WithCallBack(b.fileUploadedCallBack)
		b.KssDriver = drv
	} else {
		panic("kss is used but unknown driver type :" + config.DriverType)
	}
	return nil

}

func (b *Backend) fileUploadedCallBack(event kss.FileUpdatedEvent) error {
	nillog := logger.FromContext(nil)
	// We create an empty Tx to be able to call commitWithNotification
	// TODO store informatino about the file (etags and size at least) in the resource.
	// The Tx will be useful in this case and this avoid to refactor commitWithNotification to be
	// able to use it without a Tx
	tx, err := b.db.BeginTx(context.TODO(), nil)
	if err != nil {
		return fmt.Errorf("fileUploadedCallBack cannot BeginTx %w", err)
	}
	var primaryID uuid.UUID

	// we trim the potential leading and trailing "/"
	key := strings.TrimPrefix(event.Key, "/")
	key = strings.TrimSuffix(key, "/")

	ids := strings.Split(key, "/")
	if len(ids) == 0 {
		return fmt.Errorf("cannot find IDs in key %s", event.Key)
	}
	primaryID, err = uuid.Parse(ids[len(ids)-1])
	if err != nil {
		return fmt.Errorf("fileUploadedCallBack last id in in key %s is wrong, %w", event.Key, err)
	}
	var resource string

	// keys are constructed using /{parent_name}_id/uuid/{child_name}_id/uuid
	for n := range ids {
		if n%2 == 0 {
			resource += strings.TrimSuffix(ids[n], "_id") + "/"
		}
	}
	resource = strings.TrimSuffix(resource, "/")
	nillog.Infof("create OperationCompanionUploaded notification for %s", resource)

	notificationJSON, _ := json.Marshal(event)
	err = b.commitWithNotification(context.TODO(), tx, resource, core.OperationCompanionUploaded, primaryID, notificationJSON)
	if err != nil {
		return fmt.Errorf("fileUploadedCallBack cannot commitWithNotification %w", err)
	}

	return nil
}
