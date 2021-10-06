package backend

import (
	"fmt"
	"net/url"

	"github.com/relabs-tech/backends/core/backend/kss"
	"github.com/relabs-tech/backends/core/logger"
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
		u, err := url.Parse(b.publicURL)
		if err != nil {
			return fmt.Errorf("cannot parse url %s %w", b.publicURL, err)
		}

		drv, err := kss.NewLocalFilesystem(b.router, *config.LocalConfiguration, *u)
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
		b.KssDriver = drv
	} else {
		panic("kss is used but unknown driver type :" + config.DriverType)
	}
	return nil

}
