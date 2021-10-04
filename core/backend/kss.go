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
	logger.Default().Info("KSS in use")

	if config.DriverType == kss.None {
		panic("kss is used but no configuration is found")
	}

	if config.DriverType == kss.DriverTypeLocal {
		if config.LocalConfiguration == nil {
			return fmt.Errorf("kss expecting a configuration for local KSS, but got nothing")
		}
		u, err := url.Parse(b.publicURL)
		if err != nil {
			return fmt.Errorf("cannot parse url %s %w", b.publicURL, err)
		}

		drv, err := kss.New(b.router, config.LocalConfiguration.BasePath, *u, nil)
		if err != nil {
			return fmt.Errorf("cannot create new KSS driver %s %w", b.publicURL, err)
		}
		b.KssDriver = drv
	}
	return nil

}
