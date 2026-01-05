package timeseries

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/go-graphite/go-whisper"
)

var (
	DefaultRetentions = whisper.MustParseRetentionDefs("1m:30m,10m:3d,1h:90d,1d:3y")
	DailyRetentions   = whisper.MustParseRetentionDefs("1d:5y")
)

func Update(wspFilename string, value float64, timestamp int, retentions whisper.Retentions, aggregationMethod whisper.AggregationMethod, xFilesFactor float32) error {
	wsp, err := whisper.Open(wspFilename)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(wspFilename), 0o750); err != nil {
			return err
		}
		if wsp, err = whisper.Create(wspFilename, retentions, aggregationMethod, xFilesFactor); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	defer wsp.Close()
	return wsp.Update(value, timestamp)
}
