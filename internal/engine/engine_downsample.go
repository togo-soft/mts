package engine

import (
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// downsampleCatalogAdapter 将 engine.Catalog 适配为 downsample.Catalog。
type downsampleCatalogAdapter struct {
	catalog Catalog
}

func (a *downsampleCatalogAdapter) ListDatabases() []string {
	return a.catalog.ListDatabases()
}

func (a *downsampleCatalogAdapter) ListMeasurements(database string) ([]string, error) {
	return a.catalog.ListMeasurements(database)
}

func (a *downsampleCatalogAdapter) GetDatabaseRetention(database string) (time.Duration, error) {
	return a.catalog.GetDatabaseRetention(database)
}

func (a *downsampleCatalogAdapter) GetDownsampleConfig(database string) (*types.DownsampleConfig, error) {
	return a.catalog.GetDownsampleConfig(database)
}

func (a *downsampleCatalogAdapter) GetSchema(database, measurement string) (sstable.Schema, error) {
	metaSchema, err := a.catalog.GetSchema(database, measurement)
	if err != nil {
		return sstable.Schema{}, err
	}
	return shard.MetadataSchemaToSSTableSchema(metaSchema), nil
}
