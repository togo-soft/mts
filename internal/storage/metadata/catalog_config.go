package metadata

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"codeberg.org/micro-ts/mts/types"
)

func (c *catalogStore) GetRetention(database, measurement string) (time.Duration, error) {
	var d time.Duration
	err := c.db.View(func(tx *bolt.Tx) error {
		measBucket, err := getMeasBucket(tx, database, measurement)
		if err != nil {
			return err
		}
		raw := measBucket.Get([]byte("_retention"))
		if raw != nil {
			d = time.Duration(decodeUint64(raw))
		}
		return nil
	})
	return d, err
}

func (c *catalogStore) SetRetention(database, measurement string, d time.Duration) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		measBucket, err := getMeasBucket(tx, database, measurement)
		if err != nil {
			return err
		}
		return measBucket.Put([]byte("_retention"), encodeUint64(uint64(d)))
	})
}

func (c *catalogStore) GetDatabaseRetention(database string) (time.Duration, error) {
	var d time.Duration
	err := c.db.View(func(tx *bolt.Tx) error {
		dbBucket, err := getDBBucket(tx, database)
		if err != nil {
			return err
		}
		raw := dbBucket.Get([]byte("_retention"))
		if raw != nil {
			d = time.Duration(decodeUint64(raw))
		}
		return nil
	})
	return d, err
}

func (c *catalogStore) SetDatabaseRetention(database string, d time.Duration) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		dbBucket, err := getDBBucket(tx, database)
		if err != nil {
			return err
		}
		return dbBucket.Put([]byte("_retention"), encodeUint64(uint64(d)))
	})
}

func (c *catalogStore) GetDownsampleConfig(database string) (*types.DownsampleConfig, error) {
	var cfg types.DownsampleConfig
	err := c.db.View(func(tx *bolt.Tx) error {
		dbBucket, err := getDBBucket(tx, database)
		if err != nil {
			return err
		}
		raw := dbBucket.Get([]byte("_downsample"))
		if raw == nil {
			return nil
		}
		return json.Unmarshal(raw, &cfg)
	})
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *catalogStore) SetDownsampleConfig(database string, cfg *types.DownsampleConfig) error {
	if cfg == nil {
		return fmt.Errorf("downsample config is nil")
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		dbBucket, err := getDBBucket(tx, database)
		if err != nil {
			return err
		}
		data, err := json.Marshal(cfg)
		if err != nil {
			return fmt.Errorf("marshal downsample config: %w", err)
		}
		return dbBucket.Put([]byte("_downsample"), data)
	})
}

func (c *catalogStore) GetSchema(database, measurement string) (*Schema, error) {
	var s Schema
	err := c.db.View(func(tx *bolt.Tx) error {
		measBucket, err := getMeasBucket(tx, database, measurement)
		if err != nil {
			return err
		}
		raw := measBucket.Get([]byte("_schema"))
		if raw == nil {
			return nil // schema 不存在时，返回 nil 而非错误，由调用方区分
		}
		return json.Unmarshal(raw, &s)
	})
	if err != nil {
		return nil, err
	}
	if s.Version == 0 {
		return nil, nil // schema 未初始化时返回 (nil, nil)，表示数据库不存在 but schema 是合法状态
	}
	return &s, nil
}

func (c *catalogStore) SetSchema(database, measurement string, s *Schema) error {
	if s == nil {
		return fmt.Errorf("schema is nil")
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		measBucket, err := getMeasBucket(tx, database, measurement)
		if err != nil {
			return err
		}

		// 校验兼容性
		if raw := measBucket.Get([]byte("_schema")); raw != nil {
			var existing Schema
			if err := json.Unmarshal(raw, &existing); err == nil {
				if err := validateSchemaUpdate(&existing, s); err != nil {
					return err
				}
			}
		}

		s.UpdatedAt = time.Now().UnixNano()
		data, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("marshal schema: %w", err)
		}
		return measBucket.Put([]byte("_schema"), data)
	})
}

// validateSchemaUpdate 校验 schema 更新是否兼容。
func validateSchemaUpdate(old, new *Schema) error {
	oldTypes := make(map[string]int32, len(old.Fields))
	for _, f := range old.Fields {
		oldTypes[f.Name] = f.Type
	}
	for _, newField := range new.Fields {
		if oldType, exists := oldTypes[newField.Name]; exists && oldType != newField.Type {
			return fmt.Errorf("incompatible field type change for %q: %d -> %d",
				newField.Name, oldType, newField.Type)
		}
	}
	return nil
}
