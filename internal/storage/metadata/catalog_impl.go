package metadata

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

// ===================================
// catalogStore — bbolt 版 Catalog
// ===================================

type catalogStore struct {
	db *bolt.DB
}

func newCatalogStore(db *bolt.DB) *catalogStore {
	return &catalogStore{db: db}
}

func (c *catalogStore) CreateDatabase(name string) error {
	if name == "" {
		return fmt.Errorf("database name is empty")
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return fmt.Errorf("create database bucket: %w", err)
		}
		return nil
	})
}

func (c *catalogStore) DropDatabase(name string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte(name)); err != nil {
			if err == berrors.ErrBucketNotFound {
				return fmt.Errorf("database %q not found", name)
			}
			return fmt.Errorf("delete database bucket: %w", err)
		}
		return nil
	})
}

func (c *catalogStore) ListDatabases() []string {
	var names []string
	_ = c.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			if len(name) == 0 || name[0] == '_' {
				return nil
			}
			names = append(names, string(name))
			return nil
		})
	})
	sort.Strings(names)
	return names
}

func (c *catalogStore) DatabaseExists(name string) bool {
	exists := false
	_ = c.db.View(func(tx *bolt.Tx) error {
		exists = tx.Bucket([]byte(name)) != nil
		return nil
	})
	return exists
}

func (c *catalogStore) CreateMeasurement(database, name string) error {
	if database == "" {
		return fmt.Errorf("database name is empty")
	}
	if name == "" {
		return fmt.Errorf("measurement name is empty")
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		if dbBucket.Bucket([]byte(name)) != nil {
			return nil // 已存在
		}
		_, err := dbBucket.CreateBucket([]byte(name))
		if err != nil {
			return fmt.Errorf("create measurement bucket: %w", err)
		}
		return nil
	})
}

func (c *catalogStore) DropMeasurement(database, name string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		if err := dbBucket.DeleteBucket([]byte(name)); err != nil {
			if err == berrors.ErrBucketNotFound {
				return fmt.Errorf("measurement %q not found", name)
			}
			return fmt.Errorf("delete measurement bucket: %w", err)
		}
		return nil
	})
}

func (c *catalogStore) ListMeasurements(database string) ([]string, error) {
	var names []string
	err := c.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		cur := dbBucket.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			if v == nil {
				names = append(names, string(k))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func (c *catalogStore) MeasurementExists(database, name string) bool {
	exists := false
	_ = c.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return nil
		}
		exists = dbBucket.Bucket([]byte(name)) != nil
		return nil
	})
	return exists
}

func (c *catalogStore) GetRetention(database, measurement string) (time.Duration, error) {
	var d time.Duration
	err := c.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return fmt.Errorf("measurement %q not found", measurement)
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
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return fmt.Errorf("measurement %q not found", measurement)
		}
		return measBucket.Put([]byte("_retention"), encodeUint64(uint64(d)))
	})
}

func (c *catalogStore) GetSchema(database, measurement string) (*Schema, error) {
	var s Schema
	err := c.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return fmt.Errorf("measurement %q not found", measurement)
		}
		raw := measBucket.Get([]byte("_schema"))
		if raw == nil {
			return nil
		}
		return json.Unmarshal(raw, &s)
	})
	if err != nil {
		return nil, err
	}
	if s.Version == 0 {
		return nil, nil
	}
	return &s, nil
}

func (c *catalogStore) SetSchema(database, measurement string, s *Schema) error {
	if s == nil {
		return fmt.Errorf("schema is nil")
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return fmt.Errorf("measurement %q not found", measurement)
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
	for _, oldField := range old.Fields {
		for _, newField := range new.Fields {
			if oldField.Name == newField.Name && oldField.Type != newField.Type {
				return fmt.Errorf("incompatible field type change for %q: %d -> %d",
					oldField.Name, oldField.Type, newField.Type)
			}
		}
	}
	return nil
}
