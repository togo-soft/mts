package metadata

import (
	"fmt"
	"log/slog"
	"sort"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

func (c *catalogStore) CreateMeasurement(database, name string) error {
	if database == "" {
		return fmt.Errorf("database name is empty")
	}
	if name == "" {
		return fmt.Errorf("measurement name is empty")
	}
	err := c.db.Update(func(tx *bolt.Tx) error {
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
	if err == nil {
		c.measCache.Store(database+"/"+name, struct{}{})
	}
	return err
}

func (c *catalogStore) DropMeasurement(database, name string) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
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
	if err == nil {
		c.measCache.Delete(database + "/" + name)
	}
	return err
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
	key := database + "/" + name
	if _, ok := c.measCache.Load(key); ok {
		return true
	}
	exists := false
	if err := c.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return nil
		}
		exists = dbBucket.Bucket([]byte(name)) != nil
		return nil
	}); err != nil {
		slog.Warn("db.View failed", "error", err)
	}
	if exists {
		c.measCache.Store(key, struct{}{})
	}
	return exists
}
