package metadata

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

// ===================================
// catalogStore — bbolt 版 Catalog
// ===================================

type catalogStore struct {
	db        *bolt.DB
	dbCache   sync.Map // string → struct{}，database 存在性内存缓存
	measCache sync.Map // "db/meas" → struct{}，measurement 存在性内存缓存
}

func newCatalogStore(db *bolt.DB) *catalogStore {
	return &catalogStore{db: db}
}

// rebuildCache 从 bbolt 重建 DB/Meas 内存缓存（Load 时调用）。
func (c *catalogStore) rebuildCache() error {
	return c.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if len(name) == 0 || name[0] == '_' {
				return nil
			}
			dbName := string(name)
			c.dbCache.Store(dbName, struct{}{})
			return b.ForEach(func(k, v []byte) error {
				if v == nil {
					c.measCache.Store(dbName+"/"+string(k), struct{}{})
				}
				return nil
			})
		})
	})
}

func (c *catalogStore) CreateDatabase(name string) error {
	if name == "" {
		return fmt.Errorf("database name is empty")
	}
	err := c.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return fmt.Errorf("create database bucket: %w", err)
		}
		return nil
	})
	if err == nil {
		c.dbCache.Store(name, struct{}{})
	}
	return err
}

func (c *catalogStore) DropDatabase(name string) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte(name)); err != nil {
			if err == berrors.ErrBucketNotFound {
				return fmt.Errorf("database %q not found", name)
			}
			return fmt.Errorf("delete database bucket: %w", err)
		}
		return nil
	})
	if err == nil {
		c.dbCache.Delete(name)
		// 清理该 database 的所有 measurement 缓存
		prefix := name + "/"
		c.measCache.Range(func(k, _ any) bool {
			if strings.HasPrefix(k.(string), prefix) {
				c.measCache.Delete(k)
			}
			return true
		})
	}
	return err
}

func (c *catalogStore) ListDatabases() []string {
	names := make([]string, 0)
	if err := c.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			if len(name) == 0 || name[0] == '_' {
				return nil
			}
			names = append(names, string(name))
			return nil
		})
	}); err != nil {
		slog.Warn("db.View failed", "error", err)
	}
	sort.Strings(names)
	return names
}

func (c *catalogStore) DatabaseExists(name string) bool {
	if _, ok := c.dbCache.Load(name); ok {
		return true
	}
	exists := false
	if err := c.db.View(func(tx *bolt.Tx) error {
		exists = tx.Bucket([]byte(name)) != nil
		return nil
	}); err != nil {
		slog.Warn("db.View failed", "error", err)
	}
	if exists {
		c.dbCache.Store(name, struct{}{})
	}
	return exists
}


