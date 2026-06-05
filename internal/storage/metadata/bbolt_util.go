package metadata

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// getMeasBucket 获取指定 database/measurement 的 bbolt bucket。
// 如果 database 或 measurement 不存在，返回错误。
func getMeasBucket(tx *bolt.Tx, database, measurement string) (*bolt.Bucket, error) {
	dbBucket := tx.Bucket([]byte(database))
	if dbBucket == nil {
		return nil, fmt.Errorf("database %q not found", database)
	}
	measBucket := dbBucket.Bucket([]byte(measurement))
	if measBucket == nil {
		return nil, fmt.Errorf("measurement %q not found", measurement)
	}
	return measBucket, nil
}

// getDBBucket 获取指定 database 的 bbolt bucket。
// 如果 database 不存在，返回错误。
func getDBBucket(tx *bolt.Tx, database string) (*bolt.Bucket, error) {
	dbBucket := tx.Bucket([]byte(database))
	if dbBucket == nil {
		return nil, fmt.Errorf("database %q not found", database)
	}
	return dbBucket, nil
}
