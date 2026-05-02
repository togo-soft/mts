// internal/storage/util.go
package storage

import (
	"os"
	"path/filepath"
)

// safeMkdirAll 创建目录，设置权限 0700
func safeMkdirAll(path string, perm uint32) error {
	// 确保父目录存在
	parent := filepath.Dir(path)
	if parent != "" && parent != "." {
		if err := os.MkdirAll(parent, 0755); err != nil {
			return err
		}
	}
	return os.MkdirAll(path, os.FileMode(perm))
}

// safeCreate 创建文件，设置权限 0600
func safeCreate(path string, perm uint32) (*os.File, error) {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := safeMkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	}
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(perm))
}

// safeOpenFile 打开文件，设置权限
func safeOpenFile(name string, flag int, perm uint32) (*os.File, error) {
	dir := filepath.Dir(name)
	if dir != "" && dir != "." {
		if err := safeMkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	}
	return os.OpenFile(name, flag, os.FileMode(perm))
}
