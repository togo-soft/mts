// Package storage 提供存储相关的工具函数。
//
// 包括安全的文件和目录操作，确保正确的权限设置（0700 目录，0600 文件）。
// 所有函数都处理目录创建链，简化调用方的代码。
//
// 安全原则：
//
//   - 目录权限 0700 (rwx------)
//   - 文件权限 0600 (rw-------)
//   - 自动创建父目录
package storage

import (
	"os"
	"path/filepath"
)

// SafeMkdirAll 安全地创建目录及其所有父目录。
//
// 参数：
//   - path: 要创建的目录路径
//   - perm: 目录权限（通常应为 0700）
//
// 返回：
//   - error: 创建失败时返回错误
//
// 行为：
//
//	首先确保父目录存在（使用 0755 权限），
//	然后创建目标目录并应用指定的权限。
//	这是为了兼容父目录可能需要不同的权限策略。
func SafeMkdirAll(path string, perm uint32) error {
	// 确保父目录存在
	parent := filepath.Dir(path)
	if parent != "" && parent != "." {
		if err := os.MkdirAll(parent, 0755); err != nil {
			return err
		}
	}
	return os.MkdirAll(path, os.FileMode(perm))
}

// SafeCreate 安全地创建或覆盖文件。
//
// 参数：
//   - path: 文件路径
//   - perm: 文件权限（通常应为 0600）
//
// 返回：
//   - *os.File: 打开的文件句柄
//   - error:    创建失败时返回错误
//
// 行为：
//
//	如果父目录不存在，自动创建（权限 0700）。
//	如果文件已存在，会被截断。
//	文件以读写模式打开。
func SafeCreate(path string, perm uint32) (*os.File, error) {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := SafeMkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	}
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(perm))
}

// SafeOpenFile 安全地打开或创建文件。
//
// 参数：
//   - name: 文件路径
//   - flag: 打开标志（如 os.O_RDWR|os.O_CREATE）
//   - perm: 文件权限（通常应为 0600）
//
// 返回：
//   - *os.File: 打开的文件句柄
//   - error:    操作失败时返回错误
//
// 行为：
//
//	与 os.OpenFile 类似，但会自动创建父目录（权限 0700）。
//	提供更细粒度的控制（如追加模式、只读模式等）。
//	适用于需要特定打开标志的场景。
//
// 使用示例：
//
//	f, err := storage.SafeOpenFile("/data/wal/0001.wal", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
//	if err != nil {
//	    return err
//	}
//	defer f.Close()
func SafeOpenFile(name string, flag int, perm uint32) (*os.File, error) {
	dir := filepath.Dir(name)
	if dir != "" && dir != "." {
		if err := SafeMkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	}
	return os.OpenFile(name, flag, os.FileMode(perm))
}
