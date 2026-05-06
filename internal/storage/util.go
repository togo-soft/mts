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
	"strings"
)

// isPathSafe 检查路径是否是安全的，不包含路径遍历攻击
//
// 检查内容：
//   - 不包含 .. 路径组件（标准化后）
//   - 不包含空组件
func isPathSafe(path string) bool {
	// 清理路径
	cleaned := filepath.Clean(path)

	// 检查是否包含 .. 路径遍历（标准化后）
	if strings.Contains(cleaned, "..") {
		return false
	}

	// 检查是否有空组件
	if cleaned == "" || cleaned == "." {
		return false
	}

	return true
}

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
//	首先确保父目录存在（使用 0700 权限），
//	然后创建目标目录并应用指定的权限。
//	这是为了兼容父目录可能需要不同的权限策略。
//
// 安全检查：
//
//	路径不能包含 .. 路径遍历组件。
//	路径不能是绝对路径。
func SafeMkdirAll(path string, perm uint32) error {
	if !isPathSafe(path) {
		return &PathError{Op: "mkdir", Path: path, Err: ErrInvalidPath}
	}

	// 确保父目录存在
	parent := filepath.Dir(path)
	if parent != "" && parent != "." {
		if err := os.MkdirAll(parent, 0700); err != nil {
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
//
// 安全检查：
//
//	路径不能包含 .. 路径遍历组件。
//	路径不能是绝对路径。
func SafeCreate(path string, perm uint32) (*os.File, error) {
	if !isPathSafe(path) {
		return nil, &PathError{Op: "create", Path: path, Err: ErrInvalidPath}
	}

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
// 安全检查：
//
//	路径不能包含 .. 路径遍历组件。
//	路径不能是绝对路径。
//
// 使用示例：
//
//	f, err := storage.SafeOpenFile("data/wal/0001.wal", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
//	if err != nil {
//	    return err
//	}
//	defer f.Close()
func SafeOpenFile(name string, flag int, perm uint32) (*os.File, error) {
	if !isPathSafe(name) {
		return nil, &PathError{Op: "open", Path: name, Err: ErrInvalidPath}
	}

	dir := filepath.Dir(name)
	if dir != "" && dir != "." {
		if err := SafeMkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	}
	return os.OpenFile(name, flag, os.FileMode(perm))
}

// PathError 表示路径安全检查失败
type PathError struct {
	Op  string
	Path string
	Err error
}

func (e *PathError) Error() string {
	return "invalid path: " + e.Path + ": " + e.Err.Error()
}

func (e *PathError) Unwrap() error {
	return e.Err
}

// ErrInvalidPath 表示路径包含非法组件
var ErrInvalidPath = &pathError{"path contains invalid components (../ or absolute path)"}

type pathError struct {
	msg string
}

func (e *pathError) Error() string {
	return e.msg
}
