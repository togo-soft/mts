// internal/storage/util_test.go
package storage

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestSafeMkdirAll(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test", "nested", "dir")

	err := SafeMkdirAll(testPath, 0700)
	if err != nil {
		t.Fatalf("SafeMkdirAll failed: %v", err)
	}

	info, err := os.Stat(testPath)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if !info.IsDir() {
		t.Errorf("expected directory")
	}
}

func TestSafeCreate(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	f, err := SafeCreate(testFile, 0600)
	if err != nil {
		t.Fatalf("SafeCreate failed: %v", err)
	}
	_ = f.Close()

	// Permission check skipped on Windows due to ACL differences
	if runtime.GOOS != "windows" {
		info, err := os.Stat(testFile)
		if err != nil {
			t.Fatalf("stat failed: %v", err)
		}
		if info.Mode().Perm() != 0600 {
			t.Errorf("expected 0600, got %o", info.Mode().Perm())
		}
	}
}

func TestSafeCreate_NestedDir(t *testing.T) {
	tmpDir := t.TempDir()
	// 测试需要创建嵌套父目录的情况
	testFile := filepath.Join(tmpDir, "nested", "path", "test.txt")

	f, err := SafeCreate(testFile, 0600)
	if err != nil {
		t.Fatalf("SafeCreate failed: %v", err)
	}
	_ = f.Close()

	// 验证文件存在
	info, err := os.Stat(testFile)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if info.IsDir() {
		t.Error("expected file, not directory")
	}
}

func TestSafeOpenFile_AppendMode(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "append.txt")

	// 第一次创建并写入
	f, err := SafeOpenFile(testFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("SafeOpenFile failed: %v", err)
	}
	if _, err := f.Write([]byte("first")); err != nil {
		_ = f.Close()
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 第二次以追加模式打开并写入
	f, err = SafeOpenFile(testFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("SafeOpenFile failed: %v", err)
	}
	if _, err := f.Write([]byte("second")); err != nil {
		_ = f.Close()
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证内容
	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(data) != "firstsecond" {
		t.Errorf("expected 'firstsecond', got %s", string(data))
	}
}

func TestPathError(t *testing.T) {
	underlyingErr := errors.New("permission denied")
	err := &PathError{
		Op:   "open",
		Path: "/test/path",
		Err:  underlyingErr,
	}

	if err.Error() != "invalid path: /test/path: permission denied" {
		t.Errorf("unexpected error message: %s", err.Error())
	}

	unwrapped := err.Unwrap()
	if unwrapped != underlyingErr {
		t.Errorf("Unwrap should return the underlying error")
	}
}

func TestErrInvalidPath(t *testing.T) {
	if ErrInvalidPath.Error() != "path contains invalid components (../ or absolute path)" {
		t.Errorf("unexpected error message: %s", ErrInvalidPath.Error())
	}
}

func TestIsPathSafe(t *testing.T) {
	// 有效路径
	validPaths := []string{
		"relative/path",
		"another/path/here",
		"single",
		"path/./with/dots", // filepath.Clean 后变成 "path/with/dots"
	}

	for _, p := range validPaths {
		if !isPathSafe(p) {
			t.Errorf("expected %q to be safe", p)
		}
	}

	// 无效路径 - 包含 ..
	invalidPaths := []string{
		"../parent",
		"path/../../escaped",
		"",
		".",
	}

	for _, p := range invalidPaths {
		if isPathSafe(p) {
			t.Errorf("expected %q to be unsafe", p)
		}
	}
}
