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
	tests := []struct {
		name     string
		path     string
		perm     uint32
		wantErr  bool
		validate func(*testing.T, string)
	}{
		{
			name:    "simple directory",
			path:    filepath.Join(t.TempDir(), "simple"),
			perm:    0700,
			wantErr: false,
			validate: func(t *testing.T, path string) {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatalf("stat failed: %v", err)
				}
				if !info.IsDir() {
					t.Error("expected directory")
				}
			},
		},
		{
			name:    "nested directories",
			path:    filepath.Join(t.TempDir(), "a", "b", "c"),
			perm:    0700,
			wantErr: false,
			validate: func(t *testing.T, path string) {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatalf("stat failed: %v", err)
				}
				if !info.IsDir() {
					t.Error("expected directory")
				}
			},
		},
		{
			name:    "empty string",
			path:    "",
			perm:    0700,
			wantErr: true,
			validate: func(t *testing.T, path string) {},
		},
		{
			name:    "current dir",
			path:    ".",
			perm:    0700,
			wantErr: true,
			validate: func(t *testing.T, path string) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SafeMkdirAll(tt.path, tt.perm)
			if (err != nil) != tt.wantErr {
				t.Errorf("SafeMkdirAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				tt.validate(t, tt.path)
			}
		})
	}
}

func TestSafeCreate(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		perm     uint32
		wantErr  bool
		validate func(*testing.T, string)
	}{
		{
			name:    "simple file",
			path:    filepath.Join(t.TempDir(), "test.txt"),
			perm:    0600,
			wantErr: false,
			validate: func(t *testing.T, path string) {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatalf("stat failed: %v", err)
				}
				if info.IsDir() {
					t.Error("expected file, not directory")
				}
				if runtime.GOOS != "windows" {
					if info.Mode().Perm() != 0600 {
						t.Errorf("expected 0600, got %o", info.Mode().Perm())
					}
				}
			},
		},
		{
			name:    "nested path",
			path:    filepath.Join(t.TempDir(), "nested", "path", "test.txt"),
			perm:    0600,
			wantErr: false,
			validate: func(t *testing.T, path string) {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("stat failed: %v", err)
				}
			},
		},
		{
			name:    "parent traversal at root",
			path:    "../escape.txt",
			perm:    0600,
			wantErr: true,
			validate: func(t *testing.T, path string) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := SafeCreate(tt.path, tt.perm)
			if (err != nil) != tt.wantErr {
				t.Errorf("SafeCreate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if f != nil {
				if err := f.Close(); err != nil {
					t.Logf("Close failed: %v", err)
				}
			}
			if !tt.wantErr {
				tt.validate(t, tt.path)
			}
		})
	}
}

func TestSafeOpenFile(t *testing.T) {
	tests := []struct {
		name    string
		flag    int
		perm    uint32
		wantErr bool
		verify  func(*testing.T, string)
	}{
		{
			name:    "create mode",
			flag:    os.O_RDWR | os.O_CREATE,
			perm:    0600,
			wantErr: false,
			verify: func(t *testing.T, path string) {
				f, err := os.OpenFile(path, os.O_RDWR, 0600)
				if err != nil {
					t.Fatalf("open failed: %v", err)
				}
				_ = f.Close()
			},
		},
		{
			name:    "append mode - sequential writes",
			flag:    os.O_RDWR | os.O_CREATE | os.O_APPEND,
			perm:    0600,
			wantErr: false,
			verify: func(t *testing.T, path string) {
				// First write
				f1, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatalf("SafeOpenFile failed: %v", err)
				}
				if _, err := f1.Write([]byte("first")); err != nil {
					_ = f1.Close()
					t.Fatalf("Write failed: %v", err)
				}
				if err := f1.Close(); err != nil {
					t.Fatalf("Close failed: %v", err)
				}

				// Second write (append)
				f2, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
				if err != nil {
					t.Fatalf("SafeOpenFile failed: %v", err)
				}
				if _, err := f2.Write([]byte("second")); err != nil {
					_ = f2.Close()
					t.Fatalf("Write failed: %v", err)
				}
				if err := f2.Close(); err != nil {
					t.Fatalf("Close failed: %v", err)
				}

				// Verify content
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("ReadFile failed: %v", err)
				}
				if string(data) != "firstsecond" {
					t.Errorf("expected 'firstsecond', got %s", string(data))
				}
			},
		},
		{
			name:    "truncate mode",
			flag:    os.O_RDWR | os.O_CREATE | os.O_TRUNC,
			perm:    0600,
			wantErr: false,
			verify: func(t *testing.T, path string) {
				// Write some data
				f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatalf("SafeOpenFile failed: %v", err)
				}
				if _, err := f.Write([]byte("hello")); err != nil {
					_ = f.Close()
					t.Fatalf("Write failed: %v", err)
				}
				_ = f.Close()

				// Truncate by opening with O_TRUNC
				f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatalf("SafeOpenFile failed: %v", err)
				}
				_ = f.Close()

				// Verify truncated
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("ReadFile failed: %v", err)
				}
				if len(data) != 0 {
					t.Errorf("expected empty file after truncate, got %d bytes", len(data))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tt.name+".txt")
			f, err := SafeOpenFile(path, tt.flag, tt.perm)
			if (err != nil) != tt.wantErr {
				t.Errorf("SafeOpenFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if f != nil {
				if err := f.Close(); err != nil {
					t.Logf("Close failed: %v", err)
				}
			}
			if !tt.wantErr {
				tt.verify(t, path)
			}
		})
	}
}

func TestPathError(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		path     string
		errMsg   string
		unwraps  bool
	}{
		{
			name:    "open operation",
			op:      "open",
			path:    "/test/path",
			errMsg:  "invalid path: /test/path: permission denied",
			unwraps: true,
		},
		{
			name:    "create operation",
			op:      "create",
			path:    "/another/path",
			errMsg:  "invalid path: /another/path: permission denied",
			unwraps: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			underlyingErr := errors.New("permission denied")
			err := &PathError{
				Op:   tt.op,
				Path: tt.path,
				Err:  underlyingErr,
			}

			if err.Error() != tt.errMsg {
				t.Errorf("Error() = %q, want %q", err.Error(), tt.errMsg)
			}

			unwrapped := err.Unwrap()
			if tt.unwraps && unwrapped != underlyingErr {
				t.Errorf("Unwrap() = %v, want %v", unwrapped, underlyingErr)
			}
		})
	}
}

func TestErrInvalidPath(t *testing.T) {
	tests := []struct {
		name string
		msg  string
	}{
		{
			name: "default message",
			msg:  "path contains invalid components (../ or absolute path)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if ErrInvalidPath.Error() != tt.msg {
				t.Errorf("ErrInvalidPath.Error() = %q, want %q", ErrInvalidPath.Error(), tt.msg)
			}
		})
	}
}

func TestIsPathSafe(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		safe  bool
	}{
		{name: "simple relative", path: "relative/path", safe: true},
		{name: "another relative", path: "another/path/here", safe: true},
		{name: "single", path: "single", safe: true},
		{name: "cleaned dots", path: "path/./with/dots", safe: true},
		{name: "parent traversal", path: "../parent", safe: false},
		{name: "deep parent traversal", path: "path/../../escaped", safe: false},
		{name: "empty", path: "", safe: false},
		{name: "current dir", path: ".", safe: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPathSafe(tt.path); got != tt.safe {
				t.Errorf("isPathSafe(%q) = %v, want %v", tt.path, got, tt.safe)
			}
		})
	}
}
