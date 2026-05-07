package sstable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"

	"codeberg.org/micro-ts/mts/internal/storage"
	"codeberg.org/micro-ts/mts/types"
)

// flushBlock 将当前 block 缓冲写入文件
func (w *Writer) flushBlock() error {
	if w.bufPos == 0 && w.rowCount == 0 {
		return nil
	}

	info, err := w.timestamp.Stat()
	if err != nil {
		return fmt.Errorf("stat timestamp file: %w", err)
	}
	offset := uint32(info.Size())

	if _, err := w.timestamp.Write(w.buf[:w.bufPos]); err != nil {
		return fmt.Errorf("write timestamp block: %w", err)
	}

	for _, sid := range w.sidBuf {
		var sidBuf [8]byte
		binary.BigEndian.PutUint64(sidBuf[:], sid)
		if _, err := w.sids.Write(sidBuf[:]); err != nil {
			return fmt.Errorf("write sid block: %w", err)
		}
	}
	w.sidBuf = w.sidBuf[:0]

	for name, buf := range w.fieldBufs {
		if _, err := w.fields[name].Write(buf); err != nil {
			return fmt.Errorf("write field block %s: %w", name, err)
		}
		w.fieldBufs[name] = w.fieldBufs[name][:0]
	}

	blockRowCount := uint32(w.bufPos / 8)
	lastTs := int64(binary.BigEndian.Uint64(w.buf[w.bufPos-8:]))
	w.blockIndex.Add(w.firstTs, lastTs, offset, blockRowCount)

	w.bufPos = 0
	w.rowCount = 0
	w.firstTs = 0

	return nil
}

// Close 关闭 Writer，完成 SSTable 写入。
func (w *Writer) Close() error {
	if err := w.flushBlock(); err != nil {
		return fmt.Errorf("flush block: %w", err)
	}
	if err := w.writeSchema(); err != nil {
		return fmt.Errorf("write schema: %w", err)
	}
	if err := w.writeBlockIndex(); err != nil {
		return fmt.Errorf("write block index: %w", err)
	}
	if w.timestamp != nil {
		if err := w.timestamp.Close(); err != nil {
			return fmt.Errorf("close timestamp file: %w", err)
		}
	}
	if w.sids != nil {
		if err := w.sids.Close(); err != nil {
			return fmt.Errorf("close sids file: %w", err)
		}
	}
	for name, f := range w.fields {
		if err := f.Close(); err != nil {
			return fmt.Errorf("close field file %s: %w", name, err)
		}
	}
	return nil
}

func (w *Writer) writeBlockIndex() error {
	indexFile := filepath.Join(w.dataDir, "_index.bin")
	return w.blockIndex.Write(indexFile)
}

func (w *Writer) writeSchema() error {
	schemaFile, err := storage.SafeCreate(filepath.Join(w.dataDir, "_schema.json"), 0600)
	if err != nil {
		return fmt.Errorf("create schema file: %w", err)
	}
	defer func() { _ = schemaFile.Close() }()

	data, err := json.Marshal(w.schema)
	if err != nil {
		return fmt.Errorf("marshal schema: %w", err)
	}
	if _, err := schemaFile.Write(data); err != nil {
		return fmt.Errorf("write schema file: %w", err)
	}
	return nil
}

// detectFieldType 检测字段类型
func detectFieldType(val any) FieldType {
	if val == nil {
		return FieldTypeFloat64
	}

	if fv, ok := val.(*types.FieldValue); ok {
		if fv == nil || fv.Value == nil {
			return FieldTypeFloat64
		}
		switch fv.Value.(type) {
		case *types.FieldValue_FloatValue:
			return FieldTypeFloat64
		case *types.FieldValue_IntValue:
			return FieldTypeInt64
		case *types.FieldValue_StringValue:
			return FieldTypeString
		case *types.FieldValue_BoolValue:
			return FieldTypeBool
		}
		return FieldTypeFloat64
	}

	switch val.(type) {
	case float64:
		return FieldTypeFloat64
	case int64:
		return FieldTypeInt64
	case string:
		return FieldTypeString
	case bool:
		return FieldTypeBool
	}
	return FieldTypeFloat64
}
