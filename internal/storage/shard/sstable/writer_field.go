package sstable

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"codeberg.org/micro-ts/mts/internal/storage"
	"codeberg.org/micro-ts/mts/types"
)

// WritePoints 写入一批数据点到 SSTable。
func (w *Writer) WritePoints(points []*types.Point, tsSidMap map[int64]uint64) error {
	fieldNames := make(map[string]bool)
	for _, p := range points {
		for name, val := range p.Fields {
			fieldNames[name] = true
			if _, exists := w.schema.Fields[name]; !exists {
				w.schema.Fields[name] = detectFieldType(val)
			}
		}
	}

	for name := range fieldNames {
		f, err := storage.SafeOpenFile(
			filepath.Join(w.dataDir, "fields", name+".bin"),
			os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("open field file %s: %w", name, err)
		}
		w.fields[name] = f

		w.fieldBufs[name] = make([]byte, 0, BlockSize)
		w.fieldSizes[name] = w.fieldTypeSize(w.schema.Fields[name])
	}

	for _, p := range points {
		sid := tsSidMap[p.Timestamp]
		if err := w.writePointWithSid(p, sid); err != nil {
			return fmt.Errorf("write point (timestamp=%d): %w", p.Timestamp, err)
		}
	}

	return nil
}

// fieldTypeSize 返回字段类型的固定大小
func (w *Writer) fieldTypeSize(t FieldType) int {
	switch t {
	case FieldTypeFloat64, FieldTypeInt64:
		return 8
	case FieldTypeBool:
		return 1
	case FieldTypeString:
		return -1
	default:
		return 8
	}
}

// writePointWithSid 将单个 point 写入 block buffer，并记录 Sid
func (w *Writer) writePointWithSid(p *types.Point, sid uint64) error {
	if w.bufPos >= BlockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	if w.rowCount == 0 {
		w.firstTs = p.Timestamp
	}

	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(p.Timestamp))
	copy(w.buf[w.bufPos:w.bufPos+8], tsBuf[:])
	w.bufPos += 8

	for name := range w.fields {
		val, ok := p.Fields[name]
		if !ok {
			val = w.zeroValue(w.schema.Fields[name])
		}
		w.appendFieldValue(name, val)
	}

	w.sidBuf = append(w.sidBuf, sid)

	w.rowCount++
	return nil
}

// zeroValue 返回类型的零值
func (w *Writer) zeroValue(t FieldType) *types.FieldValue {
	switch t {
	case FieldTypeFloat64:
		return types.NewFieldValue(float64(0))
	case FieldTypeInt64:
		return types.NewFieldValue(int64(0))
	case FieldTypeBool:
		return types.NewFieldValue(false)
	case FieldTypeString:
		return types.NewFieldValue("")
	default:
		return types.NewFieldValue(float64(0))
	}
}

// appendFieldValue 将字段值追加到 field buffer
func (w *Writer) appendFieldValue(name string, val any) {
	buf := w.fieldBufs[name]

	if val == nil {
		buf = w.appendZeroValue(buf, w.schema.Fields[name])
		w.fieldBufs[name] = buf
		return
	}

	if fv, ok := val.(*types.FieldValue); ok {
		if fv == nil || fv.Value == nil {
			buf = w.appendZeroValue(buf, w.schema.Fields[name])
			w.fieldBufs[name] = buf
			return
		}
		switch v := fv.Value.(type) {
		case *types.FieldValue_FloatValue:
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], math.Float64bits(v.FloatValue))
			buf = append(buf, b[:]...)
		case *types.FieldValue_IntValue:
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(v.IntValue))
			buf = append(buf, b[:]...)
		case *types.FieldValue_StringValue:
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(v.StringValue)))
			buf = append(buf, lenBuf[:]...)
			buf = append(buf, v.StringValue...)
		case *types.FieldValue_BoolValue:
			if v.BoolValue {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
		w.fieldBufs[name] = buf
		return
	}

	switch v := val.(type) {
	case float64:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
		buf = append(buf, b[:]...)
	case int64:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(v))
		buf = append(buf, b[:]...)
	case string:
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(v)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, v...)
	case bool:
		if v {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	w.fieldBufs[name] = buf
}

// appendZeroValue 追加类型的零值到 buffer
func (w *Writer) appendZeroValue(buf []byte, t FieldType) []byte {
	switch t {
	case FieldTypeFloat64, FieldTypeInt64:
		var b [8]byte
		buf = append(buf, b[:]...)
	case FieldTypeBool:
		buf = append(buf, 0)
	case FieldTypeString:
		var lenBuf [4]byte
		buf = append(buf, lenBuf[:]...)
	default:
		var b [8]byte
		buf = append(buf, b[:]...)
	}
	return buf
}
