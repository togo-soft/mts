package sstable

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/micro-ts/mts/internal/storage"
	"codeberg.org/micro-ts/mts/types"
)

var errInvalidFieldName = fmt.Errorf("field name contains invalid characters")

func validateFieldName(name string) error {
	if strings.ContainsAny(name, "/\\") {
		return fmt.Errorf("%w: %q", errInvalidFieldName, name)
	}
	return nil
}

// WriteMemPoints 直接写入 MemPoint 切片，从 FieldData 字节流解码字段值到列式缓冲区。
// 跳过 MemPoint → InternalPoint → WritePoints 中间态，消除 deserializeFieldData + NewFieldValue 分配。
func (w *Writer) WriteMemPoints(points []types.MemPoint) error {
	if len(points) == 0 {
		return nil
	}

	// 第一遍：从 FieldData 轻量解析字段名和类型（跳过值）
	fieldSet := make(map[string]FieldType, 8)
	for i := range points {
		if len(points[i].FieldData) == 0 {
			continue
		}
		if err := scanFieldDataKeys(points[i].FieldData, fieldSet, &w.schema); err != nil {
			return fmt.Errorf("scan fields in point %d: %w", i, err)
		}
	}

	// 构建字段索引：字段名 → 整数索引，消除后续字符串分配
	w.fieldIdxNames = make([]string, 0, len(fieldSet))
	for name := range fieldSet {
		w.fieldIdx[name] = len(w.fieldIdxNames)
		w.fieldIdxNames = append(w.fieldIdxNames, name)
	}

	fieldsDir := filepath.Join(w.tmpDir, "fields")
	if err := storage.SafeMkdirAll(fieldsDir, 0700); err != nil {
		return fmt.Errorf("create fields tmp dir: %w", err)
	}

	for name, ft := range fieldSet {
		if err := validateFieldName(name); err != nil {
			return fmt.Errorf("invalid field name: %w", err)
		}
		if _, exists := w.fields[name]; exists {
			continue
		}
		f, err := storage.SafeOpenFile(
			filepath.Join(fieldsDir, name+".bin"),
			os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("open field file %s: %w", name, err)
		}
		w.fields[name] = f
		w.fieldBufs[name] = make([]byte, 0, w.blockSize)
		w.fieldSizes[name] = w.fieldTypeSize(ft)
	}

	// 第二遍：直接解码 FieldData 写入列式缓冲区
	for i := range points {
		if err := w.writeMemPoint(points[i]); err != nil {
			return fmt.Errorf("write mempoint %d (timestamp=%d): %w", i, points[i].Timestamp, err)
		}
	}

	return nil
}

// scanFieldDataKeys 从 FieldData 轻量扫描字段名和类型。
// 使用字节级比较避免重复字符串分配：仅在 fieldSet 中不存在时才创建新字符串。
func scanFieldDataKeys(data []byte, fieldSet map[string]FieldType, schema *Schema) error {
	if len(data) < 2 {
		return fmt.Errorf("field data too short: %d bytes", len(data))
	}
	fieldCount := int(binary.BigEndian.Uint16(data[:2]))
	pos := 2
	for range fieldCount {
		if pos+2 > len(data) {
			return fmt.Errorf("truncated key len at pos %d", pos)
		}
		kLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+kLen > len(data) {
			return fmt.Errorf("truncated key at pos %d (len=%d)", pos, kLen)
		}
		keyData := data[pos : pos+kLen]
		pos += kLen
		if pos+1 > len(data) {
			return fmt.Errorf("truncated type at pos %d", pos)
		}
		typ := data[pos]
		pos++

		ft := fieldDataTypeToFieldType(typ)
		if ft == "" {
			return fmt.Errorf("unknown field type: %d", typ)
		}

		// 字节级比较，仅在首次遇到时分配字符串
		key := lookupOrAllocKey(keyData, fieldSet)
		if _, exists := fieldSet[key]; !exists {
			fieldSet[key] = ft
		}
		if _, exists := schema.Fields[key]; !exists {
			schema.Fields[key] = ft
		}

		// 跳过值
		skip, err := skipFieldValue(data, pos, typ)
		if err != nil {
			return err
		}
		pos = skip
	}
	return nil
}

// lookupOrAllocKey 在已有 map 中按字节查找 key，找到则返回已有字符串（避免分配），未找到则分配新字符串。
func lookupOrAllocKey(data []byte, m map[string]FieldType) string {
	for k := range m {
		if len(k) == len(data) && stringEqualBytes(k, data) {
			return k
		}
	}
	return string(data)
}

// stringEqualBytes 比较 string 和 []byte 是否相等（零分配）。
func stringEqualBytes(s string, b []byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != b[i] {
			return false
		}
	}
	return true
}

// skipFieldValue 跳过 FieldData 中一个字段的值部分，返回新位置。
func skipFieldValue(data []byte, pos int, typ byte) (int, error) {
	switch typ {
	case 0, 1: // float64, int64
		pos += 8
	case 2: // string
		if pos+2 > len(data) {
			return 0, fmt.Errorf("truncated string len at pos %d", pos)
		}
		vLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2 + vLen
	case 3: // bool
		pos++
	default:
		return 0, fmt.Errorf("unknown field type: %d", typ)
	}
	return pos, nil
}

// fieldDataTypeToFieldType 将 FieldData 中的类型标签映射为 FieldType。
func fieldDataTypeToFieldType(typ byte) FieldType {
	switch typ {
	case 0:
		return FieldTypeFloat64
	case 1:
		return FieldTypeInt64
	case 2:
		return FieldTypeString
	case 3:
		return FieldTypeBool
	default:
		return ""
	}
}

// writeMemPoint 直接解析 MemPoint.FieldData 写入块缓冲区。
// 使用字段索引 + 池化 []bool 消除 per-point 字符串分配和 map 分配。
func (w *Writer) writeMemPoint(mp types.MemPoint) error {
	if w.bufPos >= w.blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	if w.rowCount == 0 {
		w.firstTs = mp.Timestamp
	}

	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(mp.Timestamp))
	copy(w.buf[w.bufPos:w.bufPos+8], tsBuf[:])
	w.bufPos += 8

	// 从池获取 written slice，复用避免每行分配
	writtenPtr := w.writtenPool.Get().(*[]bool)
	written := *writtenPtr
	if cap(written) < len(w.fieldIdxNames) {
		written = make([]bool, len(w.fieldIdxNames))
	} else {
		written = written[:len(w.fieldIdxNames)]
	}
	// 清零
	for i := range written {
		written[i] = false
	}

	data := mp.FieldData
	if len(data) > 0 {
		fieldCount := int(binary.BigEndian.Uint16(data[:2]))
		pos := 2
		for range fieldCount {
			kLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			// 字节级字段查找：获取索引，零字符串分配
			idx, err := w.lookupFieldIdx(data[pos : pos+kLen])
			if err != nil {
				return fmt.Errorf("write mempoint field lookup: %w", err)
			}
			pos += kLen
			typ := data[pos]
			pos++

			name := w.fieldIdxNames[idx]
			switch typ {
			case 0: // float64
				val := math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
				w.accumulateZoneMap(name, val)
				w.appendFieldValueIdx(idx, name, val)
				pos += 8
			case 1: // int64
				val := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
				w.accumulateZoneMap(name, val)
				w.appendFieldValueIdx(idx, name, val)
				pos += 8
			case 2: // string
				vLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
				pos += 2
				val := string(data[pos : pos+vLen])
				w.appendFieldValueIdx(idx, name, val)
				pos += vLen
			case 3: // bool
				val := data[pos] == 1
				w.appendFieldValueIdx(idx, name, val)
				pos++
			}
			written[idx] = true
		}
	}

	// 为当前行中不存在的字段写入零值
	for i, name := range w.fieldIdxNames {
		if !written[i] {
			w.appendFieldValueIdx(i, name, w.zeroValue(w.schema.Fields[name]))
		}
	}

	w.sidBuf = append(w.sidBuf, mp.Sid)
	w.rowCount++

	// 归还 written slice 到池
	*writtenPtr = written[:0]
	w.writtenPool.Put(writtenPtr)

	return nil
}

// lookupFieldIdx 通过字节级比较查找字段索引，零字符串分配。
// 未找到时返回错误而非 -1，防止越界访问。
func (w *Writer) lookupFieldIdx(data []byte) (int, error) {
	for i, name := range w.fieldIdxNames {
		if len(name) == len(data) && stringEqualBytes(name, data) {
			return i, nil
		}
	}
	return -1, fmt.Errorf("field not found: %q", string(data))
}

// appendFieldValueIdx 将字段值追加到 field buffer（使用字段索引）。
func (w *Writer) appendFieldValueIdx(idx int, name string, val any) {
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

// WritePointRows 直接写入 PointRow 切片，跳过 InternalField 中间转换。
// 首次调用执行字段发现和文件映射建立，后续调用复用缓存跳过 O(N*F) 字段扫描。
func (w *Writer) WritePointRows(rows []*types.PointRow) error {
	if !w.fieldDiscoveryDone {
		if err := w.discoverFields(rows); err != nil {
			return err
		}
		w.fieldDiscoveryDone = true
	}

	for _, row := range rows {
		if err := w.writePointRow(row); err != nil {
			return fmt.Errorf("write point (timestamp=%d): %w", row.Timestamp, err)
		}
	}

	return nil
}

// discoverFields 扫描行中的所有字段，检测类型并建立字段文件映射。
func (w *Writer) discoverFields(rows []*types.PointRow) error {
	fieldNames := make(map[string]bool)
	for _, row := range rows {
		for _, fe := range row.Fields {
			fieldNames[fe.Key] = true
			if _, exists := w.schema.Fields[fe.Key]; !exists {
				w.schema.Fields[fe.Key] = detectFieldType(fe.Value)
			}
		}
	}

	fieldsDir := filepath.Join(w.tmpDir, "fields")
	if err := storage.SafeMkdirAll(fieldsDir, 0700); err != nil {
		return fmt.Errorf("create fields tmp dir: %w", err)
	}

	for name := range fieldNames {
		if _, exists := w.fields[name]; exists {
			continue
		}
		f, err := storage.SafeOpenFile(
			filepath.Join(fieldsDir, name+".bin"),
			os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("open field file %s: %w", name, err)
		}
		w.fields[name] = f
		w.fieldBufs[name] = make([]byte, 0, w.blockSize)
		w.fieldSizes[name] = w.fieldTypeSize(w.schema.Fields[name])
	}
	return nil
}

// WritePoints 写入一批 InternalPoint 到 SSTable。
// 首次调用执行字段发现，后续调用复用缓存跳过扫描。
func (w *Writer) WritePoints(points []types.InternalPoint) error {
	if !w.fieldDiscoveryDone {
		if err := w.discoverFieldsFromInternal(points); err != nil {
			return err
		}
		w.fieldDiscoveryDone = true
	}

	for _, ip := range points {
		if err := w.writeInternalPoint(ip); err != nil {
			return fmt.Errorf("write point (timestamp=%d): %w", ip.Timestamp, err)
		}
	}

	return nil
}

// discoverFieldsFromInternal 从 InternalPoint 切片扫描字段并建立文件映射。
func (w *Writer) discoverFieldsFromInternal(points []types.InternalPoint) error {
	fieldNames := make(map[string]bool)
	for _, ip := range points {
		for _, fe := range ip.Fields {
			fieldNames[fe.Key] = true
			if _, exists := w.schema.Fields[fe.Key]; !exists {
				w.schema.Fields[fe.Key] = detectFieldType(fe.Value)
			}
		}
	}

	fieldsDir := filepath.Join(w.tmpDir, "fields")
	if err := storage.SafeMkdirAll(fieldsDir, 0700); err != nil {
		return fmt.Errorf("create fields tmp dir: %w", err)
	}

	for name := range fieldNames {
		if _, exists := w.fields[name]; exists {
			continue
		}
		f, err := storage.SafeOpenFile(
			filepath.Join(fieldsDir, name+".bin"),
			os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("open field file %s: %w", name, err)
		}
		w.fields[name] = f
		w.fieldBufs[name] = make([]byte, 0, w.blockSize)
		w.fieldSizes[name] = w.fieldTypeSize(w.schema.Fields[name])
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

// writePointRow 将单个 PointRow 直接写入 block buffer（跳过 InternalField 转换）。
func (w *Writer) writePointRow(row *types.PointRow) error {
	if w.bufPos >= w.blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	if w.rowCount == 0 {
		w.firstTs = row.Timestamp
	}

	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(row.Timestamp))
	copy(w.buf[w.bufPos:w.bufPos+8], tsBuf[:])
	w.bufPos += 8

	for name := range w.fields {
		val := row.GetFieldValue(name)
		w.accumulateZoneMap(name, val)
		w.appendFieldValue(name, val)
	}

	w.sidBuf = append(w.sidBuf, row.Sid)

	w.rowCount++
	return nil
}

// writeInternalPoint 将单个 InternalPoint 写入 block buffer。
func (w *Writer) writeInternalPoint(ip types.InternalPoint) error {
	if w.bufPos >= w.blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	if w.rowCount == 0 {
		w.firstTs = ip.Timestamp
	}

	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(ip.Timestamp))
	copy(w.buf[w.bufPos:w.bufPos+8], tsBuf[:])
	w.bufPos += 8

	for name := range w.fields {
		val, ok := findInternalField(ip.Fields, name)
		if !ok {
			val = w.zeroValue(w.schema.Fields[name])
		}
		w.accumulateZoneMap(name, val)
		w.appendFieldValue(name, val)
	}

	w.sidBuf = append(w.sidBuf, ip.Sid)

	w.rowCount++
	return nil
}

// findInternalField 从紧凑字段切片中查找指定名称的字段值。
func findInternalField(fields []types.InternalField, name string) (*types.FieldValue, bool) {
	for _, f := range fields {
		if f.Key == name {
			return f.Value, true
		}
	}
	return nil, false
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

// accumulateZoneMap 为 ZoneMap 累积数值字段的 min/max。
func (w *Writer) accumulateZoneMap(name string, val any) {
	switch v := val.(type) {
	case float64:
		za, ok := w.zoneMapCurr[name]
		if !ok {
			za = &zoneAccumulator{}
			w.zoneMapCurr[name] = za
		}
		za.update(v)
	case int64:
		za, ok := w.zoneMapCurr[name]
		if !ok {
			za = &zoneAccumulator{}
			w.zoneMapCurr[name] = za
		}
		za.update(float64(v))
	case *types.FieldValue:
		if v != nil && v.Value != nil {
			switch fv := v.Value.(type) {
			case *types.FieldValue_FloatValue:
				za, ok := w.zoneMapCurr[name]
				if !ok {
					za = &zoneAccumulator{}
					w.zoneMapCurr[name] = za
				}
				za.update(fv.FloatValue)
			case *types.FieldValue_IntValue:
				za, ok := w.zoneMapCurr[name]
				if !ok {
					za = &zoneAccumulator{}
					w.zoneMapCurr[name] = za
				}
				za.update(float64(fv.IntValue))
			}
		}
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
