package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"codeberg.org/micro-ts/mts/internal/storage"
)

// flushBlock 将当前 block 缓冲写入临时文件。
func (w *Writer) flushBlock() error {
	if w.bufPos == 0 && w.rowCount == 0 {
		return nil
	}

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
		// 记录此 block 在 temp 文件中的字节起始偏移
		curOff, err := w.fields[name].Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("seek field %s for offset: %w", name, err)
		}
		w.fieldByteOffsets[name] = append(w.fieldByteOffsets[name], curOff)

		if _, err := w.fields[name].Write(buf); err != nil {
			return fmt.Errorf("write field block %s: %w", name, err)
		}
		w.fieldBufs[name] = w.fieldBufs[name][:0]
	}

	// 记录当前 block 的 ZoneMap
	bzm := BlockZoneMap{FieldZMaps: make([]ZoneMapEntry, 0, len(w.zoneMapCurr))}
	for name, acc := range w.zoneMapCurr {
		if acc.initialized {
			bzm.FieldZMaps = append(bzm.FieldZMaps, ZoneMapEntry{
				FieldName: name, Min: acc.min, Max: acc.max,
			})
		}
	}
	w.zoneMapIndex.Blocks = append(w.zoneMapIndex.Blocks, bzm)
	w.zoneMapCurr = make(map[string]*zoneAccumulator)

	lastTs := int64(binary.BigEndian.Uint64(w.buf[w.bufPos-8:]))
	w.blockIndex.Add(w.firstTs, lastTs, uint32(w.totalRows), uint32(w.rowCount))
	w.totalRows += uint32(w.rowCount)

	w.bufPos = 0
	w.rowCount = 0
	w.firstTs = 0

	return nil
}

// Close 关闭 Writer，编码并合并临时文件到单一 .bin 文件。
func (w *Writer) Close() error {
	if err := w.flushBlock(); err != nil {
		return fmt.Errorf("flush block: %w", err)
	}

	// 关闭所有临时文件
	_ = w.timestamp.Close()
	_ = w.sids.Close()
	for _, f := range w.fields {
		_ = f.Close()
	}

	// 获取字段名并按字典序排序
	fieldNames := make([]string, 0, len(w.fields))
	for name := range w.fields {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)

	// 创建最终的单文件
	outPath := filepath.Join(w.dataDir, fmt.Sprintf("sst_%d.bin", w.seq))
	outFile, err := storage.SafeCreate(outPath, 0600)
	if err != nil {
		_ = os.RemoveAll(w.tmpDir)
		return fmt.Errorf("create output file: %w", err)
	}
	cleanupErr := func(cause error) error {
		_ = outFile.Close()
		_ = os.Remove(outPath)
		_ = os.RemoveAll(w.tmpDir)
		return fmt.Errorf("failed to finalize SSTable: %w", cause)
	}

	// 写入占位 header (64B)
	var placeholder [HeaderSize]byte
	if _, err := outFile.Write(placeholder[:]); err != nil {
		return cleanupErr(err)
	}

	rowCount := int(w.totalRows)

	// 跟踪各段偏移量和大小
	var timestampsOffset, timestampsSize uint64
	var sidsOffset, sidsSize uint64
	type fieldInfo struct {
		offset   uint64
		size     uint64
		encoding EncodingType
	}
	fieldInfoMap := make(map[string]fieldInfo)

	// 构建 BlockSectionMap
	blockMap := &BlockSectionMap{}

	currentOffset := uint64(HeaderSize)

	// 1. 编码并写入 timestamps
	timestampsOffset = currentOffset
	timestampsEncoded, tsOffsets, tsEncoding, err := w.encodeTimestampsSection(rowCount)
	if err != nil {
		return cleanupErr(err)
	}
	if _, err := outFile.Write(timestampsEncoded); err != nil {
		return cleanupErr(err)
	}
	timestampsSize = uint64(len(timestampsEncoded))
	blockMap.Sections = append(blockMap.Sections, BlockSectionOffsets{
		Name: "_timestamps", Offsets: tsOffsets,
	})
	currentOffset += timestampsSize

	// 2. 编码并写入 sids
	sidsOffset = currentOffset
	sidsEncoded, sidOffsets, err := w.encodeSidsSection(rowCount)
	if err != nil {
		return cleanupErr(err)
	}
	if _, err := outFile.Write(sidsEncoded); err != nil {
		return cleanupErr(err)
	}
	sidsSize = uint64(len(sidsEncoded))
	blockMap.Sections = append(blockMap.Sections, BlockSectionOffsets{
		Name: "_sids", Offsets: sidOffsets,
	})
	currentOffset += sidsSize

	// 3. 编码并写入每个 field
	for _, name := range fieldNames {
		fi := fieldInfo{offset: currentOffset}
		encoded, fieldOffsets, enc, err := w.encodeFieldSection(name, rowCount)
		if err != nil {
			return cleanupErr(err)
		}
		if _, err := outFile.Write(encoded); err != nil {
			return cleanupErr(err)
		}
		fi.size = uint64(len(encoded))
		fi.encoding = enc
		fieldInfoMap[name] = fi
		blockMap.Sections = append(blockMap.Sections, BlockSectionOffsets{
			Name: name, Offsets: fieldOffsets,
		})
		currentOffset += fi.size
	}

	// 4. 写入 block index
	blockIndexOffset := currentOffset
	indexData, err := w.encodeBlockIndex()
	if err != nil {
		return cleanupErr(err)
	}
	if _, err := outFile.Write(indexData); err != nil {
		return cleanupErr(err)
	}
	currentOffset += uint64(len(indexData))

	// 5. 写入 _block_map section
	blockMapOffset := currentOffset
	blockMapData := blockMap.Marshal()
	if _, err := outFile.Write(blockMapData); err != nil {
		return cleanupErr(err)
	}
	currentOffset += uint64(len(blockMapData))

	// 5.5 写入 _zone_map section
	zoneMapOffset := currentOffset
	zoneMapData := w.zoneMapIndex.Marshal()
	if _, err := outFile.Write(zoneMapData); err != nil {
		return cleanupErr(err)
	}
	currentOffset += uint64(len(zoneMapData))

	// 6. 构建 Section Table
	sectionTable := SectionTable{
		Entries: []SectionEntry{
			{Type: SectionTimestamps, Name: "_timestamps", Offset: timestampsOffset, Size: timestampsSize, Encoding: tsEncoding, Compression: w.compressAlgo},
			{Type: SectionSids, Name: "_sids", Offset: sidsOffset, Size: sidsSize, Encoding: EncodingVarint, Compression: w.compressAlgo},
			{Type: SectionIndex, Name: "_index", Offset: blockIndexOffset, Size: uint64(len(indexData)), Encoding: EncodingRaw, Compression: CompressionNone},
			{Type: SectionIndex, Name: "_block_map", Offset: blockMapOffset, Size: uint64(len(blockMapData)), Encoding: EncodingRaw, Compression: CompressionNone},
			{Type: SectionIndex, Name: "_zone_map", Offset: zoneMapOffset, Size: uint64(len(zoneMapData)), Encoding: EncodingRaw, Compression: CompressionNone},
		},
	}
	for _, name := range fieldNames {
		fi := fieldInfoMap[name]
		sectionTable.Entries = append(sectionTable.Entries, SectionEntry{
			Type: SectionField, Name: name, Offset: fi.offset, Size: fi.size, Encoding: fi.encoding, Compression: w.compressAlgo,
		})
	}

	// 7. 写入 Section Table
	sectionTableData := sectionTable.Marshal()
	sectionTableOffset := currentOffset
	if _, err := outFile.Write(sectionTableData); err != nil {
		return cleanupErr(err)
	}

	// 8. 回填 header
	header := FileHeader{
		Magic:              Magic,
		Version:            FileVersion,
		RowCount:           w.totalRows,
		FieldCount:         uint16(len(fieldNames)),
		BlockCount:         uint16(w.blockIndex.Len()),
		BlockSize:          uint16(w.blockSize),
		Flags:              w.flags | FlagHasZoneMap,
		TimestampsOffset:   timestampsOffset,
		SidsOffset:         sidsOffset,
		BlockIndexOffset:   blockIndexOffset,
		SectionTableOffset: sectionTableOffset,
	}
	headerBuf := header.Marshal()
	if _, err := outFile.WriteAt(headerBuf[:], 0); err != nil {
		return cleanupErr(err)
	}

	if w.syncOnClose {
		if err := outFile.Sync(); err != nil {
			_ = outFile.Close()
			_ = os.Remove(outPath)
			_ = os.RemoveAll(w.tmpDir)
			return fmt.Errorf("sync output file: %w", err)
		}
	}
	if err := outFile.Close(); err != nil {
		_ = os.Remove(outPath)
		_ = os.RemoveAll(w.tmpDir)
		return fmt.Errorf("close output file: %w", err)
	}

	// 清理临时目录
	_ = os.RemoveAll(w.tmpDir)

	return nil
}
