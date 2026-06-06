package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"codeberg.org/micro-ts/mts/internal/storage"
)

func (w *Writer) recordBlockZoneMap() {
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
}

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

	w.recordBlockZoneMap()

	lastTs := int64(binary.BigEndian.Uint64(w.buf[w.bufPos-8:]))
	w.blockIndex.Add(w.firstTs, lastTs, uint32(w.totalRows), uint32(w.rowCount))
	w.totalRows += uint32(w.rowCount)

	w.bufPos = 0
	w.rowCount = 0
	w.firstTs = 0

	return nil
}

type sstableOutput struct {
	file *os.File
	path string
}

func (o *sstableOutput) fail(cause error) error {
	_ = o.file.Close()
	_ = os.Remove(o.path)
	return fmt.Errorf("failed to finalize SSTable: %w", cause)
}

func (o *sstableOutput) cleanup() {
	_ = o.file.Close()
	_ = os.Remove(o.path)
}

type closeState struct {
	out              *sstableOutput
	fieldNames       []string
	timestampsOffset uint64
	timestampsSize   uint64
	sidsOffset       uint64
	sidsSize         uint64
	tsEncoding       EncodingType
	fieldInfoMap     map[string]struct {
		offset   uint64
		size     uint64
		encoding EncodingType
	}
	blockMap         *BlockSectionMap
	currentOffset    uint64
	blockIndexOffset uint64
	blockMapOffset   uint64
	zoneMapOffset    uint64
	indexData        []byte
	blockMapData     []byte
	zoneMapData      []byte
}

func (w *Writer) closeTempFiles() ([]string, []error) {
	var closeErrs []error
	if err := w.timestamp.Close(); err != nil {
		closeErrs = append(closeErrs, fmt.Errorf("close timestamp file: %w", err))
	}
	if err := w.sids.Close(); err != nil {
		closeErrs = append(closeErrs, fmt.Errorf("close sids file: %w", err))
	}
	for name, f := range w.fields {
		if err := f.Close(); err != nil {
			closeErrs = append(closeErrs, fmt.Errorf("close field %s: %w", name, err))
		}
	}
	fieldNames := make([]string, 0, len(w.fields))
	for name := range w.fields {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)
	return fieldNames, closeErrs
}

func (w *Writer) createOutputFile() (*sstableOutput, error) {
	outPath := filepath.Join(w.dataDir, fmt.Sprintf("sst_%d.bin", w.seq))
	outFile, err := storage.SafeCreate(outPath, 0600)
	if err != nil {
		_ = os.RemoveAll(w.tmpDir)
		return nil, fmt.Errorf("create output file: %w", err)
	}
	return &sstableOutput{file: outFile, path: outPath}, nil
}

func (s *closeState) writeDataSections(w *Writer) error {
	s.timestampsOffset = s.currentOffset
	timestampsEncoded, tsOffsets, tsEncoding, err := w.encodeTimestampsSection(int(w.totalRows))
	if err != nil {
		return s.out.fail(err)
	}
	if _, err := s.out.file.Write(timestampsEncoded); err != nil {
		return s.out.fail(err)
	}
	s.timestampsSize = uint64(len(timestampsEncoded))
	s.tsEncoding = tsEncoding
	s.blockMap.Sections = append(s.blockMap.Sections,
		BlockSectionOffsets{Name: "_timestamps", Offsets: tsOffsets})
	s.currentOffset += s.timestampsSize

	s.sidsOffset = s.currentOffset
	sidsEncoded, sidOffsets, err := w.encodeSidsSection(int(w.totalRows))
	if err != nil {
		return s.out.fail(err)
	}
	if _, err := s.out.file.Write(sidsEncoded); err != nil {
		return s.out.fail(err)
	}
	s.sidsSize = uint64(len(sidsEncoded))
	s.blockMap.Sections = append(s.blockMap.Sections,
		BlockSectionOffsets{Name: "_sids", Offsets: sidOffsets})
	s.currentOffset += s.sidsSize

	return s.writeFieldsSections(w)
}

func (s *closeState) writeFieldsSections(w *Writer) error {
	for _, name := range s.fieldNames {
		fi := struct {
			offset   uint64
			size     uint64
			encoding EncodingType
		}{offset: s.currentOffset}
		encoded, fieldOffsets, enc, err := w.encodeFieldSection(name, int(w.totalRows))
		if err != nil {
			return s.out.fail(err)
		}
		if _, err := s.out.file.Write(encoded); err != nil {
			return s.out.fail(err)
		}
		fi.size = uint64(len(encoded))
		fi.encoding = enc
		s.fieldInfoMap[name] = fi
		s.blockMap.Sections = append(s.blockMap.Sections,
			BlockSectionOffsets{Name: name, Offsets: fieldOffsets})
		s.currentOffset += fi.size
	}
	return nil
}

func (s *closeState) writeMetadataSections(w *Writer) error {
	s.blockIndexOffset = s.currentOffset
	indexData, err := w.encodeBlockIndex()
	if err != nil {
		return s.out.fail(err)
	}
	s.indexData = indexData
	if _, err := s.out.file.Write(indexData); err != nil {
		return s.out.fail(err)
	}
	s.currentOffset += uint64(len(indexData))

	s.blockMapOffset = s.currentOffset
	s.blockMapData = s.blockMap.Marshal()
	if _, err := s.out.file.Write(s.blockMapData); err != nil {
		return s.out.fail(err)
	}
	s.currentOffset += uint64(len(s.blockMapData))

	s.zoneMapOffset = s.currentOffset
	s.zoneMapData = w.zoneMapIndex.Marshal()
	if _, err := s.out.file.Write(s.zoneMapData); err != nil {
		return s.out.fail(err)
	}
	s.currentOffset += uint64(len(s.zoneMapData))
	return nil
}

func (s *closeState) writeSectionTable(w *Writer) (uint64, error) {
	entries := []SectionEntry{
		{Type: SectionTimestamps, Name: "_timestamps", Offset: s.timestampsOffset, Size: s.timestampsSize, Encoding: s.tsEncoding, Compression: w.compressAlgo},
		{Type: SectionSids, Name: "_sids", Offset: s.sidsOffset, Size: s.sidsSize, Encoding: EncodingVarint, Compression: w.compressAlgo},
		{Type: SectionIndex, Name: "_index", Offset: s.blockIndexOffset, Size: uint64(len(s.indexData)), Encoding: EncodingRaw, Compression: CompressionNone},
		{Type: SectionIndex, Name: "_block_map", Offset: s.blockMapOffset, Size: uint64(len(s.blockMapData)), Encoding: EncodingRaw, Compression: CompressionNone},
		{Type: SectionIndex, Name: "_zone_map", Offset: s.zoneMapOffset, Size: uint64(len(s.zoneMapData)), Encoding: EncodingRaw, Compression: CompressionNone},
	}
	for _, name := range s.fieldNames {
		fi := s.fieldInfoMap[name]
		entries = append(entries, SectionEntry{
			Type: SectionField, Name: name, Offset: fi.offset, Size: fi.size, Encoding: fi.encoding, Compression: w.compressAlgo,
		})
	}

	sectionTableData := (&SectionTable{Entries: entries}).Marshal()
	sectionTableOffset := s.currentOffset
	if _, err := s.out.file.Write(sectionTableData); err != nil {
		return 0, s.out.fail(err)
	}
	return sectionTableOffset, nil
}

func (s *closeState) writeHeader(w *Writer, sectionTableOffset uint64) error {
	header := FileHeader{
		Magic:              Magic,
		Version:            FileVersion,
		RowCount:           w.totalRows,
		FieldCount:         uint16(len(s.fieldNames)),
		BlockCount:         uint16(w.blockIndex.Len()),
		BlockSize:          uint16(w.blockSize),
		Flags:              w.flags | FlagHasZoneMap,
		TimestampsOffset:   s.timestampsOffset,
		SidsOffset:         s.sidsOffset,
		BlockIndexOffset:   s.blockIndexOffset,
		SectionTableOffset: sectionTableOffset,
	}
	headerBuf := header.Marshal()
	if _, err := s.out.file.WriteAt(headerBuf[:], 0); err != nil {
		return s.out.fail(err)
	}
	return nil
}

func (w *Writer) finalizeOutput(out *sstableOutput, closeErrs []error) error {
	if w.syncOnClose {
		if err := out.file.Sync(); err != nil {
			out.cleanup()
			_ = os.RemoveAll(w.tmpDir)
			return fmt.Errorf("sync output file: %w", err)
		}
	}
	if err := out.file.Close(); err != nil {
		_ = os.Remove(out.path)
		_ = os.RemoveAll(w.tmpDir)
		return fmt.Errorf("close output file: %w", err)
	}
	if err := os.RemoveAll(w.tmpDir); err != nil {
		closeErrs = append(closeErrs, fmt.Errorf("clean tmp dir: %w", err))
	}
	if len(closeErrs) > 0 {
		return errors.Join(closeErrs...)
	}
	return nil
}

func (w *Writer) Close() error {
	if err := w.flushBlock(); err != nil {
		return fmt.Errorf("flush block: %w", err)
	}
	if err := w.checkCtx(); err != nil {
		_ = os.RemoveAll(w.tmpDir)
		return fmt.Errorf("sstable write cancelled: %w", err)
	}

	fieldNames, closeErrs := w.closeTempFiles()

	out, err := w.createOutputFile()
	if err != nil {
		return err
	}

	var placeholder [HeaderSize]byte
	if _, err := out.file.Write(placeholder[:]); err != nil {
		return out.fail(err)
	}

	s := &closeState{
		out:           out,
		fieldNames:    fieldNames,
		fieldInfoMap:  make(map[string]struct {
			offset   uint64
			size     uint64
			encoding EncodingType
		}),
		blockMap:      &BlockSectionMap{},
		currentOffset: HeaderSize,
	}

	if err := s.writeDataSections(w); err != nil {
		return err
	}
	if err := s.writeMetadataSections(w); err != nil {
		return err
	}

	sectionTableOffset, err := s.writeSectionTable(w)
	if err != nil {
		return err
	}
	if err := s.writeHeader(w, sectionTableOffset); err != nil {
		return err
	}

	return w.finalizeOutput(out, closeErrs)
}
