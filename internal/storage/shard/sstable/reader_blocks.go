package sstable

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

func (r *Reader) readTimestamps(f *os.File) ([]int64, error) {
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return decodeTimestampBatch(data), nil
}

func (r *Reader) readTimestampRange(f *os.File, offset uint32, numRows uint32) ([]int64, error) {
	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	bytesNeeded := int(numRows) * 8
	data := make([]byte, bytesNeeded)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, err
	}

	return decodeTimestampBatch(data), nil
}

func decodeTimestampBatch(data []byte) []int64 {
	timestamps := make([]int64, 0, len(data)/8)
	for i := 0; i+8 <= len(data); i += 8 {
		ts := int64(binary.BigEndian.Uint64(data[i : i+8]))
		timestamps = append(timestamps, ts)
	}
	return timestamps
}

func (r *Reader) readSids(dataDir string, expectedCount int) ([]uint64, error) {
	sidFile, err := os.Open(filepath.Join(dataDir, "_sids.bin"))
	if err != nil {
		if os.IsNotExist(err) {
			return make([]uint64, expectedCount), nil
		}
		return nil, err
	}
	defer func() { _ = sidFile.Close() }()

	data, err := io.ReadAll(sidFile)
	if err != nil {
		return nil, err
	}

	return decodeSidBatch(data), nil
}

func (r *Reader) readSidsRange(dataDir string, offset uint32, numRows uint32) ([]uint64, error) {
	sidFile, err := os.Open(filepath.Join(dataDir, "_sids.bin"))
	if err != nil {
		if os.IsNotExist(err) {
			return make([]uint64, 0), nil
		}
		return nil, err
	}
	defer func() { _ = sidFile.Close() }()

	if _, err := sidFile.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	bytesNeeded := int(numRows) * 8
	data := make([]byte, bytesNeeded)
	if _, err := io.ReadFull(sidFile, data); err != nil {
		return nil, err
	}

	return decodeSidBatch(data), nil
}

func decodeSidBatch(data []byte) []uint64 {
	sids := make([]uint64, 0, len(data)/8)
	for i := 0; i+8 <= len(data); i += 8 {
		sid := binary.BigEndian.Uint64(data[i : i+8])
		sids = append(sids, sid)
	}
	return sids
}
