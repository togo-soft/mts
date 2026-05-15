package writer

import (
	"encoding/binary"
	"fmt"
	"sync"

	"codeberg.org/micro-ts/mts/types"
)

const pointVersion byte = 2

var walBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

// serializePointForWALPooled 将 ts + sid + FieldData 序列化到池化缓冲区。
func serializePointForWALPooled(ts int64, sid uint64, fieldData []byte) ([]byte, func()) {
	size := 1 + 8 + 8 + len(fieldData)
	bufPtr := walBufPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	buf[0] = pointVersion
	binary.BigEndian.PutUint64(buf[1:9], uint64(ts))
	binary.BigEndian.PutUint64(buf[9:17], sid)
	copy(buf[17:], fieldData)
	*bufPtr = buf

	release := func() {
		walBufPool.Put(bufPtr)
	}
	return buf, release
}

// deserializeFromWAL 从 WAL 完整格式解析出 MemPoint。
func deserializeFromWAL(data []byte) (types.MemPoint, error) {
	if len(data) < 19 {
		return types.MemPoint{}, fmt.Errorf("wal data too short: %d bytes", len(data))
	}
	if data[0] != pointVersion {
		return types.MemPoint{}, fmt.Errorf("unsupported point version: %d (expected %d)", data[0], pointVersion)
	}
	ts := int64(binary.BigEndian.Uint64(data[1:9]))
	sid := binary.BigEndian.Uint64(data[9:17])
	fieldData := make([]byte, len(data)-17)
	copy(fieldData, data[17:])
	return types.MemPoint{
		Timestamp: ts,
		Sid:       sid,
		FieldData: fieldData,
	}, nil
}
