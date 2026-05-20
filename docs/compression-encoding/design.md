# Design: 数据类型压缩编码

## 架构概览

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SSTable File                                 │
├─────────────────────────────────────────────────────────────────────┤
│ HEADER (64B)                                                        │
├─────────────────────────────────────────────────────────────────────┤
│ TIMESTAMPS SECTION  [Encoding=DeltaVarint]                          │
│   [base:varint][delta1:varint][dod2:varint]...                      │
├─────────────────────────────────────────────────────────────────────┤
│ SIDS SECTION  [Encoding=Varint]                                     │
│   [sid0:varint][sid1:varint]...                                     │
├─────────────────────────────────────────────────────────────────────┤
│ FIELD SECTIONS (每个字段独立编码)                                     │
│   float64: [Encoding=XORFloat] [first:8B][control_bits...]          │
│   int64:   [Encoding=ZigZagVarint] [zz0:varint][zz1:varint]...     │
│   string:  [Encoding=DictString] [dict] [indices:varint...]         │
│   bool:    [Encoding=BitmapBool] [bits: ceil(N/8)B]                 │
├─────────────────────────────────────────────────────────────────────┤
│ BLOCK INDEX                                                         │
├─────────────────────────────────────────────────────────────────────┤
│ SECTION TABLE (SectionEntry 增加 encoding 字段)                      │
│   [type:1B][encoding:1B][nameLen:1B][offset:8B][size:8B][name]     │
└─────────────────────────────────────────────────────────────────────┘
```

## 编码算法详细设计

### 1. Delta-of-Delta + Varint (时间戳 int64)

```
编码:
  base_ts = uint64(ts[0])
  delta1  = uint64(ts[1] - ts[0])
  dod[i]  = uint64((ts[i] - ts[i-1]) - (ts[i-1] - ts[i-2]))   (i >= 2)

编码格式:
  [base_ts: varint][delta1: varint][dod_2: varint]...

解码:
  ts[0] = int64(base_ts)
  ts[1] = ts[0] + int64(delta1)
  ts[i] = 2*ts[i-1] - ts[i-2] + int64(dod_i)   (i >= 2)
```

对于等间隔时间序列，dod[i] 恒为 0，每个 dod 仅需 1 字节 Varint。

### 2. Varint (SID uint64)

```
编码格式:
  [sid0: varint][sid1: varint]...
```

复用已有 `compression.PutVarint` / `compression.Varint`。

### 3. ZigZag + Varint (int64 字段)

```
ZigZag 映射:
  Encode: zigzag(n) = uint64(n << 1) ^ uint64(n >> 63)
  Decode: unzigzag(m) = int64(m >> 1) ^ -int64(m & 1)

编码格式:
  [zz_0: varint][zz_1: varint]...
```

小绝对值（如 -1, 1, 2）映射为小的无符号数，Varint 只需 1 字节。

### 4. XOR Float (float64 字段)

Gorilla 论文的 XOR 压缩算法：

```
编码格式:
  [first_value: 8B IEEE 754 BigEndian]   -- 首个值原始存储
  [control_bits...]                       -- 后续值的压缩比特流

对每个后续值 (i >= 1):
  xor = math.Float64bits(v[i]) ^ math.Float64bits(v[i-1])
  
  if xor == 0:
      写入 1 bit '0'
  else:
      写入 1 bit '1'
      leading  = clz(xor)        // 前导零个数 (0-64)
      trailing = ctz(xor)        // 后导零个数 (0-64)
      meaningful = 64 - leading - trailing
      
      写入 leading      (6 bits, 0-63, 64 特殊处理)
      写入 meaningful-1 (6 bits, 0-63, 表示 1-64 个有效位)
      写入 xor >> trailing 的高 meaningful 位 (meaningful bits)
```

新增 `BitWriter` / `BitReader` 辅助类型用于比特级读写。

### 5. 字典编码 (string 字段)

```
编码格式:
  [dict_count: 2B BigEndian]
  [dict_entries...]
    每个: [str_len: 2B BigEndian][str_data: str_len 字节]
  [indices...]
    每个: [index: varint]

回退条件: 若编码后大小 >= 原始大小，回退为原始编码。
```

原始编码格式（回退时使用）:
```
  [str_len: 2B BigEndian][str_data]...
```

### 6. 位图编码 (bool 字段)

```
编码格式:
  [bits: ceil(rowCount/8) 字节]
  
  字节 j 的第 k bit (MSB first) 对应行号 (j*8 + k):
    1 = true, 0 = false
```

## 格式变更

### SectionEntry 格式

在原基础上增加 1 字节 encoding 字段：

```
[type:1B][encoding:1B][nameLen:1B][offset:8B][size:8B][name:nameLen]
```

固定开销从 18 字节变为 19 字节。

### 新增 EncodingType

```go
type EncodingType uint8

const (
    EncodingRaw           EncodingType = 0  // 原始编码
    EncodingDeltaVarint   EncodingType = 1  // Delta-of-Delta + Varint (时间戳)
    EncodingVarint        EncodingType = 2  // Varint (SID)
    EncodingZigZagVarint  EncodingType = 3  // ZigZag + Varint (int64)
    EncodingXORFloat      EncodingType = 4  // XOR 浮点 (float64)
    EncodingDictString    EncodingType = 5  // 字典编码 (string)
    EncodingBitmapBool    EncodingType = 6  // 位图 (bool)
)
```

## 写入流程

```
Writer.Close():
  1. flushBlock() -- 无变化，临时文件仍为原始编码
  2. 对每个段编码后写入最终文件:
     timestamps:  读取原始 int64[] → Delta-of-Delta Encode → Varint Encode → 写入
     sids:        读取原始 uint64[] → Varint Encode → 写入
     int64字段:   读取原始 int64[] → ZigZag Encode → Varint Encode → 写入
     float64字段: 读取原始 float64[] → XOR Encode → 写入
     string字段:  读取原始 []string → Dict Encode → 写入 (失败回退 Raw)
     bool字段:    读取原始 []bool → Bitmap Encode → 写入
  3. SectionTable 中记录每个段的 encoding 和编码后大小
```

## 读取流程

```
Reader:
  1. 解析 SectionTable，读取每个 SectionEntry 的 encoding
  2. 根据 encoding 选择解码器:
     EncodingRaw          → 直接解析为原始格式
     EncodingDeltaVarint  → Varint Decode → Delta-of-Delta Decode
     EncodingVarint       → Varint Decode
     EncodingZigZagVarint → Varint Decode → Unzigzag
     EncodingXORFloat     → XOR Decode
     EncodingDictString   → Dict Decode
     EncodingBitmapBool   → Bitmap Decode
```

## 文件结构

```
compression/
├── delta.go          # Delta 编码 (已有)
├── varint.go         # Varint 编码 (已有)
├── zigzag.go         # ZigZag 编码 (新增)
├── xor_float.go      # XOR 浮点编码 (新增)
├── dict_string.go    # 字典编码 (新增)
├── bitmap.go         # 位图编码 (新增)
├── bit_io.go         # BitWriter/BitReader (新增)
├── encode.go         # EncodeSection / DecodeSection 统一入口 (新增)
└── *_test.go         # 测试文件
```

## 关键决策

1. **Close 时编码**: 临时文件保持原始格式，仅 Close 合并时编码。实现简单，不改变 flushBlock 逻辑。

2. **段级编码**: 对整个段执行编码而非逐块编码，更长的序列给 Delta/XOR 更多压缩机会。

3. **字典回退**: string 编码后若大于原始数据，自动回退为 EncodingRaw。避免低基数场景反效果。

4. **SID 用 Varint 而非 Delta**: SID 按时间戳排序后不保证单调递增，Varint 直接对小整数生效。
