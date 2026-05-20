# Requirements: 单文件 SSTable 格式

## 问题

当前每个 SSTable 是一个目录，内含 13+ 个独立文件（`_timestamps.bin`, `_sids.bin`, `_index.bin`, `fields/*.bin`）。1M 数据点产生约 7200 个文件，导致：
- 大量 syscall（每列一次 open/close）
- 文件系统 inode 压力
- 目录遍历缓慢（readdir 扫描 fields/）
- 分布式文件系统（HDFS, S3）不适配

## 目标

The system SHALL store each SSTable as a single file while preserving the internal columnar layout for compression and selective column reads.

## 功能需求 (EARS)

### FR1: 单文件写入
WHEN Writer.Close() is called, the system SHALL produce exactly one file named `sst_{seq}.bin` in the data directory, containing all timestamps, sids, field values, block index, and section metadata.

### FR2: 单文件读取
WHEN Reader is created with a file path, the system SHALL locate all sections (timestamps, sids, fields, block index) via the section table embedded at the end of the file, and SHALL support reading specific columns via pread without loading entire column data.

### FR3: 向后兼容
The system SHALL use a new magic number (`TSERSTBL`) and version number distinct from the current multi-file format, ensuring old data can be detected and rejected with a clear error message.

### FR4: 列式布局保留
The system SHALL maintain the current block-based, per-column storage layout within the single file, ensuring compression characteristics and selective column read capability are preserved.

### FR5: 文件数减少
The system SHALL reduce the per-SSTable file count from (3 + N_fields) to exactly 1. For a workload with 10 fields, this represents a 93% reduction in file count.

### FR6: 接口兼容
WHEN shard_flush.go, compaction/merge.go, and compaction/level.go create SSTables via sstable.NewWriter, the system SHALL use the same function signature with only the output path changing from directory to file.

### FR7: SSTable 序列号恢复
WHEN recoverSSTSeq scans the data directory, the system SHALL recognize `sst_N.bin` files (new format) while still handling legacy `sst_N/` directories gracefully.

### FR8: 读性能
WHEN reading data from a single-file SSTable, the number of syscalls SHALL be less than or equal to the current multi-file approach. The system SHOULD support future mmap optimization via the section table format.

### FR9: `.writing` 标记
WHEN a compaction manager marks an SSTable as in-write, the system SHALL use `.writing` flag files adjacent to the SSTable file (e.g., `sst_{seq}.bin.writing`), consistent with the current convention.

### FR10: Level Compaction 兼容
WHEN Level Compaction moves SSTables between levels, the system SHALL operate on single files instead of directories, using os.Rename on the single file.
