# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- gRPC API Key authentication interceptor (`-auth-key` flag)
- TLS support for gRPC server (`-tls-cert`, `-tls-key` flags)
- grpc_health_v1 service for Kubernetes probes
- Graceful shutdown with 10s timeout fallback
- Message size limits (4MB MaxRecv/MaxSend)
- Context deadline support in framework helpers

### Changed
- Default server port is now :2026
- Server logs warning when running in insecure mode (no TLS)

### Fixed
- QueryRange stream error handling now logs and closes gracefully

## [0.1.0] - 2026-05-01

### Added
- MemTable with skip list implementation
- WAL (Write-Ahead Log) for crash recovery
- SSTable with block-based storage
- bbolt-based metadata store
- gRPC API with Write, WriteBatch, QueryRange operations
- Streaming query support via QueryIterator
- Compression support (Snappy, LZ4)
- Level compaction strategy
- Data retention policy

### Features
- High-performance write path (177万+ TPS)
- Time-range queries with nanosecond precision
- Automatic MemTable flush
- Shard management by time windows
- Series tracking with SID
