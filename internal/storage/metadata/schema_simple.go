package metadata

import "sync"

// SimpleSchemaStore 是纯内存的 Schema 存储，供外部测试使用。
type SimpleSchemaStore struct {
	mu      sync.RWMutex
	schemas map[string]*Schema // key: "db/measurement"
}

// NewSimpleSchemaStore 创建纯内存 SchemaStore。
func NewSimpleSchemaStore() *SimpleSchemaStore {
	return &SimpleSchemaStore{
		schemas: make(map[string]*Schema),
	}
}

func schemaKey(db, measurement string) string {
	return db + "/" + measurement
}

// GetSchema 获取 schema。
func (s *SimpleSchemaStore) GetSchema(db, measurement string) (*Schema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if schema, ok := s.schemas[schemaKey(db, measurement)]; ok {
		return schema, nil
	}
	return nil, nil
}

// SetSchema 设置 schema。
func (s *SimpleSchemaStore) SetSchema(db, measurement string, schema *Schema) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.schemas[schemaKey(db, measurement)] = schema
	return nil
}
