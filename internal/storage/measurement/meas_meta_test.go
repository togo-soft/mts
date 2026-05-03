package measurement

import "testing"

func TestTagsHash(t *testing.T) {
    tests := []struct {
        name string
        tags map[string]string
    }{
        {"empty", map[string]string{}},
        {"single", map[string]string{"host": "server1"}},
        {"multiple", map[string]string{"host": "server1", "region": "us"}},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            h := tagsHash(tt.tags)
            if h == 0 && len(tt.tags) > 0 {
                t.Error("hash should not be 0 for non-empty tags")
            }
        })
    }
}

func TestTagsEqual(t *testing.T) {
    tags1 := map[string]string{"host": "server1", "region": "us"}
    tags2 := map[string]string{"region": "us", "host": "server1"}
    tags3 := map[string]string{"host": "server2"}

    if !tagsEqual(tags1, tags2) {
        t.Error("tags with same content but different order should be equal")
    }
    if tagsEqual(tags1, tags3) {
        t.Error("tags with different content should not be equal")
    }
}