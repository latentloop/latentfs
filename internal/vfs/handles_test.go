package vfs

import (
	"sync"
	"testing"
)

func TestNewHandleManager(t *testing.T) {
	hm := NewHandleManager()
	if hm == nil {
		t.Fatal("NewHandleManager returned nil")
	}
	if hm.handles == nil {
		t.Error("handles map is nil")
	}
	if hm.nextHandle != 1 {
		t.Errorf("nextHandle = %d, want 1", hm.nextHandle)
	}
}

func TestAllocate(t *testing.T) {
	hm := NewHandleManager()

	h1 := hm.Allocate(1, "/file1.txt", false, 0, 100)
	h2 := hm.Allocate(2, "/dir", true, 0, 100)
	h3 := hm.Allocate(3, "/file2.txt", false, 0, 100)

	if h1 == 0 || h2 == 0 || h3 == 0 {
		t.Error("handles should not be 0")
	}
	if h1 == h2 || h2 == h3 || h1 == h3 {
		t.Error("handles should be unique")
	}
	if h1 != 1 || h2 != 2 || h3 != 3 {
		t.Error("handles should be sequential")
	}
}

func TestGet(t *testing.T) {
	hm := NewHandleManager()

	h := hm.Allocate(42, "/test.txt", false, 0644, 100)

	info, ok := hm.Get(h)
	if !ok {
		t.Fatal("Get returned not ok")
	}
	if info.ino != 42 {
		t.Errorf("ino = %d, want 42", info.ino)
	}
	if info.path != "/test.txt" {
		t.Errorf("path = %s, want /test.txt", info.path)
	}
	if info.isDir != false {
		t.Error("isDir should be false")
	}
	if info.flags != 0644 {
		t.Errorf("flags = %d, want 0644", info.flags)
	}
}

func TestGet_NotFound(t *testing.T) {
	hm := NewHandleManager()

	_, ok := hm.Get(999)
	if ok {
		t.Error("Get should return not ok for nonexistent handle")
	}
}

func TestRelease(t *testing.T) {
	hm := NewHandleManager()

	h := hm.Allocate(1, "/test.txt", false, 0, 100)

	// Verify it exists
	_, ok := hm.Get(h)
	if !ok {
		t.Fatal("handle should exist before release")
	}

	// Release it
	hm.Release(h)

	// Verify it's gone
	_, ok = hm.Get(h)
	if ok {
		t.Error("handle should not exist after release")
	}
}

func TestRelease_Nonexistent(t *testing.T) {
	hm := NewHandleManager()

	// Should not panic
	hm.Release(999)
}

func TestUpdateDirPos(t *testing.T) {
	hm := NewHandleManager()

	h := hm.Allocate(1, "/dir", true, 0, 100)

	// Initial position should be 0
	pos := hm.GetDirPos(h)
	if pos != 0 {
		t.Errorf("initial dirPos = %d, want 0", pos)
	}

	// Update position
	hm.UpdateDirPos(h, 10)

	pos = hm.GetDirPos(h)
	if pos != 10 {
		t.Errorf("updated dirPos = %d, want 10", pos)
	}
}

func TestUpdateDirPos_Nonexistent(t *testing.T) {
	hm := NewHandleManager()

	// Should not panic
	hm.UpdateDirPos(999, 10)

	// GetDirPos for nonexistent should return 0
	pos := hm.GetDirPos(999)
	if pos != 0 {
		t.Errorf("dirPos for nonexistent = %d, want 0", pos)
	}
}

func TestSetDirEnumDone(t *testing.T) {
	hm := NewHandleManager()

	h := hm.Allocate(1, "/dir", true, 0, 100)

	// Initial should be false
	if hm.IsDirEnumDone(h) {
		t.Error("initial dirEnumDone should be false")
	}

	// Set to true
	hm.SetDirEnumDone(h, true)
	if !hm.IsDirEnumDone(h) {
		t.Error("dirEnumDone should be true after SetDirEnumDone(true)")
	}

	// Set back to false
	hm.SetDirEnumDone(h, false)
	if hm.IsDirEnumDone(h) {
		t.Error("dirEnumDone should be false after SetDirEnumDone(false)")
	}
}

func TestIsDirEnumDone_Nonexistent(t *testing.T) {
	hm := NewHandleManager()

	// Nonexistent handle should return false
	if hm.IsDirEnumDone(999) {
		t.Error("IsDirEnumDone for nonexistent should be false")
	}
}

func TestSetDirEnumDone_Nonexistent(t *testing.T) {
	hm := NewHandleManager()

	// Should not panic
	hm.SetDirEnumDone(999, true)
}

func TestConcurrentAccess(t *testing.T) {
	hm := NewHandleManager()
	const numGoroutines = 100
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				h := hm.Allocate(int64(id*1000+j), "/test", false, 0, 100)
				hm.Get(h)
				hm.UpdateDirPos(h, j)
				hm.GetDirPos(h)
				hm.SetDirEnumDone(h, true)
				hm.IsDirEnumDone(h)
				hm.Release(h)
			}
		}(i)
	}

	wg.Wait()
}

func TestOpenHandleFields(t *testing.T) {
	hm := NewHandleManager()

	h := hm.Allocate(100, "/path/to/file.txt", true, 0755, 100)

	info, _ := hm.Get(h)

	if info.ino != 100 {
		t.Errorf("ino = %d, want 100", info.ino)
	}
	if info.path != "/path/to/file.txt" {
		t.Errorf("path = %s, want /path/to/file.txt", info.path)
	}
	if !info.isDir {
		t.Error("isDir should be true")
	}
	if info.flags != 0755 {
		t.Errorf("flags = %d, want 0755", info.flags)
	}
	if info.dirPos != 0 {
		t.Errorf("initial dirPos = %d, want 0", info.dirPos)
	}
	if info.dirEnumDone {
		t.Error("initial dirEnumDone should be false")
	}
}

func TestPinnedEpoch(t *testing.T) {
	hm := NewHandleManager()

	// Allocate handles with different epochs
	h1 := hm.Allocate(1, "/file1.txt", false, 0, 100)
	h2 := hm.Allocate(2, "/file2.txt", false, 0, 200)
	h3 := hm.Allocate(3, "/file3.txt", false, 0, 150)

	// Verify each handle has the correct pinned epoch
	info1, _ := hm.Get(h1)
	if info1.pinnedEpoch != 100 {
		t.Errorf("h1 pinnedEpoch = %d, want 100", info1.pinnedEpoch)
	}

	info2, _ := hm.Get(h2)
	if info2.pinnedEpoch != 200 {
		t.Errorf("h2 pinnedEpoch = %d, want 200", info2.pinnedEpoch)
	}

	info3, _ := hm.Get(h3)
	if info3.pinnedEpoch != 150 {
		t.Errorf("h3 pinnedEpoch = %d, want 150", info3.pinnedEpoch)
	}
}

func TestHandleReuse(t *testing.T) {
	hm := NewHandleManager()

	// Allocate and release
	h1 := hm.Allocate(1, "/a", false, 0, 100)
	hm.Release(h1)

	// Allocate again - handles should NOT be reused
	h2 := hm.Allocate(2, "/b", false, 0, 100)

	if h2 == h1 {
		t.Error("handles should not be reused after release")
	}
	if h2 != h1+1 {
		t.Errorf("next handle = %d, want %d", h2, h1+1)
	}
}

func TestClear(t *testing.T) {
	hm := NewHandleManager()

	// Allocate some handles
	h1 := hm.Allocate(1, "/file1.txt", false, 0, 100)
	h2 := hm.Allocate(2, "/file2.txt", false, 0, 100)
	h3 := hm.Allocate(3, "/dir", true, 0, 100)

	// Verify they exist
	if _, ok := hm.Get(h1); !ok {
		t.Fatal("h1 should exist")
	}
	if _, ok := hm.Get(h2); !ok {
		t.Fatal("h2 should exist")
	}
	if _, ok := hm.Get(h3); !ok {
		t.Fatal("h3 should exist")
	}

	// Clear all handles
	count := hm.Clear()

	// Verify count
	if count != 3 {
		t.Errorf("Clear returned %d, want 3", count)
	}

	// Verify all handles are gone
	if _, ok := hm.Get(h1); ok {
		t.Error("h1 should not exist after Clear")
	}
	if _, ok := hm.Get(h2); ok {
		t.Error("h2 should not exist after Clear")
	}
	if _, ok := hm.Get(h3); ok {
		t.Error("h3 should not exist after Clear")
	}
}

func TestClear_Empty(t *testing.T) {
	hm := NewHandleManager()

	// Clear empty manager should return 0
	count := hm.Clear()
	if count != 0 {
		t.Errorf("Clear on empty manager returned %d, want 0", count)
	}
}

func TestClear_PreservesNextHandle(t *testing.T) {
	hm := NewHandleManager()

	// Allocate some handles
	hm.Allocate(1, "/a", false, 0, 100)
	hm.Allocate(2, "/b", false, 0, 100)
	h3 := hm.Allocate(3, "/c", false, 0, 100)

	// Clear
	hm.Clear()

	// Allocate new handle - should continue from where we left off
	h4 := hm.Allocate(4, "/d", false, 0, 100)

	if h4 <= h3 {
		t.Errorf("new handle %d should be greater than last handle %d", h4, h3)
	}
}
