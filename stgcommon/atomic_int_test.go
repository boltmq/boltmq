package stgcommon

import "testing"

func TestAtomicInt64_get_set(t *testing.T) {
	atomicInt64 := NewAtomicIn64(10)
	if 10 != atomicInt64.Get() {
		t.Error("AtomicIn64 instance error")
		t.Fail()
	}

	atomicInt64.Set(9)
	value := atomicInt64.Get()

	if 9 != value {
		t.Error("AtomicIn64 set or get method error, expection:9, actuality:", value)
		t.Fail()
	}
}

func TestAtomicInt64_AddAndGet(t *testing.T) {
	atomicInt64 := NewAtomicIn64(10)

	oldValue := atomicInt64.GetAndAdd(12)
	if 10 != oldValue {
		t.Error("AtomicIn64 AddAndGet method error, expection:10, actuality:", oldValue)
		t.Fail()
	}

	newValue := atomicInt64.Get()
	if 22 != newValue {
		t.Error("AtomicIn64 AddAndGet method error, expection:22, actuality:", oldValue)
		t.Fail()
	}

	oldValue = atomicInt64.GetAndAdd(-2)
	if 22 != oldValue {
		t.Error("AtomicIn64 AddAndGet method error, expection:10, actuality:", oldValue)
		t.Fail()
	}

	newValue = atomicInt64.Get()
	if 20 != newValue {
		t.Error("AtomicIn64 AddAndGet method error, expection:20, actuality:", oldValue)
		t.Fail()
	}

}
