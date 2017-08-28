package list

import (
	"bytes"
	"sync"

	"git.oschina.net/cloudzone/smartgo/stgcommon/list"
)

// BufferLinkedList holds the elements, where each element points to the next and previous element, thread saft
type BufferLinkedList struct {
	bufferList *list.BufferLinkedList
	sync.RWMutex
}

// New instantiates a new empty list
func NewBufferLinkedList() *BufferLinkedList {
	return &BufferLinkedList{
		bufferList: list.NewBufferLinkedList(),
	}
}

// Add appends a value (one or more) at the end of the list (same as Append())
func (bll *BufferLinkedList) Add(values ...*bytes.Buffer) {
	bll.Lock()
	bll.bufferList.Add(values...)
	bll.Unlock()
}

// Append appends a value (one or more) at the end of the list (same as Add())
func (bll *BufferLinkedList) Append(values ...*bytes.Buffer) {
	bll.Lock()
	bll.bufferList.Append(values...)
	bll.Unlock()
}

// Prepend prepends a values (or more)
func (bll *BufferLinkedList) Prepend(values ...*bytes.Buffer) {
	bll.Lock()
	bll.bufferList.Prepend(values...)
	bll.Unlock()
}

// Get returns the element at index.
// Second return parameter is true if index is within bounds of the array and array is not empty, otherwise false.
func (bll *BufferLinkedList) Get(index int) (*bytes.Buffer, bool) {
	bll.RLock()
	defer bll.RUnlock()
	return bll.bufferList.Get(index)
}

// Remove removes one or more elements from the list with the supplied indices.
func (bll *BufferLinkedList) Remove(index int) {
	bll.Lock()
	bll.bufferList.Remove(index)
	bll.Unlock()
}

// Contains check if values (one or more) are present in the set.
// All values have to be present in the set for the method to return true.
// Performance time complexity of n^2.
// Returns true if no arguments are passed at all, i.e. set is always super-set of empty set.
func (bll *BufferLinkedList) Contains(values ...*bytes.Buffer) bool {
	bll.RLock()
	defer bll.RUnlock()
	return bll.bufferList.Contains(values...)
}

// Values returns all elements in the list.
func (bll *BufferLinkedList) Values() []*bytes.Buffer {
	bll.RLock()
	defer bll.RUnlock()
	return bll.bufferList.Values()
}

// Empty returns true if list does not contain any elements.
func (bll *BufferLinkedList) Empty() bool {
	bll.RLock()
	defer bll.RUnlock()
	return bll.bufferList.Empty()
}

// Size returns number of elements within the list.
func (bll *BufferLinkedList) Size() int {
	bll.RLock()
	defer bll.RUnlock()
	return bll.bufferList.Size()
}

// Clear removes all elements from the list.
func (bll *BufferLinkedList) Clear() {
	bll.Lock()
	bll.bufferList.Clear()
	bll.Unlock()
}

// Sort sorts values (in-place) using.
func (bll *BufferLinkedList) Sort(comparator list.Comparator) {
	bll.Lock()
	bll.bufferList.Sort(comparator)
	bll.Unlock()
}

// Swap swaps values of two elements at the given indices.
func (bll *BufferLinkedList) Swap(i, j int) {
	bll.Lock()
	bll.bufferList.Swap(i, j)
	bll.Unlock()
}

// Insert inserts values at specified index position shifting the value at that position (if any) and any subsequent elements to the right.
// Does not do anything if position is negative or bigger than list's size
// Note: position equal to list's size is valid, i.e. append.
func (bll *BufferLinkedList) Insert(index int, values ...*bytes.Buffer) {
	bll.Lock()
	bll.bufferList.Insert(index, values...)
	bll.Unlock()
}

// String returns a string representation of container
func (bll *BufferLinkedList) String() string {
	bll.RLock()
	defer bll.RUnlock()
	return bll.bufferList.String()
}
