package sync

import (
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewMap(t *testing.T) {
	m := NewMap()
	if m == nil {
		t.Error("NewMap is faild")
		return
	}

	if m.ConcurrentMap == nil {
		t.Error("NewMap is faild")
		return
	}

	//t.Log("NewMap success")
}

func TestPut(t *testing.T) {
	m := NewMap()

	var (
		firstKey = 1
		firstVal = 100
	)
	previou, err := m.Put(firstKey, firstVal)
	if previou != nil || err != nil {
		t.Errorf("Put %v, %v firstly, return %v, %v, want nil, nil", firstKey, firstVal, previou, err)
	}

	//t.Log("Put success")
}

func testAllFn(t *testing.T, datas map[interface{}]interface{}) {
	var (
		i            int
		firstKey     interface{}
		firstVal     interface{}
		secondaryKey interface{}
		secondaryVal interface{}
	)
	for k, v := range datas {
		if i == 0 {
			firstKey, firstVal = k, v
		} else if i == 1 {
			secondaryKey, secondaryVal = k, v
			break
		}
		i++
	}

	m := NewMap()

	//test Put first key-value pair
	previou, err := m.Put(firstKey, firstVal)
	if previou != nil || err != nil {
		t.Errorf("Put %v, %v firstly, return %v, %v, want nil, nil", firstKey, firstVal, previou, err)
	}

	//test Put again
	previou, err = m.Put(firstKey, firstVal)
	if previou != firstVal || err != nil {
		t.Errorf("Put %v, %v second time, return %v, %v, want %v, nil", firstKey, firstVal, previou, err, firstVal)
	}

	//test PutIfAbsent, if value is incorrect, PutIfAbsent will be ignored
	v := rand.Float32()
	previou, err = m.PutIfAbsent(firstKey, v)
	if previou != firstVal || err != nil {
		t.Errorf("PutIfAbsent %v, %v three time, return %v, %v, want %v, nil", firstKey, v, previou, err, firstVal)
	}

	//test Get
	val, err := m.Get(firstKey)
	if val != firstVal || err != nil {
		t.Errorf("Get %v, return %v, %v, want %v, nil", firstKey, val, err, firstVal)
	}

	//test Size
	s := m.Size()
	if s != 1 {
		t.Errorf("Get size of m, return %v, want 1", s)
	}

	//test PutAll
	m.PutAll(datas)
	s = m.Size()
	if s != int32(len(datas)) {
		t.Errorf("Get size of m, return %v, want %v", s, len(datas))
	}

	//test remove a key-value pair, if value is incorrect, RemoveKV will return be ignored and return false
	ok, err := m.RemoveEntry(secondaryKey, v)
	if ok != false || err != nil {
		t.Errorf("RemoveKV %v, %v, return %v, %v, want false, nil", secondaryKey, v, ok, err)
	}

	//test replace a value for a key
	previou, err = m.Replace(secondaryKey, v)
	if previou != secondaryVal || err != nil {
		t.Errorf("Replace %v, %v, return %v, %v, want %v, nil", secondaryKey, v, previou, err, secondaryVal)
	}

	//test replace a value for a key-value pair, if value is incorrect, replace will ignored and return false
	ok, err = m.CompareAndReplace(secondaryKey, secondaryVal, v)
	if ok != false || err != nil {
		t.Errorf("ReplaceWithOld  %v, %v, %v, return %v, %v, want false, nil", secondaryKey, secondaryVal, v, ok, err)
	}

	//test replace a value for a key-value pair, if value is correct, replace will success
	ok, err = m.CompareAndReplace(secondaryKey, v, secondaryVal)
	if ok != true || err != nil {
		t.Errorf("ReplaceWithOld %v, %v, %v, return %v, %v, want true, nil", secondaryKey, v, secondaryVal, ok, err)
	}

	//test remove a key
	previou, err = m.Remove(secondaryKey)
	if previou != secondaryVal || err != nil {
		t.Errorf("Remove %v, return %v, %v, want %v, nil", secondaryKey, previou, err, secondaryVal)
	}

	//test clear
	m.Clear()
	if m.Size() != 0 {
		t.Errorf("Get size of m after calling Clear(), return %v, want 0", val)
	}
}

func TestIntKey(t *testing.T) {
	testAllFn(t, map[interface{}]interface{}{
		1: 10,
		2: 20,
		3: 30,
		4: 40,
	})
}

func TestStringKey(t *testing.T) {
	testAllFn(t, map[interface{}]interface{}{
		strconv.Itoa(1): 10,
		strconv.Itoa(2): 20,
		strconv.Itoa(3): 30,
		strconv.Itoa(4): 40,
	})
}

func TestFloat32Key(t *testing.T) {
	testAllFn(t, map[interface{}]interface{}{
		float32(1): 10,
		float32(2): 20,
		float32(3): 30,
		float32(4): 40,
	})
}

func TestFloat64Key(t *testing.T) {
	testAllFn(t, map[interface{}]interface{}{
		float64(1): 10,
		float64(2): 20,
		float64(3): 30,
		float64(4): 40,
	})
}

//size of small is less than word size
//the memory layout is different with struct what size is greater than word size before golang 1.4
type small struct {
	Id   byte
	name byte
}

//test small struct
func TestSmallStruct(t *testing.T) {
	a, b, c, d := small{1, 1}, small{2, 2}, small{3, 3}, small{4, 4}
	testAllFn(t, map[interface{}]interface{}{
		a: 10,
		b: 20,
		c: 30,
		d: 40,
	})

	//test using the interface object and original value as key, two value should return the same hash code
	cm := NewMap()
	cm.Put(a, 10)
	e := small{1, 1}
	if v, err := cm.Get(e); v != 10 || err != nil {
		t.Errorf("Get %v, return %v, %v, want %v", &e, v, err, 10)
	}
}

// Tests a map with a single bucket, with all keys having different lengths.
func TestSingleBucketMapStringKeys_NoDupLen(t *testing.T) {
	testMapLookups(t, NewMapFromOtherMap(map[interface{}]interface{}{
		"x":                      "x1val",
		"xx":                     "x2val",
		"foo":                    "fooval",
		"xxxx":                   "x4val",
		"xxxxx":                  "x5val",
		"xxxxxx":                 "x6val",
		strings.Repeat("x", 128): "longval",
	}))
}

func testMapLookups(t *testing.T, m *Map) {
	for itr := m.Iterator(); itr.HasNext(); {
		k, v, _ := itr.Next()
		if v1, err := m.Get(k); v1 != v || err != nil {
			t.Fatalf("m[%q] = %q; want %q", k, v1, v)
		}
	}
}

func TestConcurrent(t *testing.T) {
	var (
		numCpu       = runtime.NumCPU()
		writeN       = 2*numCpu + 1
		readN        = 3*numCpu + 1
		n            = 1000000
		repeat int32 = 0
		wWg          = new(sync.WaitGroup)
		cDone        = make(chan struct{})
	)
	runtime.GOMAXPROCS(runtime.GOMAXPROCS(numCpu))

	wWg.Add(writeN)
	cm := NewMap()

	//start writeN goroutines to write to map with repeated keys, and count the total number of repeated key
	for i := 0; i < writeN; i++ {
		j := i
		go func() {
			for k := 0; k < n; k++ {
				//0-99999, 50000-149999, 100000-19999, 150000-249999,200000-29999, 250000-349999
				key := k + (j * n / 2)
				if previous, err := cm.Put(key, strconv.Itoa(key)+strings.Repeat(" ", j)); err != nil {
					t.Errorf("Get error %v when concurrent write map", err)
					return
				} else if previous != nil {
					//count the total number of repeated key
					atomic.AddInt32(&repeat, 1)
				}
			}
			wWg.Done()
		}()
	}

	go func() {
		wWg.Wait()
		close(cDone)
	}()

	//start readN goroutines to iterate the map
	rWg := new(sync.WaitGroup)
	rWg.Add(readN)
	for i := 0; i < readN; i++ {
		go func() {
			for {

				for itr := cm.Iterator(); itr.HasNext(); {
					ki, vi, _ := itr.Next()
					k, v := ki.(int), vi.(string)
					if strconv.Itoa(k) != strings.Trim(v, " ") {
						t.Errorf("Get %v by %v, want %v == strings.Trim(\"%v\")", v, k, v, k)
						return
					}
				}

				//exit read goroutines if all write goroutines are done
				exit := false
				select {
				case <-cDone:
					exit = true
					break
				case <-time.After(1 * time.Microsecond):
				}

				if exit {
					break
				}
			}
			rWg.Done()
		}()
	}

	//Start a goroutines to count the size of concurrentMap and total number of repeated keys
	//after all write goroutines are done
	cLast := make(chan struct{})
	go func() {
		wWg.Wait()
		if repeat != int32((writeN-1)*(n/2)) {
			t.Errorf("Repeat %v, want %v", repeat, (writeN-1)*(n/2))
		}

		size := cm.Size()
		if size != int32(n/2+writeN*(n/2)) {
			t.Errorf("Size is %v, want %v", size, n/2+writeN*(n/2))
		}

		cm.Clear()
		size = cm.Size()
		if size != 0 {
			t.Errorf("Size is %v after calling Clear(), want %v", size, 0)
		}
		close(cLast)
	}()

	rWg.Wait()
	<-cLast
	runtime.GC()
}
