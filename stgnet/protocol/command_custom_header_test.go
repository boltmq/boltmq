package protocol

import (
	"strconv"
	"testing"
)

type testCommandCustomHeader struct {
	Name  string
	I     int
	J     int16
	K     int8
	G     uint8
	M     uint16
	Age   int32
	Age1  uint32
	Sond  int64
	Sond1 uint64
	Flag  bool
}

func (t *testCommandCustomHeader) CheckFields() error {
	return nil
}

func TestEncodeCommandCustomHeader(t *testing.T) {
	tc := &testCommandCustomHeader{}
	tc.Name = "luoji"
	tc.I = 10
	tc.J = 11
	tc.K = 12
	tc.G = 13
	tc.M = 14
	tc.Age = 18
	tc.Age1 = 180
	tc.Sond = 2000
	tc.Sond1 = 20000
	tc.Flag = true

	m := encodeCommandCustomHeader(tc)

	if m["Name"] != tc.Name {
		t.Errorf("Test faild: Name value %s != %s", m["Name"], tc.Name)
	}

	if m["I"] != strconv.FormatInt(int64(tc.I), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["I"], tc.I)
	}

	if m["J"] != strconv.FormatInt(int64(tc.J), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["J"], tc.J)
	}

	if m["K"] != strconv.FormatInt(int64(tc.K), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["K"], tc.K)
	}

	if m["G"] != strconv.FormatUint(uint64(tc.G), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["G"], tc.G)
	}

	if m["M"] != strconv.FormatUint(uint64(tc.M), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["M"], tc.M)
	}

	if m["Age"] != strconv.FormatInt(int64(tc.Age), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["Age"], tc.Age)
	}

	if m["Age1"] != strconv.FormatUint(uint64(tc.Age1), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["Age1"], tc.Age1)
	}

	if m["Sond"] != strconv.FormatInt(int64(tc.Sond), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["Sond"], tc.Sond)
	}

	if m["Sond1"] != strconv.FormatUint(uint64(tc.Sond1), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["Sond1"], tc.Sond1)
	}

	if m["Flag"] != strconv.FormatBool(tc.Flag) {
		t.Errorf("Test faild: Name value %s != %t", m["Flag"], tc.Flag)
	}
}

func TestDecodeCommandCustomHeader(t *testing.T) {
	extFields := make(map[string]string, 2)
	extFields["Name"] = "luoji"
	extFields["I"] = "10"
	extFields["J"] = "11"
	extFields["K"] = "12"
	extFields["G"] = "13"
	extFields["M"] = "14"
	extFields["Age"] = "18"
	extFields["Age1"] = "180"
	extFields["Sond"] = "2000"
	extFields["Sond1"] = "20000"
	extFields["Flag"] = "true"

	tc := &testCommandCustomHeader{}
	e := decodeCommandCustomHeader(extFields, tc)
	if e != nil {
		t.Errorf("Test faild: %s", e)
		return
	}

	if extFields["Name"] != tc.Name {
		t.Errorf("Test faild: Name value %s != %s", extFields["Name"], tc.Name)
	}

	if extFields["I"] != strconv.FormatInt(int64(tc.I), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["I"], tc.I)
	}

	if extFields["J"] != strconv.FormatInt(int64(tc.J), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["J"], tc.J)
	}

	if extFields["K"] != strconv.FormatInt(int64(tc.K), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["K"], tc.K)
	}

	if extFields["G"] != strconv.FormatUint(uint64(tc.G), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["G"], tc.G)
	}

	if extFields["M"] != strconv.FormatUint(uint64(tc.M), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["M"], tc.M)
	}

	if extFields["Age"] != strconv.FormatInt(int64(tc.Age), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["Age"], tc.Age)
	}

	if extFields["Age1"] != strconv.FormatUint(uint64(tc.Age1), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["Age1"], tc.Age1)
	}

	if extFields["Sond"] != strconv.FormatInt(int64(tc.Sond), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["Sond"], tc.Sond)
	}

	if extFields["Sond1"] != strconv.FormatUint(uint64(tc.Sond1), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["Sond1"], tc.Sond1)
	}

	if extFields["Flag"] != strconv.FormatBool(tc.Flag) {
		t.Errorf("Test faild: Name value %s != %t", extFields["Flag"], tc.Flag)
	}
}
