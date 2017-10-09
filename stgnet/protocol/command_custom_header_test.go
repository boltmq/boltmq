package protocol

import (
	"strconv"
	"testing"
)

type (
	intType    int
	int32Type  int32
	int16Type  int16
	int8Type   int8
	uint8Type  uint8
	uint16Type uint16
	uint32Type uint32
	int64Type  int64
	uint64Type uint64
	sType      string
	bType      bool
)

type testCommandCustomHeader struct {
	Name     string
	I        int
	J        int16
	K        int8
	G        uint8
	M        uint16
	Age      int32
	Age1     uint32
	Sond     int64
	Sond1    uint64
	Flag     bool
	Itype    intType
	I32type  int32Type
	I16type  int16Type
	I8type   int8Type
	Ui8type  uint8Type
	Ui16type uint16Type
	Ui32type uint32Type
	I64type  int64Type
	Ui64type uint64Type
	Stype    sType
	Btype    bType
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
	tc.Itype = 100
	tc.I32type = 101
	tc.I16type = 102
	tc.I8type = 103
	tc.Ui8type = 104
	tc.Ui16type = 105
	tc.Ui32type = 106
	tc.I64type = 107
	tc.Ui64type = 108
	tc.Stype = "jerrylou"
	tc.Btype = true

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

	if m["Itype"] != strconv.FormatInt(int64(tc.Itype), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["Itype"], tc.Itype)
	}

	if m["I32type"] != strconv.FormatInt(int64(tc.I32type), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["I32type"], tc.I32type)
	}

	if m["I16type"] != strconv.FormatInt(int64(tc.I16type), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["I16type"], tc.I16type)
	}

	if m["I8type"] != strconv.FormatInt(int64(tc.I8type), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["I8type"], tc.I8type)
	}

	if m["I64type"] != strconv.FormatInt(int64(tc.I64type), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["I64type"], tc.I64type)
	}

	if m["Ui8type"] != strconv.FormatInt(int64(tc.Ui8type), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["Ui8type"], tc.Ui8type)
	}

	if m["Ui16type"] != strconv.FormatInt(int64(tc.Ui16type), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["Ui16type"], tc.Ui16type)
	}

	if m["Ui32type"] != strconv.FormatInt(int64(tc.Ui32type), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["Ui32type"], tc.Ui32type)
	}

	if m["Ui64type"] != strconv.FormatInt(int64(tc.Ui64type), 10) {
		t.Errorf("Test faild: Name value %s != %d", m["Ui64type"], tc.Ui64type)
	}

	if m["Btype"] != strconv.FormatBool(bool(tc.Btype)) {
		t.Errorf("Test faild: Name value %s != %t", m["Btype"], tc.Btype)
	}

	if m["Stype"] != string(tc.Stype) {
		t.Errorf("Test faild: Name value %s != %s", m["Stype"], tc.Stype)
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
	extFields["Itype"] = "110"
	extFields["I32type"] = "111"
	extFields["I16type"] = "112"
	extFields["I8type"] = "113"
	extFields["I64type"] = "114"
	extFields["Ui8type"] = "115"
	extFields["Ui16type"] = "116"
	extFields["Ui32type"] = "117"
	extFields["Ui64type"] = "118"
	extFields["Btype"] = "true"
	extFields["Stype"] = "jerrylou"

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

	if extFields["Itype"] != strconv.FormatInt(int64(tc.Itype), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["Itype"], tc.Itype)
	}

	if extFields["I8type"] != strconv.FormatInt(int64(tc.I8type), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["I8type"], tc.I8type)
	}

	if extFields["I16type"] != strconv.FormatInt(int64(tc.I16type), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["I16type"], tc.I16type)
	}

	if extFields["I32type"] != strconv.FormatInt(int64(tc.I32type), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["I32type"], tc.I32type)
	}

	if extFields["I64type"] != strconv.FormatInt(int64(tc.I64type), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["I64type"], tc.I64type)
	}

	if extFields["Ui8type"] != strconv.FormatInt(int64(tc.Ui8type), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["Ui8type"], tc.Ui8type)
	}

	if extFields["Ui16type"] != strconv.FormatInt(int64(tc.Ui16type), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["Ui16type"], tc.Ui16type)
	}

	if extFields["Ui32type"] != strconv.FormatInt(int64(tc.Ui32type), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["Ui32type"], tc.Ui32type)
	}

	if extFields["Ui64type"] != strconv.FormatInt(int64(tc.Ui64type), 10) {
		t.Errorf("Test faild: Name value %s != %d", extFields["Ui64type"], tc.Ui64type)
	}

	if extFields["Stype"] != string(tc.Stype) {
		t.Errorf("Test faild: Name value %s != %s", extFields["Stype"], tc.Stype)
	}

	if extFields["Btype"] != strconv.FormatBool(bool(tc.Btype)) {
		t.Errorf("Test faild: Name value %s != %t", extFields["Btype"], tc.Btype)
	}
}
