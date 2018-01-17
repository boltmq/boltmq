// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package common

import (
	"os"
	"testing"
)

func TestParentDirectory(t *testing.T) {
	var (
		tv     = "testdata/001"
		expect = "testdata"
	)

	dir := ParentDirectory(tv)
	if dir != expect {
		t.Errorf("%s parent Directory is %s", tv, expect)
	}
}

func TestPathExist(t *testing.T) {
	var (
		dir = "testdata/001"
	)

	_, err := PathExists(dir)
	if err != nil {
		t.Errorf("%s path exist: %s", dir, err)
		return
	}
}

func TestEnsureDir(t *testing.T) {
	var (
		dir = "testdata/001"
	)

	err := EnsureDir(dir)
	if err != nil {
		t.Errorf("%s path exist: %s", dir, err)
		return
	}

	os.RemoveAll(ParentDirectory(dir))
}
