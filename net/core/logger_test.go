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
package core

import "testing"

func TestConsoleLog(t *testing.T) {

	bootstrap := NewBootstrap()
	bootstrap.Trace("this is a test")
	bootstrap.Debug("this is a test")
	bootstrap.Trace("this is a test")
	bootstrap.Warn("this is a test")
	bootstrap.Error("this is a test")
	bootstrap.Fatal("this is a test")

	bootstrap.Tracef("this is a %s", "test")
	bootstrap.Debugf("this is a %s", "test")
	bootstrap.Tracef("this is a %s", "test")
	bootstrap.Warnf("this is a %s", "test")
	bootstrap.Errorf("this is a %s", "test")
	bootstrap.Fatalf("this is a %s", "test")
}
