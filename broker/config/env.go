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
package config

import "os/user"

const (
	envBoltmqBrokerConfigPath = "BOLTMQ_BROKER_CONFIG_PATH"
)

var envNullDefaultValues = map[string]string{
	"BOLTMQ_BROKER_CONFIG_PATH": "etc/broker.toml",
}

func defaultValue(envar string) string {
	if v, ok := envNullDefaultValues[envar]; ok {
		return v
	}

	return ""
}

func home() (string, error) {
	user, err := user.Current()
	if nil == err {
		return user.HomeDir, nil
	}

	return "", err
}
