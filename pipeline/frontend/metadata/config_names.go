// Copyright 2023 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadata

import (
	"reflect"
	"sync"
)

// configNames are the names of the built-in variables the server sets when
// it reads a config, those Environ returns for some metadata, even when it
// leaves one out for being empty. The agent sets others when a step runs,
// such as CI_WORKSPACE and CI_STEP_NAME.
var configNames = sync.OnceValue(func() map[string]bool {
	names := make(map[string]bool)
	for _, event := range []Event{EventPush, EventPull, EventPullClosed, EventPullMetadata, EventTag, EventRelease, EventDeploy, EventCron, EventManual} {
		var m Metadata
		fill(reflect.ValueOf(&m).Elem())
		m.Step = Step{} // set by the agent
		m.Curr.Event, m.Prev.Event = event, event
		m.Curr.Commit.Ref = "refs/tags/x"
		if event.IsPull() {
			m.Curr.Commit.Ref = "refs/pull/1/head"
		}
		m.Curr.Commit.Refspec, m.Prev.Commit.Refspec = "a:b", "a:b"
		for name := range m.Environ() {
			names[name] = true
		}
	}
	return names
})

// IsConfigName tells whether name is the name of a built-in variable the
// server sets when it reads a config.
func IsConfigName(name string) bool { return configNames()[name] }

// fill sets every field under v to a value that is not empty.
func fill(v reflect.Value) {
	switch v.Kind() {
	case reflect.String:
		v.SetString("x")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.String {
			v.Set(reflect.ValueOf([]string{"x"}).Convert(v.Type()))
		}
	case reflect.Map:
		if v.Type().Key().Kind() == reflect.String && v.Type().Elem().Kind() == reflect.String {
			v.Set(reflect.ValueOf(map[string]string{"x": "x"}).Convert(v.Type()))
		}
	case reflect.Struct:
		for _, field := range v.Fields() {
			if field.CanSet() {
				fill(field)
			}
		}
	}
}
