// Copyright 2026 Woodpecker Authors
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

package jsonnet

import "syscall"

// limitMemory caps the data (heap) memory of this process; unlike the
// address space, it does not count what the Go runtime only reserves.
func limitMemory(bytes uint64) error {
	return syscall.Setrlimit(syscall.RLIMIT_DATA, &syscall.Rlimit{Cur: bytes, Max: bytes})
}
