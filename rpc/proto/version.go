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

package proto

// Version is the version of the woodpecker.proto file. Agent and server
// must report the same one, so it is increased by 1 only for a change the
// wire cannot accept from the other side (a removed or retyped field, a
// changed meaning). A new optional field whose zero value keeps the old
// behavior needs no bump: an older peer leaves it unset.
const Version int32 = 16
