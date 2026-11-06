// Copyright 2026 LaGrunge
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

package events

// ring keeps the latest records, whose ids follow each other. It grows to
// its size as records come.
type ring struct {
	records []Record
	size    int
	// start is the index of the oldest record, n how many there are.
	start, n int
}

func newRing(size int) *ring {
	return &ring{size: size}
}

func (r *ring) push(rec Record) {
	if r.n < r.size {
		r.records = append(r.records, rec)
		r.n++
		return
	}
	r.records[r.start] = rec
	r.start = (r.start + 1) % r.size
}

// oldest is the id of the oldest record kept, and false if there is none.
func (r *ring) oldest() (int64, bool) {
	if r.n == 0 {
		return 0, false
	}
	return r.records[r.start].Event.ID, true
}

// after calls f, oldest first, with the records whose id is greater than id.
func (r *ring) after(id int64, f func(*Record)) {
	oldest, ok := r.oldest()
	if !ok {
		return
	}
	skip := 0
	if id >= oldest {
		skip = int(min(id-oldest+1, int64(r.n)))
	}
	for i := skip; i < r.n; i++ {
		f(&r.records[(r.start+i)%len(r.records)])
	}
}
