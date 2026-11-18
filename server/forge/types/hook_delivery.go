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

package types

import "time"

// HookDelivery is one attempt of a forge to deliver a webhook.
type HookDelivery struct {
	// ID is the forge's, what it delivers again by.
	ID string
	// GUID is the delivery's: the same in every attempt of it, sent with
	// each.
	GUID        string
	Event       string
	DeliveredAt time.Time
	// StatusCode is what the server answered, 0 if it did not.
	StatusCode int
}

// Reached tells whether the attempt reached the server: it answered 2xx.
func (d *HookDelivery) Reached() bool {
	return d.StatusCode >= 200 && d.StatusCode < 300
}
