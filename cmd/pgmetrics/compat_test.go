/*
 * Copyright 2026 RapidLoop, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rapidloop/pgmetrics"
)

// oldFixture121 is a minimal snapshot in the pre-contract shape
// (ModelSchemaVersion 1.21): no "collection" key anywhere.
const oldFixture121 = `{
  "meta": {
    "version": "1.21",
    "at": 1700000000,
    "mode": "postgres",
    "user": "postgres",
    "user_agent": "pgmetrics/1.17.0"
  },
  "start_time": 1699999000,
  "databases": [],
  "tablespaces": []
}`

func TestOldSnapshot121Compatibility(t *testing.T) {
	var m pgmetrics.Model
	if err := json.Unmarshal([]byte(oldFixture121), &m); err != nil {
		t.Fatalf("decoding 1.21 fixture: %v", err)
	}
	if m.Metadata.Version != "1.21" {
		t.Fatalf("meta.version = %q", m.Metadata.Version)
	}
	if m.Collection != nil {
		t.Fatalf("old fixture must decode with Collection == nil")
	}

	// human render: must not panic and must not show the status section
	var human bytes.Buffer
	writeHumanTo(&human, options{tooLongSec: 60}, &m)
	if strings.Contains(human.String(), "Collection Status") {
		t.Fatalf("old snapshot must not render a collection status section")
	}

	// csv render: must not emit collection rows
	var csbuf bytes.Buffer
	cw := csv.NewWriter(&csbuf)
	if err := model2csv(&m, cw); err != nil {
		t.Fatalf("model2csv on old fixture: %v", err)
	}
	cw.Flush()
	if strings.Contains(csbuf.String(), "pgmetrics.collection") {
		t.Fatalf("old snapshot must not emit csv collection rows")
	}

	// re-encode: an old snapshot re-saved keeps the additive field absent
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	if err := enc.Encode(&m); err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if strings.Contains(out.String(), "collection") {
		t.Fatalf("re-encoded old snapshot must stay collection-free:\n%s", out.String())
	}
}
