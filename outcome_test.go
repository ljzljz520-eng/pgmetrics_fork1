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

package pgmetrics

import (
	"encoding/json"
	"strings"
	"testing"
)

func mkOutcome(domain, target, status string, required bool) DomainOutcome {
	return DomainOutcome{
		Domain:   domain,
		Target:   target,
		Status:   status,
		Required: required,
	}
}

func TestCollectionStatusConstants(t *testing.T) {
	cases := map[string]string{
		CollectionStatusSuccess:          "success",
		CollectionStatusOmitted:          "omitted",
		CollectionStatusUnsupported:      "unsupported",
		CollectionStatusPermissionDenied: "permission_denied",
		CollectionStatusTimeout:          "timeout",
		CollectionStatusFailed:           "failed",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("status constant wire value: got %q want %q", got, want)
		}
	}
	if CollectionContractVersion != "1.0" {
		t.Errorf("contract version: got %q want 1.0", CollectionContractVersion)
	}
	if ModelSchemaVersion != "1.22" {
		t.Errorf("model schema version: got %q want 1.22", ModelSchemaVersion)
	}
}

func TestRecordTerminalAndUnique(t *testing.T) {
	r := &CollectionReport{}
	if !r.Record(mkOutcome("locks", "db1", CollectionStatusSuccess, false)) {
		t.Fatal("first Record should be stored")
	}
	if r.Record(mkOutcome("locks", "db1", CollectionStatusFailed, false)) {
		t.Fatal("second Record for same (domain,target) must be ignored")
	}
	if !r.Record(mkOutcome("locks", "db2", CollectionStatusSuccess, false)) {
		t.Fatal("different target should be stored")
	}
	if !r.Record(mkOutcome("tables", "db1", CollectionStatusSuccess, false)) {
		t.Fatal("different domain should be stored")
	}
	if len(r.Outcomes) != 3 {
		t.Fatalf("outcomes len: got %d want 3", len(r.Outcomes))
	}
	if o, ok := r.Outcome("locks", "db1"); !ok || o.Status != CollectionStatusSuccess {
		t.Fatalf("terminal outcome changed: %+v ok=%v", o, ok)
	}
	if _, ok := r.Outcome("nope", ""); ok {
		t.Fatal("missing outcome reported as present")
	}
}

func TestAggregate(t *testing.T) {
	cases := []struct {
		name string
		pre  string // pre-set Status
		add  []DomainOutcome
		want string
	}{
		{
			name: "all success",
			add: []DomainOutcome{
				mkOutcome("a", "", CollectionStatusSuccess, true),
				mkOutcome("b", "db", CollectionStatusSuccess, false),
			},
			want: CollectionOverallSuccess,
		},
		{
			name: "skips never lower trust",
			add: []DomainOutcome{
				mkOutcome("a", "", CollectionStatusSuccess, true),
				mkOutcome("b", "", CollectionStatusOmitted, false),
				mkOutcome("c", "db", CollectionStatusUnsupported, false),
			},
			want: CollectionOverallSuccess,
		},
		{
			name: "empty success rows still success",
			add: []DomainOutcome{
				mkOutcome("replication_slots", "", CollectionStatusSuccess, false),
			},
			want: CollectionOverallSuccess,
		},
		{
			name: "optional failure is degraded",
			add: []DomainOutcome{
				mkOutcome("a", "", CollectionStatusSuccess, true),
				mkOutcome("b", "", CollectionStatusTimeout, false),
				mkOutcome("c", "", CollectionStatusPermissionDenied, false),
			},
			want: CollectionOverallDegraded,
		},
		{
			name: "required failure is failed",
			add: []DomainOutcome{
				mkOutcome("a", "", CollectionStatusSuccess, true),
				mkOutcome("b", "", CollectionStatusFailed, false),
				mkOutcome("c", "", CollectionStatusFailed, true),
			},
			want: CollectionOverallFailed,
		},
		{
			name: "required permission denied is failed",
			add: []DomainOutcome{
				mkOutcome("settings", "", CollectionStatusPermissionDenied, true),
			},
			want: CollectionOverallFailed,
		},
		{
			name: "aborted stays aborted",
			pre:  CollectionOverallAborted,
			add: []DomainOutcome{
				mkOutcome("a", "", CollectionStatusSuccess, true),
			},
			want: CollectionOverallAborted,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &CollectionReport{Status: tc.pre}
			for _, o := range tc.add {
				r.Record(o)
			}
			if got := r.Aggregate(); got != tc.want {
				t.Fatalf("Aggregate: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestFinalizeAndExitCode(t *testing.T) {
	// clean run
	r := &CollectionReport{}
	r.Record(mkOutcome("a", "", CollectionStatusSuccess, true))
	r.Finalize(100)
	if r.Status != CollectionOverallSuccess || r.EndedAt != 100 || r.ExitCode() != 0 {
		t.Fatalf("clean run: status=%q ended=%d exit=%d", r.Status, r.EndedAt, r.ExitCode())
	}

	// optional failure: degraded, exit 0
	r = &CollectionReport{}
	r.Record(mkOutcome("a", "", CollectionStatusSuccess, true))
	r.Record(mkOutcome("b", "", CollectionStatusTimeout, false))
	r.Finalize(100)
	if r.Status != CollectionOverallDegraded || r.ExitCode() != 0 {
		t.Fatalf("degraded run: status=%q exit=%d", r.Status, r.ExitCode())
	}

	// required failure: failed, exit 1
	r = &CollectionReport{}
	r.Record(mkOutcome("a", "", CollectionStatusFailed, true))
	r.Finalize(100)
	if r.Status != CollectionOverallFailed || r.ExitCode() != 1 {
		t.Fatalf("failed run: status=%q exit=%d", r.Status, r.ExitCode())
	}

	// nil report exits 0 (old snapshot replay)
	var nilReport *CollectionReport
	if nilReport.ExitCode() != 0 {
		t.Fatal("nil report must map to exit code 0")
	}
}

func TestCollectionJSONRoundTrip(t *testing.T) {
	r := &CollectionReport{
		ContractVersion: CollectionContractVersion,
		Policy:          PolicyBestEffort,
		Status:          CollectionOverallDegraded,
		StartedAt:       90,
		EndedAt:         100,
		Outcomes: []DomainOutcome{
			{
				Domain:         "replication_slots",
				Target:         "db1",
				Status:         CollectionStatusPermissionDenied,
				StartedAt:      91,
				EndedAt:        92,
				DurationMillis: 1000,
				Code:           "insufficient_privilege",
				Summary:        "permission denied for view pg_replication_slots",
				Required:       false,
			},
		},
	}
	m := Model{
		Metadata:   Metadata{Version: ModelSchemaVersion, At: 90},
		Collection: r,
	}
	out, err := json.Marshal(&m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(out)
	for _, want := range []string{
		`"collection":{`,
		`"contract_version":"1.0"`,
		`"policy":"best_effort"`,
		`"status":"degraded"`,
		`"domain":"replication_slots"`,
		`"target":"db1"`,
		`"code":"insufficient_privilege"`,
		`"duration_millis":1000`,
		`"version":"1.22"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("serialized model missing %s in:\n%s", want, s)
		}
	}
	if strings.Contains(s, `"Required"`) {
		t.Error("Required field must not be serialized")
	}

	// round trip preserves data
	var back Model
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Collection == nil || len(back.Collection.Outcomes) != 1 {
		t.Fatalf("collection lost in round trip: %+v", back.Collection)
	}
	o := back.Collection.Outcomes[0]
	if o.Domain != "replication_slots" || o.Code != "insufficient_privilege" || o.DurationMillis != 1000 {
		t.Fatalf("outcome mismatch: %+v", o)
	}
	if back.Collection.Status != CollectionOverallDegraded {
		t.Fatalf("status mismatch: %q", back.Collection.Status)
	}
}

func TestOldSnapshotWithoutCollection(t *testing.T) {
	// current model with nil collection omits the key
	m := Model{Metadata: Metadata{Version: "1.21"}}
	out, err := json.Marshal(&m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(out), "collection") {
		t.Fatalf("nil Collection must be omitted, got: %s", out)
	}

	// a 1.21-era JSON document (no collection key) parses cleanly
	old := `{"meta":{"version":"1.21","at":42},"settings":{}}`
	var m2 Model
	if err := json.Unmarshal([]byte(old), &m2); err != nil {
		t.Fatalf("old snapshot should parse: %v", err)
	}
	if m2.Collection != nil {
		t.Fatal("old snapshot must decode with nil Collection")
	}
	if m2.Metadata.Version != "1.21" || m2.Metadata.At != 42 {
		t.Fatalf("old fields mismatch: %+v", m2.Metadata)
	}
}
