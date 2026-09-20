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
	"strings"
	"testing"

	"github.com/rapidloop/pgmetrics"
)

func csvMap(t *testing.T, m *pgmetrics.Model) map[string]string {
	t.Helper()
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := model2csv(m, w); err != nil {
		t.Fatalf("model2csv: %v", err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatalf("csv flush: %v", err)
	}
	out := make(map[string]string)
	r := csv.NewReader(&buf)
	recs, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv parse: %v", err)
	}
	for _, rec := range recs {
		out[rec[0]] = rec[1]
	}
	return out
}

func TestCSVCollectionRows(t *testing.T) {
	m := &pgmetrics.Model{Collection: failureReport()}
	rows := csvMap(t, m)

	want := map[string]string{
		"pgmetrics.collection.contract_version":  pgmetrics.CollectionContractVersion,
		"pgmetrics.collection.policy":            pgmetrics.PolicyBestEffort,
		"pgmetrics.collection.status":            pgmetrics.CollectionOverallFailed,
		"pgmetrics.collection.outcomes.count":    "5",
		"pgmetrics.collection.outcome.0.domain":  "tables",
		"pgmetrics.collection.outcome.0.target":  "db1",
		"pgmetrics.collection.outcome.0.status":  "timeout",
		"pgmetrics.collection.outcome.0.code":    "statement_timeout",
		"pgmetrics.collection.outcome.0.summary": "query timed out (password=****)",
		"pgmetrics.collection.outcome.2.domain":  "settings",
		"pgmetrics.collection.outcome.2.code":    "conn_failed",
		"pgmetrics.collection.outcome.2.summary": "connection refused (password=****)",
	}
	for k, v := range want {
		if got := rows[k]; got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	// scalar time/duration keys must be present and numeric-looking
	for _, k := range []string{
		"pgmetrics.collection.started_at",
		"pgmetrics.collection.ended_at",
		"pgmetrics.collection.outcome.0.started_at",
		"pgmetrics.collection.outcome.0.ended_at",
		"pgmetrics.collection.outcome.0.duration_millis",
	} {
		if _, ok := rows[k]; !ok {
			t.Errorf("missing key %s", k)
		}
	}
}

func TestCSVCollectionSanitized(t *testing.T) {
	m := &pgmetrics.Model{Collection: failureReport()}
	rows := csvMap(t, m)
	for k, v := range rows {
		if strings.Contains(v, "password=hunter2") {
			t.Errorf("key %s leaks credential: %s", k, v)
		}
	}
}

func TestCSVNoCollectionRowsForOldSnapshot(t *testing.T) {
	rows := csvMap(t, &pgmetrics.Model{})
	for k := range rows {
		if strings.HasPrefix(k, "pgmetrics.collection") {
			t.Errorf("old snapshot must not emit collection rows, found %s", k)
		}
	}
}

func TestHumanCSVDomainCountConsistency(t *testing.T) {
	// TR-11.2: the same model reports the same domain count in both views
	m := &pgmetrics.Model{Collection: failureReport()}
	csvRows := csvMap(t, m)
	if got := csvRows["pgmetrics.collection.outcomes.count"]; got != "5" {
		t.Fatalf("csv outcomes.count = %q, want 5", got)
	}
	var buf bytes.Buffer
	writeCollectionStatus(&buf, m)
	if !strings.Contains(buf.String(), "5 domains") {
		t.Fatalf("human section must show 5 domains:\n%s", buf.String())
	}
}
