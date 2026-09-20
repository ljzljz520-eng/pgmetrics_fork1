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
	"strings"
	"testing"

	"github.com/rapidloop/pgmetrics"
)

func healthyReport() *pgmetrics.CollectionReport {
	return &pgmetrics.CollectionReport{
		ContractVersion: pgmetrics.CollectionContractVersion,
		Policy:          pgmetrics.PolicyBestEffort,
		Status:          pgmetrics.CollectionOverallSuccess,
		Outcomes: []pgmetrics.DomainOutcome{
			{Domain: "settings", Status: pgmetrics.CollectionStatusSuccess, Rows: 1},
			{Domain: "tables", Target: "db1", Status: pgmetrics.CollectionStatusSuccess, Rows: 3},
		},
	}
}

func TestCollectionStatusHealthySingleLine(t *testing.T) {
	var buf bytes.Buffer
	writeCollectionStatus(&buf, &pgmetrics.Model{Collection: healthyReport()})
	out := buf.String()
	if !strings.Contains(out, "Collection Status: success (2 domains)") {
		t.Fatalf("want healthy one-liner, got:\n%s", out)
	}
	if strings.Contains(out, "Overall:") {
		t.Fatalf("healthy output must not render the multi-line section:\n%s", out)
	}
}

func TestCollectionStatusNilRendersNothing(t *testing.T) {
	var buf bytes.Buffer
	writeCollectionStatus(&buf, &pgmetrics.Model{})
	if buf.Len() != 0 {
		t.Fatalf("Collection == nil must render nothing, got:\n%s", buf.String())
	}
}

func failureReport() *pgmetrics.CollectionReport {
	// summaries must already be sanitized by the collector; the renderer
	// is a trust boundary consumer
	return &pgmetrics.CollectionReport{
		ContractVersion: pgmetrics.CollectionContractVersion,
		Policy:          pgmetrics.PolicyBestEffort,
		Status:          pgmetrics.CollectionOverallFailed,
		Outcomes: []pgmetrics.DomainOutcome{
			{Domain: "tables", Target: "db1", Status: pgmetrics.CollectionStatusTimeout,
				Code: "statement_timeout", Summary: "query timed out (password=****)"},
			{Domain: "bloat", Status: pgmetrics.CollectionStatusPermissionDenied,
				Code: "insufficient_privilege", Summary: "permission denied for view pg_stat_user_tables"},
			{Domain: "settings", Status: pgmetrics.CollectionStatusFailed,
				Code: "conn_failed", Summary: "connection refused (password=****)", Required: true},
			{Domain: "publications", Status: pgmetrics.CollectionStatusUnsupported,
				Code: "version_unsupported", Summary: "requires PostgreSQL 10 or later"},
			{Domain: "citus", Status: pgmetrics.CollectionStatusOmitted,
				Code: "extension_absent", Summary: "citus extension is not installed"},
		},
	}
}

func TestCollectionStatusFailuresSection(t *testing.T) {
	var buf bytes.Buffer
	writeCollectionStatus(&buf, &pgmetrics.Model{Collection: failureReport()})
	out := buf.String()

	wantSubs := []string{
		"Collection Status:",
		"Overall:             failed (policy: best_effort, 5 domains)",
		"settings:            failed (conn_failed) — connection refused (password=****)",
		"tables (target: db1): timeout (statement_timeout) — query timed out (password=****)",
		"bloat:",
		"permission_denied (insufficient_privilege)",
		"1 unsupported, 1 omitted",
	}
	for _, s := range wantSubs {
		if !strings.Contains(out, s) {
			t.Errorf("missing %q in:\n%s", s, out)
		}
	}
	// unsupported/omitted domains are folded, never listed as entries
	if strings.Contains(out, "    publications:") || strings.Contains(out, "    citus:") {
		t.Errorf("skip-class domains must be folded:\n%s", out)
	}
}

func TestCollectionStatusReplayTrustBoundary(t *testing.T) {
	// raw credentials may only appear upstream; once a summary has been
	// sanitized the rendered output carries the masked form only
	r := failureReport()
	var buf bytes.Buffer
	writeCollectionStatus(&buf, &pgmetrics.Model{Collection: r})
	out := buf.String()
	if strings.Contains(out, "password=hunter2") ||
		strings.Contains(out, "password='hunter2'") {
		t.Fatalf("rendered output leaks a credential: %s", out)
	}
	if !strings.Contains(out, "password=****") {
		t.Fatalf("masked summary should survive rendering:\n%s", out)
	}
}
