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

package collector

import (
	"io"
	"log"
	"os"
	"testing"

	"github.com/rapidloop/pgmetrics"
)

// deadUnixConfig points at a nonexistent unix socket directory, so the
// driver fails immediately and deterministically without touching the
// network. Connect failures classify as failure-class outcomes.
func deadUnixConfig(strict bool) CollectConfig {
	return CollectConfig{
		Host:                "/nonexistent-pgmetrics-outcome-socket",
		Port:                5432,
		User:                "pgmetrics",
		TimeoutSec:          3,
		LockTimeoutMillisec: 50,
		Strict:              strict,
	}
}

// discardLogs silences the expected failure warnings for one test.
func discardLogs(t *testing.T) {
	t.Helper()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
}

// Best-effort policy: a required connection failure does not abort the
// run; the partial model is returned together with a "failed" report and
// exit code 1, and post-failure optional domains still get their skip
// outcomes (logs: not local).
func TestOrchestrationBestEffortRequiredFailure(t *testing.T) {
	discardLogs(t)
	model, report := CollectWithReport(deadUnixConfig(false), nil)
	if model == nil || report == nil {
		t.Fatal("model and report must be returned")
	}
	if model.Collection != report {
		t.Fatal("report must be attached to the model")
	}
	od, ok := report.Outcome(domainConnection, "")
	if !ok {
		t.Fatal("connection outcome must be recorded")
	}
	if !od.Required || !pgmetrics.IsFailureClass(od.Status) {
		t.Fatalf("connection must be a required failure-class outcome, got %+v", od)
	}
	if !report.HasRequiredFailure() || report.ExitCode() != 1 {
		t.Fatalf("required failure must yield exit code 1, status %q", report.Status)
	}
	if report.Status != pgmetrics.CollectionOverallFailed {
		t.Fatalf("best-effort run status = %q, want failed", report.Status)
	}
	// the run was not aborted, so gated skip domains were still visited
	if _, ok := report.Outcome(domainLogs, ""); !ok {
		t.Fatal("logs domain must still be recorded (not_local skip) after a non-aborted run")
	}
}

// Strict policy: a required connection failure aborts the run; later
// domains (logs/rds/azure) are not visited and the overall status is
// "aborted".
func TestOrchestrationStrictRequiredFailureAborts(t *testing.T) {
	discardLogs(t)
	_, report := CollectWithReport(deadUnixConfig(true), nil)
	if report == nil {
		t.Fatal("report must be returned")
	}
	if report.Status != pgmetrics.CollectionOverallAborted {
		t.Fatalf("strict run status = %q, want aborted", report.Status)
	}
	if report.ExitCode() != 1 {
		t.Fatal("aborted run must yield exit code 1")
	}
	if _, ok := report.Outcome(domainConnection, ""); !ok {
		t.Fatal("connection outcome must be recorded")
	}
	if _, ok := report.Outcome(domainLogs, ""); ok {
		t.Fatal("logs domain must not be visited after an abort")
	}
}

// With --all-dbs, a database_list failure stops the run before any
// per-target collection starts (no unnamed fallback target).
func TestOrchestrationDatabaseListFailureStopsRun(t *testing.T) {
	for _, strict := range []bool{false, true} {
		discardLogs(t)
		cfg := deadUnixConfig(strict)
		cfg.AllDBs = true
		_, report := CollectWithReport(cfg, nil)

		od, ok := report.Outcome(domainDatabaseList, "")
		if !ok || !od.Required || !pgmetrics.IsFailureClass(od.Status) {
			t.Fatalf("strict=%v: database_list must be a required failure, got %+v ok=%v",
				strict, od, ok)
		}
		if _, ok := report.Outcome(domainConnection, ""); ok {
			t.Fatalf("strict=%v: no target connection may be attempted after list failure", strict)
		}
		if _, ok := report.Outcome(domainSettings, ""); ok {
			t.Fatalf("strict=%v: no cluster domain may run after list failure", strict)
		}
		want := pgmetrics.CollectionOverallFailed
		if strict {
			want = pgmetrics.CollectionOverallAborted
		}
		if report.Status != want {
			t.Fatalf("strict=%v: overall status = %q, want %q", strict, report.Status, want)
		}
	}
}
