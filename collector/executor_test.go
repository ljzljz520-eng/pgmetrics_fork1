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
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rapidloop/pgmetrics"
)

// newTestCollector returns a collector with an initialized collection
// report. Stderr noise from the executor is discarded.
func newTestCollector(t *testing.T, strict bool) *collector {
	t.Helper()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	c := &collector{}
	c.initCollectionReport(CollectConfig{Strict: strict})
	return c
}

func TestRunDomainBestEffortRecordsFailuresAndContinues(t *testing.T) {
	c := newTestCollector(t, false)

	calls := 0
	// required domain fails
	if err := c.runDomain(domainSettings, "", func() (int, error) {
		calls++
		return 0, errors.New("boom")
	}); err != nil {
		t.Fatalf("best-effort required failure must return nil, got %v", err)
	}
	// optional domain also fails, run continues
	if err := c.runDomain(domainLocks, "", func() (int, error) {
		calls++
		return 0, errors.New("kaboom")
	}); err != nil {
		t.Fatalf("best-effort optional failure must return nil, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected both domain functions to run, got %d calls", calls)
	}

	if c.aborted {
		t.Fatal("best-effort run must not be marked aborted")
	}

	o1, ok := c.report.Outcome(domainSettings, "")
	if !ok || !pgmetrics.IsFailureClass(o1.Status) || !o1.Required {
		t.Fatalf("required failure outcome wrong: %+v ok=%v", o1, ok)
	}
	o2, ok := c.report.Outcome(domainLocks, "")
	if !ok || !pgmetrics.IsFailureClass(o2.Status) || o2.Required {
		t.Fatalf("optional failure outcome wrong: %+v ok=%v", o2, ok)
	}

	if got := c.report.Aggregate(); got != pgmetrics.CollectionOverallFailed {
		t.Fatalf("aggregate = %q, want failed", got)
	}
	if c.report.ExitCode() != 1 {
		t.Fatal("required failure must map to exit code 1")
	}
}

func TestRunDomainStrictRequiredAborts(t *testing.T) {
	c := newTestCollector(t, true)

	err := c.runDomain(domainSettings, "", func() (int, error) {
		return 0, errors.New("boom")
	})
	if !errors.Is(err, errAborted) {
		t.Fatalf("want errAborted, got %v", err)
	}
	if !c.aborted {
		t.Fatal("collector must be marked aborted")
	}
	if c.report.Status != pgmetrics.CollectionOverallAborted {
		t.Fatalf("report status = %q, want aborted", c.report.Status)
	}
	// Aggregate preserves the aborted state
	if got := c.report.Aggregate(); got != pgmetrics.CollectionOverallAborted {
		t.Fatalf("aggregate = %q, want aborted", got)
	}
	if c.report.ExitCode() != 1 {
		t.Fatal("aborted run must map to exit code 1")
	}
	// only the one outcome exists
	if len(c.report.Outcomes) != 1 {
		t.Fatalf("want 1 outcome, got %d", len(c.report.Outcomes))
	}
}

func TestRunDomainStrictOptionalFailureContinues(t *testing.T) {
	c := newTestCollector(t, true)

	called := false
	if err := c.runDomain(domainLocks, "", func() (int, error) {
		called = true
		return 0, errors.New("boom")
	}); err != nil {
		t.Fatalf("strict optional failure must return nil, got %v", err)
	}
	if !called {
		t.Fatal("optional domain function must have been called")
	}
	if c.aborted {
		t.Fatal("optional failure must not abort a strict run")
	}

	// a later required domain can still run and succeed
	if err := c.runDomain(domainSettings, "", func() (int, error) {
		return 7, nil
	}); err != nil {
		t.Fatalf("later success must return nil, got %v", err)
	}
	if got := c.report.Aggregate(); got != pgmetrics.CollectionOverallDegraded {
		t.Fatalf("aggregate = %q, want degraded", got)
	}
	if c.report.ExitCode() != 0 {
		t.Fatal("degraded (optional-only failures) must map to exit code 0")
	}
}

func TestRunDomainSuccessRecordsRowsAndTiming(t *testing.T) {
	c := newTestCollector(t, false)

	if err := c.runDomain(domainDatabases, "appdb", func() (int, error) {
		return 42, nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	o, ok := c.report.Outcome(domainDatabases, "appdb")
	if !ok {
		t.Fatal("outcome not recorded")
	}
	if o.Status != pgmetrics.CollectionStatusSuccess || o.Rows != 42 {
		t.Fatalf("outcome wrong: %+v", o)
	}
	if o.StartedAt == 0 || o.EndedAt == 0 || o.EndedAt < o.StartedAt {
		t.Fatalf("bad timestamps: %+v", o)
	}
	if o.Code != "" || o.Summary != "" {
		t.Fatalf("success outcome must carry no code/summary: %+v", o)
	}
	if c.report.Aggregate() != pgmetrics.CollectionOverallSuccess {
		t.Fatal("clean run must aggregate to success")
	}
}

func TestRunDomainSkipInsideFunctionIsTerminal(t *testing.T) {
	c := newTestCollector(t, false)

	if err := c.runDomain(domainLocks, "", func() (int, error) {
		c.skipOption(domainLocks, "", "disabled via --omit")
		return 0, nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	o, ok := c.report.Outcome(domainLocks, "")
	if !ok {
		t.Fatal("outcome not recorded")
	}
	if o.Status != pgmetrics.CollectionStatusOmitted {
		t.Fatalf("status = %q, want omitted", o.Status)
	}
	if o.Code != codeDisabledByOption {
		t.Fatalf("code = %q, want %q", o.Code, codeDisabledByOption)
	}
	// skips never lower trust
	if c.report.Aggregate() != pgmetrics.CollectionOverallSuccess {
		t.Fatal("omitted-only run must aggregate to success")
	}
}

func TestRunDomainFailureSummaryIsSanitized(t *testing.T) {
	c := newTestCollector(t, false)

	if err := c.runDomain(domainSettings, "", func() (int, error) {
		return 0, errors.New("FATAL: password='supersecret123' is invalid")
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	o, _ := c.report.Outcome(domainSettings, "")
	if strings.Contains(o.Summary, "supersecret123") {
		t.Fatalf("summary leaks credential: %q", o.Summary)
	}
	if !strings.Contains(o.Summary, "****") {
		t.Fatalf("summary should mask the secret value: %q", o.Summary)
	}
}

func TestExitCodeNilReport(t *testing.T) {
	var r *pgmetrics.CollectionReport
	if r.ExitCode() != 0 {
		t.Fatal("nil report must map to exit code 0")
	}
}

func TestDomainCatalogCompleteness(t *testing.T) {
	knownModes := map[string]bool{
		modePostgres: true, modePgBouncer: true, modePgpool: true,
	}
	for key, spec := range domainCatalog {
		if spec.name != key {
			t.Errorf("catalog key %q does not match spec name %q", key, spec.name)
		}
		if spec.name == "" {
			t.Errorf("catalog key %q has empty name", key)
		}
		if len(spec.modes) == 0 {
			t.Errorf("domain %q has no modes", key)
		}
		for _, m := range spec.modes {
			if !knownModes[m] {
				t.Errorf("domain %q references unknown mode %q", key, m)
			}
		}
	}

	// required core set per FR-12
	pg := domainNamesForMode(modePostgres)
	for _, d := range []string{
		domainConnection, domainCurrentUser, domainSettings, domainSystemInfo,
		domainActivity, domainDatabases, domainCurrentDatabase, domainDatabaseList,
	} {
		if !pg[d] {
			t.Errorf("required core domain %q missing for postgres mode", d)
		}
		if !domainCatalog[d].required {
			t.Errorf("core domain %q must be required", d)
		}
	}
	pb := domainNamesForMode(modePgBouncer)
	for _, d := range []string{domainConnection, domainPBPools, domainPBServers} {
		if !pb[d] || !domainCatalog[d].required {
			t.Errorf("required pgbouncer domain %q wrong", d)
		}
	}
	pp := domainNamesForMode(modePgpool)
	for _, d := range []string{domainConnection, domainCurrentUser, domainPPVersion, domainPPNodes} {
		if !pp[d] || !domainCatalog[d].required {
			t.Errorf("required pgpool domain %q wrong", d)
		}
	}
}

func TestIsLockTimeoutErrorWrapped(t *testing.T) {
	lockErr := &pgconn.PgError{Code: "55P03", Message: "canceling statement due to lock timeout"}
	wrapped := fmt.Errorf("pg_stat_user_tables query failed: %w", lockErr)
	if !isLockTimeoutError(lockErr) {
		t.Fatal("bare 55P03 must be detected")
	}
	if !isLockTimeoutError(wrapped) {
		t.Fatal("wrapped 55P03 must be detected through %w (size-drop retry depends on it)")
	}
	stmtErr := &pgconn.PgError{Code: "57014"}
	if isLockTimeoutError(fmt.Errorf("%w", stmtErr)) {
		t.Fatal("57014 is not a lock timeout")
	}
	if isLockTimeoutError(io.EOF) || isLockTimeoutError(nil) {
		t.Fatal("non-pg errors must be negative")
	}
}

// A required-domain error classified as a non-failure state must not
// abort a strict run; only genuine failure-class statuses do.
func TestStrictAbortsOnlyFailureClass(t *testing.T) {
	c := newTestCollector(t, true)
	err := c.runDomain(domainSettings, "", func() (int, error) {
		return 0, newDomainError(codeUndefinedObject, errors.New("relation does not exist"))
	})
	if err != nil {
		t.Fatalf("unsupported-class error must not abort strict run, got %v", err)
	}
	if c.aborted {
		t.Fatal("run must not be marked aborted after a non-failure classification")
	}
	od, ok := c.report.Outcome(domainSettings, "")
	if !ok || od.Status != pgmetrics.CollectionStatusUnsupported {
		t.Fatalf("want unsupported outcome, got %+v ok=%v", od, ok)
	}

	err = c.runDomain(domainDatabases, "", func() (int, error) {
		return 0, newDomainError(codeQueryError, errors.New("boom"))
	})
	if !errors.Is(err, errAborted) {
		t.Fatalf("required-domain failure under strict must return errAborted, got %v", err)
	}
	if !c.aborted {
		t.Fatal("run must be marked aborted after a required failure")
	}
}
