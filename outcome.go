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

// CollectionContractVersion is the version of the CollectionOutcome
// contract (the CollectionReport / DomainOutcome types below). It is
// independent from ModelSchemaVersion: the model schema describes the
// metrics payload, while the contract version describes the shape and
// semantics of collection status metadata. Version history:
//
//	1.0 - initial versioned collection outcome contract
const CollectionContractVersion = "1.0"

// Per-domain collection statuses. These are the six terminal states a
// stable collection domain can be in.
const (
	// CollectionStatusSuccess: the domain query ran and a value (which may
	// legitimately be an empty set or zero) is present in the model.
	CollectionStatusSuccess = "success"
	// CollectionStatusOmitted: collection was deliberately not attempted
	// because of a user option (--omit/--no-sizes), an environment
	// condition (not local) or a disabled server-side feature.
	CollectionStatusOmitted = "omitted"
	// CollectionStatusUnsupported: the domain is not applicable to this
	// server version, platform, engine variant (e.g. Aurora) or because a
	// required extension/object is absent.
	CollectionStatusUnsupported = "unsupported"
	// CollectionStatusPermissionDenied: the database user lacks the
	// privilege required to query this domain.
	CollectionStatusPermissionDenied = "permission_denied"
	// CollectionStatusTimeout: the query exceeded the context deadline,
	// statement_timeout or lock_timeout.
	CollectionStatusTimeout = "timeout"
	// CollectionStatusFailed: any other failure (connection, IO, scan
	// mismatch, collector defect, ...).
	CollectionStatusFailed = "failed"
)

// Collection policies.
const (
	// PolicyBestEffort (the default) records domain failures and continues
	// the run; the snapshot is emitted together with the status report.
	PolicyBestEffort = "best_effort"
	// PolicyStrict aborts the run as soon as a required domain fails.
	PolicyStrict = "strict"
)

// Overall run statuses (CollectionReport.Status).
const (
	CollectionOverallSuccess  = "success"  // every attempted domain succeeded
	CollectionOverallDegraded = "degraded" // only optional domains failed
	CollectionOverallFailed   = "failed"   // one or more required domains failed
	CollectionOverallAborted  = "aborted"  // strict policy aborted on a required failure
)

// DomainOutcome records the result of collecting one stable collection
// domain against one target ("" denotes the cluster/global target, a
// database name denotes a database-scoped domain).
type DomainOutcome struct {
	Domain         string `json:"domain"`           // stable domain name, see the collector catalog
	Target         string `json:"target,omitempty"` // database name etc., "" for cluster-level
	Status         string `json:"status"`           // one of the CollectionStatus* constants
	StartedAt      int64  `json:"started_at"`       // unix time, seconds
	EndedAt        int64  `json:"ended_at"`         // unix time, seconds
	DurationMillis int64  `json:"duration_millis"`  // wall time spent in the domain

	// Code is a stable, machine-readable error/skip code (e.g.
	// "insufficient_privilege", "statement_timeout"). Empty on success.
	Code string `json:"code,omitempty"`
	// Summary is a human-readable, credential-free one-line description.
	Summary string `json:"summary,omitempty"`
	// Rows is the number of records the domain produced (1 for scalar
	// domains). success + Rows==0 is how a genuine empty result is told
	// apart from a failed/skipped domain.
	Rows int `json:"rows,omitempty"`

	// Required marks domains whose failure makes the snapshot untrusted;
	// it is policy metadata used for aggregation and is not serialized.
	Required bool `json:"-"`
}

// CollectionReport is the single status source shared by JSON output,
// the human report, CSV output and CLI exit codes.
type CollectionReport struct {
	ContractVersion string          `json:"contract_version"` // always CollectionContractVersion
	Policy          string          `json:"policy"`           // PolicyBestEffort or PolicyStrict
	Status          string          `json:"status"`           // one of the CollectionOverall* constants
	StartedAt       int64           `json:"started_at"`       // run start, unix seconds
	EndedAt         int64           `json:"ended_at"`         // run end, unix seconds
	Outcomes        []DomainOutcome `json:"outcomes"`
}

func outcomeKey(domain, target string) string {
	return domain + "\x00" + target
}

// Record appends an outcome for the (Domain, Target) pair. An outcome is
// terminal: a second record for the same pair is ignored. It reports
// whether the record was stored.
func (r *CollectionReport) Record(o DomainOutcome) bool {
	for i := range r.Outcomes {
		if r.Outcomes[i].Domain == o.Domain && r.Outcomes[i].Target == o.Target {
			return false
		}
	}
	r.Outcomes = append(r.Outcomes, o)
	return true
}

// Outcome returns the recorded outcome for a domain/target pair.
func (r *CollectionReport) Outcome(domain, target string) (DomainOutcome, bool) {
	for _, o := range r.Outcomes {
		if o.Domain == domain && o.Target == target {
			return o, true
		}
	}
	return DomainOutcome{}, false
}

// IsFailureClass reports whether a per-domain status represents an
// execution failure (as opposed to success or an explicit skip).
func IsFailureClass(status string) bool {
	switch status {
	case CollectionStatusPermissionDenied, CollectionStatusTimeout, CollectionStatusFailed:
		return true
	}
	return false
}

// HasRequiredFailure reports whether at least one required domain ended
// in a failure-class status.
func (r *CollectionReport) HasRequiredFailure() bool {
	for _, o := range r.Outcomes {
		if o.Required && IsFailureClass(o.Status) {
			return true
		}
	}
	return false
}

// Aggregate computes the overall status from the recorded outcomes. An
// explicitly aborted run (strict policy) stays aborted. Otherwise any
// required failure yields "failed", any optional failure yields
// "degraded", and the run is "success". omitted/unsupported outcomes
// never lower the trust level.
func (r *CollectionReport) Aggregate() string {
	if r.Status == CollectionOverallAborted {
		return CollectionOverallAborted
	}
	optionalFailure := false
	for _, o := range r.Outcomes {
		if !IsFailureClass(o.Status) {
			continue
		}
		if o.Required {
			return CollectionOverallFailed
		}
		optionalFailure = true
	}
	if optionalFailure {
		return CollectionOverallDegraded
	}
	return CollectionOverallSuccess
}

// Finalize computes the overall status from the outcomes (unless the run
// was aborted) and stamps the end time.
func (r *CollectionReport) Finalize(endedAt int64) {
	r.Status = r.Aggregate()
	r.EndedAt = endedAt
}

// ExitCode maps the report to a CLI exit code: 1 when the run was
// aborted or a required domain failed, 0 otherwise. Snapshot replay
// callers decide separately whether to honor this.
func (r *CollectionReport) ExitCode() int {
	if r == nil {
		return 0
	}
	if r.Status == CollectionOverallAborted || r.HasRequiredFailure() {
		return 1
	}
	return 0
}

// CountByStatus returns the number of outcomes in each status.
func (r *CollectionReport) CountByStatus() map[string]int {
	m := make(map[string]int)
	for _, o := range r.Outcomes {
		m[o.Status]++
	}
	return m
}
