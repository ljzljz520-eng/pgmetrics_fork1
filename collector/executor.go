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
	"log"
	"time"

	"github.com/rapidloop/pgmetrics"
)

// Collection modes. These match the values stored in Metadata.Mode.
const (
	modePostgres  = "postgres"
	modePgBouncer = "pgbouncer"
	modePgpool    = "pgpool"
)

// Stable collection domain names. These strings are part of the
// CollectionOutcome contract: never renumber or rename.
const (
	// connection / bootstrapping (all modes)
	domainConnection   = "connection"
	domainDatabaseList = "database_list"
	domainCurrentUser  = "current_user"
	domainSettings     = "settings"
	domainSystemInfo   = "system_info"
	domainLocalProbe   = "local_probe"

	// postgres cluster-level domains
	domainControlSystem      = "control_system"
	domainControlCheckpoint  = "control_checkpoint"
	domainLastXact           = "last_xact"
	domainBGWriter           = "bg_writer"
	domainReplicationOut     = "replication_outgoing"
	domainReplicationIn      = "replication_incoming"
	domainRecovery           = "recovery"
	domainWALArchiver        = "wal_archiver"
	domainActivity           = "activity"
	domainBackendTypeCounts  = "backend_type_counts"
	domainDatabases          = "databases"
	domainTablespaces        = "tablespaces"
	domainRoles              = "roles"
	domainReplicationSlots   = "replication_slots"
	domainWALCounts          = "wal_counts"
	domainNotification       = "notification"
	domainLocks              = "locks"
	domainWAL                = "wal"
	domainVacuumProgress     = "vacuum_progress"
	domainCheckpointer       = "checkpointer"
	domainStatIO             = "stat_io"
	domainStatLocks          = "stat_locks"
	domainStatRecovery       = "stat_recovery"
	domainProgressCluster    = "progress_cluster"
	domainProgressCreateIdx  = "progress_create_index"
	domainProgressAnalyze    = "progress_analyze"
	domainProgressBasebackup = "progress_basebackup"
	domainProgressCopy       = "progress_copy"
	domainProgressRepack     = "progress_repack"
	domainSystem             = "system"
	domainLogs               = "logs"
	domainRDS                = "rds"
	domainAzure              = "azure"

	// postgres database-scoped domains (target = database name)
	domainCurrentDatabase = "current_database"
	domainTables          = "tables"
	domainIndexDefs       = "index_defs"
	domainIndexes         = "indexes"
	domainSequences       = "sequences"
	domainFunctions       = "functions"
	domainExtensions      = "extensions"
	domainTriggers        = "triggers"
	domainStatements      = "statements"
	domainBloat           = "bloat"
	domainPublications    = "publications"
	domainSubscriptions   = "subscriptions"
	domainCitus           = "citus"

	// pgbouncer domains
	domainPBPools     = "pb_pools"
	domainPBServers   = "pb_servers"
	domainPBClients   = "pb_clients"
	domainPBStats     = "pb_stats"
	domainPBDatabases = "pb_databases"

	// pgpool domains
	domainPPVersion      = "pp_version"
	domainPPNodes        = "pp_nodes"
	domainPPHealthStats  = "pp_health_stats"
	domainPPBackendStats = "pp_backend_stats"
	domainPPCache        = "pp_cache"
)

// domainSpec describes one stable collection domain.
type domainSpec struct {
	name     string
	required bool // failure marks the snapshot untrusted
	modes    []string
}

// domainCatalog is the single registry of collection domains. A domain
// not listed here is not part of the outcome contract.
var domainCatalog = map[string]domainSpec{
	// bootstrapping
	domainConnection:   {domainConnection, true, []string{modePostgres, modePgBouncer, modePgpool}},
	domainDatabaseList: {domainDatabaseList, true, []string{modePostgres}},
	domainCurrentUser:  {domainCurrentUser, true, []string{modePostgres, modePgpool}},
	domainSettings:     {domainSettings, true, []string{modePostgres}},
	domainSystemInfo:   {domainSystemInfo, true, []string{modePostgres}},
	domainLocalProbe:   {domainLocalProbe, false, []string{modePostgres}},

	// cluster-level
	domainControlSystem:      {domainControlSystem, false, []string{modePostgres}},
	domainControlCheckpoint:  {domainControlCheckpoint, false, []string{modePostgres}},
	domainLastXact:           {domainLastXact, false, []string{modePostgres}},
	domainBGWriter:           {domainBGWriter, false, []string{modePostgres}},
	domainReplicationOut:     {domainReplicationOut, false, []string{modePostgres}},
	domainReplicationIn:      {domainReplicationIn, false, []string{modePostgres}},
	domainRecovery:           {domainRecovery, false, []string{modePostgres}},
	domainWALArchiver:        {domainWALArchiver, false, []string{modePostgres}},
	domainActivity:           {domainActivity, true, []string{modePostgres}},
	domainBackendTypeCounts:  {domainBackendTypeCounts, false, []string{modePostgres}},
	domainDatabases:          {domainDatabases, true, []string{modePostgres}},
	domainTablespaces:        {domainTablespaces, false, []string{modePostgres}},
	domainRoles:              {domainRoles, false, []string{modePostgres}},
	domainReplicationSlots:   {domainReplicationSlots, false, []string{modePostgres}},
	domainWALCounts:          {domainWALCounts, false, []string{modePostgres}},
	domainNotification:       {domainNotification, false, []string{modePostgres}},
	domainLocks:              {domainLocks, false, []string{modePostgres}},
	domainWAL:                {domainWAL, false, []string{modePostgres}},
	domainVacuumProgress:     {domainVacuumProgress, false, []string{modePostgres}},
	domainCheckpointer:       {domainCheckpointer, false, []string{modePostgres}},
	domainStatIO:             {domainStatIO, false, []string{modePostgres}},
	domainStatLocks:          {domainStatLocks, false, []string{modePostgres}},
	domainStatRecovery:       {domainStatRecovery, false, []string{modePostgres}},
	domainProgressCluster:    {domainProgressCluster, false, []string{modePostgres}},
	domainProgressCreateIdx:  {domainProgressCreateIdx, false, []string{modePostgres}},
	domainProgressAnalyze:    {domainProgressAnalyze, false, []string{modePostgres}},
	domainProgressBasebackup: {domainProgressBasebackup, false, []string{modePostgres}},
	domainProgressCopy:       {domainProgressCopy, false, []string{modePostgres}},
	domainProgressRepack:     {domainProgressRepack, false, []string{modePostgres}},
	domainSystem:             {domainSystem, false, []string{modePostgres}},
	domainLogs:               {domainLogs, false, []string{modePostgres}},
	domainRDS:                {domainRDS, false, []string{modePostgres}},
	domainAzure:              {domainAzure, false, []string{modePostgres}},

	// database-scoped
	domainCurrentDatabase: {domainCurrentDatabase, true, []string{modePostgres}},
	domainTables:          {domainTables, false, []string{modePostgres}},
	domainIndexDefs:       {domainIndexDefs, false, []string{modePostgres}},
	domainIndexes:         {domainIndexes, false, []string{modePostgres}},
	domainSequences:       {domainSequences, false, []string{modePostgres}},
	domainFunctions:       {domainFunctions, false, []string{modePostgres}},
	domainExtensions:      {domainExtensions, false, []string{modePostgres}},
	domainTriggers:        {domainTriggers, false, []string{modePostgres}},
	domainStatements:      {domainStatements, false, []string{modePostgres}},
	domainBloat:           {domainBloat, false, []string{modePostgres}},
	domainPublications:    {domainPublications, false, []string{modePostgres}},
	domainSubscriptions:   {domainSubscriptions, false, []string{modePostgres}},
	domainCitus:           {domainCitus, false, []string{modePostgres}},

	// pgbouncer
	domainPBPools:     {domainPBPools, true, []string{modePgBouncer}},
	domainPBServers:   {domainPBServers, true, []string{modePgBouncer}},
	domainPBClients:   {domainPBClients, false, []string{modePgBouncer}},
	domainPBStats:     {domainPBStats, false, []string{modePgBouncer}},
	domainPBDatabases: {domainPBDatabases, false, []string{modePgBouncer}},

	// pgpool
	domainPPVersion:      {domainPPVersion, true, []string{modePgpool}},
	domainPPNodes:        {domainPPNodes, true, []string{modePgpool}},
	domainPPHealthStats:  {domainPPHealthStats, false, []string{modePgpool}},
	domainPPBackendStats: {domainPPBackendStats, false, []string{modePgpool}},
	domainPPCache:        {domainPPCache, false, []string{modePgpool}},
}

// errAborted is returned up the scheduling stack when a required domain
// failed under the strict policy and the run must stop immediately.
var errAborted = errors.New("collection aborted due to a required domain failure")

// initCollectionReport creates the run status report.
func (c *collector) initCollectionReport(o CollectConfig) {
	policy := pgmetrics.PolicyBestEffort
	if o.Strict {
		policy = pgmetrics.PolicyStrict
	}
	c.report = &pgmetrics.CollectionReport{
		ContractVersion: pgmetrics.CollectionContractVersion,
		Policy:          policy,
		Status:          pgmetrics.CollectionOverallSuccess,
		StartedAt:       time.Now().Unix(),
	}
	c.strict = o.Strict
}

func (c *collector) isStrict() bool { return c.strict }

// runDomain executes one collection domain through the unified executor.
// It records the DomainOutcome (timing, classified status/code, sanitized
// summary, rows) and emits a sanitized stderr warning on failures.
//
// Under the best-effort policy it always returns nil (failures are only
// recorded). Under the strict policy a failure of a required domain sets
// the run status to aborted and returns errAborted; optional failures are
// recorded and the run continues.
//
// A domain function may itself record a skip outcome (skip/skipVersion/
// skipOption) and return nil; in that case the terminal skip outcome wins
// and no success outcome is overwritten.
func (c *collector) runDomain(domain, target string, fn func() (rows int, err error)) error {
	required := false
	if spec, ok := domainCatalog[domain]; ok {
		required = spec.required
	}

	start := time.Now()
	rows, err := fn()
	end := time.Now()

	if err == nil {
		// a skip recorded inside the domain function is terminal
		if _, exists := c.report.Outcome(domain, target); exists {
			return nil
		}
		outcome := pgmetrics.DomainOutcome{
			Domain:         domain,
			Target:         target,
			Status:         pgmetrics.CollectionStatusSuccess,
			StartedAt:      start.Unix(),
			EndedAt:        end.Unix(),
			DurationMillis: end.Sub(start).Milliseconds(),
			Rows:           rows,
			Required:       required,
		}
		if c.notes != nil {
			key := domain + "\x00" + target
			if note, ok := c.notes[key]; ok {
				outcome.Summary = note
				delete(c.notes, key)
			}
		}
		c.report.Record(outcome)
		return nil
	}

	return c.recordFailure(domain, target, required, start, end, err)
}

// note attaches a public, safe summary to the domain's success outcome
// (used by tolerant domains, e.g. size collection retried without sizes
// after a lock timeout).
func (c *collector) note(domain, target, summary string) {
	if c.notes == nil {
		c.notes = make(map[string]string)
	}
	c.notes[domain+"\x00"+target] = summary
}

// noteOnce is note but keeps the first summary for a domain, so that
// repeated benign warnings during a single collection do not spam reports.
func (c *collector) noteOnce(domain, target, summary string) {
	if c.notes == nil {
		c.notes = make(map[string]string)
	}
	key := domain + "\x00" + target
	if _, ok := c.notes[key]; !ok {
		c.notes[key] = summary
	}
}

// runOption runs a domain unless it is disabled by a user option, in
// which case it records an omitted(disabled_by_option) outcome.
func (c *collector) runOption(domain, target, reason string, disabled bool, fn func() (int, error)) error {
	if disabled {
		c.skipOption(domain, target, reason)
		return nil
	}
	return c.runDomain(domain, target, fn)
}

// runDBOption is runOption against the current database target.
func (c *collector) runDBOption(domain, reason string, disabled bool, fn func() (int, error)) error {
	return c.runOption(domain, c.curTarget, reason, disabled, fn)
}

// recordFailure stores a classified failure outcome and applies the
// policy: it returns errAborted only when a required domain fails under
// the strict policy.
func (c *collector) recordFailure(domain, target string, required bool, start, end time.Time, err error) error {
	status, code := classify(err)
	summary := sanitize(err.Error())
	c.report.Record(pgmetrics.DomainOutcome{
		Domain:         domain,
		Target:         target,
		Status:         status,
		StartedAt:      start.Unix(),
		EndedAt:        end.Unix(),
		DurationMillis: end.Sub(start).Milliseconds(),
		Code:           code,
		Summary:        summary,
		Required:       required,
	})
	// a note staged for this domain's success is now stale
	if c.notes != nil {
		delete(c.notes, domain+"\x00"+target)
	}

	where := domain
	if target != "" {
		where = fmt.Sprintf("%s (target: %s)", domain, target)
	}
	log.Printf("warning: %s collection failed: %s", where, summary)

	// an error classified into a non-failure state (e.g. unsupported or
	// omitted) is informational, not a required-domain failure: the
	// strict policy aborts only on genuine failure-class results.
	if required && c.strict && pgmetrics.IsFailureClass(status) {
		c.aborted = true
		c.report.Status = pgmetrics.CollectionOverallAborted
		log.Printf("required domain %q failed in strict mode, aborting the run", where)
		return errAborted
	}
	return nil
}

// runFatal records a failure for a non-catalog internal step (used only
// for states that should be unreachable, e.g. an unknown run mode).
func (c *collector) runFatal(domain, target string, required bool, err error) error {
	now := time.Now()
	return c.recordFailure(domain, target, required, now, now, err)
}

// runVersioned runs a domain when the server version is at least minVersion,
// otherwise records an unsupported(version_unsupported) outcome. requires is
// a human label such as "9.6" (used in the skip summary).
func (c *collector) runVersioned(domain, requires string, minVersion int, fn func() (int, error)) error {
	if c.version < minVersion {
		c.skipVersion(domain, "", "requires PostgreSQL "+requires+" or later")
		return nil
	}
	return c.runDomain(domain, "", fn)
}

// runDBVersioned is runVersioned against the current database target.
func (c *collector) runDBVersioned(domain, requires string, minVersion int, fn func() (int, error)) error {
	if c.version < minVersion {
		c.skipVersion(domain, c.curTarget, "requires PostgreSQL "+requires+" or later")
		return nil
	}
	return c.runDomain(domain, c.curTarget, fn)
}

// skip records a non-success terminal state for a domain that was not
// executed (version/platform gate, user option, environment condition).
// The code determines the resulting status via codeStatus.
func (c *collector) skip(domain, target, code, summary string) {
	status := pgmetrics.CollectionStatusFailed
	if s, ok := codeStatus[code]; ok {
		status = s
	}
	now := time.Now().Unix()
	c.report.Record(pgmetrics.DomainOutcome{
		Domain:    domain,
		Target:    target,
		Status:    status,
		StartedAt: now,
		EndedAt:   now,
		Code:      code,
		Summary:   summary,
		Required:  domainCatalog[domain].required,
	})
}

// skipOption records an omission caused by a user-supplied option
// (--omit, --no-sizes, ...).
func (c *collector) skipOption(domain, target, summary string) {
	c.skip(domain, target, codeDisabledByOption, summary)
}

// skipVersion records an unsupported state caused by a server version gate.
func (c *collector) skipVersion(domain, target, summary string) {
	c.skip(domain, target, codeVersionUnsupported, summary)
}

// skipPlatform records an unsupported state caused by the OS/platform.
func (c *collector) skipPlatform(domain, target, summary string) {
	c.skip(domain, target, codePlatformUnsupported, summary)
}

// domainNamesForMode returns the sorted-by-registration-independent set
// of domain names applicable to a mode. Used for the catalog contract test.
func domainNamesForMode(mode string) map[string]bool {
	m := make(map[string]bool)
	for _, spec := range domainCatalog {
		for _, sm := range spec.modes {
			if sm == mode {
				m[spec.name] = true
			}
		}
	}
	return m
}
