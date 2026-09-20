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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rapidloop/pgmetrics"
	"golang.org/x/mod/semver"
)

func (c *collector) collectPgpool() error {
	c.result.Pgpool = &pgmetrics.Pgpool{}

	var semversion string
	if err := c.runDomain(domainPPVersion, "", func() (int, error) {
		v, err := c.getPPVersion()
		if err != nil {
			return 0, err
		}
		semversion = v
		return 1, nil
	}); err != nil {
		return err
	}
	if err := c.runDomain(domainPPNodes, "", c.getPPNodes); err != nil {
		return err
	}
	if err := c.runDomain(domainPPHealthStats, "", func() (int, error) {
		return c.getPPHCStats(semversion)
	}); err != nil {
		return err
	}
	if err := c.runDomain(domainPPBackendStats, "", func() (int, error) {
		return c.getPPBEStats(semversion)
	}); err != nil {
		return err
	}
	if err := c.runDomain(domainPPCache, "", c.getPPCache); err != nil {
		return err
	}
	return nil
}

/*
 * SHOW POOL_VERSION
 *
 *     all versions: single column, single row, value like "4.4.2 (nurikoboshi)"
 */

func (c *collector) getPPVersion() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// get raw version
	var version string
	if err := c.db.QueryRowContext(ctx, "SHOW POOL_VERSION").Scan(&version); err != nil {
		return "", fmt.Errorf("pgpool: show pool_version query failed: %w", err)
	}

	// check if semver
	semversion := "v" + version
	if parts := strings.Fields(semversion); len(parts) > 1 {
		semversion = parts[0]
	}
	if !semver.IsValid(semversion) {
		return "", newDomainError(codeQueryError,
			fmt.Errorf("pgpool: show pool_version query: invalid version %q", version))
	}
	c.result.Pgpool.Version = version // use full version in output

	return semversion, nil // for internal use
}

/*
 * SHOW POOL_NODES
 *
 *     4.0 (10) node_id, hostname, port, status, lb_weight, role, select_cnt,
 *     load_balance_node, replication_delay, last_status_change
 *
 *     4.1, 4.2 (12) node_id, hostname, port, status, lb_weight, role,
 *     select_cnt, load_balance_node, replication_delay, replication_state,
 *     replication_sync_state, last_status_change
 *
 *     4.3, 4.4, 4.5, 4.6, 4.7 (14) node_id, hostname, port, status, pg_status,
 *	   lb_weight, role, pg_role, select_cnt, load_balance_node, replication_delay,
 *     replication_state, replication_sync_state, last_status_change
 */

func (c *collector) getPPNodes() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW POOL_NODES")
	if err != nil {
		return 0, fmt.Errorf("pgpool: show pool_nodes query failed: %w", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}

	n := 0
	for rows.Next() {
		var b pgmetrics.PgpoolBackend
		var lastStatusChange, replicationDelay string
		if ncols == 10 {
			err = rows.Scan(&b.NodeID, &b.Hostname, &b.Port, &b.Status,
				&b.LBWeight, &b.Role, &b.SelectCount, &b.LoadBalanceNode,
				&replicationDelay, &lastStatusChange)
		} else if ncols == 12 {
			err = rows.Scan(&b.NodeID, &b.Hostname, &b.Port, &b.Status,
				&b.LBWeight, &b.Role, &b.SelectCount, &b.LoadBalanceNode,
				&replicationDelay, &b.ReplicationState, &b.ReplicationSyncState,
				&lastStatusChange)
		} else if ncols == 14 {
			err = rows.Scan(&b.NodeID, &b.Hostname, &b.Port, &b.Status,
				&b.PgStatus, &b.LBWeight, &b.Role, &b.PgRole, &b.SelectCount,
				&b.LoadBalanceNode, &replicationDelay, &b.ReplicationState,
				&b.ReplicationSyncState, &lastStatusChange)
		} else {
			return n, newDomainError(codeInternalError, fmt.Errorf(
				"pgpool: unsupported number of columns %d in 'SHOW POOL_NODES'", ncols))
		}
		if err != nil {
			return n, newDomainError(codeScanError, fmt.Errorf("pgpool: show pool_nodes query scan failed: %w", err))
		}
		b.LastStatusChange = pgpoolScanTime(lastStatusChange)
		if strings.Contains(replicationDelay, "second") {
			if parts := strings.Fields(replicationDelay); len(parts) == 2 {
				b.ReplicationDelaySeconds, _ = strconv.ParseFloat(parts[0], 64)
			}
		} else {
			b.ReplicationDelay, _ = strconv.ParseInt(replicationDelay, 10, 64)
		}
		c.result.Pgpool.Backends = append(c.result.Pgpool.Backends, b)
		n++
	}
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("pgpool: show pool_nodes query rows failed: %w", err)
	}
	return n, nil
}

func pgpoolScanTime(val string) (out int64) {
	_, offset := time.Now().Zone()
	if t, err := time.Parse("2006-01-02 15:04:05", val); err == nil {
		out = t.Unix() - int64(offset)
	}
	return
}

/*
 * SHOW POOL_HEALTH_CHECK_STATS
 *
 *     4.0, 4.1 not present
 *
 *     4.2, 4.3, 4.4, 4.5, 4.6, 4.7 (20) node_id, hostname, port, status, role,
 *     last_status_change, total_count, success_count, fail_count, skip_count,
 *     retry_count, average_retry_count, max_retry_count, max_duration,
 *     min_duration, average_duration, last_health_check,
 *     last_successful_health_check, last_skip_health_check,
 *     last_failed_health_check
 */

func (c *collector) getPPHCStats(semversion string) (int, error) {
	if semver.Compare(semversion, "v4.2") < 0 { // is version < 4.2
		c.skipVersion(domainPPHealthStats, "",
			"pgpool health check stats require pgpool-II 4.2 or later")
		return 0, nil // no health check stats
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW POOL_HEALTH_CHECK_STATS")
	if err != nil {
		return 0, fmt.Errorf("pgpool: show pool_health_check_stats query failed: %w", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}
	if ncols != 20 {
		return 0, newDomainError(codeInternalError, fmt.Errorf(
			"pgpool: unsupported number of columns %d in 'SHOW POOL_HEALTH_CHECK_STATS'", ncols))
	}

	n := 0
	for rows.Next() {
		var b pgmetrics.PgpoolBackend
		var lastStatusChange, lastHealthCheck, lastSuccessHealthCheck,
			lastSkipHealthCheck, lastFailedHealthCheck, avgRetryCount,
			avgDuration string
		err = rows.Scan(&b.NodeID, &b.Hostname, &b.Port, &b.Status,
			&b.Role, &lastStatusChange, &b.HCTotalCount, &b.HCSuccessCount,
			&b.HCFailCount, &b.HCSkipCount, &b.HCRetryCount, &avgRetryCount,
			&b.HCMaxRetryCount, &b.HCMaxDurationMillis, &b.HCMinDurationMillis,
			&avgDuration, &lastHealthCheck, &lastSuccessHealthCheck,
			&lastSkipHealthCheck, &lastFailedHealthCheck)
		if err != nil {
			return n, newDomainError(codeScanError, fmt.Errorf("pgpool: show pool_health_check_stats query scan failed: %w", err))
		}
		for i := range c.result.Pgpool.Backends {
			b0 := &c.result.Pgpool.Backends[i]
			if b0.NodeID == b.NodeID {
				b0.HCTotalCount = b.HCTotalCount
				b0.HCSuccessCount = b.HCSuccessCount
				b0.HCFailCount = b.HCFailCount
				b0.HCSkipCount = b.HCSkipCount
				b0.HCRetryCount = b.HCRetryCount
				b0.HCAvgRetryCount, _ = strconv.ParseFloat(avgRetryCount, 64)
				b0.HCMaxRetryCount = b.HCMaxRetryCount
				b0.HCMaxDurationMillis = b.HCMaxDurationMillis
				b0.HCMinDurationMillis = b.HCMinDurationMillis
				b0.HCAvgDurationMillis, _ = strconv.ParseFloat(avgDuration, 64)
				b0.HCLastHealthCheck = pgpoolScanTime(lastHealthCheck)
				b0.HCLastSuccessHealthCheck = pgpoolScanTime(lastSuccessHealthCheck)
				b0.HCLastSkipHealthCheck = pgpoolScanTime(lastSkipHealthCheck)
				b0.HCLastFailedHealthCheck = pgpoolScanTime(lastFailedHealthCheck)
				break
			}
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("pgpool: show pool_health_check_stats query rows failed: %w", err)
	}
	return n, nil
}

/*
 * SHOW POOL_BACKEND_STATS
 *
 *     4.0, 4.1 not present
 *
 *     4.2, 4.3, 4.4, 4.5, 4.6, 4.7 (14) node_id, hostname, port, status, role,
 *	   select_cnt, insert_cnt, update_cnt, delete_cnt, ddl_cnt, other_cnt,
 *	   panic_cnt, fatal_cnt, error_cnt
 */

func (c *collector) getPPBEStats(semversion string) (int, error) {
	if semver.Compare(semversion, "v4.2") < 0 { // is version < v4.2
		c.skipVersion(domainPPBackendStats, "",
			"pgpool backend stats require pgpool-II 4.2 or later")
		return 0, nil // no backend stats
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW POOL_BACKEND_STATS")
	if err != nil {
		return 0, fmt.Errorf("pgpool: show pool_backend_stats query failed: %w", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}
	if ncols != 14 {
		return 0, newDomainError(codeInternalError, fmt.Errorf(
			"pgpool: unsupported number of columns %d in 'SHOW POOL_BACKEND_STATS'", ncols))
	}

	n := 0
	for rows.Next() {
		var b pgmetrics.PgpoolBackend
		err = rows.Scan(&b.NodeID, &b.Hostname, &b.Port, &b.Status,
			&b.Role, &b.SelectCount, &b.InsertCount, &b.UpdateCount,
			&b.DeleteCount, &b.DDLCount, &b.OtherCount, &b.PanicCount,
			&b.FatalCount, &b.ErrorCount)
		if err != nil {
			return n, newDomainError(codeScanError, fmt.Errorf("pgpool: show pool_backend_stats query scan failed: %w", err))
		}
		for i := range c.result.Pgpool.Backends {
			b0 := &c.result.Pgpool.Backends[i]
			if b0.NodeID == b.NodeID {
				b0.SelectCount = b.SelectCount
				b0.InsertCount = b.InsertCount
				b0.UpdateCount = b.UpdateCount
				b0.DeleteCount = b.DeleteCount
				b0.DDLCount = b.DDLCount
				b0.OtherCount = b.OtherCount
				b0.PanicCount = b.PanicCount
				b0.FatalCount = b.FatalCount
				b0.ErrorCount = b.ErrorCount
				break
			}
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("pgpool: show pool_backend_stats query rows failed: %w", err)
	}
	return n, nil
}

/*
 * SHOW POOL_CACHE
 *
 *     4.0, 4.1, 4.2 (9) num_cache_hits, num_selects, cache_hit_ratio,
 *     num_hash_entries, used_hash_entries, num_cache_entries,
 *     used_cache_enrties_size, free_cache_entries_size,
 *     fragment_cache_entries_size
 *
 *     4.3, 4.4, 4.5, 4.6, 4.7 (9) num_cache_hits, num_selects, cache_hit_ratio,
 *     num_hash_entries, used_hash_entries, num_cache_entries,
 *     used_cache_entries_size, free_cache_entries_size,
 *     fragment_cache_entries_size
 *
 *     spelling of used_cache_enrties_size fixed in 4.3
 */

func (c *collector) getPPCache() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW POOL_CACHE")
	if err != nil {
		return 0, fmt.Errorf("pgpool: show pool_cache query failed: %w", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}
	if ncols != 9 {
		return 0, newDomainError(codeInternalError, fmt.Errorf(
			"pgpool: unsupported number of columns %d in 'SHOW POOL_CACHE'", ncols))
	}

	q := &c.result.Pgpool.QueryCache
	n := 0
	for rows.Next() {
		err = rows.Scan(&q.NumCacheHits, &q.NumSelects, &q.CacheHitRatio,
			&q.NumHashEntries, &q.UsedHashEntries, &q.NumCacheEntries,
			&q.UsedCacheEntriesSize, &q.FreeCacheEntriesSize,
			&q.FragmentCacheEntriesSize)
		if err != nil {
			return n, newDomainError(codeScanError, fmt.Errorf("pgpool: show pool_cache query scan failed: %w", err))
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("pgpool: show pool_cache query rows failed: %w", err)
	}
	return n, nil
}
