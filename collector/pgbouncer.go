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
	"database/sql"
	"fmt"

	"github.com/rapidloop/pgmetrics"
)

//------------------------------------------------------------------------------
// PgBouncer

func (c *collector) collectPgBouncer() error {
	c.result.PgBouncer = &pgmetrics.PgBouncer{}
	if err := c.runDomain(domainPBPools, "", c.getPBPools); err != nil {
		return err
	}
	if err := c.runDomain(domainPBServers, "", c.getPBServers); err != nil {
		return err
	}
	if err := c.runDomain(domainPBClients, "", c.getPBClients); err != nil {
		return err
	}
	if err := c.runDomain(domainPBStats, "", c.getPBStats); err != nil {
		return err
	}
	if err := c.runDomain(domainPBDatabases, "", c.getPBDatabases); err != nil {
		return err
	}
	return nil
}

/*
 * PgBouncer "SHOW POOLS" changes across recent PgBouncer versions:
 *
 * 1.15: (12) database, user, cl_active, cl_waiting, sv_active, sv_idle,
 *            sv_used, sv_tested, sv_login, maxwait, maxwait_us, pool_mode
 * 1.16: (13) database, user, cl_active, cl_waiting, cl_cancel_req, sv_active,
 *            sv_idle, sv_used, sv_tested, sv_login, maxwait, maxwait_us, pool_mode
 * 1.17: same as 1.16
 * 1.18: (16) database, user, cl_active, cl_waiting, cl_active_cancel_req,
 *            cl_waiting_cancel_req, sv_active, sv_active_cancel,
 *            sv_being_canceled, sv_idle, sv_used, sv_tested, sv_login, maxwait,
 *            maxwait_us, pool_mode
 * 1.19: same as 1.18
 * 1.20: same as 1.19
 * 1.21: same as 1.20
 * 1.22: same as 1.21
 * 1.23: same as 1.22
 * 1.24: (17) database, user, cl_active, cl_waiting, cl_active_cancel_req,
 *            cl_waiting_cancel_req, sv_active, sv_active_cancel,
 *            sv_being_canceled, sv_idle, sv_used, sv_tested, sv_login,
 *            maxwait, maxwait_us, pool_mode, load_balance_hosts
 * 1.25: same as 1.24
 */

func (c *collector) getPBPools() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW POOLS")
	if err != nil {
		return 0, fmt.Errorf("pgbouncer: show pools query failed: %w", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}

	n := 0
	for rows.Next() {
		var pool pgmetrics.PgBouncerPool
		var maxWaitUs float64
		if ncols == 12 {
			err = rows.Scan(&pool.Database, &pool.UserName, &pool.ClActive,
				&pool.ClWaiting, &pool.SvActive, &pool.SvIdle, &pool.SvUsed,
				&pool.SvTested, &pool.SvLogin, &pool.MaxWait, &maxWaitUs,
				&pool.Mode)
		} else if ncols == 13 {
			err = rows.Scan(&pool.Database, &pool.UserName, &pool.ClActive,
				&pool.ClWaiting, &pool.ClCancelReq, &pool.SvActive, &pool.SvIdle,
				&pool.SvUsed, &pool.SvTested, &pool.SvLogin, &pool.MaxWait,
				&maxWaitUs, &pool.Mode)
		} else if ncols == 16 {
			err = rows.Scan(&pool.Database, &pool.UserName, &pool.ClActive,
				&pool.ClWaiting, &pool.ClActiveCancelReq, &pool.ClWaitingCancelReq,
				&pool.SvActive, &pool.SvActiveCancel, &pool.SvBeingCanceled,
				&pool.SvIdle, &pool.SvUsed, &pool.SvTested, &pool.SvLogin,
				&pool.MaxWait, &maxWaitUs, &pool.Mode)
		} else if ncols == 17 {
			var lbhosts sql.NullString
			err = rows.Scan(&pool.Database, &pool.UserName, &pool.ClActive,
				&pool.ClWaiting, &pool.ClActiveCancelReq, &pool.ClWaitingCancelReq,
				&pool.SvActive, &pool.SvActiveCancel, &pool.SvBeingCanceled,
				&pool.SvIdle, &pool.SvUsed, &pool.SvTested, &pool.SvLogin,
				&pool.MaxWait, &maxWaitUs, &pool.Mode, &lbhosts)
			pool.LoadBalanceHosts = lbhosts.String
		} else {
			return n, newDomainError(codeInternalError, fmt.Errorf(
				"pgbouncer: unsupported number of columns %d in 'SHOW POOLS'", ncols))
		}
		if err != nil {
			return n, newDomainError(codeScanError, fmt.Errorf("pgbouncer: show pools query failed: %w", err))
		}
		pool.MaxWait += maxWaitUs / 1e6
		c.result.PgBouncer.Pools = append(c.result.PgBouncer.Pools, pool)
		n++
	}
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("pgbouncer: show pools query failed: %w", err)
	}
	return n, nil
}

/*
 * PgBouncer "SHOW SERVERS" changes across recent PgBouncer versions:
 *
 * 1.15: (17) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls
 * 1.16: same as 1.15
 * 1.17: same as 1.16
 * 1.18: (18) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls, application_name
 * 1.19: same as 1.18
 * 1.20: same as 1.19
 * 1.21: (19) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls, application_name, prepared_statements
 * 1.22: same as 1.21
 * 1.23: (20) type, user, database, replication, state, addr, port, local_addr,
 *            local_port, connect_time, request_time, wait, wait_us,
 *            close_needed, ptr, link, remote_pid, tls, application_name,
 *            prepared_statements
 * 1.24: (21) type, user, database, replication, state, addr, port, local_addr,
 *            local_port, connect_time, request_time, wait, wait_us,
 *            close_needed, ptr, link, remote_pid, tls, application_name,
 *            prepared_statements, id
 * 1.25: same as 1.24
 */

func (c *collector) getPBServers() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW SERVERS")
	if err != nil {
		return 0, fmt.Errorf("pgbouncer: show servers query failed: %w", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}

	n := 0
	for rows.Next() {
		var s [18]sql.NullString
		var state string
		var wait, waitUs float64
		if ncols == 16 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12])
		} else if ncols == 17 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13])
		} else if ncols == 18 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13], &s[14])
		} else if ncols == 19 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13], &s[14], &s[15])
		} else if ncols == 20 {
			err = rows.Scan(&s[0], &s[1], &s[2], &s[3], &state, &s[4], &s[5], &s[6],
				&s[7], &s[8], &s[9], &wait, &waitUs, &s[10], &s[11], &s[12],
				&s[13], &s[14], &s[15], &s[16])
		} else if ncols == 21 {
			err = rows.Scan(&s[0], &s[1], &s[2], &s[3], &state, &s[4], &s[5], &s[6],
				&s[7], &s[8], &s[9], &wait, &waitUs, &s[10], &s[11], &s[12],
				&s[13], &s[14], &s[15], &s[16], &s[17])
		} else {
			return n, newDomainError(codeInternalError, fmt.Errorf(
				"pgbouncer: unsupported number of columns %d in 'SHOW SERVERS'", ncols))
		}
		if err != nil {
			return n, newDomainError(codeScanError, fmt.Errorf("pgbouncer: show servers query failed: %w", err))
		}
		wait += waitUs / 1e6 // convert usec -> sec
		if wait > c.result.PgBouncer.SCMaxWait {
			c.result.PgBouncer.SCMaxWait = wait
		}
		switch state {
		case "active":
			c.result.PgBouncer.SCActive++
		case "idle":
			c.result.PgBouncer.SCIdle++
		case "used":
			c.result.PgBouncer.SCUsed++
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("pgbouncer: show servers query failed: %w", err)
	}
	return n, nil
}

/*
 * PgBouncer "SHOW CLIENTS" changes across recent PgBouncer versions:
 *
 * 1.15: (17) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls
 * 1.16: same as 1.15
 * 1.17: same as 1.16
 * 1.18: (18) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls, application_name
 * 1.19: same as 1.18
 * 1.20: same as 1.19
 * 1.21: (19) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls, application_name, prepared_statements
 * 1.22: same as 1.21
 * 1.23: (20) type, user, database, replication, state, addr, port, local_addr,
 *            local_port, connect_time, request_time, wait, wait_us,
 *            close_needed, ptr, link, remote_pid, tls, application_name,
 *            prepared_statements
 * 1.24: (21) type, user, database, replication, state, addr, port, local_addr,
 *            local_port, connect_time, request_time, wait, wait_us,
 *            close_needed, ptr, link, remote_pid, tls, application_name,
 *            prepared_statements, id
 * 1.25: same as 1.24
 */

func (c *collector) getPBClients() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW CLIENTS")
	if err != nil {
		return 0, fmt.Errorf("pgbouncer: show clients query failed: %w", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}

	var totalWait float64
	n := 0
	for rows.Next() {
		var s [18]sql.NullString
		var state string
		var wait, waitUs float64
		if ncols == 16 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12])
		} else if ncols == 17 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13])
		} else if ncols == 18 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13], &s[14])
		} else if ncols == 19 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13], &s[14], &s[15])
		} else if ncols == 20 {
			err = rows.Scan(&s[0], &s[1], &s[2], &s[3], &state, &s[4], &s[5], &s[6],
				&s[7], &s[8], &s[9], &wait, &waitUs, &s[10], &s[11], &s[12],
				&s[13], &s[14], &s[15], &s[16])
		} else if ncols == 21 {
			err = rows.Scan(&s[0], &s[1], &s[2], &s[3], &state, &s[4], &s[5], &s[6],
				&s[7], &s[8], &s[9], &wait, &waitUs, &s[10], &s[11], &s[12],
				&s[13], &s[14], &s[15], &s[16], &s[17])
		} else {
			return n, newDomainError(codeInternalError, fmt.Errorf(
				"pgbouncer: unsupported number of columns %d in 'SHOW CLIENTS'", ncols))
		}
		if err != nil {
			return n, newDomainError(codeScanError, fmt.Errorf("pgbouncer: show clients query failed: %w", err))
		}
		wait += waitUs / 1e6 // convert usec -> sec
		switch state {
		case "active":
			c.result.PgBouncer.CCActive++
		case "waiting":
			c.result.PgBouncer.CCWaiting++
			if wait > c.result.PgBouncer.CCMaxWait {
				c.result.PgBouncer.CCMaxWait = wait
			}
			totalWait += wait
		case "idle":
			c.result.PgBouncer.CCIdle++
		case "used":
			c.result.PgBouncer.CCUsed++
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("pgbouncer: show clients query failed: %w", err)
	}
	if c.result.PgBouncer.CCWaiting > 0 {
		c.result.PgBouncer.CCAvgWait = totalWait / float64(c.result.PgBouncer.CCWaiting)
	}
	return n, nil
}

/*
 * PgBouncer "SHOW STATS" changes across recent PgBouncer versions:
 *
 * 1.15: (15) database, total_xact_count, total_query_count, total_received,
 *            total_sent, total_xact_time, total_query_time, total_wait_time,
 *            avg_xact_count, avg_query_count, avg_recv, avg_sent,
 *            avg_xact_time, avg_query_time, avg_wait_time
 * 1.16: same as 1.15
 * 1.17: same as 1.16
 * 1.18: same as 1.17
 * 1.19: same as 1.18
 * 1.20: same as 1.19
 * 1.21: same as 1.20
 * 1.22: same as 1.21
 * 1.23: (17) database, total_xact_count, total_query_count,
 *			  total_server_assignment_count, total_received, total_sent,
 *			  total_xact_time, total_query_time, total_wait_time,
 *			  avg_xact_count, avg_query_count, avg_server_assignment_count,
 *            avg_recv, avg_sent, avg_xact_time, avg_query_time, avg_wait_time
 * 1.24: (23) database, total_xact_count, total_query_count,
 *			  total_server_assignment_count, total_received, total_sent,
 *			  total_xact_time, total_query_time, total_wait_time,
 *			  total_client_parse_count, total_server_parse_count, total_bind_count,
 *			  avg_xact_count, avg_query_count, avg_server_assignment_count,
 *            avg_recv, avg_sent, avg_xact_time, avg_query_time, avg_wait_time,
 *			  avg_client_parse_count, avg_server_parse_count, avg_bind_count
 * 1.25: same as 1.24
 */

func (c *collector) getPBStats() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW STATS")
	if err != nil {
		return 0, fmt.Errorf("pgbouncer: show stats query failed: %w", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}

	n := 0
	for rows.Next() {
		var stat pgmetrics.PgBouncerStat
		if ncols == 15 {
			err = rows.Scan(&stat.Database, &stat.TotalXactCount, &stat.TotalQueryCount,
				&stat.TotalReceived, &stat.TotalSent, &stat.TotalXactTime,
				&stat.TotalQueryTime, &stat.TotalWaitTime, &stat.AvgXactCount,
				&stat.AvgQueryCount, &stat.AvgReceived, &stat.AvgSent, &stat.AvgXactTime,
				&stat.AvgQueryTime, &stat.AvgWaitTime)
		} else if ncols == 17 {
			err = rows.Scan(&stat.Database, &stat.TotalXactCount,
				&stat.TotalQueryCount, &stat.TotalServerAssignmentCount,
				&stat.TotalReceived, &stat.TotalSent, &stat.TotalXactTime,
				&stat.TotalQueryTime, &stat.TotalWaitTime, &stat.AvgXactCount,
				&stat.AvgQueryCount, &stat.AvgServerAssignmentCount,
				&stat.AvgReceived, &stat.AvgSent, &stat.AvgXactTime,
				&stat.AvgQueryTime, &stat.AvgWaitTime)
		} else if ncols == 23 {
			err = rows.Scan(&stat.Database, &stat.TotalXactCount,
				&stat.TotalQueryCount, &stat.TotalServerAssignmentCount,
				&stat.TotalReceived, &stat.TotalSent, &stat.TotalXactTime,
				&stat.TotalQueryTime, &stat.TotalWaitTime,
				&stat.TotalClientParseCount, &stat.TotalServerParseCount,
				&stat.TotalBindCount, &stat.AvgXactCount,
				&stat.AvgQueryCount, &stat.AvgServerAssignmentCount,
				&stat.AvgReceived, &stat.AvgSent, &stat.AvgXactTime,
				&stat.AvgQueryTime, &stat.AvgWaitTime, &stat.AvgClientParseCount,
				&stat.AvgServerParseCount, &stat.AvgBindCount)
		} else {
			return n, newDomainError(codeInternalError, fmt.Errorf(
				"pgbouncer: unsupported number of columns %d in 'SHOW STATS'", ncols))
		}
		if err != nil {
			return n, newDomainError(codeScanError, fmt.Errorf("pgbouncer: show stats query failed: %w", err))
		}
		// convert usec -> sec
		stat.TotalXactTime /= 1e6
		stat.TotalQueryTime /= 1e6
		stat.TotalWaitTime /= 1e6
		stat.AvgXactTime /= 1e6
		stat.AvgQueryTime /= 1e6
		stat.AvgWaitTime /= 1e6
		c.result.PgBouncer.Stats = append(c.result.PgBouncer.Stats, stat)
		n++
	}
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("pgbouncer: show stats query failed: %w", err)
	}
	return n, nil
}

/*
 * PgBouncer "SHOW DATABASES" changes across recent PgBouncer versions:
 *
 * 1.15: (12) name, host, port, database, force_user, pool_size, reserve_pool,
 *            pool_mode, max_connections, current_connections, paused, disabled
 * 1.16: (13) name, host, port, database, force_user, pool_size, min_pool_size,
 *            reserve_pool, pool_mode, max_connections, current_connections,
 *            paused, disabled
 * 1.17: same as 1.16
 * 1.18: same as 1.17
 * 1.19: same as 1.18
 * 1.20: same as 1.19
 * 1.21: same as 1.20
 * 1.22: same as 1.21
 * 1.23: (14) name, host, port, database, force_user, pool_size, min_pool_size,
 *            reserve_pool, server_lifetime, pool_mode, max_connections,
 *            current_connections, paused, disabled
 * 1.24: (17) name, host, port, database, force_user, pool_size, min_pool_size,
 *            reserve_pool, server_lifetime, pool_mode, load_balance_hosts,
 *			  max_connections, current_connections, max_client_connections,
 *			  current_client_connections, paused, disabled
 * 1.25: same as 1.24
 */

func (c *collector) getPBDatabases() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		return 0, fmt.Errorf("pgbouncer: show databases query failed: %w", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}

	n := 0
	for rows.Next() {
		var db pgmetrics.PgBouncerDatabase
		var host, user sql.NullString
		var s [5]sql.NullString
		var paused, disabled int
		if ncols == 12 {
			err = rows.Scan(&db.Database, &host, &db.Port, &db.SourceDatabase,
				&user, &s[0], &s[1], &s[2], &db.MaxConn, &db.CurrConn, &paused,
				&disabled)
		} else if ncols == 13 {
			err = rows.Scan(&db.Database, &host, &db.Port, &db.SourceDatabase,
				&user, &s[0], &s[1], &s[2], &s[3], &db.MaxConn, &db.CurrConn,
				&paused, &disabled)
		} else if ncols == 14 {
			err = rows.Scan(&db.Database, &host, &db.Port, &db.SourceDatabase,
				&user, &s[0], &s[1], &s[2], &s[3], &s[4], &db.MaxConn, &db.CurrConn,
				&paused, &disabled)
		} else if ncols == 17 {
			var lbhosts sql.NullString
			err = rows.Scan(&db.Database, &host, &db.Port, &db.SourceDatabase,
				&user, &s[0], &s[1], &s[2], &s[3], &s[4], &lbhosts, &db.MaxConn,
				&db.CurrConn, &db.MaxClientConn, &db.CurrClientConn,
				&paused, &disabled)
			db.LoadBalanceHosts = lbhosts.String
		} else {
			return n, newDomainError(codeInternalError, fmt.Errorf(
				"pgbouncer: unsupported number of columns %d in 'SHOW DATABASES'", ncols))
		}
		if err != nil {
			return n, newDomainError(codeScanError, fmt.Errorf("pgbouncer: show databases query failed: %w", err))
		}
		db.Host = host.String
		db.Paused = paused == 1
		db.Disabled = disabled == 1
		db.User = user.String
		c.result.PgBouncer.Databases = append(c.result.PgBouncer.Databases, db)
		n++
	}
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("pgbouncer: show databases query failed: %w", err)
	}
	return n, nil
}
