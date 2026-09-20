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
	"errors"
	"net"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rapidloop/pgmetrics"
)

// Stable, machine-readable error/skip codes. They are part of the
// CollectionOutcome contract: never renumber or rename an existing code.
const (
	codeConnFailed          = "conn_failed" // could not connect / connection lost
	codeAuthFailed          = "auth_failed" // authentication/authorization failure at connect
	codeInsufficientPriv    = "insufficient_privilege"
	codeStatementTimeout    = "statement_timeout"
	codeLockTimeout         = "lock_timeout"
	codeContextDeadline     = "context_deadline"
	codeUndefinedObject     = "undefined_object"     // table/view/function missing: version/extension mismatch
	codeExtensionAbsent     = "extension_absent"     // required extension not installed
	codeScanError           = "scan_error"           // row scanning failed
	codeQueryError          = "query_error"          // any other SQL error
	codeVersionUnsupported  = "version_unsupported"  // skipped: server version too old/new
	codePlatformUnsupported = "platform_unsupported" // skipped: OS/platform cannot provide it
	codeAuroraUnsupported   = "aurora_unsupported"   // skipped: AWS Aurora does not implement it
	codeFeatureDisabled     = "feature_disabled"     // skipped: server-side feature turned off
	codeNotLocal            = "not_local"            // skipped: server is not on this machine
	codeDisabledByOption    = "disabled_by_option"   // skipped: --omit/--no-* user option
	codeIOError             = "io_error"             // local file / cloud API IO failure
	codeInternalError       = "internal_error"       // collector defect / unexpected state
)

// DomainError is the typed error returned by collection query functions.
// It carries a stable code on top of the underlying error; the executor
// classifies it instead of inspecting raw driver errors again.
type DomainError struct {
	Code string
	Err  error
}

func (e *DomainError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.Code
	}
	return e.Err.Error()
}

func (e *DomainError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// newDomainError wraps err with a stable code. A nil err yields nil.
func newDomainError(code string, err error) *DomainError {
	if err == nil {
		return nil
	}
	return &DomainError{Code: code, Err: err}
}

// codeStatus maps the stable codes (both error codes and skip codes) to
// the six domain statuses.
var codeStatus = map[string]string{
	codeConnFailed:          pgmetrics.CollectionStatusFailed,
	codeAuthFailed:          pgmetrics.CollectionStatusPermissionDenied,
	codeInsufficientPriv:    pgmetrics.CollectionStatusPermissionDenied,
	codeStatementTimeout:    pgmetrics.CollectionStatusTimeout,
	codeLockTimeout:         pgmetrics.CollectionStatusTimeout,
	codeContextDeadline:     pgmetrics.CollectionStatusTimeout,
	codeUndefinedObject:     pgmetrics.CollectionStatusUnsupported,
	codeExtensionAbsent:     pgmetrics.CollectionStatusUnsupported,
	codeScanError:           pgmetrics.CollectionStatusFailed,
	codeQueryError:          pgmetrics.CollectionStatusFailed,
	codeVersionUnsupported:  pgmetrics.CollectionStatusUnsupported,
	codePlatformUnsupported: pgmetrics.CollectionStatusUnsupported,
	codeAuroraUnsupported:   pgmetrics.CollectionStatusUnsupported,
	codeFeatureDisabled:     pgmetrics.CollectionStatusOmitted,
	codeNotLocal:            pgmetrics.CollectionStatusOmitted,
	codeDisabledByOption:    pgmetrics.CollectionStatusOmitted,
	codeIOError:             pgmetrics.CollectionStatusFailed,
	codeInternalError:       pgmetrics.CollectionStatusFailed,
}

// classify maps any error to a per-domain status and a stable code.
// classify(nil) returns the success status with an empty code.
func classify(err error) (status, code string) {
	if err == nil {
		return pgmetrics.CollectionStatusSuccess, ""
	}

	// typed collector errors win: their code is authoritative
	var de *DomainError
	if errors.As(err, &de) {
		if s, ok := codeStatus[de.Code]; ok {
			return s, de.Code
		}
		return pgmetrics.CollectionStatusFailed, de.Code
	}

	// timeouts from the Go side
	if errors.Is(err, context.DeadlineExceeded) {
		return pgmetrics.CollectionStatusTimeout, codeContextDeadline
	}
	if errors.Is(err, context.Canceled) {
		return pgmetrics.CollectionStatusFailed, codeQueryError
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return pgmetrics.CollectionStatusTimeout, codeContextDeadline
	}

	// PostgreSQL server-side errors
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return classifyPgCode(pe.Code), pgErrorCode(pe.Code)
	}

	// network/connection errors that are not wrapped as PgError
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return pgmetrics.CollectionStatusFailed, codeConnFailed
	}

	return pgmetrics.CollectionStatusFailed, codeQueryError
}

// classifyPgCode maps a PostgreSQL SQLSTATE to a domain status.
func classifyPgCode(sqlstate string) string {
	switch sqlstate {
	case "42501", // insufficient_privilege
		"28000", // invalid_authorization_specification
		"28P01": // invalid_password
		return pgmetrics.CollectionStatusPermissionDenied
	case "57014": // query_canceled (statement_timeout)
		return pgmetrics.CollectionStatusTimeout
	case "55P03": // lock_not_available (lock_timeout)
		return pgmetrics.CollectionStatusTimeout
	case "42P01", // undefined_table
		"42P02", // undefined_parameter
		"42883": // undefined_function
		return pgmetrics.CollectionStatusUnsupported
	case "08000", // connection_exception
		"08001", // sqlclient_unable_to_establish_sqlconnection
		"08003", // connection_does_not_exist
		"08004", // sqlserver_rejected_establishment_of_sqlconnection
		"08006", // connection_failure
		"08007", // transaction_resolution_unknown
		"57P03": // cannot_connect_now
		return pgmetrics.CollectionStatusFailed
	default:
		return pgmetrics.CollectionStatusFailed
	}
}

// pgErrorCode maps a PostgreSQL SQLSTATE to a stable code.
func pgErrorCode(sqlstate string) string {
	switch sqlstate {
	case "42501":
		return codeInsufficientPriv
	case "28000", "28P01":
		return codeAuthFailed
	case "57014":
		return codeStatementTimeout
	case "55P03":
		return codeLockTimeout
	case "42P01", "42P02", "42883":
		return codeUndefinedObject
	case "08000", "08001", "08003", "08004", "08006", "08007", "57P03":
		return codeConnFailed
	default:
		return codeQueryError
	}
}

var (
	// key=value secrets as produced by libpq-style connection strings,
	// e.g. password='sec ret' or password=sec (sslkey points at a private
	// key file and is treated as sensitive as well).
	rxKVSecret = regexp.MustCompile(`(?i)(password|sslpassword|sslkey|passfile)\s*=\s*('(?:[^']|'')*'|[^\s']+)`)
	// URI secrets, e.g. postgres://user:secret@host:5432/db
	rxURISecret = regexp.MustCompile(`(?i)\b(postgres(?:ql)?://)([^:/?#\s]*):([^@/?#\s]*)@`)
)

const maskedSecret = "****"

// sanitize removes credentials and other secrets from a message before it
// is persisted to a snapshot or written to any output. It is the single
// chokepoint through which every DomainOutcome.Summary passes. The
// function is idempotent.
func sanitize(s string) string {
	if s == "" {
		return s
	}
	s = rxURISecret.ReplaceAllString(s, "${1}${2}:"+maskedSecret+"@")
	s = rxKVSecret.ReplaceAllString(s, "$1="+maskedSecret)
	return s
}

// joinSummary renders a short, credential-free summary for an error.
func joinSummary(prefix string, err error) string {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	msg = sanitize(msg)
	if prefix == "" {
		return msg
	}
	if msg == "" {
		return prefix
	}
	if strings.HasSuffix(prefix, ":") {
		return prefix + " " + msg
	}
	return prefix + ": " + msg
}
