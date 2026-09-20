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
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rapidloop/pgmetrics"
)

func pgErr(code, msg string) *pgconn.PgError {
	return &pgconn.PgError{Code: code, Message: msg}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus string
		wantCode   string
	}{
		{"nil is success", nil, pgmetrics.CollectionStatusSuccess, ""},
		{"insufficient privilege", pgErr("42501", "permission denied for view pg_replication_slots"),
			pgmetrics.CollectionStatusPermissionDenied, codeInsufficientPriv},
		{"invalid authorization", pgErr("28000", "no pg_hba.conf entry"),
			pgmetrics.CollectionStatusPermissionDenied, codeAuthFailed},
		{"invalid password", pgErr("28P01", "password authentication failed"),
			pgmetrics.CollectionStatusPermissionDenied, codeAuthFailed},
		{"statement timeout", pgErr("57014", "canceling statement due to statement timeout"),
			pgmetrics.CollectionStatusTimeout, codeStatementTimeout},
		{"lock timeout", pgErr("55P03", "lock timeout"),
			pgmetrics.CollectionStatusTimeout, codeLockTimeout},
		{"undefined table", pgErr("42P01", `relation "pg_stat_statements" does not exist`),
			pgmetrics.CollectionStatusUnsupported, codeUndefinedObject},
		{"undefined parameter", pgErr("42P02", "missing parameter"),
			pgmetrics.CollectionStatusUnsupported, codeUndefinedObject},
		{"undefined function", pgErr("42883", "function pg_control_system() does not exist"),
			pgmetrics.CollectionStatusUnsupported, codeUndefinedObject},
		{"cannot connect now", pgErr("57P03", "the database system is starting up"),
			pgmetrics.CollectionStatusFailed, codeConnFailed},
		{"unknown sqlstate", pgErr("XX999", "weird"),
			pgmetrics.CollectionStatusFailed, codeQueryError},
		{"context deadline", context.DeadlineExceeded,
			pgmetrics.CollectionStatusTimeout, codeContextDeadline},
		{"context canceled", context.Canceled,
			pgmetrics.CollectionStatusFailed, codeQueryError},
		{"wrapped pg error", fmt.Errorf("query x failed: %w", pgErr("42501", "denied")),
			pgmetrics.CollectionStatusPermissionDenied, codeInsufficientPriv},
		{"wrapped deadline", fmt.Errorf("query: %w", context.DeadlineExceeded),
			pgmetrics.CollectionStatusTimeout, codeContextDeadline},
		{"typed domain error", newDomainError(codeScanError, errors.New("column mismatch")),
			pgmetrics.CollectionStatusFailed, codeScanError},
		{"typed skip code", newDomainError(codeExtensionAbsent, errors.New("no citus")),
			pgmetrics.CollectionStatusUnsupported, codeExtensionAbsent},
		{"plain error", errors.New("boom"),
			pgmetrics.CollectionStatusFailed, codeQueryError},
		{"net op error", &net.OpError{Op: "dial", Err: errors.New("refused")},
			pgmetrics.CollectionStatusFailed, codeConnFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotStatus, gotCode := classify(tc.err)
			if gotStatus != tc.wantStatus || gotCode != tc.wantCode {
				t.Fatalf("classify(%v): got (%q,%q) want (%q,%q)",
					tc.err, gotStatus, gotCode, tc.wantStatus, tc.wantCode)
			}
		})
	}
}

func TestDomainError(t *testing.T) {
	if newDomainError(codeScanError, nil) != nil {
		t.Fatal("newDomainError with nil err must be nil")
	}
	de := newDomainError(codeIOError, io.ErrUnexpectedEOF)
	if de.Error() != "unexpected EOF" {
		t.Fatalf("Error() = %q", de.Error())
	}
	if !errors.Is(de, io.ErrUnexpectedEOF) {
		t.Fatal("Unwrap should expose the cause")
	}
	var asDE *DomainError
	if !errors.As(fmt.Errorf("wrap: %w", de), &asDE) || asDE.Code != codeIOError {
		t.Fatal("errors.As must find the DomainError")
	}
	var nilDE *DomainError
	if nilDE.Error() != "" || nilDE.Unwrap() != nil {
		t.Fatal("nil DomainError methods must be zero-valued")
	}
}

func TestSanitize(t *testing.T) {
	cases := []struct {
		name string
		in   string
		bad  []string // substrings that must not survive
		ok   []string // substrings that must survive
	}{
		{
			name: "kv quoted password",
			in:   "failed to connect using password='s3cr3t' host=x",
			bad:  []string{"s3cr3t"},
			ok:   []string{"password=****", "host=x"},
		},
		{
			name: "kv bare password",
			in:   "pg_settings query failed: password=t0psecret, timeout",
			bad:  []string{"t0psecret"},
			ok:   []string{"password=****"},
		},
		{
			name: "kv uppercase keyword",
			in:   "PASSWORD='s3cr3t'",
			bad:  []string{"s3cr3t"},
			ok:   []string{"PASSWORD=****"},
		},
		{
			name: "uri with url-encoded password",
			in:   "dial tcp: postgresql://u:p%40ss@host:5432/db: refused",
			bad:  []string{"p%40ss"},
			ok:   []string{"postgresql://u:****@host:5432/db"},
		},
		{
			name: "uri postgres scheme",
			in:   "postgres://admin:hunter2@db.example/app",
			bad:  []string{"hunter2"},
			ok:   []string{"postgres://admin:****@db.example/app"},
		},
		{
			name: "secret in the middle of an error",
			in:   `FATAL: failed: password='s3cr3t' while connecting`,
			bad:  []string{"s3cr3t"},
			ok:   []string{"while connecting"},
		},
		{
			name: "clean text passes through",
			in:   "permission denied for view pg_replication_slots",
			bad:  nil,
			ok:   []string{"permission denied for view pg_replication_slots"},
		},
		{
			name: "uri without password untouched",
			in:   "postgres://host:5432/db",
			ok:   []string{"postgres://host:5432/db"},
		},
		{
			name: "empty string",
			in:   "",
			ok:   []string{""},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := sanitize(tc.in)
			for _, b := range tc.bad {
				if strings.Contains(out, b) {
					t.Errorf("secret %q survived: %s", b, out)
				}
			}
			for _, w := range tc.ok {
				if !strings.Contains(out, w) {
					t.Errorf("expected %q in output: %s", w, out)
				}
			}
			// idempotency
			if again := sanitize(out); again != out {
				t.Errorf("sanitize not idempotent:\n %s\n %s", out, again)
			}
		})
	}
}

func TestJoinSummary(t *testing.T) {
	if got := joinSummary("pg_settings query failed", fmt.Errorf("password='s3cr3t' denied")); got != "pg_settings query failed: password=**** denied" {
		t.Fatalf("got %q", got)
	}
	if got := joinSummary("", errors.New("x")); got != "x" {
		t.Fatalf("got %q", got)
	}
	if got := joinSummary("boom:", nil); got != "boom:" {
		t.Fatalf("got %q", got)
	}
}
