package s3sql

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// openDuckDB opens an in-memory DuckDB connection, ensures the
// httpfs extension is loaded, applies the auto-derived S3
// settings, and finally runs any caller-supplied init SQL.
//
// User-supplied statements run last so they can override any of
// the auto-derived settings (e.g. force a different endpoint,
// disable SSL, add a CREATE SECRET).
func openDuckDB(client *s3.Client, extra []string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	if err := ensureHTTPFS(db); err != nil {
		db.Close()
		return nil, err
	}

	// Default settings that apply to every store.
	baseSettings := []string{
		// Cache parquet footers so the introspection LIMIT 0
		// and the main query don't double-read metadata.
		"SET enable_object_cache=true",
	}

	// S3 settings auto-derived from the aws-sdk client.
	derived := deriveDuckDBS3Settings(client)

	// Apply in order: base defaults, derived, then user extras
	// so extras win.
	for _, stmt := range baseSettings {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, err
		}
	}
	for _, stmt := range derived {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, err
		}
	}
	for _, stmt := range extra {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

// ensureHTTPFS tries LOAD first and falls back to INSTALL only
// when LOAD fails. This avoids a network roundtrip on every
// New() once the extension is cached, works in air-gapped
// environments with pre-installed extensions, and prevents
// INSTALL from silently upgrading an already-present version.
func ensureHTTPFS(db *sql.DB) error {
	if _, err := db.Exec("LOAD httpfs"); err == nil {
		return nil
	}
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		return err
	}
	_, err := db.Exec("LOAD httpfs")
	return err
}

// deriveDuckDBS3Settings translates the S3Client's Options into
// DuckDB httpfs SET statements. Only emits settings for options
// the caller has explicitly configured; defaults stay DuckDB's.
//
// Credentials are deliberately NOT derived: SDK credential
// providers can rotate (IAM role refresh, SSO session expiry),
// but DuckDB's httpfs has no refresh callback. Users with
// rotating credentials should CREATE SECRET (...) PROVIDER
// credential_chain via ExtraInitSQL so DuckDB resolves
// credentials itself.
func deriveDuckDBS3Settings(client *s3.Client) []string {
	opts := client.Options()
	var stmts []string

	if opts.BaseEndpoint != nil {
		if u, err := url.Parse(*opts.BaseEndpoint); err == nil {
			host := u.Host
			if host == "" {
				// URL without a scheme parses as path-only; fall
				// back to the raw value so MinIO-style
				// "minio:9000" strings still work.
				host = *opts.BaseEndpoint
			}
			stmts = append(stmts,
				fmt.Sprintf("SET s3_endpoint='%s'",
					sqlEscape(host)))
			if u.Scheme == "http" {
				stmts = append(stmts, "SET s3_use_ssl=false")
			}
		}
	}
	if opts.Region != "" {
		stmts = append(stmts,
			fmt.Sprintf("SET s3_region='%s'",
				sqlEscape(opts.Region)))
	}
	if opts.UsePathStyle {
		stmts = append(stmts, "SET s3_url_style='path'")
	}
	return stmts
}

// sqlEscape escapes apostrophes for SET-statement string
// literals. Not strictly needed for hostnames/regions that don't
// contain quotes, but defensive against unexpected input.
func sqlEscape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
