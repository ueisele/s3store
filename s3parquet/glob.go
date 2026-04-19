package s3parquet

import "github.com/ueisele/s3store/internal/core"

// readPlan is a package-local alias to keep call sites terse;
// the plan logic itself lives in internal/core (shared with
// s3sql). Exactly the same shape, no wrapping.
type readPlan = core.ReadPlan

// buildReadPlan forwards to core.BuildReadPlan. Kept as a
// package-local name so the existing s3parquet call sites don't
// have to reach across the module for every invocation.
var buildReadPlan = core.BuildReadPlan

// hiveKeyOfDataFile forwards to core.HiveKeyOfDataFile.
var hiveKeyOfDataFile = core.HiveKeyOfDataFile
