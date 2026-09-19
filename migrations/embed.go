// Package migrations carries the policy database's schema, embedded.
//
// The .sql files live at the repository root rather than beside the code that
// applies them, because they are the thing an operator reads first and a
// reviewer diffs most carefully. This file is what lets the binary carry them:
// `go:embed` cannot reach a parent directory, so the embed has to live here.
package migrations

import "embed"

// FS holds every migration, in order.
//
//go:embed *.sql
var FS embed.FS
