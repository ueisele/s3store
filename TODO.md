# TODO

## Error-message convention cleanup

The package's error chain currently produces strings of the shape
`"s3store: outer: s3store: inner"` because both leaf errors and
wrap layers prefix the package name. Pick one convention across
the package:

- **(a) Leaves keep `s3store:`, wrap layers drop it.** New wrap
  shape: `fmt.Errorf("head token-commit: %w", err)`. Matches the
  existing `"WithIdempotentRead: %w"` site (option-validation
  wrap in `idempotency.go`) — that one already follows the
  convention, so picking this direction means most leaves are
  already correct and most wrap sites need a touch.
- **(b) Wrap layers keep `s3store:`, leaves drop it.** New leaf
  shape: `errors.New("IdempotencyToken must not be empty")`.
  Touches every leaf — `validateIdempotencyToken`,
  `headTokenCommit`, `readTokenCommitMeta`, `loadDurationConfig`,
  `validatePartitionKeyParts`, every `validateProjectionDef*`,
  etc. Larger blast radius.

(a) is cheaper (most leaves are already in shape) and keeps the
package name visible at the deepest point of the chain (where it
identifies the source most usefully). Recommend (a). ~30–50 sites
in source + a similar count in test assertions. Mechanical edit;
no semantics change. Verify with all four gates.
