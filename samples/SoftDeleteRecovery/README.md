# SoftDeleteRecovery

## What it shows

`DeleteTreeAsync` is a **soft delete**: the tree is immediately made
inaccessible (reads and writes throw `InvalidOperationException`) but its data
is retained for a configurable grace window. During that window
`RecoverTreeAsync` brings the tree - and all its data - back. This sample
deletes a populated tree, shows reads are blocked, recovers it with data intact,
then contrasts that with `PurgeTreeAsync`, which destroys the data immediately
and makes recovery impossible.

## Run it

```
dotnet run --project samples/SoftDeleteRecovery
```

## Expected output

```
Silo starting... ready.

Seeding tree 'customers' with 5 keys...
  accessible = True, count = 5

Soft-deleting the tree (DeleteTreeAsync)...
  accessible after delete = False (expected False)
  attempting a read... blocked: This tree has been deleted and is no longer accessible.

Recovering the tree (RecoverTreeAsync)...
  accessible after recover = True (expected True)
  count after recover      = 5 (expected 5)
  customer:3               = "name-3" (data intact)

Now permanently destroying the tree (DeleteTreeAsync + PurgeTreeAsync)...
  attempting RecoverTreeAsync after purge... refused: Cannot recover a tree whose data has already been purged.

Done: soft-delete blocked access and was reversible; purge was permanent.
```

## When to use

- Guarding against accidental deletion: a soft delete with a retention window
  (`LatticeOptions.SoftDeleteDuration`, default 72 hours) gives operators time to
  undo a mistaken `DeleteTreeAsync`.
- Decommissioning a tree while keeping a safety net until you are certain the
  data is no longer needed.
- Use `RecoverTreeAsync` any time before the grace window elapses and purge
  begins.

## When not to use

- When you must reclaim storage immediately and are certain recovery will never
  be needed: `PurgeTreeAsync` (shown at the end) bypasses the grace window and
  destroys the data now. It is irreversible - recovery after purge is refused.
- Per-key removal - use `DeleteAsync` / `DeleteRangeAsync`; tree deletion is for
  disposing of an entire tree.

## Feature doc

- [Tree Deletion](../../docs/lattice/tree-deletion.md)
