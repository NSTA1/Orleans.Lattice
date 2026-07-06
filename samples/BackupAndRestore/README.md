# BackupAndRestore

Demonstrates the `Orleans.Lattice.Backup` surface end to end against a single
in-process silo using the default in-cluster backup sink.

## What it shows

1. Registering a backup **scope** and a full + incremental **schedule** with
   `ILatticeBackupScheduler.EnsureScheduleAsync`.
2. Seeding a tree with keys.
3. Triggering a **full** backup on demand
   (`ILatticeBackupScheduler.TriggerFullBackupAsync`).
4. Mutating the tree (change, add, and delete keys).
5. Triggering an **incremental** backup layered on the full base
   (`ILatticeBackupScheduler.TriggerIncrementalBackupAsync`).
6. Listing the backup catalog with `ILatticeBackupCatalogStore.ListAsync`.
7. Restoring the latest backup - base full plus increment folded into one
   faithful image - into a brand-new tree with
   `ILatticeBackupRestoreService.RestoreAsync`.
8. Printing the restored values next to the live values and a small inventory
   summary computed from the public catalog store.

The daily-full / hourly-incremental schedule reminders are registered to show
the API, but the captures are driven synchronously through the scheduler's
on-demand trigger methods so the sample runs to completion deterministically
rather than waiting on the one-minute reminder floor.

## Run it

```bash
dotnet run --project samples/BackupAndRestore
```

Everything runs in memory (in-memory grain storage, in-memory reminders, and the
default in-cluster sink), so nothing is written to disk and no external services
are required. Each run starts from an empty cluster.

## Where to look next

- `ILatticeBackupScheduler` - trigger backups on demand, register schedules, and
  prune a scope's chain.
- `ILatticeBackupCatalogStore` - enumerate the catalogued backups.
- `ILatticeBackupRestoreService` - restore a backup (and its base chain) into a
  target tree, or revert a shadow-cutover restore.
- The `orleans.lattice.backup.*` metrics (on the core `orleans.lattice` meter)
  report capture / restore durations, bytes and entries processed, failures by
  phase and reason, and live inventory gauges.
