# Managing backups from the Explorer

The Orleans.Lattice Explorer has a top-level area switcher above the per-tree
detail tabs. It starts with two areas - **Explore** (the tree browser) and
**Backups** (backup and restore management) - and is built so a future area can
join the switcher without reworking the shell.

## The area switcher

The switcher is the app-level navigation tier. It is deliberately separate from
the per-tree detail tabs (Metrics, Topology, Data, History), which live inside
the Explore area and describe a single tree. Selecting an area swaps the whole
working surface: Explore shows the navigation rail and the selected view;
Backups shows the backup catalog and its controls.

Areas are registered in one place, so adding a new area is a single-entry change
rather than a shell rewrite. Each registered area carries its display label and
an advisory rule that decides whether the area is currently available to the
connected user.

## Capability-aware, grey-out not hide

An area or action the connected user cannot use is shown **disabled (greyed
out)**, not hidden. The Backups area entry is enabled when the connected
endpoint reports at least list / read backup access; otherwise it stays visible
but greyed, so the user can see the capability exists without being able to
enter it. Inside the Backups area, the capture and incremental controls enable
or disable from the capability report for the scope(s) selected for capture,
while each listed backup's own restore and delete buttons gate on that backup's
scope - so a backup the caller may read and restore is actionable regardless of
what scope is currently selected for a new capture.

The capture-scope capability report is gathered on demand for the selected
scope; each listed backup's scope capability is probed when the list loads and
cached per tree for the session, and the cache is cleared on an explicit
refresh so a permission change is picked up - never re-probed on every render.

## Advisory, not a security boundary

The grey-out is a usability affordance only. The **server remains the
fail-closed enforcement point**: every real backup or restore action is
authorized on the server when it runs, regardless of what the cached capability
report said. If the report was over-optimistic - for example the grant changed
after it was cached - the action still fails closed on the server, and the
Explorer surfaces a clean "not permitted" message rather than an unhandled
error. The capability probe itself has no side effects; it never captures,
restores, or deletes anything.

## What the Backups area can do

- List the backups visible to the connected user, with their scope, kind
  (full or incremental), and creation time. Each row's id carries a copy button.
- Select one or more trees to capture. A single **Backup** button dispatches by
  the selected kind and selection: a full capture of one tree, an incremental
  layered on a chosen base backup (single tree only), or - when more than one
  tree is selected for a full capture - a single **backup set** that captures one
  member backup per tree under one set manifest, optionally with a shared
  cross-tree consistency fence.
- Choose the incremental base from a dropdown of the existing full backups.
- Restore a backup into a target tree, choosing the restore mode from the
  dropdown: **Repair missing (non-destructive)** (in-place) or **Point-in-time
  replace (destructive)** (shadow-cutover). The two modes are explained below.
- Delete a backup, behind a confirmation prompt that warns the action cannot be
  undone.

### Choosing a restore mode

The dropdown offers two modes with very different semantics:

- **Repair missing (non-destructive)** - the in-place mode. The backup is merged
  into the live tree by last-writer-wins. Every restored entry keeps its original
  hybrid-logical-clock timestamp from when it was captured, and any entry already
  present in the tree was necessarily written *later* than the backup was taken.
  Because last-writer-wins keeps the entry with the higher timestamp, a restored
  entry can never out-rank anything currently in the tree: it can only ever fill a
  slot that is *absent*. So this mode is purely additive - it heals keys that are
  missing (lost, corrupted, or deleted and already reaped) and **never clobbers a
  newer live write**. It is safe to run online, alongside live traffic, and can be
  repeated without harm. It is *not* a rollback: a key whose value changed after
  the backup keeps its newer value, and a key deleted after the backup stays
  deleted until its tombstone is reaped.
- **Point-in-time replace (destructive)** - the shadow-cutover mode. Instead of
  merging, it builds a fresh shadow tree from the backup while live traffic keeps
  running, then atomically swaps the tree registry alias so the logical tree
  points at the shadow. A reader sees the whole old tree or the whole new tree,
  never a mix. Because it replaces rather than merges, it does not fight
  last-writer-wins against live data, so the restored tree holds *exactly* the
  backup contents and all writes made after the backup are dropped. This is the
  true point-in-time-recovery path. The previous physical tree is retained, so the
  restore is revertible.

Rule of thumb: reach for **Repair missing** to fill gaps in a running tree
without risking newer data; reach for **Point-in-time replace** to roll a tree
back to exactly how it looked when the backup was taken.

Backups are always enumerated through the backup control API, so a backup whose
scope the caller may not read never appears in the list. Every action reports
its outcome inline, and a server denial is shown as a "not permitted" affordance
rather than an error.

When a restore fails a precondition rather than an authorization check, the
outcome carries the server's reason. A common misconfiguration is a coordinated
(replicated-tree) restore whose backup sink is not actually shared across every
cluster: the peer that did not capture the backup cannot resolve it, so the
saga aborts. Rather than an opaque internal error, the Explorer surfaces a
friendly message explaining that the backup store must be reachable from every
cluster - a configuration problem for the operator to fix, not a transient
failure to retry.

## See also

- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) - the backup
  control facade, including its read-only capability probe.
- [`Orleans.Lattice.Api.Backup.Grpc`](../lattice.api.backup.grpc/README.md) - the
  gRPC binding and typed client the Explorer drives.
