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
  (full or incremental), and creation time.
- Work across two sub-tabs, **New Backup** and **Existing Backups**. The panel
  remembers which sub-tab was last open (a durable UI preference), so it reopens
  where you left it.
- **New Backup**: pick the scope by clicking trees in the tree list; each click
  adds the tree to an *Included in backup* list, one line per tree, with an *x*
  to its left to remove it again. Selecting a single tree is a single-tree
  capture; selecting more than one tree captures a **backup set** - one member
  backup per tree under one set manifest, always at a shared cross-tree
  consistency fence (multiple trees imply cross-tree consistency, so there is no
  separate toggle). Choose Full or Incremental with the kind radios; the
  base-backup dropdown appears only when Incremental is selected and lists the
  existing full backups. A single **Backup** button dispatches by the selected
  kind and selection.
- Optionally **schedule a recurring backup** while creating one: tick *Schedule
  recurring* and pick an interval in hours and minutes. Clicking **Backup** then
  both captures immediately and registers a recurring schedule for the selected
  tree, following the chosen kind (full or incremental). The schedule is a
  first-class runtime registration - an Orleans reminder that survives silo
  restarts - and overrides the startup-configured cadence for that kind. An
  interval below the scheduler minimum (one minute) is clamped up. Scheduling
  targets a single tree, so it is unavailable for a multi-tree backup set.
- On a successful capture the panel switches to **Existing Backups** and
  highlights the backup that was just created.
- **Existing Backups**: click a row to select it; its restore and delete
  controls appear only while the row is selected. A filter row above the list
  narrows it by kind and scope (each a drop-down of just the values actually
  present), and by name and creation time (starts-with text boxes with the same
  debounce and clear button as the tree key search). The list is ordered
  newest-first and shown one page at a time. Restore a backup into a target
  tree, choosing the restore mode from the dropdown: **Repair missing items
  (non-destructive)** (in-place) or **Point-in-time replace (destructive)**
  (shadow-cutover). The two modes are explained below. Delete a backup behind a
  confirmation prompt that warns the action cannot be undone.

### Choosing a restore mode

The dropdown offers two modes with very different semantics:

- **Repair missing items (non-destructive)** - the in-place mode. The backup is merged
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

## Filtering, sorting, and paging the list

The Existing Backups filters, the newest-first ordering, and the paging are all
evaluated on the server, not by fetching the whole catalog and trimming it in
the browser. To keep that efficient no matter how many backups have
accumulated, the backup service maintains a catalog **index** that keeps the
list query fast: only the rows that match the active filter are read, already in
newest-first order, one page at a time. The index is maintained automatically
and kept in step with the catalog; you do not create, refresh, or manage it. It
can be turned off in configuration, in which case the same list is served by a
slower full scan with identical results.

## See also

- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) - the backup
  control facade, including its read-only capability probe.
- [`Orleans.Lattice.Api.Backup.Grpc`](../lattice.api.backup.grpc/README.md) - the
  gRPC binding and typed client the Explorer drives.
