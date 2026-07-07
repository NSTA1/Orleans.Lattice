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
- Restore a backup into a target tree, choosing the restore mode: **in-place**
  merges the backup into the target by last-writer-wins (writes made after the
  backup was taken survive), while **shadow-cutover** rebuilds the tree from the
  backup and swaps it in, so the restored tree holds exactly the backup contents
  - the point-in-time-recovery path that drops post-backup writes.
- Delete a backup, behind a confirmation prompt that warns the action cannot be
  undone.

Backups are always enumerated through the backup control API, so a backup whose
scope the caller may not read never appears in the list. Every action reports
its outcome inline, and a server denial is shown as a "not permitted" affordance
rather than an error.

## See also

- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) - the backup
  control facade, including its read-only capability probe.
- [`Orleans.Lattice.Api.Backup.Grpc`](../lattice.api.backup.grpc/README.md) - the
  gRPC binding and typed client the Explorer drives.
