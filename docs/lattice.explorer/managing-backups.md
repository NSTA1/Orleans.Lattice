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
enter it. Inside the Backups area, the capture, incremental, restore, and delete
controls enable or disable per scope from the same advisory capability report.

The capability report is gathered once after sign-in or reconnect and cached for
the session, then refreshed when the authentication changes - never re-probed on
every render.

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
- Capture a full backup of a scope, or an incremental backup layered on a base
  backup.
- Restore a backup into a target tree.
- Delete a backup.

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
