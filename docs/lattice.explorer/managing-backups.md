# Managing backups from the Explorer

The **Backups** area is the Explorer surface for listing, capturing, restoring, checking, scheduling, and maintaining Lattice backups. It drives the backup control facade. The Explorer probes capabilities to shape the UI, but the cluster authorizes every real operation when it runs.

Backups is tenant-scoped. In a tenant-rooted Explorer, caller-supplied tree names are scoped to the active tenant before the cluster authorizes or acts on them. Backups that the caller may not read are not listed.

## Availability

Backups is visible when the backup capability probe says the caller can list backups for the area-level sentinel scope. A definite answer is remembered for the circuit. A fault is not remembered, so the next navigation asks again.

The area is unavailable, not hidden, when the cluster serves backup control but the caller holds no backup grant. The exact sentence shown is:

> You do not hold a backup grant on this cluster. Ask an administrator for one.

A denial from the probe is shown the same way. The area is hidden when the cluster does not serve backup control, the backup control facade is absent, the connection is not configured, or the probe faults.

Backup page actions also use per-scope probes. If a probe denies a page action, the control is replaced with text such as **You may not restore this backup's tree**, **You may not delete this backup**, or **You may not schedule backups of this tree**. If the server denies an operation anyway, the page shows:

> You are not permitted to do this. Ask an administrator for the backup grant on this tree.

Other faults are shown as plain sentences, including missing backups, unsupported in-process maintenance operations, unreachable cluster, invalid requests, restore-validation failures, and the special message for a multi-cluster restore whose backup store is not shared by every cluster.

## Addresses

The Backups area is tenant-scoped. These route forms exist in the shipped pages:

| Page | Plain address | Tenant-rooted form | Notes |
| --- | --- | --- | --- |
| Catalogue | `/backups` | `/t/{tenant}/backups` | Lists backups newest first. Query keys: `kind`, `name`, `tree`. |
| Backup details | `/backups/{backupId}` | `/t/{tenant}/backups/{backupId}` | Describes one backup and offers export, restore, and delete. |
| Capture | `/backups/new` | `/t/{tenant}/backups/new` | Captures a full, incremental, or set backup. `?tree={tree}` seeds the tree field. |
| Schedules | `/backups/schedules` | `/t/{tenant}/backups/schedules` | Manages a tree's full and incremental schedules. Query key: `tree`. |
| Health | `/backups/health` | `/t/{tenant}/backups/health` | Lists backup health where health monitoring applies. `?backup={id}` focuses one backup. |
| Maintenance | `/backups/maintenance` | `/t/{tenant}/backups/maintenance` | Rebuilds and checks the catalogue as cluster operations, and shows the latest run of each. |
| Operation status | `/backups/operations/{operationId}` | `/t/{tenant}/backups/operations/{operationId}` | Shows a backup or restore operation tracked by the cluster, or a staged operation started in this Explorer circuit. |

## Navigation row

The Catalogue, Schedules, Health and Maintenance pages share the Backups page row:

- **Catalogue**;
- **Schedules**;
- **Health**, only when health monitoring is available for this deployment;
- **Maintenance**.

The Health address is not found when health monitoring is unavailable.

A page below them (capturing a backup, one backup's page, and an operation's status page) is none of the row's pages, so instead of a row with nothing selected it shows a **Back to the catalogue** link.

## Catalogue

The Catalogue page lists the newest backups first, 25 at a time. Filters are sent to the server:

- `kind=full` or `kind=incremental`;
- `name={prefix}` for backup-name prefix;
- `tree={treeId}` for one tree.

The **Filter by tree** field is a [picker](navigation-model.md#pickers) that suggests the trees you can reach but accepts any name, and Enter applies it.

When inventory extensions are served, the lede summarises total backups, full and incremental counts, catalogue bytes, and newest backup time. If inventory is not served, the area falls back to the newest catalogue row. A selected tree filter also shows schedule status for that tree: full and incremental schedule registration, last successes, last scheduled run outcome, and chain depth, with a link to that tree's Schedules page.

Rows link to the Backup details page. When health monitoring is available, rows also show the latest stored health report as a health pill. The page lists the caller's 10 most recent backup and restore operations as the cluster tracks them - so an operation started in another tab or before a reload is listed - each with its state and, while it runs, its phase and progress in real units. A denied listing is hidden; any other fault says the operations could not be listed. Staged operations from the current circuit that never reached the cluster are listed separately.

## Capture

The Capture page can start three staged operations:

- **Full** captures a whole tree, a key prefix, or one key.
- **Incremental** captures changes since a selected full backup. The page can find up to 50 newest full backups for the named tree and requires a base before capture.
- **Set of trees** captures one full backup per tree under one set manifest. The set can be captured at one cross-tree consistency fence.

A capture requires a name. A full or incremental capture requires a tree, and a prefix or key when that scope is selected. A set requires at least one tree. The **Tree** and **Tree to add** fields are pickers that accept only a tree you can reach; the key or prefix is typed. Submitting starts a staged operation and navigates to `/backups/operations/{id}`. Its first stage checks access; its second starts the capture on the cluster, which accepts it and runs it in the background. The page then hands off to the cluster operation's own address.

A succeeded capture links to the captured backup pages and, for a set, reports the number of backups captured.

## Backup details

The Backup details page describes one backup: id, tree, scope, kind, captured time, base backup when present, set name when present, capturing cluster, total artifact size, and health link when monitoring is available. App-owned trees are labelled with their owning app when the Apps surfaces can be read; if the app declares the tree rebuildable, the page says re-deriving may replace a restore.

The **Restore chain** section lists the backups replayed in order, oldest first. Incremental chains can therefore be restored to the selected restore point.

The **Artifacts** section lists artifact id, size, chunk count, and an **Export** action. Export streams the artifact to the browser. Export denials and faults are shown inline.

### Restore

Restore is offered only when the scope probe grants restore. The form chooses a target tree, mode, optional restore point from the chain, and, where the connection serves backup operations, a cold-restore option that reads from the backup store alone. Cold restore runs on the cluster as a tracked operation, so it is offered over the gRPC binding too. The target tree defaults to the backup's own tree and is a picker that also accepts a new name: naming an existing tree is flagged, because restoring replaces what it holds, and a new name restores into a new tree.

Both restore modes are confirmed before the operation starts:

- **Repair missing items (non-destructive)** uses in-place restore. It merges the backup into the live tree so restored entries fill missing keys while newer live writes win.
- **Point-in-time replace (destructive)** builds a fresh copy from the backup and cuts the tree over to it. Writes after the backup are dropped. The previous tree is kept so the restore can be reverted.

The confirmation is named **Restore this backup?** and requires the target tree name. If the target belongs to an app that declares the tree rebuildable, the confirmation warns that re-deriving may be a better choice.

A restore checks access and that the backup still exists, starts the restore on the cluster, and hands off to the cluster operation's status page. A finished point-in-time restore can be reverted from that status page.

### Delete

Delete is offered only when the scope probe grants delete. It opens a destructive confirmation named **Delete this backup?** and requires the backup name. The confirmation says the backup is removed from the catalogue and the backup store, unshared artifacts are deleted, incremental backups built on it can no longer be restored, and the action cannot be undone.

## Schedules

The Schedules page works on one tree, selected with `?tree={tree}` or the **Show schedules** form, whose **Tree** field is a picker that accepts only a tree you can reach. It probes the whole-tree backup scope, then shows full and incremental schedule rows when the caller may list backup status.

Each row shows whether a schedule is registered, its interval, last run, and last success. A registered schedule can be cancelled when the caller may capture that backup kind. Cancelling opens a dialog named **Cancel this schedule?** and states that existing backups are kept.

The registration form can create or change a full or incremental schedule. Its **Every** field is a [duration field](theming-and-density.md#dates-times-and-durations) in hours and minutes; it refuses a box that is not a whole number and an interval under one minute, the shortest the scheduler runs. Successful saves and cancellations reload the schedule status.

## Health

Health appears only when the backup sink is durable and external. The Health page lists the newest 25 backups and their latest stored health reports. A focused address, `/backups/health?backup={id}`, shows one backup, its latest report, **Check now**, and periodic monitoring settings.

**Check now** starts a health check on the cluster as a tracked operation and follows it on the page with the shared progress display, counting the backup's artifacts checked; when it succeeds the page reads the fresh report. The check keeps running if the tab is closed. Reopening the backup's health page while a check started from the Explorer is still running picks that check up instead of starting another; **Open the check's page** links to its operation status page.

A health report shows status, checked time, explanation, whether the manifest is present, missing or uncommitted artifacts, hash mismatches, and peer-cluster visibility when applicable. Focused health actions are offered only when the scope probe grants list authority for that backup.

Periodic monitoring can be turned on or off per backup. Its **Verify every** field is a [duration field](theming-and-density.md#dates-times-and-durations) in hours and minutes, at least one minute; the monitor applies the config on its next sweep.

## Maintenance

Rebuild and check run on the cluster as tracked operations, so they are served over the gRPC binding too. A connection that does not serve backup operations at all shows:

> This connection does not serve this operation. Run it from a silo host, where the backup control API is in process.

**Rebuild the catalogue** scans every manifest in the backup store and records it in the catalogue. It is safe to run again because existing rows are reconciled in place. Starting it opens a dialog named **Rebuild the catalogue?**.

**Check the catalogue against the store** finds catalogue rows whose backup is gone from the store. The check changes nothing. If a check finds orphan rows, **Remove orphan rows...** opens a destructive confirmation named **Remove orphan rows?**. Removing orphan rows touches only the catalogue; the store is not touched, and a rebuild restores any row whose backup reappears.

Each section shows its latest run as the cluster reports it - started from this page, another tab, or another client - with the shared progress display while it runs (manifests re-registered or rows checked, with no total, then orphan rows removed when pruning) and a one-sentence outcome linking to the run's status page. A closed tab never stops a run. **Remove orphan rows...** is offered when the latest check found orphan rows it did not remove. The page looks back through at most the caller's 200 most recent backup operations.

## Operation status pages

Captures, restores, health checks, and catalogue rebuilds and checks are accept-then-poll operations tracked by the cluster (see [Backup and restore operations](../lattice.api.backup/operations.md)). Their status page at `/backups/operations/{id}` reads the cluster's status, so it survives closing the tab, a reload, or another circuit: open the address again and it picks the operation up where it is. While the page is open it reads the status every few seconds, backing off while reads fail, and stops once the operation finishes. It shows:

- the title and a status pill (**Cancelling** once a cancel was asked for);
- while it runs, the step among the operation's phases and a progress bar over the current phase's units - determinate only when the phase total is known, otherwise the count so far, never an invented percentage;
- **Cancel operation** while it runs;
- once it stops early, the phase and units it stopped at and why;
- once it succeeds, links to what it produced, its figures, and the orphan rows a catalogue check found;
- revert controls for a completed point-in-time restore, and a link to the revert once one was made.

An operation id the caller cannot see - another tenant's, or one past its retention - is not found. The progress display is the shared `LtOperationProgress` component, so every area that runs long operations draws them the same way.

Reverts still run as staged operations in the current circuit. Their status page can be left and resumed while the circuit lives, and shows the title and start or finish time, a status pill and message, the ordered stages with the current stage marked, and facts and links.

Reverting a point-in-time restore opens a destructive confirmation named **Revert this restore?**. It says the tree returns to the copy it held before the restore and every write made since the restore is dropped. The revert itself becomes a new operation status page.

## Palette and address completions

Backups contributes one command:

| Command id | Label | Target |
| --- | --- | --- |
| `backups.capture` | `Capture backup...` | `/backups`, invoking it opens `/backups/new` |

The visible **Capture backup...** link on the Catalogue page carries the same command id.

The address line completes backups in two ways:

- `backup:{idPrefix}` and `/backups/{idPrefix}` scan catalogued ids in id order, up to 2000 scanned ids, and stop early when the id order proves no later id can match.
- free text searches backup names by prefix, newest first, using the current address-query result limit.

Only backups the caller may read are returned.

## Limits and caching

- Catalogue and Health lists show 25 rows per page.
- Incremental base lookup reads up to 50 newest full backups of the tree.
- Backup-id completions scan at most 2000 ids.
- Scope probes return no capabilities when they fault.
- Health availability is remembered once known; a fault reads as unavailable but is not remembered as a definitive true value.
- Inventory not served withdraws the inventory for the circuit; a denied inventory keeps it offered. Cold restore and catalogue maintenance do not depend on it: they follow whether backup operations are served, and only a listing refused as not served withdraws them.
- Captures, restores, health checks, rebuilds and catalogue checks run on the cluster and outlive the circuit. Staged reverts are kept in the current Explorer circuit; ending the circuit cancels one still running.
- The operation list shows the 10 most recent operations and is reused for 2 seconds per caller and tenant.

## Server authority

The backup control facade composes caller-supplied tree names through the active tenant, then uses the same effective scope for authorization and operation. Listing hides manifests the caller may not read. Capture, schedule, delete, health, export, restore, cold restore, catalogue rebuild, catalogue scrub, and revert each authorize on the server before touching data.

## See also

- [Explorer overview](README.md)
- [Navigation model](navigation-model.md)
- [Area availability](area-availability.md)
- [Areas reference](areas.md#backups)
- [Lattice Apps](lattice-apps.md)
- [Backup engine](../lattice.backup/README.md)
- [Backup API](../lattice.api.backup/README.md)
- [Backup gRPC binding](../lattice.api.backup.grpc/README.md)