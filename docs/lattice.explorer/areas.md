# The Explorer areas

The Explorer is made from nine built-in areas. Each area owns one top-level
address, reads only the cluster facades named below, and appears in the spine
only after its availability probe succeeds or returns a user-actionable
Unavailable reason. Every field below that names an existing tree, region, subject,
tenant or key is a type-ahead [picker](navigation-model.md#pickers).

| Area | Key and root address | Facades read or driven | Scope | Visibility probe |
| --- | --- | --- | --- | --- |
| Data | `data`, `/data` | State API, including `ILatticeStateClient`; `ILatticeTreeAdmin` for tag-index and view actions | Tenant-scoped | Reads one tree-catalogue page. No state reader, no served state API, or a permission denial makes the area Hidden. A disconnected first load is Unavailable with a sign-in or connect reason; other catalogue faults are Unavailable with a fixed retry sentence. |
| Apps | `apps`, `/apps` | `ILatticeAppWorkspace`, `ILatticeAppsControl`, `ILatticeAppCatalog`, and `ILatticeAuthAdmin` for role binding | Tenant-scoped | Probes workspace apps, catalogue capabilities and control capabilities. It is Visible when the caller has a workspace answer, can browse the catalogue, or can list installed apps. Probe faults and missing facades deny the relevant flags, so a head serving none of them is Hidden. |
| Access | `access`, `/access` | `ILatticeAuthAdmin` | Cluster-wide | Reads the smallest group catalogue page. A successful page makes the area Visible. A missing facade hides it. An anonymous denial is Unavailable with "Sign in to administer access on this cluster."; a signed-in denial or any other fault hides it. |
| Schema | `schema`, `/schema` | `ILatticeSchemaControl` | Tenant-scoped | Probes schema capabilities against a reserved, side-effect-free tree id. Any schema grant makes the area Visible. A refused anonymous caller sees Unavailable with "Sign in to manage schema on this cluster."; a signed-in refusal, missing facade, unserved cluster or fault is Hidden. |
| Tenancy | `tenancy`, `/tenancy` and `/t/{tenant}/tenancy` | Tenant self-service, lifecycle, access, grant, region and quota facades; apps control for installed-app counts | Mixed: the directory is cluster-wide; `/t/{tenant}/tenancy` follows the active tenant | Requires tenancy to be active and the self-service facade to exist. It proves whether the caller is an operator or administers the scoped tenant. Operators and scoped tenant admins see it; refused anonymous callers see Unavailable with "Sign in to see the tenants you administer."; refused signed-in callers and other faults are Hidden. |
| Replication | `replication`, `/replication` | `ILatticeReplicationStatus`, `ILatticeReplicationControl` | Tenant-scoped addresses, over the caller's admitted replication view | Reads the peer-status report, or falls back to the enrolment report. Status success makes the area Visible. If status fails but enrolment names at least one manageable tree, it is Visible. Missing facades, denied reads and other faults are Hidden. |
| Backups | `backups`, `/backups` | `ILatticeBackupControl` | Tenant-scoped | Probes backup capabilities against a reserved scope. `CanList` makes the area Visible. A denied or grantless caller sees Unavailable with "You do not hold a backup grant on this cluster. Ask an administrator for one."; an unserved, unconfigured, unreachable or faulted control facade is Hidden. |
| Telemetry | `telemetry`, `/telemetry` | `ILatticeTelemetry` | Tenant-scoped while tenancy is on | Reads the telemetry catalogue once. Any successful catalogue read, including an empty catalogue, makes the area Visible. A refused caller, a cluster with no telemetry facade, or a catalogue fault is Hidden. |
| Cluster | `cluster`, `/cluster` | `ILatticeTreeAdmin`, `ILatticeReplicationStatus`, and the state connection for cluster identity and tree catalogue | Cluster-wide | Requires a tree-admin facade and a configured cluster connection. It probes shallow storage usage. Success makes it Visible; a denied probe is Hidden; no connection is Unavailable with "Connect to a cluster to see its estate."; an unserved tree-admin operation is Unavailable; transient faults are Unavailable and are not remembered. |

## Data

Data is the Explorer's state browser. It has two routed pages:

| Address | What it shows |
| --- | --- |
| `/data`, `/t/{tenant}/data` | Every tree and view the caller can reach, followed, with tenancy on, by the trees other tenants share with the tenant through an approved grant. Rows show the logical name, kind, owning app, shard count, lifecycle and source view tree. A filter or column that cannot apply is left out: the **Shared by** column and the **Shared with this tenant** filter appear only where sharing applies (tenancy on, at a tenant other than the reserved `default` one), the **App** column only when some tree belongs to an app, and the **Source** column only when there is a view. The columns follow the whole listing, not the filtered rows, so typing a filter never makes them come and go. The table is virtualised and can be filtered by text and by kind: All, Trees, Views, and, where sharing applies, **Shared with this tenant**. |
| `/data/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}`, plus the tenant-rooted equivalent | One tree workspace. The route accepts up to six logical tree-id path segments. The page shows the logical id, kind, owning app link, view source, shard count and live-key count when metrics are available. |

The directory uses `?filter=` for the text filter. A tree workspace keeps state
in query keys: `?tab=` chooses one of `keys`, `history`, `metrics`,
`dead-letters`, `tag-indexes` or `views`; `?prefix=` narrows key scans and live
tails; `?key=` opens an entry; `?at=` pins History to a UTC instant;
`?index=` and `?tag=` select a tag index and tag.

The Keys tab pages through the tree by prefix or by tag. It offers Live and
Snapshot scan modes, page-size choices from the shared paging ladder, Previous
and Next page buttons, and optional live updates from the state API change
feed. If a scan cursor expires, the page offers a restart from the first page.
If a live feed is not offered, it disables live updates; if the feed moves past
the cursor, it reports that live updates stopped and offers a restart. The key
table clips long keys in cells but keeps the full key in details, and selecting
one writes `?key=` and opens the entry panel. At the expanded width the entry
stands beside the key table as a split view, kept in view while the list
scrolls; at narrower widths it opens below the table.

The entry panel reads the selected key through the state reader and renders the
value automatically, with alternate renderers when CRDT members are present. It
does not echo server fault details. Permission denials become "You do not have
permission to read this entry.", unserved operations become "This cluster does
not let you read this entry.", missing entries become "It no longer exists, or
you cannot see it.", and transient failures ask the reader to try again.

The History tab has three modes. With neither `?key=` nor `?prefix=`, it asks
for a key, in a [picker](navigation-model.md#pickers) that suggests the tree's keys
starting with what you type (one bounded prefix scan per query) and accepts any key. With `?prefix=`, it follows live changes under that prefix and keeps
at most 200 changes on screen. With `?key=`, it loads durable revisions 50 at a
time, newest first by default, with value diffs, CRDT member changes, retention
boundary notes and a live tail. A revision that kept only the value's size and
hash is marked "metadata only", and the timeline explains what that means once,
rather than under each such revision. The **As of (UTC)** field starts empty,
with a hint giving its form (`yyyy-MM-ddTHH:mm:ssZ`, empty for the latest).
`?at=` marks the revision that was in effect at the chosen UTC instant, and
disables the live tail while the point-in-time view is active.

The Metrics tab shows per-tree measures: lifecycle, shards, live keys,
tombstones, depth, shards splitting, views and view lag where the cluster
reports them. When detail metrics are paused, the affected measures read
"Paused". The Dead letters tab reads a count and then pages strict-mode dead
letters 50 at a time, with source, key, reason and clipped details. Both tabs
show fixed sentences when the state API is unavailable, denied or faulted.

The Tag indexes tab lists indexes covering the tree, their reconcile state and
covered-tree count. Selecting an index with `?index=` shows covered trees, tags,
and, with `?tag=`, member keys. Member rows link only to trees the caller can
see and say "A tree you cannot see" otherwise. The Reconcile action is shown
only when the caller can administer the index; it opens a destructive
confirmation because it reads every covered key and removes stale membership
rows.

The Views tab lists materialised views over a tree, or the view itself when the
current workspace is a view. It shows kind, projection version and apply lag.
For callers that can administer the source tree, Reconcile compares the
expected view with the live one and swaps it only when they differ; Rebuild
builds a new generation and swaps it in. Both actions are behind destructive
confirmations and both read every source key.

App-owned trees use logical ids shaped like `a/{slug}/...`. Data links their
owner badge to `/apps/{slug}`, and tag and view member rows preserve those
logical links. Physical state ids, tenant-composed ids, restore shadows and view
generation trees are not shown.

Trees shared through a cross-tenant grant (see
[Trees shared through a grant](tenant-scope.md#trees-shared-through-a-grant)) are
listed as **Shared tree** rows with their owner and access, and a shared prefix as
one **Shared prefix** row that does not open. A shared tree keeps its full id and
is addressed under the tenant's own root, as in `/t/globex/data/t/acme/orders`; its
page carries "Shared by acme" and access pills and offers no administration. Tree
pickers and address completions offer shared trees with their owner and access,
and Home's Data line counts them ("..., and 2 shared with this tenant."). When the
tenant's grants cannot be listed, the directory shows its own trees with a note.

The Data palette command is:

| Command id | Label | Effect |
| --- | --- | --- |
| `data.refresh` | Refresh the tree directory | Drops the circuit's loaded tree and view list, reloads it, and updates the directory and workspace surfaces. |

The directory remembers a successful tree/view catalogue for one caller,
endpoint and active tenant. It forgets the list when sign-in, endpoint or active
tenant changes. A load follows at most 400 catalogue pages of 500 entries each,
and a view catalogue denial leaves views absent rather than hiding the trees.

## Apps

Apps is the first-class surface for Lattice Apps: "Your apps", the source
catalogue, consent review and app lifecycle. It reads the app workspace,
catalogue and control facades, with auth used only when role bindings are
reviewed, and it appears when at least one of those views is available to the
caller. Its commands are `apps.install`, plus per-app `apps.upgrade.{slug}` and
`apps.disable.{slug}` entries when the caller can perform those lifecycle
actions. See [Lattice Apps](lattice-apps.md) for the app frame, catalogue,
review and lifecycle flows.

## Access

Access is the cluster-wide policy area for auth rules, groups and explaining an
access decision. It is visible only to callers who can read the group catalogue
through `ILatticeAuthAdmin`; anonymous refusal stays visible as an Unavailable
sign-in prompt, while signed-in refusal hides the area. Its commands are
`access.explain`, `access.create-rule` and `access.create-group`. See
[Managing access](managing-access.md) for rule editing, group management and
explain output.

## Schema

Schema covers schema policy, version configuration, compliance, remediation and
dead letters for each tree the caller can manage through `ILatticeSchemaControl`.
The area-wide probe asks for capabilities on a reserved tree id and admits the
area only when at least one schema capability is present. Its commands are
`schema.scan-compliance` and `schema.all-trees`. See
[Managing schema](managing-schema.md) for the directory, tree pages and
operation model.

## Tenancy

Tenancy has two roots: `/tenancy` for an operator's tenant directory and
`/t/{tenant}/tenancy` for the active tenant administration view. Either way one
tenant is shown under the same tabs: Overview, Members, Quota, Regions and
Sharing. It requires the
tenant self-service facade and tenancy to be active, then proves whether the
caller is an operator or administers the scoped tenant. Operators get the
`tenancy.create-tenant` and `tenancy.set-regions` ("Set a tenant's regions",
`/tenancy?set-regions=true`) commands; whoever administers a non-default scoped
tenant gets `tenancy.change-residency` ("Change residency") and
`tenancy.offer-grant`.

A tenant's Regions page splits **Allowed regions (set by a platform operator)**
from **Residency (where the tenant's data is kept)**, and says what each region's
lifecycle status means for the tenant: a Provisioning region waits for a platform
operator of the hosting deployment to promote it, and once a tenant has any
residency it is served only in Online regions. A change that would leave no
Online region is confirmed first, and so is creating a tenant with an initial
residency. The directory's **Resident in** column, and the **Resident in** and
**Allowed** lines of a tenant's overview, link to its Regions page, and Home
counts tenants with no residency set, reading at most 50 tenants. See
[Regions and residency](tenant-scope.md#regions-and-residency).

See [Tenant scope](tenant-scope.md) for re-rooting, tenant selection, grants,
regions and quota.

## Replication

Replication shows the estate's peer links, enrolled trees and one tree's links.
It reads `ILatticeReplicationStatus` for peer status and
`ILatticeReplicationControl` for enrolment and enable/disable actions.

| Address | What it shows |
| --- | --- |
| `/replication`, `/t/{tenant}/replication` | The estate map and link table, one row per tree, peer and direction. |
| `/replication/trees`, `/t/{tenant}/replication/trees` | Replicated trees the caller may manage, with state, merge mode, enrolment source, owner and link health. |
| `/replication/trees/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}`, plus the tenant-rooted equivalent | One tree's per-peer links, refreshed while the browser page is visible. |

The estate and trees pages share `?health=`, `?region=` and `?app=` filters.
`?health=` accepts the known health labels and ignores unknown values rather
than emptying the page. `?region=` filters peer regions. `?app=` matches trees
whose logical id is `a/{slug}/...`.

The estate page draws this region and its peer regions as an order diagram, then
shows every link in a sortable table. Refreshing runs the `replication.refresh`
command or the visible Refresh button, invalidates the cached read and reads the
peer report again. If the status read is denied, unserved or faulted, the page
shows a fixed empty state and still links to the enrolled-trees page, because an
operator may be allowed to manage enrolment without reading the full status
estate. A truncated status read says so and recommends filtering.

The enrolled-trees page reads enrolment and status together. The table shows
enabled, disabled or ambiguous state, merge mode, whether enrolment is runtime,
static or both, app ownership and worst link health. A caller with the control
facade and a readable enrolment report can enable replication for a new tree or
for a disabled runtime tree. Enabling asks for the logical tree id (a picker
that accepts only a tree you can reach), merge mode and optional bootstrap source
cluster (a picker that suggests the known regions and accepts any id); app-owned ids are rejected because their
enrolment follows the app install. Disabling is a destructive confirmation: it
stops new changes from replicating, leaves peer data in place, and keeps the
merge mode for a later enable. Static-only enrolments cannot be toggled here.

The estate and enrolled-trees pages share a section row, **Estate** and
**Enrolled trees**. One tree's page is below both, so in place of the row it
shows a **Back to enrolled trees** link.

The tree detail page reads one tree's links afresh, then starts a visibility
aware refresh loop after the first browser render. While the page is visible it
refreshes every 5 seconds; when the tab is hidden it stops; when it becomes
visible it refreshes immediately and resumes. If a later refresh fails, the page
keeps the previous links and reports that the last refresh failed. If neither
the enrolment report nor the status links name the tree, it navigates to not
found.

Reads of the estate and enrolment are cached per circuit for 15 seconds for
directory, Home, completion and non-refresh page reads. Sign-in and connection
changes invalidate both caches. Peer-status reads ask for pages of 1000 links
and follow at most 50 pages; repeated continuation tokens also stop the read, so
a broken server cannot loop the UI forever.

The Replication palette commands are:

| Command id | Label | Effect |
| --- | --- | --- |
| `replication.refresh` | Refresh replication status | Invalidates the peer-status cache and re-reads visible replication status. |
| `replication.trees` | Show enrolled trees | Opens `/replication/trees`. |

App-owned trees are detected only from the logical id prefix `a/{slug}/...`.
Rows link to `/apps/{slug}/replication`, and the Replication area does not offer
per-tree enable or disable controls for them.

## Backups

Backups covers backup catalogue, capture, restore, schedules, health and
catalogue maintenance through `ILatticeBackupControl`. It appears when the
capability probe says the caller can list backups; a denied or grantless caller
sees an Unavailable grant sentence, while an unserved or faulted backup control
surface is Hidden. Its palette command is `backups.capture`, labelled "Capture
backup...". The Catalogue, Schedules, Health and Maintenance pages share a page
row; a page below them (capturing a backup, one backup, one operation) shows a
**Back to the catalogue** link instead. See [Managing backups](managing-backups.md) for backup scopes,
capture, restore and maintenance.

## Telemetry

Telemetry lists the metric catalogue as boards, then draws one board as charts
or tables. It uses `ILatticeTelemetry` for the catalogue and each query.

| Address | What it shows |
| --- | --- |
| `/telemetry`, `/t/{tenant}/telemetry` | The board list resolved from the catalogue the caller may read. |
| `/telemetry/{board}`, `/t/{tenant}/telemetry/{board}` | One board, such as `throughput`, `latency`, `storage`, `pressure`, `tenant` or `other`. |

The address carries every chart state. `?range=` names a relative range such as
`15m`, `1h`, `6h`, `24h` or `7d`; the parser accepts positive `s`, `m`, `h` and
`d` tokens up to 400 days. `?from=` and `?to=` pin an absolute UTC window and
override `?range=`. `?step=` names an explicit step, with toolbar choices from
15 seconds to 1 hour; without it, charts choose the finest ladder step that
keeps the request within the chart point budget and the target of about 240
points. `?tree=` narrows charts that accept a tree filter and leaves a note on
charts that do not. `?scope=all` asks for every tenant when tenancy is active.
`?view=table` draws time series as tables rather than charts.

The built-in boards are Throughput, Latency, Storage and Pressure. The Tenant
board is listed only while tenancy is on. An Other board appears when the
catalogue contains queries not claimed by a curated board. A board with no
available charts is omitted from the index; a board that has some missing charts
shows how many of its expected charts were admitted and names the omitted ones.
An empty catalogue is not an error: the page says the cluster offers no
telemetry queries to the caller, either because no backend is configured or
because grants and the metric allow-list admit none.

Chart requests are evaluated one by one. A permission denial or missing query
removes that chart from the board and adds a "Not shown" note. Other failures
stay on the chart: bounds failures ask for a shorter range or coarser step,
backend and transient transport failures offer a retry, an unserved telemetry
facade says the cluster does not serve telemetry queries, and invalid arguments
say the cluster refused the parameters. A successful chart reports the tenant
scope the facade actually applied; if the user asked for every tenant but the
cluster answered for the active tenant, the board says so.

The chart view is an SVG line chart with keyboard readout. Left and right arrows
move through points, Home and End jump to the ends, and Escape clears the pinned
readout. Table view shows the values by time. Per-tree chart labels link to the
same board with `?tree=` set, and the tree filter note links to the Data area for
the logical tree.

The Telemetry palette command is:

| Command id | Label | Effect |
| --- | --- | --- |
| `telemetry.refresh` | Refresh telemetry | Drops the shared catalogue, re-reads it, redraws every chart and asks the area directory to re-probe. |

The catalogue read is shared per circuit by the availability probe, Home status,
completions and pages. Sign-in changes, connection changes and Refresh
invalidate it. A caller cancellation only stops that caller waiting; the shared
read carries on for the next reader.

## Cluster

Cluster is the cluster-wide operations area. It never follows the active tenant,
although the router accepts tenant-rooted forms so the layout can redirect them.
It reads `ILatticeTreeAdmin` for administration, the state connection for
cluster identity and tree catalogue, and `ILatticeReplicationStatus` for the
region diagram.

| Address | What it shows |
| --- | --- |
| `/cluster` | Estate overview: cluster id, service id, storage use, region diagram and links to Trees, WAL placement and Orphaned leaves. |
| `/cluster/trees` | Every logical tree in the cluster, with owner, shard count, WAL partitions and lifecycle. |
| `/cluster/trees/{tree-path}` | One tree's summary, configuration, shards, storage and lifecycle tabs. |
| `/cluster/trees/{tree-path}/tools` | Compaction, projection digest and bulk load tools. |
| `/cluster/trees/{tree-path}/reshard` | Resumable online reshard status and staging. |
| `/cluster/trees/{tree-path}/resize` | Resumable online resize status, staging and undo. |
| `/cluster/trees/{tree-path}/snapshot` | Resumable snapshot status and staging. |
| `/cluster/wal?tree=&partition=&target=` | WAL placement audit, move planning, execution and source reclaim. |
| `/cluster/orphans?tree=` | Orphaned-leaf survey, audit and repair. |

The route under `/cluster` accepts at most eight path segments after `cluster`.
The Trees segment itself counts, so a tree id that is too deep is still listed
but has no Cluster address. If a tree id's last segment is a view word such as
`tools`, the overview link adds a trailing `overview` segment so the route is
unambiguous.

The overview reads cluster identity and shallow storage usage in parallel. The
storage card can refresh the shallow summary or open a destructive confirmation
for a deep re-measure. Deep re-measure walks every leaf of every shard of every
tree, changes nothing, and is described as expensive. The region diagram rolls
up the replication peer report, draws the local region and peers, marks stalled
peers without relying on colour alone, and links to Replication.

The tree list is filtered by tree name, app or tenant. It filters out restore
shadows and physical resize or restore targets, so rows show logical trees only.
App and tenant ownership are parsed from `t/{tenant}/a/{app}/...` and shown as
text. The visible `cluster.reshard-tree` control opens a dialog whose tree field
is a picker over the cluster's logical trees, and navigates to that tree's reshard
page. Every Cluster field that names an existing tree (the WAL and orphaned-leaf
audits, the alias target, the reshard dialog) accepts only a listed tree, while a
snapshot's destination is suggested against the existing trees and refused if it
already exists.

A tree page begins with a side-effect-free capability probe. If the caller has
no tree-admin capability over the tree, it shows "Nothing you can administer".
If diagnostics or admin authority allow it, the page checks the tree
configuration and sends not-found when the tree does not exist. Its tabs show:
summary statistics and operation status; configuration and history retention;
shard map, diagnostics and hotness; storage and WAL placement; and lifecycle.
Denied probes become an all-deny answer, so controls stay hidden even though
the cluster still authorises every real operation when attempted.

Configuration and history-retention saves are forward-only configuration
changes, so they do not ask for destructive confirmation. Lifecycle operations
do: Delete soft-deletes the tree and schedules purge, Recover restores normal
reads and writes, Purge permanently removes tree state and is not an app
operation, and Set alias points the logical name at another physical tree.
**Recoverable until** is shown with the recovery deadline only while the tree can
still be recovered. Purge is accept-then-poll: the call can return while the shard
walk is still running, so the tab says the purge was accepted, follows it with a
"N of M shards purged" bar, and says the tree is purged only once the status
reports the purge complete (or warns if it stopped before finishing). The
Shards tab can run a confirmed deep read to count tombstones because it walks
every leaf. The Storage tab links to the WAL page.

The tools page exposes only the tools the capability probe admits. Compaction
requires admin authority, asks for a shard index, and opens a destructive
confirmation because it forces an out-of-cycle tombstone pass. Projection digest
requires diagnostic authority and reads a shard hash for replica comparison.
Bulk load requires the bulk-load grant, accepts strictly ascending `key=value`
lines, sends chunks of 256 entries, and can resume from the first
unacknowledged chunk under the same operation id after a failure.

Reshard, Resize and Snapshot are resumable operation pages. They read current
status, stage user input, show a Review section and then require destructive
confirmation before starting. Reshard only grows, with a maximum target of 4096
physical shards. Resize rebuilds the tree into a shadow at the requested node
capacity, can be undone while the old tree remains recoverable, and warns that
undo loses writes that reached only the resized copy. Snapshot copies live
entries into a new tree, either online or offline, and allows optional sizing.

These operations, like purge below, are accept-then-poll: the cluster accepts the
request and runs it on its own, reminder-anchored, so it survives a silo restart
and carries on if you leave the page. While one is running, its page asks for
status every 2 seconds. A read that fails does not end the follow: the page waits
twice as long after each failure, up to 30 seconds, and returns to every 2 seconds
once a read succeeds. Coming back to the page's address resumes following, because
the status is the cluster's.

Each running operation is drawn as a progress bar (the `LtProgress` primitive),
with the step it is on in words and a line naming its units. The same bar appears
on the operation's own page and on the tree's summary tab:

| Operation | Steps shown | Units |
| --- | --- | --- |
| Resize | Copying the tree at the new size, pointing the tree's name at the copy, turning requests away from the old copy, retiring the old copy | One per shard the copy drains, then one for each of the three steps after the copy: "3 of 8 shards copied, then 3 steps to finish", then "Copy complete. Step 2 of 3 to finish." |
| Snapshot | Taking the source out of service (offline) or starting to forward live writes (online), copying shards, returning a copied shard to service (offline) | Shards copied: "3 of 8 shards copied." |
| Reshard | Splitting shards | The bar measures the physical shards added since the reshard started, out of the shards it adds; the line reads the current count against the target: "5 of 8 physical shards." |
| Purge | Purging shards, then Purged | Shards purged: "3 of 8 shards purged." |

A bar is determinate only when the cluster reports a total; otherwise it is
hatched and shows the step alone, never an invented percentage, which is what a
cluster that predates progress reporting produces. A small total is drawn as one
segment per unit. The percentage is rounded down, so a bar never reads 100% before
the last unit is done. A change of step is announced once through a polite live
region; the moving percentage is not.

An accepted undo of a resize shows the resize as **Undoing**, with an
indeterminate bar labelled "Undoing the resize", because an unwind reports no
units. The page reads the undo before the resize's own in-progress flag, so an
undo of a resize that had already finished is still followed until it has unwound.

The WAL page first audits placement for a named tree. The move planner's target
provider key is a picker over the provider keys the resolving silo reports for that
tree; without a tree to audit it accepts a typed key. A move plan is addressed
entirely by `?tree=`, `?partition=` and `?target=`, so refreshing or returning
to the link resumes the same preview. Planning changes nothing. Executing a
move quiesces the partition briefly, copies the tail, flips placement and
retains the source; reclaiming discards that retained source and removes the
ability to revert. Execute and Reclaim both require destructive confirmations
and the TreeLifecycle grant.

The Orphaned leaves page audits a named tree for leaves that are in a shard's
sibling chain but unreachable from the root. It can run a read-only survey of
every orphan key, stop a running pass, and drive a pass batch by batch up to
1000 batches. It distinguishes clean, partial and not-judged verdicts. Repair
is shown only with TreeLifecycle authority and repairable findings, is behind a
destructive confirmation, unsplices only leaves whose keys were shown readable
elsewhere, and always audits again when repair completes.

The Cluster palette commands are:

| Command id | Label | Effect |
| --- | --- | --- |
| `cluster.reshard-tree` | Reshard tree... | Opens the reshard chooser on `/cluster/trees`, then navigates to the chosen tree's reshard page. |
| `cluster.plan-wal-move` | Plan WAL move... | Opens the WAL move planner on `/cluster/wal`, then navigates to the query-addressed plan. |

Cluster faults are shown as fixed, short sentences. An authorisation denial is
"You do not have permission to do this."; missing trees say the cluster does
not know the tree; unserved operations say the cluster does not serve that
operation; timeouts say the cluster did not answer in time. Pages show the
fixed error sentence beside the surface that made the call, or keep the status
that the cluster owns and let the caller return to the same address later.

## See also

- [Explorer overview](README.md)
- [Navigation model](navigation-model.md)
- [Area availability](area-availability.md)
- [Lattice Apps](lattice-apps.md)
- [Managing access](managing-access.md)
- [Managing schema](managing-schema.md)
- [Tenant scope](tenant-scope.md)
- [Managing backups](managing-backups.md)
- [State API](../lattice.api.state/README.md)
- [Replication API](../lattice.api.replication/README.md)
- [Telemetry API](../lattice.api.telemetry/README.md)
- [Tree administration API](../lattice.api.treeadmin/README.md)
- [Apps API](../lattice.api.apps/README.md)
- [Auth API](../lattice.api.auth/README.md)
- [Schema API](../lattice.api.schema/README.md)
- [Tenant administration API](../lattice.api.tenantadmin/README.md)
- [Backup API](../lattice.api.backup/README.md)
