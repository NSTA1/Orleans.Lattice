# Managing schema from the Explorer

The **Schema** area is a native compiled-in Explorer area. No extra registration is needed. It appears when the schema control facade's capability probe grants the caller at least one schema capability.

Schema is tenant-scoped. In a tenant-rooted Explorer, the same address under `/t/{tenant}` scopes the caller-supplied tree name through the active tenant before the cluster authorizes or acts on it.

## Availability

Schema is visible when its fail-closed capability probe returns any schema grant. The probe names a reserved sentinel tree and has no side effects. A definite visible or refused answer is remembered until the sign-in state or connection changes. A transport fault is not remembered, so the next navigation asks again.

The area is hidden when the schema facade is absent, when a signed-in identity receives no grant, when the probe is denied, when the cluster does not serve schema administration, or when the connection is unavailable.

An anonymous caller whose probe returns no grants sees the area as unavailable. The exact sentence shown is:

> Sign in to manage schema on this cluster.

Per-tree grants are probed separately and reused briefly. If the caller has no grant on a selected tree, the tree page says:

> You may not manage this tree's schema

Panel-level denials are shown beside the panel, for example **You may not read this tree's policy**, **You may not scan this tree**, or **You may not read this tree's dead letters**.

## Addresses

The Schema area is tenant-scoped. These route forms exist in the shipped pages:

| Page | Plain address | Tenant-rooted form | Notes |
| --- | --- | --- | --- |
| Schema directory | `/schema` | `/t/{tenant}/schema` | Lists governed trees by default. Use `?show=all` to include ungoverned trees and `?filter={text}` to filter by tree id. |
| Tree workspace | `/schema/{tree-path}` | `/t/{tenant}/schema/{tree-path}` | Supports tree ids carried in up to six path segments after `/schema`. The tab is selected with `?tab=...`. |

The area uses these query keys:

- `show=all` lists every tree, not only trees with schema state.
- `filter={text}` filters the directory by tree id.
- `tab=policy|versions|compliance|remediation|dead-letters` selects a tree-workspace tab. Missing or unknown values fall back to `policy`.
- `scan=start` on the compliance tab starts one read-only compliance scan, then the page removes the key from the address so refresh or history navigation does not start another scan.

## Directory page

The directory page shows the logical trees under a schema policy, version config, or app declaration. The **Under schema** and **All trees** links switch between governed trees and every logical tree. The filter narrows by tree id. **Refresh** reads again. **Scan compliance...** opens a picker listing visible trees that have a policy.

Each row shows:

- the logical tree id, linking to `/schema/{tree}`;
- policy state: a policy summary, **None**, **Not permitted**, **Not available**, or **Could not read**;
- versioning state: the target version summary, **Unversioned**, **Not permitted**, **Not available**, or **Could not read**;
- the last compliance scan result recorded in this Explorer circuit;
- the declaring app, when an installed app manifest declares the tree's schema.

A declaring app link goes to `/apps/{slug}` in the same tenant. A row with a policy has a **Scan compliance** action linking to `/schema/{tree}?tab=compliance&scan=start`.

The directory shows a truncated note when the cluster holds more trees than one bounded listing inspects. A tree can still be opened directly by address.

## Tree workspace and tabs

A tree workspace heading shows the logical tree id, badges for policy and version state, and app declaration metadata when present. The declaring app line links to `/apps/{slug}` and names the app version, schema family, schema version, and strict-ingest flag.

The workspace has five tabs.

### Policy

The Policy tab reads, sets, edits, and clears the tree's write-validation policy. A tree with no policy accepts every value. The editor can add these rule shapes:

- well-formed UTF-8;
- one JSON document;
- largest size in bytes;
- regular-expression match, optionally under a member path.

The member path, and a remediation step's member or new name, is a [picker](navigation-model.md#pickers) that suggests the member paths the tree's policy already names. It accepts any path, because a value's members are not known to the cluster until a rule names them.

A policy needs at least one rule; to accept every value, clear the policy instead. Saving replaces the whole policy and affects new writes immediately. Existing stored values are not changed.

**Strict ingest** controls whether replicated and restored values are checked too. When it is on, a value that fails strict ingest is diverted to dead letters instead of being applied.

Clearing a policy uses a destructive confirmation named **Clear this tree's policy**. The confirmation states that every value will be accepted from then on, strict ingest stops diverting values, and the rules are not kept.

### Versions

The Versions tab manages the tree's envelope-version config. A version is a stamp, not a shape: new writes are stamped with the schema family and target version, and older values are upgraded when they are read or migrated. The Explorer does not show the shape difference between versions because those registrations live in code on the silos.

When versioning is off, **Turn on versioning** asks for a schema family, a target version of 1 or more, and strict ingest. When versioning is on, the tab can:

- **Advance target version...** after a confirmation named **Advance to version N**;
- **Advance and migrate...** after a confirmation named **Advance to version N and migrate**;
- **Migrate stored values...** after a review dialog;
- **Change config** by replacing the config as typed;
- **Turn off versioning** after a destructive confirmation named **Turn off versioning**.

Advancing can only move the target version up. Migration and advance-and-migrate are staged background operations. The tab links to the Remediation tab while one is running.

If the cluster has not registered schema versioning, the tab says **Versioning is not available** instead of failing opaquely.

### Compliance

The Compliance tab runs a read-only scan of every value in the tree against the current policy. The scan changes nothing. It can be started from the tab, from the directory picker, or by the address `?tab=compliance&scan=start`.

While a scan runs, the tab shows **Scanning every value of {tree}...** and a **Stop scanning** control. Stopping a scan reports that it was stopped before it finished. A finished scan shows scanned, compliant, and non-compliant counts, finish time, and a breakdown of non-compliance reasons. The last scan result is kept for this Explorer circuit and summarised in the directory.

If the tree has no policy, the tab says there is nothing to scan against and links back to the Policy tab.

### Remediation

The Remediation tab is the status page for a tree's background migration or remediation, and the place to start a remediation when the caller may manage schema.

A remediation rewrites every value through ordered transform steps, checks each rewritten value against the current policy, and cuts over only when every value passes. If a value still fails, nothing is cut over. The editor can add only top-level member steps: set, remove, and rename. Conditional or computed transforms are registered in code on the silos and are not authored here.

Starting a remediation opens a destructive confirmation named **Remediate this tree**. It says every value is rewritten and checked, that a successful run cuts over to the rewritten values, and that a failed value leaves the tree unchanged. The operation runs in the background and the page can be left while it runs.

The status section reads the cluster's own remediation status and also shows operations started in the current circuit. It displays stages (**Confirmed**, **Running in the cluster**, **Finished**), operation id when present, values checked, aborted-key detail, failure text, **Refresh status**, and **Clear this result** for a finished circuit operation. Running status is read again every 2 seconds.

### Dead letters

The Dead letters tab counts strict-mode dead letters on arrival and lists entries on demand because the queue can be large. It is read-only: there is no replay or delete action here.

**Load dead letters** reads the first 100 entries. **Load more dead letters** increases the limit by another 100. Rows show key, reason, source, diverted time, and value size. The detail view shows the full key and a text preview of the rejected value's first bytes.

## Palette and address completions

Schema contributes these commands:

| Command id | Label | Target |
| --- | --- | --- |
| `schema.scan-compliance` | `Scan compliance...` | `/schema` and opens the picker on the directory page |
| `schema.all-trees` | `Show every tree's schema` | `/schema?show=all` |

The visible controls carry the same command ids: the **Scan compliance...** button and the **All trees** link.

Address completions list governed trees from the remembered directory read. Search mode matches tree ids; app mode matches declaring app slugs; address mode matches `/schema/...` paths. Completions stay in the current tenant and include details such as **Schema policy**, **Schema versioning**, **Schema policy and versioning**, or **Schema declared by an app**.

## Limits and caching

- The tree catalogue is cached for 30 seconds and follows at most 20 catalogue pages per read.
- A directory listing inspects at most 500 logical trees at a time and probes up to 8 trees concurrently.
- The directory read is cached for 30 seconds.
- Per-tree grants are cached for 30 seconds unless a refresh is requested.
- Dead-letter reads load 100 entries at a time.
- Running operation status is re-read every 2 seconds while it is running.
- Compliance scan results are remembered only in the current Explorer circuit.

## Server authority

The schema control facade scopes caller-supplied tree names through the active tenant, then authorizes and acts on the same effective tree. Read authority gates policy reads, version reads, remediation status, dead-letter reads, and compliance scans. Schema-admin authority gates policy changes, version changes, and remediation. Version operations require the versioning add-on on the silo.

## See also

- [Explorer overview](README.md)
- [Navigation model](navigation-model.md)
- [Area availability](area-availability.md)
- [Areas reference](areas.md#schema)
- [Lattice Apps](lattice-apps.md)
- [Schema engine](../lattice.schema/README.md)
- [Schema API](../lattice.api.schema/README.md)
- [Schema gRPC binding](../lattice.api.schema.grpc/README.md)