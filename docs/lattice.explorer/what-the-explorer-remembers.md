# What the Explorer remembers

The Explorer remembers a small, enumerated set of view preferences so you do
not have to rebuild your working context on every visit. This page is the
contract: what is remembered, at what scope, for how long, and how to clear it.

## The remembered keys

| Key | What it holds |
| --- | --- |
| `shell.area` | The active area |
| `shell.catalog-kind` | Whether the catalog lists trees, views or tag indexes |
| `shell.selection` | The selected tree, view or tag index |
| `shell.surface` | The active selection surface |
| `shell.tenant` | The active tenant scope |
| `shell.all-tenants` | Whether the all-tenant view is requested |
| `shell.hide-inaccessible` | Whether areas you cannot open are hidden rather than shown demoted (defaults to showing them) |
| `appearance.theme` | The chosen theme |
| `appearance.contrast` | The chosen contrast level |
| `appearance.density` | The chosen density |
| `tenants.surface` | The active sub-surface of the Tenant administration area |
| `mytenant.surface` | The active sub-surface of the My tenant area |
| `access.surface` | The active sub-surface of the Access area |
| `backups.surface` | The active sub-surface of the Backups area |
| `schema.surface` | The active sub-surface of the Schema area |
| `telemetry.query` | The selected query in the Telemetry area |

The last two are contributed by plugins rather than declared by the shell: an
area registers its own keys on the same catalog when its panel mounts, so a
deployment gains them by rendering the area and the reset affordance discloses
and clears them with no further wiring. The set is therefore extensible without
editing the shell. Each is namespaced to its own area rather than sharing a bare
`surface` key, because a route keeps its parameters across an area change and two
areas sharing a key would overwrite one another. Every key is declared once and
registered, rather than written through ad hoc calls scattered across
components. A key that is not registered cannot be read or written at all, which
is what keeps this list honest.

## Scope

Shell keys are scoped **per user and per cluster**. Switching account or
switching cluster does not resurrect someone else's view, and does not carry one
cluster's selection into another where it may not exist.

Appearance keys are scoped **per user**, because a theme is a property of the
person, not of the cluster they happen to be looking at.

## Storage and lifetime

Preferences are held in a single browser storage entry,
`orleans.lattice.explorer.preferences.v1`, with retention and owner-based
cleanup. The web head encrypts the document with ASP.NET Data Protection; the
desktop head uses the platform preference store.

One value is deliberately kept outside that encrypted document: a small,
non-secret record of the last applied appearance, used to put the right palette
on the page at first paint. See
[Theming and density](theming-and-density.md#applying-a-theme-without-a-flash).

## When a remembered value no longer resolves

A remembered value can become invalid: a tree is deleted, an area's grant is
revoked, a tenant is suspended. The console never restores such a value blindly.

- The value is validated against what the caller can currently reach.
- If it no longer resolves, the console falls back to a safe default.
- Where a user would otherwise be confused about why they did not land where
  they left off, the fallback is explained rather than silent.
- The stale value is forgotten rather than left to fail again.

Tenant scope is the sharpest case of this and is handled fail-closed: a
remembered tenant is re-validated against the caller's current accessible list
on every restore, and is never re-applied on the strength of having once been
allowed. See [Tenant scope](tenant-scope.md).

## Resetting

The `/reset-view` page lists what is currently remembered and clears it. Use it
when a restored view is not what you want, or before handing a browser profile
to someone else.

## The division of labour with the URL

The URL carries where you are. Preferences carry how you like it and where you
were last time. An explicit URL always wins. Arriving at a bare `/` restores the
remembered view, but only once, when you enter the console: a later `/` within the
same session is taken at face value, including one you reach by pressing Back.
Otherwise leaving an area would be impossible - Back would return you to the home
address and the restore would put you straight back where you came from. See
[The Explorer navigation model](navigation-model.md#where-the-url-ends-and-preferences-begin).

## See also

- [The Explorer navigation model](navigation-model.md)
- [Tenant scope](tenant-scope.md)
- [Theming and density](theming-and-density.md)
