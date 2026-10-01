# What the Explorer remembers

The Explorer remembers only declared preferences. The sign-in credential is not a
view preference and is not part of this contract; see
[Where credentials live](connecting-to-an-auth-enabled-state-api.md#where-credentials-live). Each key is registered in the preference catalogue, scoped before it is stored, and cleared by the reset page. A component that does not declare a key does not persist its state.

Remembered state is never authority. Every restored value is rechecked against the live route, tenant list, area, or appearance vocabulary. If it no longer resolves, the Explorer forgets it and shows a default with an explanation where the caller provides one.

## Storage and scope

The web head stores one JSON preference document in protected browser local storage under `orleans.lattice.explorer.preferences.v1`. Protected local storage uses the browser origin and ASP.NET Core Data Protection. The in-memory preference store hydrates once per circuit, serves reads synchronously after that, and writes the whole document back after a change. Entries untouched for 90 days are pruned.

The stored name is the current scope token plus the declared key name. Shell navigation and tenant choices use `UserAndCluster`, so a different signed-in user or endpoint starts clean. Appearance uses `User`, so theme, contrast and density follow the same operator across clusters.

## Preference keys

| Key | Description |
| --- | --- |
| `appearance.contrast` | Per-user contrast choice. Values are `system`, `standard`, and `more`. |
| `appearance.density` | Per-user density choice. Values are `comfortable` and `compact`. Legacy `cosy` and `layout` values read as `comfortable`. |
| `appearance.theme` | Per-user theme choice. Values are `system`, `light` for Paper, and `dark` for Board. |
| `shell.all-tenants` | Per-user, per-cluster request to see every tenant's items. Core writes it when its tenant switcher admits a platform operator's all-tenants request (`IExplorerTenantSwitcher.SetVisibilityAsync`) and when a route is remembered (`IExplorerShellPreferences.RememberRouteAsync`); the rewritten console calls neither and does not restore it. |
| `shell.area` | Per-user, per-cluster route area. Declared by the Core session service for route restore; the rewritten console neither writes nor restores it. |
| `shell.catalog-kind` | Per-user, per-cluster catalogue kind. Declared for route restore; the rewritten console neither writes nor restores it. |
| `shell.selection` | Per-user, per-cluster selected catalogue item. Declared for route restore; the rewritten console neither writes nor restores it. |
| `shell.surface` | Per-user, per-cluster detail surface. Declared for route restore; the rewritten console neither writes nor restores it. |
| `shell.tenant` | Per-user, per-cluster tenant last switched to, through the tenant switcher, `t/` in the address line or a tenant-rooted address. It is revalidated against reachable tenants on restore; the tenant a sign-in falls back to when nothing usable is remembered is not written. |

These rows intentionally use the documented key names in backticks. The hygiene test parses this table and requires it to match the keys declared in the Explorer assemblies.

## Routes and remembered state

The address is the source of truth for the current view, and the console does not
restore a remembered page: a typed, linked, bookmarked or Back/Forward address is
always shown as it is, and a bare `/` opens Home. The four route keys above are
still declared, so they are listed and cleared by the reset page, but the
rewritten console does not use them.

Tenancy is the one remembered value that shapes addresses. The tenant view restores
`shell.tenant` only after revalidating it against the tenants the caller may reach
now, and forgets it when it no longer resolves. It is read from the browser's
preference store before the tenant is resolved, so the server prerender, which
cannot read that store, never renders a page under a guessed tenant: for a caller
who can reach more than one tenant it shows **Resolving your tenant** until the
page is interactive. An explicit `/t/{tenant}` address still goes
through the operator-gated switch and wins only if admitted; a successful switch
updates `shell.tenant`. The rewritten console neither writes nor restores
`shell.all-tenants`: Core writes it only from an admitted operator request
through `IExplorerTenantSwitcher.SetVisibilityAsync` or from
`IExplorerShellPreferences.RememberRouteAsync`, and the console calls neither.

## Resetting remembered state

The reset page is `/reset`. It lists the registered preference descriptions from the catalogue, then clears every registered key for the current scope only after the operator presses `Reset view`. It does not clear credentials, sign the user out, or change cluster data.

## First paint and appearance

Appearance has a second, non-authoritative browser record under `orleans.lattice.explorer.appearance.v2`. The blocking first-paint script reads only `theme`, `contrast`, and `density` names from that record so the document gets the right attributes before the Blazor circuit starts. The preference contract is still authoritative: once the app is interactive, the chrome restores the declared appearance keys and reapplies them.

## See also

- [Explorer overview](README.md)
- [Navigation model](navigation-model.md)
- [Theming and density](theming-and-density.md)
- [Tenant scope](tenant-scope.md)
- [Area availability](area-availability.md)
- [Lattice Apps](lattice-apps.md)
- [Accessibility conformance](accessibility-conformance.md)
