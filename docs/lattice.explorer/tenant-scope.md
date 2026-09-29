# Tenant scope in the Explorer

The Explorer's tenant view is part of Core. The web head (`AddLatticeExplorerWeb`) always registers it, through `AddExplorerTenantView()`, after the UI, so the Tenancy area's reachable-tenant list and operator gate take effect. The view publishes the caller's active tenant, the reachable tenant list and an operator-gated switcher. A head that does not register it has an inactive view: addresses are plain, `/t/{tenant}` roots are removed during canonicalisation, and catalogue reads are the same as a non-tenant cluster.

On a cluster without the tenancy add-on, every tree belongs to the reserved `default` tenant, so the active tenant is `default`. What that means for addresses is described under [The reserved default tenant](#the-reserved-default-tenant).

With tenancy on, the address grammar gains a root node: `/t/{tenant}`. A tenant-rooted address scopes Home and every tenant-scoped area to that tenant. Typing `t/{tenant}` in the address line offers reachable tenants and re-roots the current address. Choosing a different tenant is still a request, not an authority: the switch goes through the operator-gated switcher and is refused unless the caller is a platform operator.

## What is scoped

Tenant ownership is derived from physical tree ids. A tree named `t/{tenant}/{name}` belongs to that tenant. A tree with no `t/` ownership prefix belongs to the reserved `default` tenant. Platform-internal trees are not attributed to any tenant.

The visible address determines whether the tenant root is kept:

- Tenant-scoped areas keep `/t/{tenant}` when tenancy is active. The native areas that do not opt out are Data, Apps, Schema, Replication, Backups and Telemetry.
- Access and Cluster are cluster-wide. They never keep a tenant root.
- Tenancy has both shapes: `/tenancy` and `/tenancy/{tenant}...` are cluster-wide administration addresses, while `/t/{tenant}/tenancy...` is that tenant's workspace.
- Home is tenant-rooted when tenancy is active, unless the current caller is on the hidden `default` path described below.

Core applies the same rule to listings. The active-tenant view keeps only items owned by the active tenant. The all-tenant view returns the list unchanged only when the caller requested all tenants and the platform-operator gate validates them. A non-operator all-tenant request falls back to the active tenant.

Telemetry also has an address-level all-tenant view. Its `scope=all` query asks telemetry queries to request all tenants; the UI exposes the scope chooser only while tenancy is active and the caller is an operator, or while the current address already asks for all tenants. If the request is not admitted, Core's tenant view still falls back to the active tenant.

## Switching tenants

A tenant-rooted address for a tenant other than the active one asks the shell to switch. If the switch is accepted, the address becomes canonical for the requested tenant and the shell announces the new scope. If it is refused, the shell redirects to the same address under the active tenant and shows a warning toast such as `You can't scope to tenant acme, so this shows tenant default instead.` If no active tenant is established, the redirect is to the unrooted canonical form and the warning names only the refused tenant.

Successful switches and successful all-tenant toggles are remembered through the preference contract as `shell.tenant` and `shell.all-tenants`. Persistence is a convenience, not the authority; the current circuit's tenant context is updated first and every read is revalidated.

## Every call carries the tenant

Every call the Explorer makes to the cluster asserts the active tenant through the `lattice-active-tenant` header. That covers catalogue and data reads, live tails, every administration area, and an app's bridge calls. The header is read as each call starts, so the first call after a switch already carries the new tenant, and one circuit's tenant never reaches another circuit's calls. With tenancy off, with no tenant established, or scoped to the reserved `default` tenant, no header is sent, and the call is exactly what a tenant-unaware client sends.

The header is an assertion, not a grant. The cluster checks it against the caller's own tenants before it scopes anything, so asserting a tenant gives no standing in it. An install at `/t/{tenant}/apps` lands in that tenant, and a tenant admin's Data and Apps pages list that tenant's trees and apps.

The Explorer fails closed around it:

- If a signed-in caller's tenant cannot be established, no tenant-scoped page is shown, so no call falls back to the `default` tenant.
- While a switch is in flight the page is withheld. After it, every page is built afresh and every remembered answer is read again, so nothing read under one tenant is shown under another.
- An open app is bound to the tenant it was opened in, and is closed once the Explorer is scoped to another tenant.
- A staged backup operation finishes in the tenant it started in, and is listed only under that tenant.

## The reserved default tenant

`default` is the reserved tenant that owns legacy, un-prefixed trees. The shipped chrome hides tenancy for a non-operator whose active tenant is `default`: there is no tenant root, `t/` offers no tenant, and `/t/default/...` canonicalises to the plain address. The Explorer treats a caller as a platform operator exactly when the Access area is visible to them. The layout refreshes the operator verdict before it resolves each navigation. Until that verdict proves the caller is an operator for `default`, the safe answer is to hide tenancy chrome.

An operator on `default` does see tenancy chrome, because the `default` root is the way they reach tenant-aware addresses and switch to other tenants. The cluster lists only the tenants a caller administers, which never includes `default`, so the Explorer adds `default` to a proven platform operator's reachable tenants. An operator can therefore always pick it and open `/t/default/...`, and an operator with no remembered tenant starts there.

## The Tenancy area

The Tenancy area exists only when tenancy is active and the tenant self-service facade is available. It probes the caller's standing before it appears. A platform operator sees it. A tenant admin for their own scoped tenant also sees it. An unauthenticated caller gets an unavailable area with `Sign in to see the tenants you administer.` Other refusals and faults hide the area fail-closed.

The area's addresses and commands:

- `/tenancy` is the operator directory. It lists every tenant the caller can reach, shows lifecycle state, quota use, resident regions, installed-app counts, and links to the tenant workspace or Apps area. The `tenancy.create-tenant` command opens this page with `?new=true`, the same form as the visible `New tenant` button.
- `/tenancy/{tenant}` is the operator administration overview for one tenant. It links to `/tenancy/{tenant}/grants`, `/tenancy/{tenant}/access`, and `/tenancy/{tenant}/regions`. Non-operators who reach an administration address are redirected to the equivalent `/t/{tenant}/tenancy` workspace section.
- `/t/{tenant}/tenancy` is My tenant. Its sections are `/t/{tenant}/tenancy/members`, `/t/{tenant}/tenancy/quota`, `/t/{tenant}/tenancy/regions`, and `/t/{tenant}/tenancy/sharing`. It shows the tenant's state, the caller's standing, quota, residency, installed apps, and reachable sibling tenants.
- `tenancy.offer-grant` opens `/t/{tenant}/tenancy/sharing?new=true`. The visible `Offer a grant` button uses the same command id. The reserved `default` tenant does not offer or receive cross-tenant grants.

The Tenancy area supplies the reachable-tenant list used by the directory and by the `t/` completions in the address line. It is exactly the tenants the cluster names for the caller, plus `default` for a proven platform operator, and never a tenant the cluster did not name. The established tenant is listed first and suspended tenants are not offered, except that the established tenant remains available so the current scope never disappears under the caller.

## See also

- [Explorer overview](README.md)
- [Navigation model](navigation-model.md)
- [Area availability](area-availability.md)
- [Areas](areas.md#tenancy)
- [Lattice Apps](lattice-apps.md)
- [Accessibility conformance](accessibility-conformance.md)
- [Apps package documentation](../lattice.api.apps/README.md)
- [Auth package documentation](../lattice.api.auth/README.md)
