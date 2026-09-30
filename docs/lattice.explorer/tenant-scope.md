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

A tenant-rooted address for a tenant other than the active one asks the shell to switch. If the switch is accepted, the address becomes canonical for the requested tenant and the shell announces the new scope to screen readers, without drawing a toast, because the address and header already show it; the first address a circuit opens announces nothing. If it is refused, the shell redirects to the same address under the active tenant and shows a warning toast such as `You can't scope to tenant acme, so this shows tenant default instead.` If no active tenant is established, the redirect is to the unrooted canonical form and the warning names only the refused tenant.

There are three ways to ask for a switch, and all three go through the same operator-gated switch: open an address with another tenant's root, type `t/` in the address line and choose a tenant, or use the tenant switcher in the top bar.

The tenant switcher is a button naming the active tenant, which opens a type-ahead field of the tenants you can reach (up to 20 listed at once). The palette command `tenant.switch` ("Switch tenant") opens it too, and at the compact width it sits at the top of the Directory sheet. It is shown only to a signed-in caller whose tenancy is on, whom the operator-gated switcher proves may switch, and who can reach at least two tenants; otherwise it is absent, not disabled, and any fault reads as not offered. Its list is the Tenancy area's reachable-tenant list, so for a platform operator it includes the reserved `default` tenant. Choosing a tenant at a tenant-scoped address re-roots the address; at a cluster-wide address, such as an Access page, the tenant is switched in place and announced. See [The tenant switcher](navigation-model.md#the-tenant-switcher).

Successful switches and successful all-tenant toggles are remembered through the preference contract as `shell.tenant` and `shell.all-tenants`. Persistence is a convenience, not the authority; the current circuit's tenant context is updated first and every read is revalidated. The remembered tenant lives in the browser's preference store, so a server prerender cannot restore it: at an address that names no tenant, a caller who can reach more than one tenant sees a neutral **Resolving your tenant** state until the page is interactive, and the redirect to `/t/{tenant}` follows then. See [Before the page is interactive](navigation-model.md#tenancy-and-re-rooting).

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

The reserved default tenant has no registry record, so it has no workspace root: `/t/default/tenancy` canonicalises to the tenant directory, `/tenancy`, which is what an operator scoped there administers. The rule depends only on the address, so it holds while an operator verdict is still settling.

## The Tenancy area

The Tenancy area exists only when tenancy is active and the tenant self-service facade is available. It probes the caller's standing before it appears. A platform operator sees it. A tenant admin for their own scoped tenant also sees it. An unauthenticated caller gets an unavailable area with `Sign in to see the tenants you administer.` Other refusals and faults hide the area fail-closed.

The area's addresses and commands:

- `/tenancy` is the operator directory. It lists every tenant the caller can reach, shows lifecycle state, quota use, residency, installed-app counts, and links to the tenant workspace or Apps area. The `tenancy.create-tenant` command (**Create a tenant**) opens this page with `?new=true`, the same form as the visible `New tenant` button. The `tenancy.set-regions` command (**Set a tenant's regions**) opens it with `?set-regions=true`, the same picker as the visible `Set regions...` button; see [Regions and residency](#regions-and-residency).
- `/tenancy/{tenant}` is the operator administration overview for one tenant. An operator and a tenant admin see the same tabs, with the same names: **Overview**, **Members**, **Quota**, **Regions** and **Sharing**. The operator's are `/tenancy/{tenant}`, `/tenancy/{tenant}/members`, `/tenancy/{tenant}/quota`, `/tenancy/{tenant}/regions` and `/tenancy/{tenant}/sharing`; the earlier `/tenancy/{tenant}/grants` and `/tenancy/{tenant}/access` still answer, as the Sharing and Members pages. The operator's Quota tab shows the tenant's use against each limit and sets the limits, except for the reserved `default` tenant, whose limits are not set there; the overview links to it. Non-operators who reach an administration address are redirected to the equivalent `/t/{tenant}/tenancy` workspace section.
- `/t/{tenant}/tenancy` is My tenant (for the reserved `default` tenant, this root is the directory; see [The reserved default tenant](#the-reserved-default-tenant)). Its sections are `/t/{tenant}/tenancy/members`, `/t/{tenant}/tenancy/quota`, `/t/{tenant}/tenancy/regions`, and `/t/{tenant}/tenancy/sharing`. It shows the tenant's state, the caller's standing, quota, residency, installed apps, and reachable sibling tenants. Its Quota tab is read-only, even for an operator, who sets limits from the administration Quota tab.
- `tenancy.change-residency` (**Change residency**) opens `/t/{tenant}/tenancy/regions` for whoever administers the scoped tenant; the Regions tab of My tenant is its visible control. It is not offered on the reserved `default` tenant.
- `tenancy.offer-grant` opens `/t/{tenant}/tenancy/sharing?new=true`. The visible `Offer a grant` button uses the same command id. The reserved `default` tenant does not offer or receive cross-tenant grants.

The area's forms use [pickers](navigation-model.md#pickers), and each one is tenant-scoped like every other:

- **New tenant.** The tenant id suggests existing tenants and refuses one that already exists. The optional admin subjects are a multi-value picker over the identity directory. The optional allowed regions and initial residency are described in [Regions and residency](#regions-and-residency).
- **Members.** The subject id is a picker over the identity directory's users and groups.
- **Regions.** The allowed region ids are a multi-value picker over the regions this cluster knows, plus any region the tenant already lists, since a region can be allowed before this cluster replicates with it. When the cluster's regions cannot be listed, the field accepts what is typed. A new tenant's initial residency suggests only the regions chosen as allowed.
- **Grants.** The grantee tenant is a picker. For a platform operator, who can list every tenant, it accepts only a listed tenant; for anyone else it suggests the tenants they can reach and accepts any id. The scope suggests the tenant's trees and accepts a tree-name prefix too. A bare name or prefix is qualified into the granting tenant's namespace before the offer is sent, so `orders` is offered as `t/{tenant}/orders`: the cluster matches a grant's scope against the full tree id it reads, so a bare name would share nothing. Approving, rejecting or revoking a grant refreshes the Data listing in the same session.

The Tenancy area supplies the reachable-tenant list used by the directory, by the `t/` completions in the address line, and by the tenant switcher. It is exactly the tenants the cluster names for the caller, plus `default` for a proven platform operator, and never a tenant the cluster did not name. The established tenant is listed first and suspended tenants are not offered, except that the established tenant remains available so the current scope never disappears under the caller.

## Regions and residency

A tenant's Regions page, `/tenancy/{tenant}/regions` for an operator or `/t/{tenant}/tenancy/regions` in My tenant, has two labelled parts:

- **Allowed regions (set by a platform operator).** The regions the tenant may use at all. Only a platform operator can change the set: saving replaces the whole set, revoking a region is confirmed, and a region the tenant is resident in cannot be revoked. Anyone else sees the set read-only, with a note saying only a platform operator can change it.
- **Residency (where the tenant's data is kept).** The regions the tenant's admins keep its data in, chosen from the allowed set. **Resident in** reads the current residency, or `Not set`. With no residency set, the tenant is served in every region, and the page says so. Once any residency is set, the tenant is served only in regions that are Online. Each allowed region is a row with its lifecycle status and its residency control, and **Apply residency** applies the plan; **Reset** discards it.

Each status is drawn with a sentence saying what it means for the tenant:

| Status | Meaning |
| --- | --- |
| Provisioning | The region has been added and waits for a platform operator of the hosting deployment to promote it. The tenant is not served there until it is Online. |
| Backfilling | The tenant's existing data is being copied in. It is not served there until it is Online. |
| Online | The region serves the tenant. |
| Draining | The region is being removed: the tenant's data there is draining, and it no longer serves the tenant. |
| Offline | Drained; the region no longer serves the tenant. |
| Removed | Removed from the tenant's residency. |
| Not resident | The region is allowed but not in the residency. |

A residency change is confirmed when it removes a region, because removing one drains the tenant's data there. It is also confirmed when it would leave the tenant with residency and no Online region: an added region starts Provisioning, so such a change stops serving the tenant anywhere until an operator of the hosting deployment promotes one of its regions. That dialog is titled **Stop serving tenant {tenant}?**, names the regions, and applies with **Apply and stop serving**. While a tenant has residency and no Online region, its Regions page, its overview and My tenant each carry a warning that it is not served anywhere.

**Creating a tenant with regions.** The directory's **New tenant** form can also set the new tenant's **Allowed regions** and its **Initial residency**, both optional. The residency is chosen from the allowed regions: it is disabled until one is chosen, and a region removed from the allowed set leaves the residency too. A tenant created with a residency is confirmed first, in a dialog titled **Create tenant {tenant} with no Online region?**, because each of its regions starts Provisioning and the tenant is served nowhere until one is promoted; **Back** returns to the form. The tenant is created, then its allowed regions are set, then its residency, and each step reports its own outcome, so a tenant can be created even when a region step fails. The form then opens the new tenant's Regions page, or its overview when no region was chosen.

**Finding a tenant's regions.** The directory's **Resident in** column links each tenant to its Regions page, and so do the **Resident in** and **Allowed** lines of a tenant's overview and of My tenant. The directory's **Set regions...** button, and the **Set a tenant's regions** palette command, pick a tenant and open its Regions page. **Change residency** opens the scoped tenant's own Regions tab.

**Home.** The Tenancy line on Home counts tenants with no residency set. For an operator it reads, for example, "12 tenants, 1 suspended, 3 with no residency set."; it reads the residency of at most 50 tenants, and with more than that the count is a lower bound, shown as "at least 3". The reserved `default` tenant has no residency and is not counted, and a tenant whose status cannot be read is skipped. A tenant admin's line adds "It has no residency set." when their tenant has none.

## Trees shared through a grant

A tenant can share a tree, or a tree-name prefix, with another tenant through a cross-tenant grant: the granting tenant offers it from **Sharing**, and the receiving tenant approves it there. Once approved, the shared trees appear in the receiving tenant's Data area alongside its own.

- **Only approved grants.** Only a received grant in the `Active` state (one the tenant approved) shares anything. An offer not yet approved, a rejected offer and a revoked grant are left out, exactly as the cluster's tenant gate ignores them.
- **Listed with their owner and access.** Each shared row has the **Shared by** tenant and what the grant allows ("Read only", "Write only" or "Read and write"), and the **Shared with this tenant** filter shows them alone. A shared tree's page carries the same two pills.
- **Addressed under your own root.** A shared tree keeps its full id and is opened under the receiving tenant's root, so acme's `orders` shared with globex is `/t/globex/data/t/acme/orders`. It is read by exactly that id, and the cluster's tenant gate admits the crossing on the grant.
- **A prefix is one row.** A prefix grant is listed as a single **Shared prefix** row that does not open: the Explorer can list only the receiving tenant's own trees, so it cannot enumerate another tenant's trees under the prefix. Open a tree under it by its full id in the address line, and the directory resolves it against the grant.
- **No administration.** A grant carries read or write access, never administration, so a shared tree's workspace offers no reconcile or rebuild, whatever the grant allows.
- **When grants cannot be listed.** If the cluster serves no grant facade, or you may not list the tenant's grants, the directory shows only the tenant's own trees with a note saying shared trees are not listed. A grant whose scope lies outside its granting tenant's own trees shares nothing, so it is not listed and a note counts it. A shared id that matches one of the tenant's own trees is dropped, and the tenant's own tree wins.
- **Not for the default tenant.** Grants are tenant to tenant, so with tenancy off, or scoped to the reserved `default` tenant, nothing is shared, and the Data directory leaves out the **Shared by** column and the **Shared with this tenant** filter.

**A grant never bypasses the authorization policy.** The tenant gate is composed on top of the cluster's access policy: a grant only opens the boundary between the two tenants, and the policy still decides who may read. The receiving tenant's user also needs an authorization rule that allows the read (for example `Read` and `RangeRead` on `t/acme/orders`), or the shared tree is refused like any other tree.

## See also

- [Explorer overview](README.md)
- [Navigation model](navigation-model.md)
- [Area availability](area-availability.md)
- [Areas](areas.md#tenancy)
- [Lattice Apps](lattice-apps.md)
- [Accessibility conformance](accessibility-conformance.md)
- [Apps package documentation](../lattice.api.apps/README.md)
- [Auth package documentation](../lattice.api.auth/README.md)
