# Tenant scope

Three different things in the Explorer involve tenants, and conflating them is
the single most common source of confusion. This page separates them.

## Three things, not one

| Concept | What it is | Where it lives |
| --- | --- | --- |
| **Tenant scope** | Which tenant's data you are currently looking at. It re-scopes the catalog and every surface below it. | A control in the console banner |
| **Tenant administration** | Administering *other people's* tenants as a platform operator: lifecycle, quota, region authorization, the initial tenant-admin grant. | An area in the rail |
| **My tenant** | Managing *your own* tenant as its administrator: membership, cross-tenant grants, region residency, usage against quota. | An area in the rail |

The first is a lens. The second and third are places. The areas were previously
called "Tenants" and "My Tenant", which read as two names for one idea, and the
Tenant administration area's own first sub-surface was also called "Tenants", so
the word appeared twice in adjacent tiers. The areas are now **Tenant
administration** and **My tenant**, and the bare word "Tenants" is retired.

## The picker adapts to what you can reach

The scope control's shape follows both what the caller may do and how many
tenants they can actually reach, so a deployment that has no tenancy story shows
no tenancy chrome.

| Situation | What is shown |
| --- | --- |
| No tenant is established, and the caller is not a platform operator | Nothing. A non-tenant deployment looks unchanged. |
| A caller who is not a platform operator, scoped to one tenant | A quiet, non-interactive display of the current tenant. |
| A platform operator who can reach one tenant | The same quiet display. No picker, because there is nothing to pick. |
| A platform operator who can reach more than one | A drop-down listing the reachable tenants with the current one marked. |

The drop-down is offered only to a caller who validates as a platform operator,
because only such a caller may switch: fail-closed by construction, rather than
rendering a control that would always refuse. A caller scoped to the reserved
`default` tenant and holding no operator standing is shown nothing at all, which
is what keeps a single-tenant deployment free of tenancy chrome.

It is never a free-text box. Requiring a tenant id from memory was the previous
design, and it meant the console listed every tenant in one place while
offering no way to make one active from there.

The picker and the Tenant administration list read from one source of truth, so
they cannot diverge, and the list offers a "set as active tenant" action that
drives the picker directly.

## Switching is confirmed, refusals are explained

A switch reports its outcome rather than appearing to work:

- a successful switch is confirmed;
- a fail-closed refusal is explained rather than silently ignored;
- an unknown or unreachable tenant is reported as such.

Each outcome is announced in a live region, so it reaches a screen-reader user
and is not sighted-only.

A non-operator cannot switch tenant, and cannot elevate themselves into the
all-tenant view. That is enforced on the server; the console simply reports the
refusal honestly.

## An emptied catalog says the scope emptied it

When the active tenant scope filters every entry out of a catalog, the console
says so and names the tenant responsible, rather than reporting the cluster as
empty. Those are different situations with different remedies: an empty cluster
has nothing to do about it, while a scoped-out catalog is undone by switching
tenant or listing across every tenant you can reach.

The distinction is drawn on whether the scope actually removed anything, not on
whether a scope is active. A tenant that genuinely holds no trees still reports
an empty catalog, because claiming a filter that removed nothing would be as
misleading as concealing one that did.

## Remembered, and re-validated on restore

The last selected tenant is remembered per user and per cluster, and is
re-selected on your next session rather than only your next page load.

Restore is **fail-closed**. A remembered tenant can become unreachable because a
grant was revoked or the tenant was suspended or deleted, so the id is
re-validated against the caller's *current* accessible list every time it is
restored. If it no longer resolves, the console falls back to the default or
first reachable tenant **and says why**, rather than silently landing you
somewhere you did not choose. A tenant you can no longer reach is never
restored on the strength of having once been allowed.

The identity resolver establishes a tenant only when none is set. It does not
overwrite an explicit in-session switch, which is a behaviour worth stating
because the opposite once made every switch appear to do nothing.

## The reserved `default` tenant

`default` is the tenant that owns the un-prefixed trees a non-tenant deployment
writes. On a single-tenant cluster it is the only tenant and the console shows
no tenancy chrome at all. The term is explained in-product wherever it appears,
as is the all-tenant view.

## See also

- [The Explorer navigation model](navigation-model.md)
- [What the Explorer remembers](what-the-explorer-remembers.md)
- [Orleans.Lattice.Tenancy](../lattice.tenancy/README.md)
