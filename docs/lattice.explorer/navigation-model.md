# The Explorer navigation model

The Explorer console is navigated through four tiers. They are deliberately
given different shapes, because when every tier looked like a horizontal tab
strip nothing signalled hierarchy and the same word could appear twice in
adjacent tiers.

## The four tiers

| Tier | What it selects | How it renders |
| --- | --- | --- |
| Areas | The top-level capability you are working in (Explore, Backups, Access, Tenant administration, My tenant, Telemetry) | A stable vertical rail down the left of the shell |
| Catalog kind | Whether the catalog lists trees, views or tag indexes | A segmented control above the catalog |
| Selection surfaces | Which aspect of the selected tree or view you are looking at (Data, Topology, Metrics, Dead-letter) | An underlined tab strip above the detail panel |
| Plugin sub-surfaces | A section within one area, such as the surfaces of Tenant administration | A visibly subordinate segmented control, quieter than the selection tabs |

No two adjacent tiers share a shape, and no two adjacent tiers may share a
label. Where a plugin declares a sub-surface whose label matches its own area
label, the shell relabels that sub-surface to `Overview` rather than render the
same word twice. That relabelling is a backstop; plugins should name their
surfaces so it never fires.

## Why primary navigation is a rail

Areas used to be a horizontal tab strip with a fixed inline capacity, which had
two consequences beyond looking like every other tier.

First, horizontal space is scarce, so entries competed for it. An entry the
caller could not use still occupied an inline slot and pushed a usable entry
into an overflow menu. Vertical space is cheap, so the rail renders every area
and nothing is displaced.

Second, because nothing was displaced, demoting an entry became free. That is
what makes the visibility policy in
[Navigation visibility policy](navigation-visibility-policy.md) affordable: an
area you cannot open can stay visible, grouped and quiet, instead of being
hidden or silently greyed.

## Addressing a view by URL

Every navigable view has a URL. Back, forward, reload, bookmark and share all
work, and a link to a specific tree and surface opens on that exact view.

The home area is addressed directly:

```text
/explore
/explore/trees
/explore/trees/orders
/explore/trees/orders/data
```

A contributed area is namespaced under `/area/`, because its slug is only known
at run time and so cannot own a literal path segment:

```text
/area/backups
/area/tenants
/area/tenants/detail/acme/quotas
```

An area's slug is derived from its plugin id, not from its label, so renaming an
area does not move it. The Tenant administration area is registered under the
plugin id `orleans.lattice.tenants` and therefore addresses as `/area/tenants`;
My tenant addresses as `/area/mytenant`. A contributed area carries the same
optional kind, id and surface tail as the home area.

Two further routes exist: `/reset-view`, which clears remembered state, and
`/not-found`.

Parsing is forgiving. Handed a bare `/tenants`, the address parser still
resolves the area, reports the address as normalised, and lets the shell rewrite
the address bar to the canonical `/area/tenants`. That tolerance is in the
parser, which is what the shell hands an address it has already received: since
every declared route begins with a literal segment and no catch-all exists, an
un-namespaced area is not by itself a routable address a browser can request
cold.

### Paths are lower case

Every route and query key the console declares is lower case:
`/explore/trees/orders/data`, never `/Explore/Trees/...`. A hygiene test scans
the declared routes and fails the build if an upper-case segment appears.

### Every route begins with a literal segment

No declared route may begin with a route parameter, and none may contain a
catch-all. This is enforced by a hygiene test, and the reason is not stylistic.

A catch-all route at the application root matches asset paths as well as pages.
When one was briefly present, a request for `_framework/blazor.web.js` was
routed into the renderer, so an asset URL returned the whole admin console and
carried two `Content-Security-Policy` headers. Browsers enforce the
intersection of duplicated policies, so the effective policy silently stopped
being the one the middleware composed. Beginning every route with a literal
segment removes the possibility structurally rather than relying on route
precedence, which only helps when a competing literal endpoint actually exists.

## Where the URL ends and preferences begin

The URL carries *where you are*. Preferences carry *how you like it* and *where
you were last time*. Landing on a bare `/` restores the remembered view; an
explicit URL always wins over what was remembered. A single policy arbitrates
this once per session entry, so the two never disagree.

See [What the Explorer remembers](what-the-explorer-remembers.md).

## Hosting note: the route binding

The component that binds the shell's route to the browser address bar,
`ShellRouteBinding`, is rendered by the shell's own layout, deliberately outside
the application shell rather than by the routable page. Entering a contributed
area swaps out the page, and a binding owned by that page would be disposed with
it - after which nothing would perform a navigation the shell asked for and
nothing would observe a Back or Forward.

This only matters if you replace the shell's layout with your own. A custom head
that hand-rolls a layout must render `ShellRouteBinding` within it, and must
place it where an area change cannot unmount it. Omit it and the console still
works, but it keeps its route in memory only: the address bar stops following
the view, and deep links, sharing and browser history stop working with it.

## See also

- [Navigation visibility policy](navigation-visibility-policy.md)
- [What the Explorer remembers](what-the-explorer-remembers.md)
- [Tenant scope](tenant-scope.md)
- [Theming and density](theming-and-density.md)
- [Accessibility conformance](accessibility-conformance.md)
