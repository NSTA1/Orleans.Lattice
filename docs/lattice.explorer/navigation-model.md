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
/area/tenant-administration
```

Two further routes exist: `/reset-view`, which clears remembered state, and
`/not-found`.

Parsing is forgiving. A bare `/tenant-administration` still resolves to that
area, is reported as normalised, and the address bar is rewritten to the
canonical `/area/tenant-administration`.

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

## See also

- [Navigation visibility policy](navigation-visibility-policy.md)
- [What the Explorer remembers](what-the-explorer-remembers.md)
- [Tenant scope](tenant-scope.md)
- [Theming and density](theming-and-density.md)
- [Accessibility conformance](accessibility-conformance.md)
