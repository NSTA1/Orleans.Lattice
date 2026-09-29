# The Explorer navigation model

In the Explorer, the address is the navigation. Every page has one canonical
address. The browser's URL carries it, and the address line at the top of every
page shows it as a chain of nodes. You can type into that chain to go somewhere
else. A directory spine on the left lists the areas you can reach. A command
palette in the same address line runs anything a page can do.

This page covers the address grammar, the address line, completions, the
command palette, the directory spine, and how tenancy re-roots an address.

## The address grammar

An address is an optional tenant root, an area, the segments of the object
within that area, and an optional query:

```text
[/t/{tenant}]/{area}[/{segment}...][?{key}={value}&...]
```

A bare `/` (or `/t/{tenant}` when tenancy is on) is Home, the estate overview.
Some examples:

```text
/
/data
/data/a/crm/orders
/data/a/crm/orders?tab=history&key=order-42
/t/acme/replication/trees
/cluster/wal?tree=orders&partition=3
```

For the Data area, the segments below the area are the parts of the logical
tree id, so the tree `a/crm/orders` is `/data/a/crm/orders`. The full ABNF is in
the [UI package README](../../src/lattice.explorer/UI/NUGET_README.md).

The encoding rules are strict, so that every address has exactly one spelling:

- **Everything is lower case.** An area key is a lower-case letter followed by
  lower-case letters, digits and hyphens. A segment keeps `a-z`, `0-9`, `-`, `.`,
  `_` and `~`, and percent-encodes everything else as UTF-8 with upper-case hex,
  including an upper-case letter. A tree named `Orders` is addressed as
  `%4Frders`.
- **Dot segments are encoded.** The segments `.` and `..` are written `%2E` and
  `%2E%2E`, so a browser cannot collapse them.
- **Query values keep their case.** A query value keeps the RFC 3986 unreserved
  characters, upper case included, and percent-encodes the rest. Each query key
  appears at most once.
- **Some area keys are reserved.** No area may take `t`, which introduces a
  tenant root, or `not-found`, which is the not-found page.

Parsing typed or pasted input is lenient. An upper-case area key is read as
lower case, a leading `./` and a trailing `/` are ignored, a fragment is dropped,
and a raw character that the canonical form would encode is taken as itself. The
address the Explorer then shows and links to is always the canonical form.

Every link the Explorer renders is relative to the application's base path, so
the console works unchanged when a host mounts it under a subpath (see
[Running and hosting the Explorer](running-the-explorer.md)).

### Query keys

Query keys are how a page records its state in the address, so every view can
be bookmarked, shared and walked with Back and Forward. Three keys mean the same
thing wherever they appear: `key` names one key within the object, `prefix`
names a key prefix, and `at` names a point in time or a revision. Each area adds
its own keys, such as `tab` on a Data tree or `range` on a Telemetry board; they
are listed in [The Explorer areas](areas.md).

### Routes and the not-found page

Every route the Explorer declares is lower case and begins with a literal
segment, and none is a catch-all. A hygiene test fails the build otherwise. A
catch-all at the application root would also match static asset paths, so a
request for a script could be answered by the whole console. An object deeper
than an area's routes can express is carried in the query instead; for example,
an app's in-app path beyond its declared segments travels as `?path=`.

An address that does not resolve lands on the not-found page. It says that
nothing lives at that address, shows the address, and links the nearest address
that does exist for you: the root of its area when that area is visible to you,
and Home otherwise. An address in an area that is hidden from you renders the
same page, so the Explorer never confirms that such an area exists (see
[Area availability](area-availability.md)).

## The address line

When you are not typing, the address line shows the current address as a chain
of nodes in a mono typeface. Each ancestor is a link and the current node is
drawn as the marker, the order diagram's "you are here". A query is shown as one
final node, such as `?tab=history`. With a tenant root, the tenant (`t/acme`) is
the first node; otherwise the chain starts at `Home`.

Press `/` (outside a text field) or `Ctrl+K` (`Cmd+K` on a Mac), or select the
line, to turn it into an input. The input starts with the current address
selected, so typing replaces it. What you type decides what the line does:

| Input | Mode | Suggestions |
|---|---|---|
| Starts with `/` | Address | A "Go to" entry for the typed address, plus matches from every visible area. |
| Starts with `>` | Command palette | The commands whose title or id contains the text. |
| Starts with `t/` | Tenant | The tenants you may reach. Choosing one re-roots the current address. |
| Starts with `a/` | App | Matches from the visible areas' completions, such as installed apps. |
| Anything else | Search | The visible areas whose key or name matches, a "Go to" entry when the text is an area address, and matches from every visible area. |

The input is an ARIA 1.2 combobox. The arrow keys move through the suggestions,
Enter goes to the highlighted one (or the first when none is highlighted), and
Escape restores the chain and returns focus to where you started. A polite
status region announces how many suggestions there are.

### Completions

Each visible area answers completions from its own data, such as tree ids in
Data or backup ids in Backups. The Explorer asks every visible area in parallel,
each under its own time bound of two seconds, and shows each area's group as
soon as it answers. The groups always appear in directory order, however the
answers race, so the list never reshuffles under the pointer. An area that does
not answer in time contributes nothing and is named in a note ("Data did not
answer in time." or "Data could not be searched."); it never holds back the
others. A new keystroke cancels the completions still running. An area that is
unavailable to you is never asked.

## The command palette

Typing `>` turns the address line into the command palette. The palette offers:

- **Chrome commands.** `go.home` goes to Home, `go.{area}` goes to each visible
  area (for example `go.data`), and the appearance commands set the theme,
  contrast and density (`appearance.theme.system`, `appearance.theme.paper`,
  `appearance.theme.board`, `appearance.contrast.system`,
  `appearance.contrast.standard`, `appearance.contrast.more`,
  `appearance.density.comfortable` and `appearance.density.compact`).
- **Area commands.** Each visible area contributes its own, such as
  `data.refresh` or `backups.capture`. They are listed with their areas in
  [The Explorer areas](areas.md).

Choosing a command first navigates to the page the command belongs to, then runs
it there. Nothing is palette-only: every command is also a visible control on
its page, and that control carries the command's id in a `data-lt-command`
attribute. The palette is a faster way to reach a control, never the only way.

## The directory spine

The directory spine is the order diagram's sidebar: hollow nodes on a hairline,
with the current stop drawn as the ringed marker node and a heavier label. Home
heads the spine, followed by one stop per area you can reach, in a fixed order:
Data, Apps, Access, Schema, Tenancy, Replication, Backups, Telemetry and
Cluster.

- A **visible** area's stop links to its root. It may carry a short badge, such
  as a count.
- An **unavailable** area's stop is shown, demoted, with the one-sentence reason
  it cannot be opened now, such as "Sign in to administer access on this
  cluster."
- A **hidden** area has no stop at all.

How an area decides between these is described in
[Area availability](area-availability.md). The spine is asked again on every
navigation, and whenever sign-in, the connection or the configuration changes,
because each of those can change which areas you may see.

Home shows the same stops as an estate overview, each with a one-line status
under its name, such as how many trees there are. Each status arrives
independently under its own time bound, so a slow area never delays the others.

### At different widths

The Explorer is phone-first-class for reading and simple actions. It measures its
own width and uses three bands:

| Band | Width | Spine | Header | Address line |
|---|---|---|---|---|
| Expanded | 1200px and up | A full spine with badges and reasons. | Connection, appearance and identity controls in a row. | The full chain. |
| Medium | 768px to 1199px | A rail: every label, but no badges or reasons. | As expanded. | The full chain. |
| Compact | Below 768px | A slide-in sheet opened from the **Directory** button, which returns focus when it closes. | The mark and name, with a **Menu** button holding the connection, identity and appearance controls. | The last two nodes after a `...` node that opens the full chain as a list. The command palette opens as a full-screen sheet. |

Until the width has been measured, the expanded layout renders, so nothing
depends on script to be usable.

Skip links come first on every page: **Skip to directory**, **Skip to address**
and **Skip to content**.

## Tenancy and re-rooting

When the host enables tenancy, the active tenant is the root node of Home and of
every tenant-scoped address: `/t/acme/data/orders` rather than `/data/orders`.
Access and Cluster are cluster-wide and never carry a tenant root. The Tenancy
area's operator directory at `/tenancy` is cluster-wide, while its My tenant
pages at `/t/{tenant}/tenancy` are tenant-rooted. Every other area is
tenant-scoped.

The Explorer keeps every address canonical for your tenancy:

- **Tenancy off.** No address carries a tenant root, and an address that has one
  is redirected to the same address without it.
- **Tenancy on.** An address without a tenant root is rooted at the active
  tenant. A cluster-wide area's address loses any tenant root it was given.
- **Another tenant's address.** Arriving at `/t/{other}/...` is a request to
  switch to that tenant. It goes through the operator-gated tenant switch. If the
  switch succeeds, the Explorer says so. If it is refused, the Explorer redirects
  to the active tenant's equivalent address and shows a warning, so a URL can
  never scope you beyond what you may reach.

To re-root the current address, type `t/` in the address line. It completes the
tenants you may reach, marking the active one. Choosing one keeps the rest of the
address and replaces its tenant root; a cluster-wide address is unchanged.

A caller scoped to the reserved `default` tenant who is not a platform operator
sees no tenancy chrome: addresses stay plain, and `/t/default/...` is
redirected to the plain form. See [Tenant scope](tenant-scope.md) for the full
tenancy model and the Tenancy area.

## Where the address ends and preferences begin

The address carries where you are. Preferences carry how you like the console,
such as the theme and density, and an explicit address is never overridden by a
remembered value. See [What the Explorer remembers](what-the-explorer-remembers.md).

## See also

- [Area availability](area-availability.md)
- [The Explorer areas](areas.md)
- [Tenant scope](tenant-scope.md)
- [What the Explorer remembers](what-the-explorer-remembers.md)
- [Theming and density](theming-and-density.md)
- [Accessibility conformance](accessibility-conformance.md)
