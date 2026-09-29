# Orleans.Lattice.Explorer.UI

The Orleans.Lattice Explorer UI: a Razor class library for everything
**outside** a Lattice App's frame.

- The navigation and session chrome: the directory spine, the address line, the
  command palette, sign-in, re-authentication and the identity menu.
- The shell-owned native areas - Data, Apps, Access, Schema, Tenancy,
  Replication, Backups, Telemetry and Cluster - compiled in, each deciding its
  own visibility from its facade's capability probe and failing closed.
- The order-diagram design system in its Operate register: the documentation
  site's own `tokens.css` and fonts (linked at build time, never copied), the
  Explorer-only density, focus and lifecycle and health state tokens, and the
  primitives every area is drawn with.
- The credential-aware transport adapters, and the app frame host and bridge
  broker that keep a Lattice App's UI sandboxed.

Nothing in this package is an extension point. There is no public area
registration API: the only way third parties put UI into the Explorer is a
Lattice App, whose UI runs in a sandboxed, credential-free frame.

Static assets are served from `_content/Orleans.Lattice.Explorer.UI/`, with
the design system under `design/` and the navigation chrome under `shell/`. You
do not reference this package directly; the Explorer web head
(`AddLatticeExplorerWeb` and `MapLatticeExplorer`) brings it in.

## The address grammar

Every Explorer page has one canonical address, which the address line shows as
a chain of nodes and accepts as input. With tenancy on, the active tenant is the
root node of every tenant-scoped address; with tenancy off there is no tenant
node, and a `/t/...` address is redirected to the same address without one.
For the Data area, the segments below the area are the logical tree id's parts,
so the tree `a/crm/orders` is `/data/a/crm/orders`.

```abnf
address        = ( home / area-address ) [ "?" query ]
home           = "/" / tenant-root
area-address   = [ tenant-root ] "/" area *( "/" segment )
tenant-root    = "/t/" segment
area           = LCALPHA *( LCALPHA / DIGIT / "-" )   ; never "t"
segment        = 1*( seg-char / pct-encoded )           ; never "." or ".."
seg-char       = LCALPHA / DIGIT / "-" / "." / "_" / "~"
query          = param *( "&" param )
param          = key "=" value                          ; each key at most once
key            = LCALPHA *( LCALPHA / DIGIT / "-" )     ; key, prefix, at, ...
value          = *( seg-char / UCALPHA / pct-encoded )
pct-encoded    = "%" HEXUP HEXUP                        ; UTF-8 bytes
LCALPHA        = %x61-7A
UCALPHA        = %x41-5A
HEXUP          = DIGIT / %x41-46
```

Every route segment is lower case: any other character in a tenant id or a
segment, including an upper-case letter, is percent-encoded as UTF-8 with
upper-case hex, so a tree named `Orders` is addressed as `%4Frders`, and the dot
segments `.` and `..` are written `%2E` and `%2E%2E` so a browser cannot remove
them. A query value keeps the RFC 3986 unreserved characters, upper case
included, and percent-encodes the rest. An unknown address lands on the
not-found page, which names the nearest address that does exist.

This package is in progress and has not shipped a release.
