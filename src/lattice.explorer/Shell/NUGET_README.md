# Orleans.Lattice.Explorer.Shell

The Orleans.Lattice Explorer Shell: a Razor class library for everything
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

Nothing in the Shell is an extension point. There is no public area
registration API: the only way third parties put UI into the Explorer is a
Lattice App, whose UI runs in a sandboxed, credential-free frame.

Static assets are served from `_content/Orleans.Lattice.Explorer.Shell/`, with
the design system under `design/`. You do not reference this package directly;
the Explorer web head (`AddLatticeExplorerWeb` and `MapLatticeExplorer`) brings
it in.

This package is in progress and has not shipped a release.
