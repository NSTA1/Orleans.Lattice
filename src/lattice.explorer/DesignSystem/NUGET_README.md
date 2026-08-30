# Orleans.Lattice.Explorer.DesignSystem

The **design layer** for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice): the design
tokens, the single named breakpoint set, and the adaptive shell primitives every
Explorer plugin is styled against.

It has **no project dependencies** beyond the Blazor component model, so a
plugin can consume it without taking on the Explorer core or any feature
package.

## What it provides

- **A token layer** (`lattice-tokens.css`): spacing, type scale, colour
  (including the Explorer's dark default and light theme), elevation, motion,
  radius, density, and touch-target sizing, all as `--lx-*` custom properties
  defined once.
- **A named breakpoint set** (`lattice-breakpoints.css`): `compact`, `medium`,
  and `expanded`, declared once and referenced by name. This is the only
  stylesheet in the product permitted to carry a width media query; a test guard
  fails the build if any other file grows one.
- **Adaptive shell primitives**: `LatticeAdaptiveNav` (persistent sidebar,
  dismissible drawer, or bottom-and-overflow navigation), `LatticeAdaptiveTabs`
  (a tab strip that collapses to an overflow menu rather than scrolling
  off-screen), and `LatticeAdaptiveTable<TItem>` (tabular on wide viewports, a
  card list on compact).
- **Shared UI primitives** (`lattice-primitives.css`): the button family
  (`lx-btn` with `lx-btn-primary`, `lx-btn-danger`, `lx-btn-icon` and
  `lx-btn-link`), the `lx-badge` label, and the `lx-modal` dialog with its
  backdrop and action row. These are the controls every plugin composes, so
  they live here rather than in any one plugin - or, as they once did, in a
  shared monolith a plugin had to depend on the shell to reach.

## Usage

Reference the stylesheets from the host document, before any app stylesheet:

```html
<link rel="stylesheet" href="_content/Orleans.Lattice.Explorer.DesignSystem/lattice-tokens.css" />
<link rel="stylesheet" href="_content/Orleans.Lattice.Explorer.DesignSystem/lattice-breakpoints.css" />
<link rel="stylesheet" href="_content/Orleans.Lattice.Explorer.DesignSystem/lattice-primitives.css" />
```

Register the viewport seam and wrap the shell in `LatticeAdaptiveRoot`, then
take the breakpoint as a cascading parameter in any component:

```csharp
services.AddLatticeExplorerDesignSystem();
```

See the
[Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.explorer/README.md)
for the full guide.
