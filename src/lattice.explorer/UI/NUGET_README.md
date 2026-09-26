# Orleans.Lattice.Explorer.UI

The shared **Razor component class library** (RCL) for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice). Holds every
routable page, the layout, and the navigation, detail, configuration, appearance,
and authentication components, so each explorer head (web, desktop) renders an
identical UI. The admin areas (Backups, Access, Schema, Tenant administration, My
tenant, Telemetry) ship in their own plugin packages.

## What it provides

- The `Routes` root router and all routable explorer pages.
- The shared layout and reusable UI components.
- The shell-side registration helpers: `AddExplorerPluginAdapters()` (the host
  state and preference adapters every plugin needs), `AddExplorerSelectionPlugins()`
  (the one-call composite over the per-selection surfaces), `AddExplorerAppearance()`
  (theme, contrast and density), and `AddExplorerChromeSlot<TComponent>()` (a
  component contributed to a banner region).
- Dependencies on the per-selection surface packages - `Orleans.Lattice.Explorer.Plugins.Data`,
  `.Topology`, `.Metrics`, `.DeadLetter`, `.TagIndex`, and `.History` - on their
  shared kernel `Orleans.Lattice.Explorer.Plugins.Selection`, and on
  `Orleans.Lattice.Explorer.Plugins.Abstractions`, so the composite registration
  can reach every surface.
- `lattice-shell.css`, the stylesheet for the shell chrome those components
  render: the brand bar, the navigation rail, the detail panel, the area strip,
  and the authentication, tenant, and configuration surfaces. Shared UI
  primitives (buttons, badges, modals, navigation, tab strips) come from
  [`Orleans.Lattice.Explorer.DesignSystem`](https://www.nuget.org/packages/Orleans.Lattice.Explorer.DesignSystem)
  instead, so a plugin composes them without referencing this package.
- The packaged **static web assets** (the shell and appearance stylesheets, the
  first-paint appearance script, and the favicon). A referencing app serves them
  automatically at `_content/Orleans.Lattice.Explorer.UI/` with no extra wiring.

## Usage

Normally consumed transitively through a head package such as
[`Orleans.Lattice.Explorer.Web`](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Web),
which maps the components with an interactive server render mode. Reference the
static assets from the host document. The appearance script must stay a classic,
blocking script in `<head>` (no `defer` or `async`), so the chosen theme is on
the document at first paint:

```html
<link rel="stylesheet" href="_content/Orleans.Lattice.Explorer.UI/lattice-shell.css" />
<link rel="stylesheet" href="_content/Orleans.Lattice.Explorer.UI/lattice-appearance.css" />
<script src="_content/Orleans.Lattice.Explorer.UI/lattice-appearance.js"></script>
```

and, first thing in `<body>`, stamp the chosen density:

```html
<script>window.latticeAppearance && window.latticeAppearance.stamp();</script>
```

See the
[Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.explorer/README.md)
for the full guide.
