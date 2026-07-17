# Orleans.Lattice.Explorer.UI

The shared **Razor component class library** (RCL) for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice). Holds every
routable page, the layout, and the navigation, detail, backup, access, and
authentication components, so each explorer head (web, desktop) renders an
identical UI.

## What it provides

- The `Routes` root router and all routable explorer pages.
- The shared layout and reusable UI components.
- The packaged **static web assets** (css, favicon, topology interop JS). A
  referencing app serves them automatically at
  `_content/Orleans.Lattice.Explorer.UI/` with no extra wiring.

## Usage

Normally consumed transitively through a head package such as
[`Orleans.Lattice.Explorer.Web`](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Web),
which maps the components with an interactive server render mode. Reference the
static assets from the host document:

```html
<link rel="stylesheet" href="_content/Orleans.Lattice.Explorer.UI/app.css" />
```

See the
[Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.explorer/README.md)
for the full guide.
