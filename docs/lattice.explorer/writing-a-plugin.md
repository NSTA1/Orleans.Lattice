# Writing an Explorer plugin

The Explorer's UI is composed of **plugins**. A plugin owns one surface of the
console: its own view, its own domain contract, its own access gate, and its own
package. The shell has no per-plugin knowledge - it enumerates
`IExplorerPlugin` from the container, resolves each plugin's declared domain
contract, asks each plugin's gate whether it is reachable, and renders whatever
`ViewType` each one names.

That is the whole extension model. Adding a tab means adding a package and
registering it; withholding one means not registering it. There is no per-area
option flag, no enum to extend, and no `switch` in the shell to edit.

## The two surfaces

| Surface | Where it renders | Applicability |
|---|---|---|
| `ExplorerPluginSurface.Area` | The top-level area strip (Backups, Access, Schema, Tenants, My Tenant) | Always applicable; ordered by `Order` |
| `ExplorerPluginSurface.Selection` | The detail panel for the selected tree, view, or tag index | Filtered by `SelectionKinds` |

A selection plugin declares which selection kinds it applies to, so a tag-index
selection resolves to a different plugin set than a tree does through ordinary
applicability rather than through a special case in the panel.

## The minimum

A plugin is one class. It declares its identity, the component to render, the
domain contract that is the whole of its reach, and its gate:

```csharp verify
using System;
using Orleans.Lattice.Explorer.Plugins;

// The domain contract: the single seam this plugin operates against. The host
// resolves only this type for this plugin, so the plugin's reach is explicit in
// its own source and reviewable in isolation.
public interface IWidgetSurface
{
    bool IsAvailable { get; }
}

public sealed class WidgetSelectionPlugin : IExplorerPlugin<IWidgetSurface>
{
    // A static readonly descriptor: allocated once, not per render.
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = "orleans.lattice.widget",
        Label = "Widgets",
        Surface = ExplorerPluginSurface.Selection,
        Order = 600,
        SelectionKinds = ExplorerPluginSelectionKinds.Tree | ExplorerPluginSelectionKinds.View,
    };

    public ExplorerPluginDescriptor Descriptor => Registration;

    public Type ViewType => typeof(WidgetTab);

    public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
}

// Stands in for the Razor component the shell renders dynamically.
public sealed class WidgetTab
{
}
```

Implementing `IExplorerPlugin<TDomain>` rather than `IExplorerPlugin` declares
the domain contract in the type system, so `DomainContract` is filled in for you
and cannot drift from what the plugin actually resolves.

## Rules that are enforced by tests

Each of these is guarded, because each has already been got wrong at least once.

### Package identity

A new plugin package is named `Orleans.Lattice.Explorer.Plugins.<Area>`, and its
`PackageId`, `RootNamespace` and csproj **file name** all agree. Leave
`AssemblyName` unset so it defaults to the file name.

Three package ids omit the `Plugins.` segment - `Orleans.Lattice.Explorer.Access`,
`.Backup` and `.Schema`. Those are **frozen, not exemplary**: each has already
shipped to NuGet, so renaming them would break existing consumers. Do not copy
them. Judge by release status, not by what a sibling looks like:

```text
git tag | Select-String lattice.explorer.<area>
```

Zero tags means the id is still free and must follow the current rule.

This matters more than it looks. Everything in the repository uses project
references, so a wrongly named package still builds and still passes every test.
The break appears only in a consumer's restore, after publish.

### A distinct `Order`

Two area plugins sharing an `Order` compiles and renders, and hands the relative
position of two tabs to whatever tie-break the catalogue's sort happens to use.
Current area values are 100 (Backups), 200 (Access), 300 (Schema), 400 (Tenants)
and 500 (My Tenant). Pick an unused one.

### An access gate

Every plugin declares one. `ExplorerPluginAccessGates.Allowed` is a legitimate
choice for a surface that exposes no capability of its own to probe, but it is a
decision to state rather than a default to inherit - say why in the member's
remarks, as the Metrics plugin does.

A denied plugin is **never mounted**, rather than mounted and hidden. The gate is
advisory for presentation only: the server remains the sole enforcement point for
every call the plugin then issues.

### Release plumbing

A new package must be registered in two places or it silently never ships:

- a tag glob in `.github/workflows/publish.yml`;
- a row in **both** tables in `docs/RELEASING.md`.

This is not optional housekeeping. `Orleans.Lattice.Explorer.UI` is already
published and depends on the plugin packages, so an unpublished dependency breaks
restore for every consumer of the whole Explorer family.

## Styling

Style against the design system (`Orleans.Lattice.Explorer.DesignSystem`), which
ships the token layer, the named breakpoints, and the adaptive primitives.

**Reach for a primitive before writing a rule.** The design system already ships:

| Family | Classes |
|---|---|
| Layout | `lx-root`, `lx-table` (+ `lx-cell`), `lx-cardlist` (+ `lx-card`, `lx-card-title`, `lx-card-fields`) |
| Navigation | `lx-nav` family, including the drawer, bottom bar, sidebar and overflow variants |
| Tabs | `lx-tabstrip`, `lx-tab`, `lx-tabpanel`, and their overflow variants |
| Buttons | `lx-btn` with `lx-btn-primary`, `lx-btn-danger`, `lx-btn-icon`, `lx-btn-link` |
| Dialogs | `lx-modal`, `lx-modal-backdrop`, `lx-modal-actions` |
| Status | `lx-badge`, `lx-badge-muted` |

`lx-tab` and `lx-btn` spend `--lx-target-min`, so they meet the compact touch-target
size without a plugin doing anything. A hand-rolled button will not.

A plugin stylesheet should contain only what is genuinely specific to that plugin.
The `explorer-` class prefix is **retired**: a hygiene test fails the build on any
`explorer-*` class in a `.razor` class attribute or a stylesheet selector. If you find
one in an old file you are copying, it is a leftover, not a pattern.

**Never write a width media query.** Branch on the cascaded
`LatticeAdaptiveContext.Breakpoint` - `Compact`, `Medium`, or `Expanded` - which
the shell's `LatticeAdaptiveRoot` supplies. A hygiene test fails the build on a
raw breakpoint value in a plugin stylesheet.

If your plugin ships a stylesheet, its static assets are served from
`_content/<AssemblyName>/`, and you must add the `<link>` to **both** heads:

- `src/lattice.explorer/WebHosting/Components/App.razor`
- `src/lattice.explorer/Maui/wwwroot/index.html`

A missing or stale link 404s silently. There is no build error, no console error
a reviewer would notice, and no failing test - just an unstyled panel.

## Testing

Beyond the usual per-member coverage, one assertion is mandatory and is the one
most easily skipped.

**Assert the rendered markup at a named breakpoint, not the state that feeds
it.** A plugin that tests only state can pass forever while rendering a single
shape. This is not hypothetical: the shell's `MainLayout` once failed to host
`LatticeAdaptiveRoot` at all, so the cascaded breakpoint was pinned to `Expanded`
and nothing reflowed anywhere - and every test stayed green, because the
declarative half worked and the imperative half did not.

For each surface that ships an adaptive table, assert:

1. at `Expanded`, a real `<table>` with `lx-table`, and no `lx-cardlist`;
2. at `Compact`, `lx-cardlist` and **no `<table>` at all**, so the reflow
   genuinely fires rather than merely being declared;
3. every field survives the reflow individually - a card that silently drops a
   column is data loss, not a layout nit;
4. row actions remain reachable in the card, or a narrow-viewport operator is
   stranded.

`BackupsRenderHarness` and `AccessRenderHarness` are the pattern to copy. Both
build on the framework's `HtmlRenderer` and need no extra test dependency, and
both drive the real view over a real workspace with a stubbed domain rather than
mocking the view.

Tests must be reliable: nothing timing-dependent, ordering-dependent,
`Task.Delay`-race-based, or dependent on the wall clock or GC.

## Checklist

- [ ] Package named `Orleans.Lattice.Explorer.Plugins.<Area>`; `PackageId`,
      `RootNamespace` and file name agree; `AssemblyName` unset.
- [ ] Lives in its own folder under `src/lattice.explorer/Plugins/`.
- [ ] Registered in `Orleans.Lattice.slnx` and
      `src/lattice.explorer/Orleans.Lattice.Explorer.slnx`.
- [ ] Descriptor is `static readonly`; `Order` is unused by any other plugin on
      the same surface.
- [ ] Declares its domain contract via `IExplorerPlugin<TDomain>`.
- [ ] Declares an access gate, with a stated reason when it is `Allowed`.
- [ ] No width media query; branches on the cascaded breakpoint.
- [ ] Uses design-system primitives rather than hand-rolled buttons, dialogs or
      badges; no `explorer-*` class anywhere.
- [ ] Stylesheet linked in **both** heads, `NUGET_README.md`, and the csproj
      `<Description>`.
- [ ] Compact-reflow render assertions for every adaptive table: a real `<table>`
      at `Expanded`; `lx-cardlist` and no `<table>` at `Compact`; every field
      asserted individually across the reflow; and every row action still
      reachable in the card. See [Testing](#testing).
- [ ] Tag glob in `publish.yml` and rows in both `docs/RELEASING.md` tables.
- [ ] A `README.md` row under Child Packages.
