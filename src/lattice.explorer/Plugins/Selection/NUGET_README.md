# Orleans.Lattice.Explorer.Plugins.Selection

The shared kernel for Orleans.Lattice Explorer **per-selection** plugins - the
surfaces that render whatever tree, view or tag index the operator has selected.

It carries only what every per-selection plugin needs, and nothing a plugin
could use to reach around its own declared domain contract:

- `SelectionPluginViewBase` - the base a plugin's view derives from. It supplies
  the selected catalog entry as a parameter and a cancellation token that is
  cancelled when the view is torn down, so a superseded selection abandons its
  in-flight loads.
- `SelectionPluginKeys` - the stable, package-owned plugin ids the shipped
  surfaces register under, and the durable preference key the active surface is
  retained in.
- `ISelectionNestedSurface`, `ISelectionNestedSurfaceRegistry`, and
  `SelectionNestedSurfaceKeys` - the seam one surface uses to render a view
  contributed by another package inline (the Data surface's per-key History
  panel, for example), looked up by stable id so neither package references the
  other; a hosting surface shows no affordance when nothing is registered.
  `AddExplorerSelectionNestedSurface<TSurface>()` contributes a view, and
  `AddExplorerSelectionPluginHost()` registers the shared per-selection kernel.
- `SelectionStateView` and `SelectionBadgeList` - the shared empty-state block and
  readable badge run every per-selection surface renders.
- `ChangeHistoryGuidance` - the shared card a surface renders when the selection
  is a change-history view, whose rows are serialized history records rather
  than directly inspectable data.

The package contains no reader, no gRPC client, and no cluster connection.

See the [Orleans.Lattice repository](https://github.com/NSTA1/Orleans.Lattice)
for the plugin model.
