# Orleans.Lattice.Explorer.Plugins.Data

The key and value drill-down per-selection surface for the Orleans.Lattice
Explorer, shipped as a self-contained plugin: a cursor-paged key browser with a
starts-with key search, an optional tag-index filter, a value inspector that
live-follows the selected key, and decoded CRDT current state for a typed entry.

Register it on a head with:

```csharp
services.AddExplorerDataPlugin();
```

Withholding the call ships no data surface at all; nothing else has to change.

The per-key **History** button hands off to the revision-timeline surface by
plugin id through `ISelectionSurfaceSwitcher`, so this package does not
reference, render or know the shape of that one. If a head does not register the
history surface, the button is simply not shown.

It reaches the cluster only through the `IDataSurface` domain contract it
declares - never the raw state-API connection - and carries its own scoped
stylesheet written against the Explorer design-system tokens.

See the [Orleans.Lattice repository](https://github.com/NSTA1/Orleans.Lattice)
for the plugin model.
