# Orleans.Lattice.Explorer.Plugins.History

The per-key revision-timeline per-selection surface for the Orleans.Lattice
Explorer, shipped as a self-contained plugin: the retention-aware change history
of the key the operator drilled into, with value diffs, CRDT member changes,
retention-transition dividers, and a **forward-only live follow mode** that
appends new revisions as they are emitted and silently upgrades them to durable
rows once the history view records them.

Register it on a head with:

```csharp
services.AddExplorerHistorySurface();
```

The timeline is **not** a tab, and this registers none. It is reached exactly as
it always has been: from a row on the value drill-down surface, through that
row's History button, for the key the operator drilled into. Withholding the
call simply removes that button.

It is rendered inline through the shared nested-surface registry, so neither
this package nor the value drill-down package references the other.

It reaches the cluster only through the `IHistorySurface` domain contract it
declares, and carries its own scoped stylesheet written against the Explorer
design-system tokens.

See the [Orleans.Lattice repository](https://github.com/NSTA1/Orleans.Lattice)
for the plugin model.
