# Orleans.Lattice.Explorer.Plugins.Metrics

The live-metrics per-selection surface for the Orleans.Lattice Explorer, shipped
as a self-contained plugin: lifecycle, shard count, live keys, tombstones, depth
and split tiles for the selected tree or view, plus its per-shard hotness table.

Register it on a head with:

```csharp
services.AddExplorerMetricsPlugin();
```

Withholding the call ships no metrics surface at all; nothing else has to change.

The plugin reaches the cluster only through the `IMetricsSurface` domain
contract it declares, never through the raw state-API connection, and carries
its own scoped stylesheet written against the Explorer design-system tokens.

See the [Orleans.Lattice repository](https://github.com/NSTA1/Orleans.Lattice)
for the plugin model.
