# Orleans.Lattice.Explorer.Plugins.Topology

The tree-topology per-selection surface for the Orleans.Lattice Explorer,
shipped as a self-contained plugin: a load-coloured radial graph of the selected
tree's shard roots and internal nodes, with lazy subtree expansion, pan and
zoom, and a leaf-node toggle.

Register it on a head with:

```csharp
services.AddExplorerTopologyPlugin();
```

Withholding the call ships no topology surface at all; nothing else has to
change.

The plugin reaches the cluster only through the `ITopologySurface` domain
contract it declares, never through the raw state-API connection, and carries
its own scoped stylesheet and its own pan-zoom script as packaged static web
assets.

See the [Orleans.Lattice repository](https://github.com/NSTA1/Orleans.Lattice)
for the plugin model.
