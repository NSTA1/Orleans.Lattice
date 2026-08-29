# Orleans.Lattice.Explorer.Plugins.TagIndex

The tag-index browsing per-selection surface for the Orleans.Lattice Explorer,
shipped as a self-contained plugin: the trees an index covers, the tags it
carries, and the live members of a chosen tag - each navigable back into the
tree and key that holds it.

Register it on a head with:

```csharp
services.AddExplorerTagIndexPlugin();
```

This surface declares that it applies to **tag-index selections alone**, and the
generic surfaces declare that they do not. That single fact is what lets a
membership tree resolve to a different plugin set through ordinary applicability
instead of the shell special-casing it.

It reaches the cluster only through the `ITagIndexSurface` domain contract it
declares, and carries its own scoped stylesheet written against the Explorer
design-system tokens.

See the [Orleans.Lattice repository](https://github.com/NSTA1/Orleans.Lattice)
for the plugin model.
