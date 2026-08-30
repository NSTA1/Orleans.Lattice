# Orleans.Lattice.Explorer.Plugins.DeadLetter

The strict-mode dead-letter per-selection surface for the Orleans.Lattice
Explorer, shipped as a self-contained plugin: a read-only, cursor-paged view of
the entries the selected tree rejected, with a per-entry detail pane showing the
reason, the source and the rendered value preview.

Register it on a head with:

```csharp
services.AddExplorerDeadLetterPlugin();
```

Withholding the call ships no dead-letter surface at all; nothing else has to
change.

The surface never mutates, requeues or replays an entry: the state API exposes
no write path for it. It reaches the cluster only through the
`IDeadLetterSurface` domain contract it declares, and carries its own scoped
stylesheet written against the Explorer design-system tokens.

See the [Orleans.Lattice repository](https://github.com/NSTA1/Orleans.Lattice)
for the plugin model.
