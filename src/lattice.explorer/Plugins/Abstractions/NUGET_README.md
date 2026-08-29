# Orleans.Lattice.Explorer.Plugins.Abstractions

The **plugin contract** for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice). It holds
the types a plugin package implements and the host machinery that drives them,
and nothing else: no UI, no cluster access, and no dependency on the Explorer
core.

## What it provides

- **The descriptor.** A plugin declares a stable `string` plugin id, a display
  label, an ordering hint, and the surface it occupies - `Area` for the
  top-level switcher, `Selection` for the per-selection tier. One model serves
  both navigation tiers.
- **A four-state access model.** A plugin supplies its own access gate, whose
  probe resolves to `Allowed`, `Denied` (advisory grey-out), `AuthenticationRequired`
  (prompt a sign-in), or `Unavailable` (the capability is not installed on this
  cluster).
- **A keyed access store.** Results are keyed by plugin id, with an optional
  scope for a per-resource decision, and published through a change
  notification. A refresher probes every gate independently: one plugin's probe
  throwing, or never completing, cannot disturb another's, and a faulted probe
  resolves to denied, never allowed.
- **The host context.** The ambient facts a plugin may read - the current
  selection, the connection status, the active tenant and visibility, and a
  plugin-scoped preference store. It deliberately exposes no cluster connection,
  no gRPC channel, and no other plugin's services.
- **The controlled domain-model seam.** A plugin declares the single domain
  contract it operates against and the host resolves it, so a plugin's reach is
  explicit and reviewable.

## Usage

Register the host machinery once, then one call per plugin:

```csharp
services.AddExplorerPluginHost();
services.AddExplorerPlugin<BackupsPlugin>();
```

Access gating is advisory on the client. The server remains the sole
enforcement point, so every plugin action must still handle a runtime denial.

## Documentation

See the [Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/tree/main/docs/lattice.explorer).
