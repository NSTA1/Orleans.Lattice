# Orleans.Lattice.Explorer.Backup

The **Backups management area** for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice). Bridges the
backup control-API gRPC client to the explorer's head-agnostic navigation and
capability model, so the Backups area renders identically on every explorer head.

## What it provides

- The backup control-API client, wired over the same endpoint and sign-in as the
  read-only state connection.
- The catalog reader that projects backup manifests onto the explorer's
  navigation model.
- A **capability probe** that gates the Backups area and its per-scope actions,
  so the area greys out when the connected cluster does not expose the backup
  control facade or the caller may not use it.

## Usage

Normally consumed transitively through a head package such as
[`Orleans.Lattice.Explorer.Web`](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Web).
Register directly when composing a custom head:

```csharp
services.AddExplorerBackup();
```

See the
[Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.explorer/README.md)
for the full guide.
