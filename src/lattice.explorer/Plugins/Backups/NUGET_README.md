# Orleans.Lattice.Explorer.Plugins.Backups

The **Backups plugin** for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice): a
self-contained plugin package carrying its own services, Razor components, and
scoped stylesheet, so the Backups area renders identically on every explorer
head and the shared UI library takes no dependency on it.

## What it provides

- The backup control-API client, wired over the same endpoint and sign-in as the
  read-only state connection.
- The catalogue reader that projects backup manifests onto the display rows the
  area renders.
- The plugin's **controlled domain model** (`IBackupsDomain`), which is the whole
  of what the host resolves for the Backups panel: the panel receives no cluster
  connection, no gRPC channel, and no other plugin's services.
- An **access gate** that files the plugin-level decision and the per-tree
  capture / incremental / restore / delete decisions into the Explorer's keyed
  plugin access store, so the area and its per-scope actions grey out when the
  connected cluster does not expose the backup control facade or the caller may
  not use it. The gate is advisory; the server stays the sole enforcement point.
- A plugin-scoped stylesheet served at
  `_content/Orleans.Lattice.Explorer.Plugins.Backups/lattice-backups.css`,
  written against the Explorer design-system tokens.

## Usage

Normally consumed transitively through a head package such as
[`Orleans.Lattice.Explorer.Web`](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Web).
Register directly when composing a custom head:

```csharp
services.AddExplorerPluginAdapters();
services.AddExplorerBackup();
services.AddExplorerBackupsPlugin();
```

and link the plugin stylesheet from the head's document head:

```html
<link rel="stylesheet" href="_content/Orleans.Lattice.Explorer.Plugins.Backups/lattice-backups.css" />
```

See the
[Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.explorer/README.md)
for the full guide.
