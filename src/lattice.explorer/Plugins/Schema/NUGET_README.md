# Orleans.Lattice.Explorer.Schema

The Schema management area of the [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice)
Explorer, packaged as a self-contained **plugin**: its registration, its
controlled domain model, its Razor components, and its own plugin-scoped
stylesheet.

The area covers, per governed tree:

- **Enforcement policy** - view, author, and clear the rules a value must satisfy.
- **Versioning and remediation** - opt a tree into envelope versioning, advance
  the target version, migrate stored values, and read remediation status.
- **Compliance** - a read-only audit of current values against the policy.
- **Dead letters** - the strict-mode ingest queue of diverted items.

## Registering it

A head surfaces the area by registering the plugin, and withholds it by not
registering it. There is no per-area option flag:

```csharp
// Surfaces the Schema tab in the Explorer shell.
services.AddExplorerSchemaPlugin();
```

The Explorer web head (`AddLatticeExplorerWeb`) wires the schema control
services but deliberately does **not** register this plugin, so the area is
absent unless a head opts in. That preserves the long-standing default: the
versioning UI cannot yet express what differs between schema versions.

## Styling

The plugin ships its own stylesheet, built entirely on the design-system token
layer and free of any width media query (breakpoints live in exactly one file).
Reference it from the host document after the design-system stylesheets:

```html
<link rel="stylesheet" href="_content/Orleans.Lattice.Explorer.Schema/lattice-schema.css" />
```

## Gating

The plugin owns its own access gate. The plugin-level decision answers "is the
schema control endpoint reachable"; per-tree, per-action decisions are filed
under **scoped** access keys (`orleans.lattice.schema` + `{treeId}/{action}`)
so a denied action renders disabled rather than hidden. Gating is advisory - the
server remains the sole enforcement point.
