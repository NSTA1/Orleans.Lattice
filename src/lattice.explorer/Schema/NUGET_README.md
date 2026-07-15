# Orleans.Lattice.Explorer.Schema

The **Schema (enforcement, versioning, remediation, and compliance) management
area** for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice). Bridges the
schema control-API gRPC client to the explorer's head-agnostic navigation and
capability model, so the Schema area renders identically on every explorer head.

## What it provides

- The schema control-API client, wired over the same endpoint and sign-in as the
  read-only state connection.
- The policy, versioning / remediation, and compliance administration services.
- A **capability probe** that gates the Schema area, so it greys out when the
  connected cluster does not expose the schema control facade or the caller may
  not administer it.

## Usage

Normally consumed transitively through a head package such as
[`Orleans.Lattice.Explorer.Web`](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Web).
Register directly when composing a custom head:

```csharp
services.AddExplorerSchema();
```

See the
[Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.explorer/README.md)
for the full guide.
