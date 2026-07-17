# Orleans.Lattice.Explorer.Access

The **Access (membership and access-control) management area** for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice). Bridges the
auth-admin control-API gRPC client to the explorer's head-agnostic navigation and
capability model, so the Access area renders identically on every explorer head.

## What it provides

- The auth-admin control-API client, wired over the same endpoint and sign-in as
  the read-only state connection.
- The membership and policy administration services.
- A **capability probe** that gates the Access area, so it greys out when the
  connected cluster does not expose the auth control facade or the caller may not
  administer it.

## Usage

Normally consumed transitively through a head package such as
[`Orleans.Lattice.Explorer.Web`](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Web).
Register directly when composing a custom head:

```csharp
services.AddExplorerAccess();
```

See the
[Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.explorer/README.md)
for the full guide.
