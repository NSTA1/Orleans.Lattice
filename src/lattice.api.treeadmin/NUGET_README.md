# Orleans.Lattice.Api.TreeAdmin

Optional, opt-in **tree-administration control facade** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Presents a single
transport-agnostic admin surface for whole-tree lifecycle administration. A
sibling package projects this facade onto a code-first gRPC surface, and the MCP
control-plane package advertises it as a discoverable capability group.

## Design

The facade follows **composition over absorption**. Tree administration does not
re-implement operations that already have a single-responsibility facade; it
**wraps** them by delegation. This foundation composes the schema control facade
([`Orleans.Lattice.Api.Schema`](https://www.nuget.org/packages/Orleans.Lattice.Api.Schema))
so schema stays its own facade with no breaking change, while tree administration
presents one complete surface.

- **Scaffolding scope.** This foundation release ships the capability probe only.
  The whole-tree lifecycle operations (bulk-load, delete, resize, reshard, and the
  rest) land in later releases, each adding its verb and a probe flag.
- **Discoverable but empty.** The facade is registered, discoverable through the
  MCP capability advertisement, and probe-answerable from day one, so a management
  surface can bind to it before any operation exists.

## Security

The capability probe reports the caller's allowed operation set per tree with no
side effects, evaluated through the same fail-closed gates the real operations
use. The probe is a UX affordance, not a security boundary: the server still
authorizes every real operation on attempt.

- **Opt-in and absent by default.** Nothing is registered unless the host calls
  `AddLatticeTreeAdminApi()`, and once added the facade does no background work
  until a method is called.
- **Must be registered after `AddLatticeSchemaApi(...)`.** The call fails fast
  with an actionable message otherwise, because the facade composes the schema
  control facade.

## Usage

```csharp
siloBuilder
    .AddLattice(/* ... */)
    .AddLatticeSchemaEnforcement()
    .AddLatticeSchemaApi()
    .AddLatticeTreeAdminApi();
```

Bind a transport over the facade to drive tree administration remotely: the
sibling `Orleans.Lattice.Api.TreeAdmin.Grpc` package projects it onto a code-first
gRPC surface.
