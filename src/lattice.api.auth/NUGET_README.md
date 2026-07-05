# Orleans.Lattice.Api.Auth

Optional, opt-in **configuration and control facade** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice) authorization.
Exposes a single transport-agnostic admin surface that administers the
membership directory and the authorization policy store from one place. A
sibling package projects this facade onto a code-first gRPC surface.

## Design

The facade mirrors the read-only `Orleans.Lattice.Api.State` and the read-write
`Orleans.Lattice.Api.Data` facades: the facade is the contract, transports bind
over it, and it costs nothing until it is registered.

One combined admin surface carries:

- **Membership admin.** CRUD users and groups, add / remove membership edges,
  and list them.
- **Policy admin.** CRUD authorization rules and list / enumerate them.
- **`ExplainAsync`.** Returns the authorization verdict for a subject,
  operation, and scope, plus the rules that apply, for debugging policy. The
  verdict is produced by the **same access gate** the data plane consults, so
  an explanation can never disagree with the enforced decision.
- **`EffectivePermissionsAsync`.** Returns the rules currently in effect for a
  subject, for dashboards and UX. It reads the live policy store, so it
  reflects a policy change as soon as the change commits.

## Security

This is an administrative control plane, so every operation is itself
authorized as an administrator. The facade routes each call through the **same
enforcement primitive the in-cluster data path uses**: it resolves the caller
identity from the ambient credential context and requires an administrator
verdict before performing any membership or policy operation. A non-admin (or
anonymous) caller is refused fail-closed. The facade adds no bespoke,
un-authorized write path to the membership or policy trees.

- **Opt-in and absent by default.** Nothing is registered unless the host calls
  `AddLatticeAuthApi()`.
- **Must be registered after `AddLatticeAuth(...)`.** The call fails fast with an
  actionable message otherwise.
- **Zero background work.** Registration wires a lazy singleton only: no hosted
  service, timer, or reminder. Nothing runs until a facade method is called.

## Usage

```csharp
siloBuilder
    .AddLattice(/* ... */)
    .AddLatticeMembership()
    .AddLatticeAuth(options => options.BootstrapAdministrators.Add("root-admin"))
    .AddLatticeAuthApi();
```

Bind a transport over the facade to administer membership and policy remotely:
the sibling `Orleans.Lattice.Api.Auth.Grpc` package projects it onto a
code-first gRPC surface.
