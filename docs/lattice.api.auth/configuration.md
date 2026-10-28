---
agent_spec: "docs/agents/api/auth.json"
---

# Orleans.Lattice.Api.Auth configuration

The facade package (`Orleans.Lattice.Api.Auth`) documents `LatticeApiAuthOptions`, the configuration and control facade for membership and authorization policy administration. It is bound through the `AddLatticeAuthApi` registration extension and resolvable via `IOptions<LatticeApiAuthOptions>`. The sibling gRPC binding package (`Orleans.Lattice.Api.Auth.Grpc`) adds one more public options type, `LatticeAuthApiGrpcOptions`; see [gRPC binding options](#grpc-binding-options) below.

The facade adds no authorization posture of its own beyond requiring an administrator: every operation routes through the same enforcement the in-cluster data path uses, anchored on the authorization package's bootstrap root-of-trust. Its single knob bounds the debugging / dashboard reads so a single call cannot enumerate an unbounded rule set.

## `LatticeApiAuthOptions`

Bounds the introspection reads of the auth control facade. Bind it through `AddLatticeAuthApi(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `MaxExplanationRules` | `int` | `1000` | Largest number of applying rules an explain / effective-permissions introspection result collects before it stops scanning, bounding the work and payload of a single introspection call. |

## gRPC binding options

`LatticeAuthApiGrpcOptions` (namespace `Orleans.Lattice.Api.Auth.Grpc`) configures the server-side gRPC binding: whether the transport authorizer is enforced, the credential header and scheme bridged into the facade's administrator check, and the active-tenant header. It is bound on the host's service collection through `AddLatticeAuthApiGrpc(configure)` and documented in the [gRPC binding configuration](../lattice.api.auth.grpc/configuration.md).

## See also

- [Public API](api.md)
- [Architecture](architecture.md)
