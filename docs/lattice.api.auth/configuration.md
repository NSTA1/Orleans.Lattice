# Orleans.Lattice.Api.Auth configuration

The package has one public options type, `LatticeApiAuthOptions`, the configuration and control facade for membership and authorization policy administration. It is bound through the `AddLatticeAuthApi` registration extension and resolvable via `IOptions<LatticeApiAuthOptions>`.

The facade adds no authorization posture of its own beyond requiring an administrator: every operation routes through the same enforcement the in-cluster data path uses, anchored on the authorization package's bootstrap root-of-trust. Its single knob bounds the debugging / dashboard reads so a single call cannot enumerate an unbounded rule set.

## `LatticeApiAuthOptions`

Bounds the introspection reads of the auth control facade. Bind it through `AddLatticeAuthApi(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `MaxExplanationRules` | `int` | `1000` | Largest number of applying rules an explain / effective-permissions introspection result collects before it stops scanning, bounding the work and payload of a single introspection call. |
