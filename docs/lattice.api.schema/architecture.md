---
agent_spec: "docs/agents/api/schema.json"
---

# Orleans.Lattice.Api.Schema architecture

This page describes how the control facade drives the schema engine. The facade (`ILatticeSchemaControl`, a public contract in the shared `Orleans.Lattice.Api.Abstractions` package) is implemented by an internal silo singleton that every transport binding adapts over, so it is described here by behaviour. The public model records it returns and accepts are named.

## Position in the stack

```
transport binding (gRPC now, other bindings later)
        |
        v
control facade  (this package - transport-agnostic)
        |
        v
schema engine   (Orleans.Lattice.Schema - enforcement / versioning / dead letters / remediation / compliance)
        |
        v
core data plane (Orleans.Lattice)
```

The facade adds no transport of its own and no bespoke, un-authorized write path. It composes the engine's public service seams - the policy, dead-letter, versioning, remediation, compliance, and authorization surfaces - into the operations a control consumer needs. Registering it (`AddLatticeSchemaApi`) requires schema enforcement to be registered first; the ordering guard fails fast at registration otherwise.

## Fail-closed authorization on every operation

Every facade operation authorizes before it touches the admin plane, through the schema engine's internal authorization component:

- **Policy reads**, **dead-letter reads**, **version-config reads**, **runtime-catalog reads**, **remediation-status reads**, and **compliance scans** require Read authority for the tree.
- **Policy mutations**, **version-config mutations**, **runtime-catalog registrations**, **target-version advances**, **version migrations**, and **remediation** require SchemaAdmin authority for the tree.
- **Capability probes** do not perform the requested operation. They run two fail-closed probes, Read and SchemaAdmin, and translate those results into advisory flags.

Under tenancy, each operation first resolves the caller's tree name once to its effective tenant tree (`t/{tenant}/{name}` for a confined tenant, the bare name for the default tenant), and then authorizes and acts on exactly that id. Policy, version config, runtime catalog, dead letters and remediation status are all keyed by the effective tree, so tenants never share or see each other's schema settings. A caller whose tenant cannot be attributed is refused before the gate, and so is a confined tenant naming a reserved `sys-` tree or a malformed `t/` id. Another tenant's well-formed `t/{other}/{name}` reaches the access gate unchanged, which admits it only on that tenant's grant. Tracked schema operations are recorded under the caller's tenant and found by operation id only there, and only over trees the caller may read. The in-process admins (`ILatticeSchemaAdmin` and its siblings) perform none of this; they act on the id they are given. The `SchemaTenantScope` TLA+ module checks this scoping; see its [refinement note](../../spec/schema/SchemaTenantScopeRefinement.md).

Because authorization happens before the admin plane is touched, a denied caller cannot learn policy, dead-letter, versioning, remediation, or compliance details by calling a more specific operation. The capability probe is useful for user interfaces, but it is never a cacheable grant: each real operation re-authorizes immediately before it runs.

## Dead-letter streaming

`ListDeadLettersAsync` exposes diverted schema-rejected writes as an async stream. The facade forwards entries as they are read rather than materializing the whole dead-letter set, so a large rejection backlog can be inspected with bounded memory. `CountDeadLettersAsync` is the scalar companion for dashboards that need a current count without draining the entries.

Both operations are read-only and require Read authority for the tree.

## Versioning add-on boundary

Schema versioning is a separate add-on registered with `AddLatticeSchemaVersioning(...)`. The control facade can be registered without it, because policy, dead-letter, remediation, compliance, and capability operations are still useful without versioning.

When a version operation - including a runtime-catalog read or registration - is called and the add-on is not present, the facade throws a clear `InvalidOperationException`. That failure happens at call time rather than during dependency injection, so a host can expose the non-versioned control surface without registering unused versioning services.

## Remediation

`StartRemediationAsync` accepts a tracked remediation and `GetRemediationStatusAsync` reports the last or current `LatticeSchemaRemediationReport` visible for the tree. The start call is a SchemaAdmin operation because the remediation can rewrite existing values and change the policy that future writes must satisfy; the status read is Read-only.

The transform, target policy, and report semantics are owned by [`Orleans.Lattice.Schema`](../lattice.schema/README.md); the facade's job is to authorize, invoke, and expose the result consistently to transport bindings.

## Compliance audit

`StartComplianceScanAsync` accepts a tracked read-only audit. The background scan validates the tree's entries against the cached compiled policy and its terminal result is a `LatticeSchemaComplianceReport` with compliant and non-compliant counts plus a reason breakdown. A value that cannot be decoded to the tree's target version is counted as non-compliant; the scan still succeeds. When no policy is set, it returns the ungoverned report: `HasPolicy` is `false` and every count is zero.

The scan never mutates values, policy, dead-letter entries, or version config. It is a Read operation, so an operator can audit a tree without holding SchemaAdmin authority.
