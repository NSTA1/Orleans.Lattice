---
agent_spec: "docs/agents/api/schema.json"
---

# Architecture

The package attaches to core `ILatticeWriteInterceptor`, `ILatticeValueDecoder`, `ILatticeEnvelopeCodec` and optional `ILatticeMergeObserver` seams. Enforcement and versioning have separate durable per-tree configuration and cached providers; unregistered stages retain the core null behavior.

## Write and read pipelines

Enforcement loads the governing policy, then checks its ordered rules against the value body. No policy accepts unchanged. An already-enveloped value is judged at the target shape - the body a reader is served, upcast to the target version - and stored with its envelope unchanged; one that cannot be decoded to the target fails closed. Opaque CRDT deltas that are not shape-checkable JSON are accepted rather than blocking convergence. A failing ordinary write throws `LatticeSchemaViolationException` before that value is persisted. An intercepted system-origin failure is diverted to the shared dead-letter store only when the relevant per-tree strict flag applies.

When both add-ons are registered, in either order, validation runs before the envelope-stamping stage. The composite carries a transformed value forward and stops on a dead-letter decision. Either global strict flag can make core call the composed pipeline for system-origin traffic; the individual stage/per-tree checks still govern what it does. This does not add interception to lower-level plain last-writer-wins replication, backup restore or tree-merge paths.

On reads, an unenveloped value passes through unchanged. Enveloped bytes are decoded, their body is extracted, and stale versions are upcast toward the configured target through the registered chain. Without a matching per-tree schema configuration, decoding returns the stored-version body. With a matching configuration, a stored version newer than the target throws; an older version is passed to the registered upcaster chain, whose unsupported path fails. The envelope codec also gives CRDT processing access to body/version metadata without re-running an upcast during a deterministic WAL fold.

## Control and cached configuration

Host-side administration writes the reserved policy/version trees and evicts the serving silo's cached configuration before returning; every other silo follows a per-configuration-tree, in-memory epoch grain the committing silo advances, and drops its whole cache within the propagation interval (also when the epoch cannot be read, the epoch grain restarts, or cluster membership changes). It does not authorize an untrusted caller. The sibling Schema API supplies the caller-facing access gate; compliance scans use ordinary data-plane reads. Registered transform implementations and schema/upcaster chains are host composition, while per-tree policy/target state is durable control state.

## Remediation and eager migration

Background remediation builds a replacement tree from a snapshot plus subsequent work, validates/transforms the governed values, and publishes through an alias cutover. Eager migration uses the versioning transform chain instead of merely waiting for read-time upcasts. On a versioning-enabled tree both read the stored form and decode each value themselves, so a value that cannot be upcast to the target ends the run as an abort naming it, never as a thrown read fault, and releases the alias reservation. A remediation of a versioned tree writes each value back enveloped at the version the transform ran on. A cutover publishes only the physical copy the run captured; one superseded by another alias change is refused and discarded. Durable phase/status and abort reports allow operators to observe work and offending bounded previews without treating request lifetime as operation lifetime. The topic guides describe cutover, failure and cancellation details.

## Optional post-merge observer

The registration-only merge-result switch installs a non-mutating observer. It reports a violation as an annotation without rejecting or rewriting the merge; core counts each annotation on `orleans.lattice.leaf.merge_observer_events` and logs it as a warning carrying the reason. Only the foreground leaf set and typed-CRDT apply paths are observed, so it is not a complete audit of stored values; a compliance scan is.

## Source map

- [Enforcement registration](../../src/lattice.schema/LatticeSchemaEnforcementServiceCollectionExtensions.cs)
- [Versioning composition](../../src/lattice.schema/LatticeSchemaVersioningServiceCollectionExtensions.cs)
- [Write validation](../../src/lattice.schema/LatticeSchemaWriteInterceptor.cs)
- [Composed write behavior](../../src/lattice.schema/CompositeLatticeWriteInterceptor.cs)
- [Read-time version decoding](../../src/lattice.schema/LatticeSchemaVersionDecoder.cs)
- [Remediation driver](../../src/lattice.schema/SchemaRemediationDriver.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [Chaos tests](chaos-tests.md)
