# Architecture

The package attaches to core `ILatticeWriteInterceptor`, `ILatticeValueDecoder`, `ILatticeEnvelopeCodec` and optional `ILatticeMergeObserver` seams. Enforcement and versioning have separate durable per-tree configuration and cached providers; unregistered stages retain the core null behavior.

## Write and read pipelines

Enforcement loads the governing policy, then checks its ordered rules against the value body. No policy accepts unchanged. Opaque CRDT deltas that are not shape-checkable JSON are accepted rather than blocking convergence. A failing ordinary write throws `LatticeSchemaViolationException` before that value is persisted. An intercepted system-origin failure is diverted to the shared dead-letter store only when the relevant per-tree strict flag applies.

With enforcement registered before versioning, validation runs before the envelope-stamping stage. The composite carries a transformed value forward and stops on a dead-letter decision. Either global strict flag can make core call the composed pipeline for system-origin traffic; the individual stage/per-tree checks still govern what it does. This does not add interception to lower-level plain last-writer-wins replication, backup restore or tree-merge paths.

On reads, an unenveloped value passes through unchanged. Enveloped bytes are decoded, their body is extracted, and stale versions are upcast toward the configured target through the registered chain. Without a matching per-tree schema configuration, decoding returns the stored-version body. With a matching configuration, a stored version newer than the target throws; an older version is passed to the registered upcaster chain, whose unsupported path fails. The envelope codec also gives CRDT processing access to body/version metadata without re-running an upcast during a deterministic WAL fold.

## Control and cached configuration

Host-side administration writes the reserved policy/version trees and invalidates cached configuration. It does not authorize an untrusted caller. The sibling Schema API supplies the caller-facing access gate; compliance scans use ordinary data-plane reads. Registered transform implementations and schema/upcaster chains are host composition, while per-tree policy/target state is durable control state.

## Remediation and eager migration

Background remediation builds a replacement tree from a snapshot plus subsequent work, validates/transforms the governed values, and publishes through an alias cutover. Eager migration uses the versioning transform chain instead of merely waiting for read-time upcasts. Durable phase/status and abort reports allow operators to observe work and offending bounded previews without treating request lifetime as operation lifetime. The topic guides describe cutover, failure and cancellation details.

## Optional post-merge observer

The registration-only merge-result switch installs a non-mutating observer. It reports a violation as an annotation without rejecting or rewriting the merge. Core currently does not turn that annotation into a log, event or metric; enabling it is not an audit sink.

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
