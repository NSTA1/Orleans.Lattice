# Schema chaos tests

The schema suite exercises policy/version changes and durable operations while data work is in flight. These are bounded, concrete interleavings with assertions, not a proof that every crash schedule is safe.

## Covered scenarios

| Fixture | Concurrent change/fault | Assertion boundary |
|---|---|---|
| [AtomicWriteUnderPolicyChurnChaosTests](../../test/lattice.schema/Chaos/AtomicWriteUnderPolicyChurnChaosTests.cs) | Repeated policy churn during cross-tree atomic writes; seeded random policy choices. | No unexpected failures; both committed and rejected generations occur, with all-or-nothing values rather than a partially validated batch. |
| [AtomicWriteUnderVersionAdvanceChaosTests](../../test/lattice.schema/Chaos/AtomicWriteUnderVersionAdvanceChaosTests.cs) | Single-tree atomic writes during v1 -> v2 -> v3 advances and eager migration. | Exactly two advances, successful/idempotent migration and decodable all-or-nothing generations. |
| [SchemaRemediationCutoverChaosTests](../../test/lattice.schema/Chaos/SchemaRemediationCutoverChaosTests.cs) | Concurrent atomic readers across two remediation cutovers, with shard growth/shrink between them. | No torn atomic reads, three distinct physical copies, and shard counts 7 -> 3 -> 5 after the associated reshard work. |
| [SchemaOperationSiloLossChaosTests](../../test/lattice.schema/Chaos/SchemaOperationSiloLossChaosTests.cs) | Loss of the silo owning a tracked schema operation. | The accepted operation becomes Failed at its recorded Build phase; a new start resumes the same durable remediation, processes four values and succeeds. This is explicit restart, not automatic continuation of the failed accepted operation. |

The first fixture's policy RNG seed is `20260712`; the suite is not uniformly a random-fault harness. Each linked source specifies the actual scheduling hooks, workload size and completion timeout.

## Running and interpreting coverage

These fixtures carry `Category("Chaos")`. Follow the [testing policy](../../.github/instructions/testing.instructions.md) for execution scope and tier rather than replacing it with a documentation-specific runner. The docs snippet/em-dash/mojibake gates do **not** execute these scenarios; a passing documentation gate is not evidence that chaos tests were run.

Read [architecture](architecture.md), [schema enforcement](schema-enforcement.md) and [schema versioning](schema-versioning.md) for the runtime pipeline being exercised. The current fixtures are the source of truth for the tested fault/interleaving, not an assertion that production can accept malformed data through every ingest path.
