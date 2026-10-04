namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Identifies the saga decision gate a snapshot capture holds (issue #4485),
/// handed to every shard root and leaf the capture's baseline fan-out reaches.
/// A leaf resolves each prepared bucket still pending in its baseline against
/// the gate's decision snapshot (D0) through
/// <see cref="ITxRegistryGrain.GetCaptureGateStatusManyAsync"/>, so every shard
/// of the capture resolves a saga against the same decisions.
/// </summary>
/// <param name="Token">The capture's gate token.</param>
/// <param name="RegistryTreeId">The tree id the saga decision registry is keyed by (the logical tree id).</param>
[GenerateSerializer]
[Alias(TypeAliases.SnapshotDecisionGate)]
[Immutable]
internal sealed record SnapshotDecisionGate(
    [property: Id(0)] Guid Token,
    [property: Id(1)] string RegistryTreeId);
