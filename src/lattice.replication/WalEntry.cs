namespace Orleans.Lattice.Replication;

/// <summary>
/// Single offset-tagged entry as exchanged between WAL consumers and an
/// <see cref="IWalStorageProvider"/> implementation. Pairs the captured
/// <see cref="ReplogEntry"/> mutation record with the dense,
/// monotonically increasing per-shard offset assigned at append time.
/// <para>
/// This is the public provider-boundary type. The internal per-shard
/// grain RPC envelope and this provider DTO are intentionally distinct
/// — the grain envelope is part of the in-cluster grain protocol
/// surface and may evolve independently of the durability contract a
/// host plugs into.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.WalEntry)]
[Immutable]
public readonly record struct WalEntry
{
    /// <summary>
    /// The dense, monotonically-increasing per-shard offset assigned to
    /// this entry at append time. Offsets start at <c>0</c> and increase
    /// by one per successful append per shard; gaps never appear in a
    /// successfully-persisted WAL.
    /// </summary>
    [Id(0)] public long Offset { get; init; }

    /// <summary>The captured mutation record.</summary>
    [Id(1)] public ReplogEntry Entry { get; init; }
}
