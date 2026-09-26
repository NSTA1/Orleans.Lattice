namespace Orleans.Lattice;

/// <summary>
/// Single offset-tagged entry as exchanged between WAL consumers and an
/// <see cref="IWalStorageProvider"/> implementation. Pairs the captured
/// <see cref="LatticeMutation"/> mutation record with the dense,
/// monotonically increasing per-shard offset assigned at append time.
/// <para>
/// This is the public provider-boundary type. Replication-package wire
/// envelopes (e.g. <c>WalRecord</c>) and any other ship-time enrichment
/// shapes are intentionally distinct - the durability contract a host
/// plugs into stays in core so it can be consumed by single-cluster
/// deployments that do not reference the replication package.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalEntry)]
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

    /// <summary>
    /// The captured mutation record. Carries every field required to
    /// reconstruct the original write (key, value, HLC, tombstone flag,
    /// expiry, origin cluster id, vector clock, transaction id,
    /// category, declared merge mode, and pre-merge delta payload). Replication-only
    /// transport metadata such as dependency summaries is reconstructed at ship time
    /// by the replication package and is deliberately not carried on this provider boundary.
    /// </summary>
    [Id(1)] public LatticeMutation Mutation { get; init; }
}
