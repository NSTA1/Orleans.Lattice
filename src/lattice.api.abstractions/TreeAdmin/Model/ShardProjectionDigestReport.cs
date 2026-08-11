namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A per-shard leaf-projection digest: a content hash plus lightweight counts that
/// identify a shard's committed leaf state at a point in time. Two shards with the
/// same <see cref="HashHex"/> hold the same committed data; a changed hash between
/// two reads proves the shard mutated. Used for cheap divergence detection (for
/// example comparing a source shard against a replicated copy) without shipping the
/// data.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.ShardProjectionDigestReport)]
[Immutable]
public sealed record ShardProjectionDigestReport
{
    /// <summary>The logical tree id the digest was computed for.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>Zero-based physical shard index the digest covers.</summary>
    [Id(1)] public int ShardIndex { get; init; }

    /// <summary>
    /// The lowercase hex-encoded content hash of the shard's committed leaf
    /// projection. Never <see langword="null"/>; empty only when the shard holds no
    /// committed state.
    /// </summary>
    [Id(2)] public required string HashHex { get; init; }

    /// <summary>The number of live entries covered by the digest.</summary>
    [Id(3)] public long EntryCount { get; init; }

    /// <summary>
    /// The WAL checkpoint offset the digest was taken at. Pairs with
    /// <see cref="HashHex"/> to identify exactly which committed prefix the digest
    /// reflects.
    /// </summary>
    [Id(4)] public long CheckpointOffset { get; init; }

    /// <summary>The monotonic version of the shard's committed state at digest time.</summary>
    [Id(5)] public long Version { get; init; }
}
