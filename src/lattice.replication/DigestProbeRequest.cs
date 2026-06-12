namespace Orleans.Lattice.Replication;

/// <summary>
/// Wire request for the anti-entropy digest probe RPC. Carries the
/// replicated tree id and the physical shard index whose
/// <see cref="LeafProjectionDigest"/> the local cluster wants the remote
/// peer to compute and return. Travels over the existing replication
/// push transport alongside the live-push batch RPC.
/// <para>
/// The probe is read-only: a peer that receives this request computes its
/// local digest for the named shard and returns it; it never mutates data
/// or advances any cursor.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.DigestProbeRequest)]
[Immutable]
public readonly record struct DigestProbeRequest
{
    /// <summary>The replicated tree id whose shard digest is requested. Must be non-empty.</summary>
    [Id(0)] public string TreeName { get; init; }

    /// <summary>The physical shard index whose <see cref="LeafProjectionDigest"/> is requested.</summary>
    [Id(1)] public int ShardIndex { get; init; }
}
