namespace Orleans.Lattice.Replication;

/// <summary>
/// Wire response for the anti-entropy digest probe RPC. Carries the
/// remote peer's <see cref="LeafProjectionDigest"/> for the requested
/// shard, plus a flag indicating whether the peer was able to produce a
/// digest at all.
/// <para>
/// A peer sets <see cref="DigestAvailable"/> to <see langword="false"/>
/// when projection-digest maintenance is disabled for the named tree on
/// the remote side (the remote
/// <see cref="ILattice.GetLeafProjectionDigestAsync(int, CancellationToken)"/>
/// throws). In that case <see cref="Digest"/> is the default value and
/// the comparison records a non-comparable outcome rather than a
/// mismatch.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.DigestProbeResponse)]
[Immutable]
public readonly record struct DigestProbeResponse
{
    /// <summary>
    /// <see langword="true"/> when the remote peer produced a digest for
    /// the requested shard; <see langword="false"/> when the remote peer
    /// has projection-digest maintenance disabled (or latched off) for
    /// the named tree and could not produce one.
    /// </summary>
    [Id(0)] public bool DigestAvailable { get; init; }

    /// <summary>
    /// The remote peer's <see cref="LeafProjectionDigest"/> for the
    /// requested shard. Meaningful only when <see cref="DigestAvailable"/>
    /// is <see langword="true"/>; otherwise the default value.
    /// </summary>
    [Id(1)] public LeafProjectionDigest Digest { get; init; }
}
