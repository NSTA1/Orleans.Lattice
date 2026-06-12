using Orleans.Lattice;

namespace Orleans.Lattice.Replication;

/// <summary>
/// A peer's read-only answer to a <see cref="MerkleWalkProbeRequest"/>: whether
/// the peer could produce a range subtree digest and, if so, the digest itself.
/// When <see cref="Available"/> is <see langword="false"/> the Merkle-walk pass
/// cannot compare apples-to-apples for that range and aborts with the
/// remote-unavailable reason.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.MerkleWalkProbeResponse)]
[Immutable]
public readonly record struct MerkleWalkProbeResponse
{
    /// <summary>
    /// <see langword="true"/> when the peer produced a digest for the requested
    /// key-range; <see langword="false"/> when it could not (the default).
    /// </summary>
    [Id(0)]
    public bool Available { get; init; }

    /// <summary>
    /// The peer's content digest for the requested subtree key-range. Only
    /// meaningful when <see cref="Available"/> is <see langword="true"/>.
    /// </summary>
    [Id(1)]
    public LeafProjectionDigest Digest { get; init; }

    /// <summary>
    /// A response indicating the peer could not produce a range digest.
    /// </summary>
    public static MerkleWalkProbeResponse Unavailable => default;
}
