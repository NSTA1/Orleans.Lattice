namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationDigestProbeTransport"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// Reports every remote peer as unable to produce a digest so the
/// detection scheduler records a non-comparable outcome rather than a
/// spurious mismatch when no real probe transport (e.g. the gRPC
/// binding) is wired in.
/// </summary>
internal sealed class NoOpReplicationDigestProbeTransport : IReplicationDigestProbeTransport
{
    private static readonly Task<DigestProbeResponse> Unavailable =
        Task.FromResult(new DigestProbeResponse { DigestAvailable = false });

    private static readonly Task<MerkleWalkProbeResponse> MerkleWalkUnavailable =
        Task.FromResult(MerkleWalkProbeResponse.Unavailable);

    /// <inheritdoc />
    public Task<DigestProbeResponse> ProbeDigestAsync(
        string targetClusterId,
        DigestProbeRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(targetClusterId))
        {
            throw new ArgumentException(
                "targetClusterId must be non-empty.",
                nameof(targetClusterId));
        }

        if (string.IsNullOrEmpty(request.TreeName))
        {
            throw new ArgumentException(
                "DigestProbeRequest.TreeName must be non-empty.",
                nameof(request));
        }

        return Unavailable;
    }

    /// <inheritdoc />
    public Task<MerkleWalkProbeResponse> ProbeMerkleWalkAsync(
        string targetClusterId,
        MerkleWalkProbeRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(targetClusterId))
        {
            throw new ArgumentException(
                "targetClusterId must be non-empty.",
                nameof(targetClusterId));
        }

        if (string.IsNullOrEmpty(request.TreeName))
        {
            throw new ArgumentException(
                "MerkleWalkProbeRequest.TreeName must be non-empty.",
                nameof(request));
        }

        return MerkleWalkUnavailable;
    }
}
