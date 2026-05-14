namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ILatticeReplicationSecretSource"/> backed by the
/// <see cref="LatticeReplicationEnvironmentVariables"/> surface.
/// Docker-friendly: the operator drops <c>LATTICE_REPLICATION_SECRET</c>
/// into the container env block (or a Kubernetes <c>Secret</c>-backed
/// env-from binding) and the receiver authenticator finds it without
/// further code.
/// </summary>
/// <remarks>
/// <para>
/// The implementation is intentionally synchronous internally - reading
/// environment variables is a cheap memory lookup - and only adopts the
/// <see cref="ValueTask"/> shape because the public interface requires
/// it for arbitrary remote secret stores.
/// </para>
/// <para>
/// The accepted-set is parsed lazily on every call so a future call to
/// <see cref="Environment.SetEnvironmentVariable(string, string?)"/>
/// (typically by an in-process operator tool) is observed immediately.
/// The version token is a deterministic FNV-1a hash over the resolved
/// secret set; identical env state yields identical tokens across
/// processes and runtime restarts.
/// </para>
/// </remarks>
internal sealed class EnvironmentVariableSecretSource : ILatticeReplicationSecretSource
{
    /// <inheritdoc />
    public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(peerClusterId);

        // Per-peer override wins; falls back to the cluster-wide secret.
        var perPeerName = LatticeReplicationEnvironmentVariables.PeerSecretPrefix
            + NormaliseClusterId(peerClusterId);
        var perPeer = Environment.GetEnvironmentVariable(perPeerName);
        if (!string.IsNullOrWhiteSpace(perPeer))
        {
            return new ValueTask<string?>(perPeer);
        }

        var clusterWide = Environment.GetEnvironmentVariable(
            LatticeReplicationEnvironmentVariables.Secret);
        return new ValueTask<string?>(string.IsNullOrWhiteSpace(clusterWide) ? null : clusterWide);
    }

    /// <inheritdoc />
    public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
    {
        var primary = Environment.GetEnvironmentVariable(
            LatticeReplicationEnvironmentVariables.Secret);
        var accepted = Environment.GetEnvironmentVariable(
            LatticeReplicationEnvironmentVariables.AcceptedSecrets);

        var combined = new List<string>(capacity: 4);
        if (!string.IsNullOrWhiteSpace(primary))
        {
            combined.Add(primary);
        }

        if (!string.IsNullOrWhiteSpace(accepted))
        {
            foreach (var part in accepted.Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (!combined.Contains(part, StringComparer.Ordinal))
                {
                    combined.Add(part);
                }
            }
        }

        if (combined.Count == 0)
        {
            return new ValueTask<LatticeReplicationAcceptedSecrets>(LatticeReplicationAcceptedSecrets.Empty);
        }

        // Deterministic version token. Identical env state yields the
        // same token across processes and runtime restarts so a future
        // caching layer (or external diagnostic) can rely on the
        // equality. Today the CachingReplicationSecretProvider caches
        // by time rather than by version, but the contract on
        // LatticeReplicationAcceptedSecrets.Version is "equal version
        // implies equal secrets", so the source must honour it.
        var version = StableSecretSetHash.Compute(combined);
        return new ValueTask<LatticeReplicationAcceptedSecrets>(
            new LatticeReplicationAcceptedSecrets(combined, version));
    }

    /// <summary>
    /// Normalises a peer cluster id for inclusion in an environment
    /// variable name. Replaces every character outside
    /// <c>[A-Z0-9_]</c> with <c>_</c> and upper-cases the remainder, so
    /// a cluster id like <c>us-west-2</c> resolves to env var
    /// <c>LATTICE_REPLICATION_PEER_SECRET__US_WEST_2</c>.
    /// </summary>
    /// <param name="clusterId">The cluster id to normalise. Must be non-null and at most 256 characters; longer ids would alias under truncation and are rejected.</param>
    internal static string NormaliseClusterId(string clusterId)
    {
        ArgumentNullException.ThrowIfNull(clusterId);
        const int MaxLength = 256;
        if (clusterId.Length > MaxLength)
        {
            throw new ArgumentOutOfRangeException(
                nameof(clusterId),
                clusterId.Length,
                $"Cluster id length {clusterId.Length} exceeds the {MaxLength}-character env-var name limit. "
                + "Shorten the cluster id or register a custom ILatticeReplicationSecretSource that does not key off env-var names.");
        }

        Span<char> buf = stackalloc char[MaxLength];
        var len = clusterId.Length;
        for (var i = 0; i < len; i++)
        {
            var c = clusterId[i];
            if (c is (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '_')
            {
                buf[i] = c;
            }
            else if (c is >= 'a' and <= 'z')
            {
                buf[i] = (char)(c - ('a' - 'A'));
            }
            else
            {
                buf[i] = '_';
            }
        }
        return new string(buf[..len]);
    }
}
