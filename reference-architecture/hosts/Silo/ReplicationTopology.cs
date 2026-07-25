using Orleans.Lattice;

namespace Orleans.Lattice.ReferenceArchitecture.Silo;

/// <summary>
/// Parses the symmetric cross-region replication topology (enrolled peers and the
/// per-tree wire merge mode) out of configuration. Both invariants must match on
/// every region or cross-region traffic dead-letters, so they are expressed as
/// plain configuration a deployment sets identically per region.
/// </summary>
internal static class ReplicationTopology
{
    /// <summary>
    /// Parses <c>Replication:Peers</c> - a comma-separated list of
    /// <c>clusterId=endpoint</c> pairs (for example
    /// <c>"site-b=https://site-b.example:443,site-c=https://site-c.example:443"</c>)
    /// - into the receiver-enrollment peer map. The enrolled cluster ids gate
    /// which peers this region accepts replication from and ships to; the map
    /// must be reciprocal across every region.
    /// </summary>
    public static IReadOnlyDictionary<string, Uri> ParsePeers(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var peers = new Dictionary<string, Uri>(StringComparer.Ordinal);
        var raw = configuration["Replication:Peers"];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return peers;
        }

        foreach (var entry in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var separator = entry.IndexOf('=');
            if (separator <= 0 || separator == entry.Length - 1)
            {
                throw new InvalidOperationException(
                    $"Replication:Peers entry '{entry}' is not in the required 'clusterId=endpoint' form.");
            }

            var clusterId = entry[..separator].Trim();
            var endpoint = entry[(separator + 1)..].Trim();
            peers[clusterId] = new Uri(endpoint);
        }

        return peers;
    }

    /// <summary>
    /// Parses <c>Replication:Trees</c> - a comma-separated list of
    /// <c>treeName=MergeMode</c> pairs (for example
    /// <c>"orders=LwwRegister,inventory=OrSet"</c>) - into the replicated-tree to
    /// wire-merge-mode map. The merge mode declared here is the wire-merge-mode
    /// and must match on both ends of every link.
    /// </summary>
    public static IReadOnlyDictionary<string, LatticeMergeMode> ParseTrees(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var trees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);
        var raw = configuration["Replication:Trees"];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return trees;
        }

        foreach (var entry in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var separator = entry.IndexOf('=');
            if (separator <= 0 || separator == entry.Length - 1)
            {
                throw new InvalidOperationException(
                    $"Replication:Trees entry '{entry}' is not in the required 'treeName=MergeMode' form.");
            }

            var treeName = entry[..separator].Trim();
            var modeText = entry[(separator + 1)..].Trim();
            if (!Enum.TryParse<LatticeMergeMode>(modeText, ignoreCase: true, out var mode))
            {
                throw new InvalidOperationException(
                    $"Replication:Trees entry '{entry}' declares an unrecognised LatticeMergeMode '{modeText}'.");
            }

            trees[treeName] = mode;
        }

        return trees;
    }
}
