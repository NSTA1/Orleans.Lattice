using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.Shell.Areas.Replication;

/// <summary>
/// One direction between this region and a peer, rolled up over every tree that
/// replicates along it: the worst link health, the summed backlog, and how many
/// links are stalled or lagging.
/// </summary>
/// <param name="Direction">The direction, from this region's point of view.</param>
/// <param name="Links">How many <c>(tree, peer, direction)</c> links the edge carries.</param>
/// <param name="Trees">How many distinct trees replicate along it.</param>
/// <param name="Health">The worst health among its links.</param>
/// <param name="EntriesBehind">Entries behind, summed over its links.</param>
/// <param name="BytesBehind">Bytes behind, summed over its links.</param>
/// <param name="ConsecutiveErrors">The most consecutive errors any one link has seen.</param>
/// <param name="Stalled">How many of its links are stalled.</param>
/// <param name="Lagging">How many of its links are lagging.</param>
internal sealed record ReplicationEdgeSummary(
    ReplicationLinkDirection Direction,
    int Links,
    int Trees,
    ReplicationLinkHealth Health,
    long EntriesBehind,
    long BytesBehind,
    long ConsecutiveErrors,
    int Stalled,
    int Lagging)
{
    /// <summary>Rolls <paramref name="links"/> up into one edge, or <see langword="null"/> when there are none.</summary>
    /// <param name="direction">The edge's direction.</param>
    /// <param name="links">The links along it.</param>
    public static ReplicationEdgeSummary? From(ReplicationLinkDirection direction, IReadOnlyCollection<ReplicationPeerStatusEntry> links)
    {
        ArgumentNullException.ThrowIfNull(links);
        if (links.Count == 0)
        {
            return null;
        }

        var health = ReplicationLinkHealth.Healthy;
        long entries = 0, bytes = 0, errors = 0;
        int stalled = 0, lagging = 0;
        var trees = new HashSet<string>(StringComparer.Ordinal);
        foreach (var link in links)
        {
            health = ReplicationHealth.Worse(health, link.Health);
            entries = Saturating(entries, link.EntriesBehind);
            bytes = Saturating(bytes, link.BytesBehind);
            errors = Math.Max(errors, link.ConsecutiveErrors);
            stalled += link.Health == ReplicationLinkHealth.Stalled ? 1 : 0;
            lagging += link.Health == ReplicationLinkHealth.Lagging ? 1 : 0;
            trees.Add(link.TreeId);
        }

        return new ReplicationEdgeSummary(direction, links.Count, trees.Count, health, entries, bytes, errors, stalled, lagging);
    }

    private static long Saturating(long total, long value)
    {
        var addend = Math.Max(0, value);
        return total > long.MaxValue - addend ? long.MaxValue : total + addend;
    }
}
