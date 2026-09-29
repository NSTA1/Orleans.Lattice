using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.Shell.Areas.Replication;

/// <summary>
/// One enrolled tree as the trees page lists it: its enrolment, the app that owns
/// it (from the <c>a/{slug}/</c> prefix), and the worst health of its links.
/// </summary>
/// <param name="Entry">The tree's enrolment, as the enrolment facade reports it.</param>
/// <param name="AppSlug">The owning app's slug, or <see langword="null"/> for a tree no app owns.</param>
/// <param name="Health">The worst health among the tree's links, or <see langword="null"/> when it has none.</param>
/// <param name="Links">How many links the tree has.</param>
/// <param name="Peers">The peer regions the tree links with.</param>
internal sealed record ReplicationTreeRow(
    ReplicationTreeConfigEntry Entry,
    string? AppSlug,
    ReplicationLinkHealth? Health,
    int Links,
    IReadOnlySet<string> Peers)
{
    /// <summary>The logical tree id.</summary>
    public string TreeId => Entry.TreeId;

    /// <summary>
    /// Whether this page may toggle the tree: app-owned trees follow their app's
    /// install, and a tree declared only in the deployment's static map cannot be
    /// switched off at runtime.
    /// </summary>
    public bool IsToggleable => AppSlug is null && Entry.Source != ReplicationEnrollmentSource.Static;

    /// <summary>Builds the rows for <paramref name="report"/>, joined with <paramref name="links"/>, ordered by tree id.</summary>
    /// <param name="report">The enrolment report.</param>
    /// <param name="links">Every link the status report named, or none when it could not be read.</param>
    public static IReadOnlyList<ReplicationTreeRow> Build(ReplicationConfigReport report, IReadOnlyList<ReplicationPeerStatusEntry> links)
    {
        ArgumentNullException.ThrowIfNull(report);
        ArgumentNullException.ThrowIfNull(links);

        var byTree = links.ToLookup(link => link.TreeId, StringComparer.Ordinal);
        return
        [
            .. report.Trees
                .OrderBy(entry => entry.TreeId, StringComparer.Ordinal)
                .Select(entry =>
                {
                    var treeLinks = byTree[entry.TreeId].ToArray();
                    ReplicationLinkHealth? health = treeLinks.Length == 0
                        ? null
                        : treeLinks.Aggregate(ReplicationLinkHealth.Healthy, (worst, link) => ReplicationHealth.Worse(worst, link.Health));
                    return new ReplicationTreeRow(
                        entry,
                        ReplicationTreeOwnership.TryGetAppSlug(entry.TreeId, out var slug) ? slug : null,
                        health,
                        treeLinks.Length,
                        treeLinks.Select(link => link.PeerRegionId).ToHashSet(StringComparer.Ordinal));
                }),
        ];
    }

    /// <summary>Whether the row passes every filter.</summary>
    /// <param name="filter">The filters.</param>
    public bool Matches(ReplicationFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return filter.MatchesTree(TreeId)
            && (filter.Health is not { } health || Health == health)
            && (filter.Region is null || Peers.Contains(filter.Region));
    }
}
