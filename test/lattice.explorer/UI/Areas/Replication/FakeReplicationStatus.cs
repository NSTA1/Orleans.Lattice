using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// A scripted <see cref="ILatticeReplicationStatus"/>: it serves <see cref="Links"/>
/// from <see cref="LocalRegionId"/> in pages of <see cref="ServedPageSize"/>, filtered
/// by the query's tree and peer, or throws <see cref="Failure"/>. A test can hold
/// every call open with <see cref="Gate"/>, and every query is recorded.
/// </summary>
internal sealed class FakeReplicationStatus : ILatticeReplicationStatus
{
    public string LocalRegionId { get; set; } = "eu-west";

    public List<ReplicationPeerStatusEntry> Links { get; } = [];

    public int ServedPageSize { get; set; } = int.MaxValue;

    public Exception? Failure { get; set; }

    public TaskCompletionSource? Gate { get; set; }

    /// <summary>When set, every page carries this token, so a reader that follows it blindly never ends.</summary>
    public string? StuckToken { get; set; }

    public List<ReplicationPeerStatusQuery> Queries { get; } = [];

    public int Calls => Queries.Count;

    public async Task<ReplicationPeerStatusPage> GetPeerStatusAsync(ReplicationPeerStatusQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        Queries.Add(query);
        if (Gate is { } gate)
        {
            await gate.Task.WaitAsync(cancellationToken);
        }

        if (Failure is not null)
        {
            throw Failure;
        }

        var matching = Links
            .Where(link => query.TreeId is null || string.Equals(link.TreeId, query.TreeId, StringComparison.Ordinal))
            .Where(link => query.PeerRegionId is null || string.Equals(link.PeerRegionId, query.PeerRegionId, StringComparison.Ordinal))
            .ToArray();

        var offset = query.ContinuationToken is { } token && token.StartsWith("offset:", StringComparison.Ordinal)
            ? int.Parse(token["offset:".Length..], System.Globalization.CultureInfo.InvariantCulture)
            : 0;
        var size = Math.Min(ServedPageSize, query.ResolvePageSize());
        var page = matching.Skip(offset).Take(size).ToArray();
        var next = offset + page.Length < matching.Length ? "offset:" + (offset + page.Length).ToString(System.Globalization.CultureInfo.InvariantCulture) : null;

        return new ReplicationPeerStatusPage(LocalRegionId, page, StuckToken ?? next);
    }
}
