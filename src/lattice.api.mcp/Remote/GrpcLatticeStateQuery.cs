using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host adapter that implements the read-only state facade
/// (<see cref="ILatticeStateQuery"/>) by delegating to the state-API gRPC client
/// (<see cref="LatticeStateApiGrpcClient"/>), so the topology-agnostic state tool
/// module works unchanged against a cluster reached over gRPC. Requests are
/// mapped one-to-one onto the wire contract, cancellation flows through, and
/// paging cursors (scan/history continuation tokens) round-trip verbatim.
/// </summary>
/// <remarks>
/// Three state facade members have no gRPC binding yet and throw
/// <see cref="NotSupportedException"/>:
/// <see cref="GetTreeSummaryAsync"/>, <see cref="GetShardSummariesAsync"/>, and
/// <see cref="GetPhysicalShardCountAsync"/>. The remaining members are wire-backed.
/// </remarks>
internal sealed class GrpcLatticeStateQuery : ILatticeStateQuery
{
    private readonly LatticeStateApiGrpcClient _client;

    /// <summary>Initialises the adapter over the supplied state-API gRPC client.</summary>
    public GrpcLatticeStateQuery(LatticeStateApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<TreeSummaryResult> GetTreeSummaryAsync(string treeId, bool deep = true, CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "GetTreeSummaryAsync has no gRPC binding on the state-API surface; it cannot be served under the remote-host topology.");

    /// <inheritdoc />
    public Task<ShardSummariesResult> GetShardSummariesAsync(string treeId, bool deep = true, CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "GetShardSummariesAsync has no gRPC binding on the state-API surface; it cannot be served under the remote-host topology.");

    /// <inheritdoc />
    public Task<int?> GetPhysicalShardCountAsync(string treeId, CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "GetPhysicalShardCountAsync has no gRPC binding on the state-API surface; it cannot be served under the remote-host topology.");

    /// <inheritdoc />
    public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListTreesAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListViewsAsync(request, cancellationToken);

    /// <inheritdoc />
    public async Task<ClusterInfo> GetClusterInfoAsync(CancellationToken cancellationToken = default)
        => await _client.GetClusterInfoAsync(new ClusterInfoRequest(), cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListTagIndexesAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListTagValuesAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListCoveredTreesAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListIndexTagsAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default)
        => _client.ScanTagMembersAsync(request, cancellationToken);

    /// <inheritdoc />
    public async Task<TreeStructureResult> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
    {
        var response = await _client.GetTreeStructureAsync(request, cancellationToken).ConfigureAwait(false);
        return new TreeStructureResult
        {
            Status = response.Status,
            TreeId = response.TreeId,
            Roots = response.Roots,
            Truncated = response.Truncated,
        };
    }

    /// <inheritdoc />
    public async Task<EntryScanResult> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
    {
        var response = await _client.ScanEntriesAsync(request, cancellationToken).ConfigureAwait(false);
        return new EntryScanResult
        {
            Status = response.Status,
            TreeId = response.TreeId,
            Entries = response.Entries,
            ContinuationToken = response.ContinuationToken,
        };
    }

    /// <inheritdoc />
    public async Task<EntryDetailResult> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        var response = await _client.GetEntryAsync(
            new EntryGetRequest { TreeId = treeId, Key = key }, cancellationToken).ConfigureAwait(false);
        return new EntryDetailResult
        {
            Status = response.Status,
            TreeId = response.TreeId,
            Key = response.Key,
            Entry = response.Entry,
        };
    }

    /// <inheritdoc />
    public async Task<EntryHistoryResult> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)
    {
        var response = await _client.GetEntryHistoryAsync(request, cancellationToken).ConfigureAwait(false);
        return new EntryHistoryResult
        {
            Status = response.Status,
            TreeId = response.TreeId,
            Key = response.Key,
            Revisions = response.Revisions,
            ContinuationToken = response.ContinuationToken,
            Bound = response.Bound,
            EarliestAvailable = response.EarliestAvailable,
        };
    }

    /// <inheritdoc />
    public async Task CancelScanAsync(string treeId, string? continuationToken, CancellationToken cancellationToken = default)
        => await _client.CancelScanAsync(
            new EntryScanCancelRequest { TreeId = treeId, ContinuationToken = continuationToken },
            cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<int> GetDeadLetterCountAsync(string treeId, CancellationToken cancellationToken = default)
    {
        var response = await _client.GetDeadLetterCountAsync(
            new DeadLetterCountRequest { TreeId = treeId }, cancellationToken).ConfigureAwait(false);
        return response.Count;
    }

    /// <inheritdoc />
    public Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default)
        => _client.ListDeadLettersAsync(request, cancellationToken);
}
