using Grpc.Net.Client;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// The production <see cref="ILatticeStateClient"/>: owns a <see cref="GrpcChannel"/>
/// built from <see cref="LatticeConnectionSettings"/> and wraps a
/// <see cref="LatticeStateApiGrpcClient"/>. Disposing it tears down the channel,
/// so the connection layer can rebuild cleanly on an endpoint change.
/// </summary>
internal sealed class GrpcLatticeStateClient : ILatticeStateClient, IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly LatticeStateApiGrpcClient _client;

    private GrpcLatticeStateClient(GrpcChannel channel, LatticeStateApiGrpcClient client)
    {
        _channel = channel;
        _client = client;
    }

    /// <summary>
    /// Builds the channel and client for <paramref name="settings"/> through
    /// <see cref="LatticeGrpcChannelFactory"/>, which owns the transport handler,
    /// the insecure-channel safeguard, and the per-call credential pipeline.
    /// </summary>
    public static GrpcLatticeStateClient Create(LatticeConnectionSettings settings, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        var channel = LatticeGrpcChannelFactory.CreateChannel(settings);
        var invoker = LatticeGrpcChannelFactory.CreateCallInvoker(channel, settings);

        var client = LatticeStateApiGrpcClient.Create(invoker, serializerProvider);
        return new GrpcLatticeStateClient(channel, client);
    }

    public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListTreesAsync(request, cancellationToken);

    public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListViewsAsync(request, cancellationToken);

    public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListTagIndexesAsync(request, cancellationToken);

    public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListTagValuesAsync(request, cancellationToken);

    public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListCoveredTreesAsync(request, cancellationToken);

    public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListIndexTagsAsync(request, cancellationToken);

    public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default)
        => _client.ScanTagMembersAsync(request, cancellationToken);

    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
        => _client.GetTreeStructureAsync(request, cancellationToken);

    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
        => _client.ScanEntriesAsync(request, cancellationToken);

    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
        => _client.GetEntryAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<EntryHistoryResponse> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)
        => _client.GetEntryHistoryAsync(request, cancellationToken);

    public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default)
        => _client.CancelScanAsync(request, cancellationToken);

    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => _client.GetMetricsSnapshotAsync(request, cancellationToken);

    public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default)
        => _client.GetClusterInfoAsync(request, cancellationToken);

    public Task<DeadLetterCountResponse> GetDeadLetterCountAsync(DeadLetterCountRequest request, CancellationToken cancellationToken = default)
        => _client.GetDeadLetterCountAsync(request, cancellationToken);

    public Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default)
        => _client.ListDeadLettersAsync(request, cancellationToken);

    public IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(StateObserveRequest request, CancellationToken cancellationToken = default)
        => _client.ObserveChangesAsync(request, cancellationToken);

    public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => _client.ObserveMetricsAsync(request, cancellationToken);

    public void Dispose() => _channel.Dispose();
}
