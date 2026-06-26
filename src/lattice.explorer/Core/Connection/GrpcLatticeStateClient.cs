using Grpc.Core;
using Grpc.Core.Interceptors;
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
    /// Builds the channel and client for <paramref name="settings"/>, attaching
    /// the authentication headers (if any) via a metadata interceptor and
    /// enabling h2c when an unencrypted endpoint is requested.
    /// </summary>
    public static GrpcLatticeStateClient Create(LatticeConnectionSettings settings, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        if (settings.AllowUnencryptedHttp2)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        var channel = GrpcChannel.ForAddress(settings.Address);

        CallInvoker invoker = channel.CreateCallInvoker();
        if (settings.Authentication is { HasHeaders: true } auth)
        {
            var headers = auth.Headers!;
            invoker = invoker.Intercept(metadata =>
            {
                foreach (var (key, value) in headers)
                {
                    metadata.Add(key, value);
                }

                return metadata;
            });
        }

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

    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
        => _client.GetTreeStructureAsync(request, cancellationToken);

    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
        => _client.ScanEntriesAsync(request, cancellationToken);

    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
        => _client.GetEntryAsync(request, cancellationToken);

    public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default)
        => _client.CancelScanAsync(request, cancellationToken);

    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => _client.GetMetricsSnapshotAsync(request, cancellationToken);

    public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default)
        => _client.GetClusterInfoAsync(request, cancellationToken);

    public IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(StateObserveRequest request, CancellationToken cancellationToken = default)
        => _client.ObserveChangesAsync(request, cancellationToken);

    public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => _client.ObserveMetricsAsync(request, cancellationToken);

    public void Dispose() => _channel.Dispose();
}
