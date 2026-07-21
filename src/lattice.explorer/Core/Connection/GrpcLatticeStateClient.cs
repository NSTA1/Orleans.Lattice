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

        var auth = settings.Authentication;

        // A live token provider must be consulted per call so the freshest token
        // is attached. The insecure-channel safeguard is only lifted for an
        // endpoint confirmed to be plaintext with an explicit operator opt-in;
        // see BuildChannelOptions.
        var channelOptions = BuildChannelOptions(settings);

        var channel = GrpcChannel.ForAddress(settings.Address, channelOptions);

        CallInvoker invoker = channel.CreateCallInvoker();

        // Transport headers accompany every call regardless of the auth mode (for
        // example an origin-routing header a fronting proxy requires), so apply
        // them before, and independently of, the authentication interceptor - the
        // sign-in swap replaces settings.Authentication but never TransportHeaders.
        invoker = ApplyTransportHeaders(invoker, settings.TransportHeaders);

        if (auth is { HasCredentialProvider: true, CredentialProvider: { } provider })
        {
            // CallCredentials.FromInterceptor is invoked per RPC and may await, so
            // the provider can refresh a near-expiry token before the header is
            // written. The token is never captured statically on the channel.
            var callCredentials = CallCredentials.FromInterceptor(async (context, metadata) =>
            {
                var header = await provider
                    .GetAuthorizationHeaderAsync(context.CancellationToken)
                    .ConfigureAwait(false);
                if (!string.IsNullOrEmpty(header))
                {
                    metadata.Add(LatticeCallAuthentication.AuthorizationHeaderName, header);
                }
            });

            invoker = invoker.Intercept(new CallCredentialsInterceptor(callCredentials));
        }
        else if (auth is { HasHeaders: true })
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

    /// <summary>
    /// Applies the sign-in-independent transport headers (if any) to every call on
    /// <paramref name="invoker"/>, returning it unchanged when there are none.
    /// </summary>
    private static CallInvoker ApplyTransportHeaders(CallInvoker invoker, IReadOnlyDictionary<string, string>? transportHeaders)
    {
        if (transportHeaders is not { Count: > 0 } headers)
        {
            return invoker;
        }

        return invoker.Intercept(metadata =>
        {
            foreach (var (key, value) in headers)
            {
                metadata.Add(key, value);
            }

            return metadata;
        });
    }

    /// <summary>
    /// Builds the <see cref="GrpcChannelOptions"/> for <paramref name="settings"/>.
    /// gRPC refuses to send per-call credentials over a channel it cannot confirm
    /// is secure; that safeguard is only lifted
    /// (<see cref="GrpcChannelOptions.UnsafeUseInsecureChannelCallCredentials"/>
    /// set to <see langword="true"/>) when the endpoint is genuinely plaintext
    /// (an <c>http</c> address) AND the operator has explicitly opted into
    /// unencrypted transport via
    /// <see cref="LatticeConnectionSettings.AllowUnencryptedHttp2"/>. For an
    /// <c>https</c> endpoint the safeguard is left active (the flag stays
    /// <see langword="false"/>) so credentials are never sent over a channel gRPC
    /// cannot verify; credentials still attach over the confirmed-secure TLS
    /// channel through the call-credentials interceptor. This mirrors the
    /// replication transport's scheme gate.
    /// </summary>
    internal static GrpcChannelOptions BuildChannelOptions(LatticeConnectionSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        var channelOptions = new GrpcChannelOptions();
        if (settings.Authentication is { HasCredentialProvider: true }
            && settings.AllowUnencryptedHttp2
            && !IsHttpsAddress(settings.Address))
        {
            channelOptions.UnsafeUseInsecureChannelCallCredentials = true;
        }

        return channelOptions;
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="address"/> is an
    /// absolute <c>https</c> URI. A non-absolute or non-https address is treated
    /// as non-https so the insecure-channel safeguard is only ever lifted for an
    /// endpoint confirmed to be plaintext.
    /// </summary>
    private static bool IsHttpsAddress(string? address) =>
        Uri.TryCreate(address, UriKind.Absolute, out var uri)
        && string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);

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
