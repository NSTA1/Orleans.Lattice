using System.Collections.Concurrent;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// gRPC binding for <see cref="ISagaControlChannel"/>. A coordinator
/// resolves this transport through DI to drive the imperative saga
/// control RPCs (<c>Prepare</c>, <c>Commit</c>, <c>Abort</c>,
/// <c>GetStatus</c>) against a participant cluster's
/// <see cref="LatticeSagaGrpcService"/> over a long-lived HTTP/2
/// <see cref="GrpcChannel"/> per participant cluster.
/// </summary>
/// <remarks>
/// One channel is constructed lazily per <c>clusterId</c> and cached for
/// the lifetime of the transport; HTTP/2 multiplexes concurrent saga
/// calls over the underlying TCP connection. The transport reuses the
/// same hardened-defaults pipeline as
/// <see cref="GrpcRemoteSnapshotTransport"/>: the plaintext scheme is
/// rejected unless the host opts in via
/// <see cref="GrpcSagaControlChannelOptions.AllowPlaintextEndpoints"/>
/// and the shared-secret credential plus origin-cluster-id header are
/// attached to every call through
/// <see cref="GrpcChannelHardening.BuildCallCredentials"/>.
/// </remarks>
internal sealed class GrpcSagaControlChannel : ISagaControlChannel, IDisposable
{
    private readonly LatticeSagaGrpcMethods _methods;
    private readonly IOptionsMonitor<GrpcSagaControlChannelOptions> _options;
    private readonly IReplicationSecretProvider _secrets;
    private readonly IOptionsMonitor<LatticeReplicationOptions> _replicationOptions;
    private readonly ConcurrentDictionary<string, PeerChannel> _channels = new(StringComparer.Ordinal);
    private int _disposed;

    /// <summary>Initialises the transport with its dependencies.</summary>
    public GrpcSagaControlChannel(
        LatticeSagaGrpcMethods methods,
        IOptionsMonitor<GrpcSagaControlChannelOptions> options,
        IReplicationSecretProvider secrets,
        IOptionsMonitor<LatticeReplicationOptions> replicationOptions)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(secrets);
        ArgumentNullException.ThrowIfNull(replicationOptions);

        _methods = methods;
        _options = options;
        _secrets = secrets;
        _replicationOptions = replicationOptions;
    }

    /// <inheritdoc />
    public Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        => InvokeAsync(_methods.Prepare, clusterId, request, cancellationToken);

    /// <inheritdoc />
    public Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        => InvokeAsync(_methods.Commit, clusterId, request, cancellationToken);

    /// <inheritdoc />
    public Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        => InvokeAsync(_methods.Abort, clusterId, request, cancellationToken);

    /// <inheritdoc />
    public Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        => InvokeAsync(_methods.GetStatus, clusterId, request, cancellationToken);

    private async Task<SagaControlResponse> InvokeAsync(
        Method<SagaControlRequestBox, SagaControlResponseBox> method,
        string clusterId,
        SagaControlRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        var channel = ResolvePeerChannel(clusterId);
        var requestBox = new SagaControlRequestBox { Value = request };

        using var call = channel.Invoker.AsyncUnaryCall(
            method,
            host: null,
            options: new CallOptions(cancellationToken: cancellationToken),
            request: requestBox);

        var responseBox = await call.ResponseAsync.ConfigureAwait(false);
        return responseBox.Value;
    }

    private PeerChannel ResolvePeerChannel(string clusterId)
    {
        if (_channels.TryGetValue(clusterId, out var existing))
        {
            return existing;
        }

        var options = _options.CurrentValue;
        if (!options.PeerEndpoints.TryGetValue(clusterId, out var endpoint))
        {
            throw new InvalidOperationException(
                $"GrpcSagaControlChannel has no endpoint configured for participant cluster '{clusterId}'. "
                + $"Populate {nameof(LatticeReplicationGrpcOptions)}.{nameof(LatticeReplicationGrpcOptions.Peers)} "
                + "before the first saga control call to this participant.");
        }

        GrpcChannelHardening.EnforceSchemeGate(endpoint, options.AllowPlaintextEndpoints, clusterId);

        var localClusterId = !string.IsNullOrWhiteSpace(options.LocalClusterId)
            ? options.LocalClusterId!
            : _replicationOptions.CurrentValue.ClusterId;

        var channelOptions = new GrpcChannelOptions();
        var callCreds = GrpcChannelHardening.BuildCallCredentials(_secrets, clusterId, localClusterId);
        if (options.AllowPlaintextEndpoints && !string.Equals(endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            channelOptions.UnsafeUseInsecureChannelCallCredentials = true;
            channelOptions.Credentials = ChannelCredentials.Create(ChannelCredentials.Insecure, callCreds);
        }
        else
        {
            channelOptions.Credentials = ChannelCredentials.Create(ChannelCredentials.SecureSsl, callCreds);
        }

        options.ConfigureChannel?.Invoke(clusterId, channelOptions);
        var createdChannel = GrpcChannel.ForAddress(endpoint, channelOptions);
        var created = new PeerChannel(createdChannel, createdChannel.CreateCallInvoker());

        if (_channels.TryAdd(clusterId, created))
        {
            return created;
        }

        createdChannel.Dispose();
        if (_channels.TryGetValue(clusterId, out var winner))
        {
            return winner;
        }

        throw new ObjectDisposedException(nameof(GrpcSagaControlChannel));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (var peer in _channels.Values)
        {
            peer.Channel.Dispose();
        }

        _channels.Clear();
    }

    private readonly record struct PeerChannel(GrpcChannel Channel, CallInvoker Invoker);
}
