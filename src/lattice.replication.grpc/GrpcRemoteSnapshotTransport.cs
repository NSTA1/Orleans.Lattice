using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// gRPC binding for <see cref="IRemoteSnapshotTransport"/>. The
/// receiver-side <see cref="RemoteSnapshotProvider"/> resolves this
/// transport through DI to fetch a cross-cluster snapshot from the
/// sender-side <see cref="LatticeRemoteSnapshotGrpcService"/> over a
/// long-lived HTTP/2 <see cref="GrpcChannel"/> per sender cluster.
/// </summary>
/// <remarks>
/// <para>
/// One channel is constructed lazily per <c>sourceClusterId</c> and
/// cached for the lifetime of the transport; HTTP/2 multiplexes
/// concurrent snapshot calls over the underlying TCP connection. The
/// transport reuses the same hardened-defaults pipeline as
/// <see cref="GrpcPushTransport"/>: the plaintext scheme is rejected
/// unless the host opts in via
/// <see cref="GrpcRemoteSnapshotTransportOptions.AllowPlaintextEndpoints"/>
/// and the shared-secret credential is attached to every call through
/// <see cref="GrpcChannelHardening.BuildCallCredentials"/>.
/// </para>
/// <para>
/// Each method validates the receiver-supplied arguments against the
/// transport contract (non-empty <c>treeName</c> and
/// <c>sourceClusterId</c>) before invoking the channel so failures
/// surface as <see cref="ArgumentException"/> rather than as gRPC
/// status errors. The <c>RequestSnapshotAsync</c> stream propagates
/// caller cancellation through the gRPC call options and observes
/// the cancellation token while enumerating the response stream so
/// the receiver can abort the drain cleanly.
/// </para>
/// </remarks>
internal sealed class GrpcRemoteSnapshotTransport : IRemoteSnapshotTransport, IDisposable
{
    private readonly LatticeRemoteSnapshotGrpcMethods _methods;
    private readonly IOptionsMonitor<GrpcRemoteSnapshotTransportOptions> _options;
    private readonly IReplicationSecretProvider _secrets;
    private readonly IOptionsMonitor<LatticeReplicationOptions> _replicationOptions;
    private readonly ConcurrentDictionary<string, PeerChannel> _channels = new(StringComparer.Ordinal);
    private int _disposed;

    /// <summary>
    /// Initialises the transport with its dependencies.
    /// </summary>
    public GrpcRemoteSnapshotTransport(
        LatticeRemoteSnapshotGrpcMethods methods,
        IOptionsMonitor<GrpcRemoteSnapshotTransportOptions> options,
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
    public async Task<RemoteSnapshotMetadata> GetMetadataAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        var channel = ResolvePeerChannel(sourceClusterId);
        var requestBox = new RemoteSnapshotMetadataRequestBox
        {
            Value = new RemoteSnapshotMetadataRequest
            {
                TreeName = treeName,
                SourceClusterId = sourceClusterId,
                FromAsOfHlc = fromAsOfHlc,
            },
        };

        using var call = channel.Invoker.AsyncUnaryCall(
            _methods.GetMetadata,
            host: null,
            options: new CallOptions(cancellationToken: cancellationToken),
            request: requestBox);

        var responseBox = await call.ResponseAsync.ConfigureAwait(false);
        return responseBox.Value;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        var channel = ResolvePeerChannel(sourceClusterId);
        var requestBox = new RemoteSnapshotMetadataRequestBox
        {
            Value = new RemoteSnapshotMetadataRequest
            {
                TreeName = treeName,
                SourceClusterId = sourceClusterId,
                FromAsOfHlc = fromAsOfHlc,
            },
        };

        using var call = channel.Invoker.AsyncServerStreamingCall(
            _methods.RequestSnapshot,
            host: null,
            options: new CallOptions(cancellationToken: cancellationToken),
            request: requestBox);

        while (true)
        {
            // Translate gRPC's StatusCode.Cancelled into the canonical
            // OperationCanceledException so receivers can rely on the
            // same cancellation contract regardless of which
            // IRemoteSnapshotTransport binding they are wired to. We
            // can't wrap the yield itself in a try/catch (iterators
            // forbid it), so the MoveNext call is wrapped instead and
            // the yield runs against the safely-advanced cursor.
            bool more;
            try
            {
                more = await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false);
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(ex.Status.Detail, ex, cancellationToken);
            }

            if (!more)
            {
                yield break;
            }

            yield return call.ResponseStream.Current.Value.Entry;
        }
    }

    private PeerChannel ResolvePeerChannel(string sourceClusterId)
    {
        if (_channels.TryGetValue(sourceClusterId, out var existing))
        {
            return existing;
        }

        var options = _options.CurrentValue;
        if (!options.SenderEndpoints.TryGetValue(sourceClusterId, out var endpoint))
        {
            throw new InvalidOperationException(
                $"GrpcRemoteSnapshotTransport has no endpoint configured for sender cluster '{sourceClusterId}'. "
                + $"Populate {nameof(GrpcRemoteSnapshotTransportOptions)}.{nameof(GrpcRemoteSnapshotTransportOptions.SenderEndpoints)} "
                + "before the first GetMetadataAsync / RequestSnapshotAsync call to this sender.");
        }

        GrpcChannelHardening.EnforceSchemeGate(endpoint, options.AllowPlaintextEndpoints, sourceClusterId);

        var localClusterId = !string.IsNullOrWhiteSpace(options.LocalClusterId)
            ? options.LocalClusterId!
            : _replicationOptions.CurrentValue.ClusterId;

        var channelOptions = new GrpcChannelOptions();
        var callCreds = GrpcChannelHardening.BuildCallCredentials(_secrets, sourceClusterId, localClusterId);
        if (options.AllowPlaintextEndpoints && !string.Equals(endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            channelOptions.UnsafeUseInsecureChannelCallCredentials = true;
            channelOptions.Credentials = ChannelCredentials.Create(ChannelCredentials.Insecure, callCreds);
        }
        else
        {
            channelOptions.Credentials = ChannelCredentials.Create(ChannelCredentials.SecureSsl, callCreds);
        }

        options.ConfigureChannel?.Invoke(sourceClusterId, channelOptions);
        var createdChannel = GrpcChannel.ForAddress(endpoint, channelOptions);
        var created = new PeerChannel(createdChannel, createdChannel.CreateCallInvoker());

        if (_channels.TryAdd(sourceClusterId, created))
        {
            return created;
        }

        createdChannel.Dispose();
        if (_channels.TryGetValue(sourceClusterId, out var winner))
        {
            return winner;
        }

        throw new ObjectDisposedException(nameof(GrpcRemoteSnapshotTransport));
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