using Orleans.Lattice.BPlusTree.Grains;
using System.Collections.Concurrent;
using System.Diagnostics;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// gRPC streaming push transport. Implements
/// <see cref="IReplicationTransport"/> on top of a long-lived,
/// HTTP/2-multiplexed <see cref="GrpcChannel"/> per peer cluster, and
/// invokes the unary <c>Push</c> RPC defined by
/// <see cref="LatticeReplicationGrpcMethod"/>.
/// </summary>
/// <remarks>
/// <para>
/// The transport caches one <see cref="GrpcChannel"/> per
/// <c>TargetClusterId</c>; the first <c>SendAsync</c> to a given peer
/// constructs the channel via
/// <see cref={`GrpcChannel.ForAddress(Uri, GrpcChannelOptions)`}, and
/// subsequent calls reuse it. HTTP/2 multiplexes concurrent batches
/// over the underlying TCP connection, and gRPC retry policies
/// (configured via <see cref="GrpcChannelOptions.ServiceConfig"/>) are
/// the supported mechanism for transient-error backoff.
/// </para>
/// <para>
/// On each <c>SendAsync</c> the transport decodes
/// <see cref="ReplicationBatch.Payload"/> back into a typed
/// <see cref="ReplicationBatchEnvelope"/>, then hands the envelope to
/// the gRPC marshaller. The marshaller re-encodes via the same
/// <see cref="IReplicationBatchEncoder"/> directly into the gRPC
/// stream's <see cref="System.Buffers.IBufferWriter{T}"/>, so the
/// outbound bytes never allocate a managed buffer beyond the encoded
/// length the encoder needs. The decode-then-encode round-trip on the
/// sender side is the cost of the current opaque-bytes
/// <see cref="IReplicationTransport"/> seam; a future iteration may
/// widen the seam to accept a typed envelope and remove the round-trip
/// entirely.
/// </para>
/// <para>
/// Each <c>SendAsync</c> records a
/// <see cref="LatticeReplicationMetrics.ShipDuration"/> sample tagged
/// with the tree, peer, and outcome (<c>ok</c> on a successful ack,
/// <c>error</c> on any thrown exception). The peer-level gauges
/// (<c>entries_behind</c> etc.) are owned by the outbound shipper, not
/// the transport.
/// </para>
/// </remarks>
internal sealed class GrpcPushTransport : IReplicationTransport, IDisposable
{
    private readonly LatticeReplicationGrpcMethod _method;
    private readonly IReplicationBatchEncoder _encoder;
    private readonly IOptionsMonitor<GrpcPushTransportOptions> _options;
    private readonly IReplicationSecretProvider _secrets;
    private readonly IOptionsMonitor<LatticeReplicationOptions> _replicationOptions;
    private readonly ConcurrentDictionary<string, PeerChannel> _channels = new(StringComparer.Ordinal);
    private int _disposed;

    /// <summary>
    /// Initialises the transport with its dependencies.
    /// </summary>
    public GrpcPushTransport(
        LatticeReplicationGrpcMethod method,
        IReplicationBatchEncoder encoder,
        IOptionsMonitor<GrpcPushTransportOptions> options,
        IReplicationSecretProvider secrets,
        IOptionsMonitor<LatticeReplicationOptions> replicationOptions)
    {
        ArgumentNullException.ThrowIfNull(method);
        ArgumentNullException.ThrowIfNull(encoder);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(secrets);
        ArgumentNullException.ThrowIfNull(replicationOptions);

        _method = method;
        _encoder = encoder;
        _options = options;
        _secrets = secrets;
        _replicationOptions = replicationOptions;
    }

    /// <inheritdoc />
    public async Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        if (string.IsNullOrEmpty(batch.TargetClusterId))
        {
            throw new ArgumentException(
                "ReplicationBatch.TargetClusterId must be non-empty.",
                nameof(batch));
        }

        if (string.IsNullOrEmpty(batch.TreeName))
        {
            throw new ArgumentException(
                "ReplicationBatch.TreeName must be non-empty.",
                nameof(batch));
        }

        if (string.IsNullOrEmpty(batch.OriginClusterId))
        {
            throw new ArgumentException(
                "ReplicationBatch.OriginClusterId must be non-empty.",
                nameof(batch));
        }

        var channel = ResolvePeerChannel(batch.TargetClusterId);
        var envelope = BuildEnvelope(batch);
        var envelopeBox = new ReplicationBatchEnvelopeBox { Value = envelope };

        var stopwatch = ValueStopwatch.StartNew();
        var outcome = "error";
        try
        {
            using var call = channel.Invoker.AsyncUnaryCall(
                _method.Push,
                host: null,
                options: new CallOptions(cancellationToken: cancellationToken),
                request: envelopeBox);

            var ackBox = await call.ResponseAsync.ConfigureAwait(false);
            outcome = "ok";

            // Count successfully shipped entries on ack. The envelope's
            // Entries collection is the authoritative count; an empty
            // (heartbeat / keep-alive) batch contributes zero. Pairs
            // with WalEntriesAppended on the producer side so operators
            // can compute the growth-rate vs. ship-rate ratio.
            var entryCount = envelope.Entries?.Count ?? 0;
            if (entryCount > 0)
            {
                LatticeReplicationMetrics.WalEntriesShipped.Add(
                    entryCount,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, batch.TreeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, batch.TargetClusterId));
            }

            return ackBox.Value;
        }
        finally
        {
            LatticeReplicationMetrics.ShipDuration.Record(
                stopwatch.GetElapsedMilliseconds(),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, batch.TreeName),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, batch.TargetClusterId),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, outcome));
        }
    }

    private ReplicationBatchEnvelope BuildEnvelope(ReplicationBatch batch)
    {
        // An empty payload is the heartbeat / keep-alive shape. We
        // construct an empty-entries envelope rather than feed an
        // empty buffer through the decoder (which would throw on
        // empty input by design).
        if (batch.Payload.IsEmpty)
        {
            return new ReplicationBatchEnvelope
            {
                WireVersion = ReplicationBatchEnvelope.CurrentVersion,
                TreeName = batch.TreeName,
                OriginClusterId = batch.OriginClusterId,
                Entries = Array.Empty<WalRecord>(),
            };
        }

        // The IReplicationTransport seam carries opaque bytes; the
        // gRPC wire is type-shaped. Decode once on the sender so the
        // typed envelope can flow through the gRPC marshaller, which
        // re-encodes via the same canonical encoder directly into the
        // gRPC stream's buffer writer (zero managed allocation
        // beyond the encoded length). The decode-encode round-trip
        // here is the cost of the current bytes-shaped seam; widening
        // the seam to accept an envelope directly is a future
        // optimisation tracked separately.
        return _encoder.Decode(batch.Payload);
    }

    private PeerChannel ResolvePeerChannel(string targetClusterId)
    {
        if (_channels.TryGetValue(targetClusterId, out var existing))
        {
            return existing;
        }

        var options = _options.CurrentValue;
        if (!options.PeerEndpoints.TryGetValue(targetClusterId, out var endpoint))
        {
            throw new InvalidOperationException(
                $"GrpcPushTransport has no endpoint configured for target cluster '{targetClusterId}'. "
                + $"Populate {nameof(GrpcPushTransportOptions)}.{nameof(GrpcPushTransportOptions.PeerEndpoints)} "
                + "before the first SendAsync call to this peer.");
        }

        // Security gate: refuse to ship to a non-https peer unless the
        // host has explicitly signed off on plaintext via
        // GrpcPushTransportOptions.AllowPlaintextEndpoints.
        GrpcChannelHardening.EnforceSchemeGate(endpoint, options.AllowPlaintextEndpoints, targetClusterId);

        // Local cluster id stamped on the x-lattice-replication-origin
        // header. The options surface is the explicit override; the
        // implicit fallback is LatticeReplicationOptions.ClusterId
        // (already validated non-empty by the options validator).
        var localClusterId = !string.IsNullOrWhiteSpace(options.LocalClusterId)
            ? options.LocalClusterId!
            : _replicationOptions.CurrentValue.ClusterId;

        var channelOptions = new GrpcChannelOptions();

        // Attach the shared-secret CallCredentials. When the channel
        // is https the composite credentials carry TLS plus
        // call-credentials together; when the channel is plaintext
        // (only possible with the opt-in), the secret still travels
        // via UnsafeUseInsecureChannelCallCredentials so the
        // receiver-side interceptor can still authenticate the call.
        var callCreds = GrpcChannelHardening.BuildCallCredentials(_secrets, targetClusterId, localClusterId);
        if (options.AllowPlaintextEndpoints && !string.Equals(endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            channelOptions.UnsafeUseInsecureChannelCallCredentials = true;
            channelOptions.Credentials = ChannelCredentials.Create(ChannelCredentials.Insecure, callCreds);
        }
        else
        {
            channelOptions.Credentials = ChannelCredentials.Create(ChannelCredentials.SecureSsl, callCreds);
        }

        // Host-supplied ConfigureChannel runs *after* the hardened
        // defaults so the host can replace any of them (e.g. supply
        // an explicit ChannelCredentials with a custom mTLS chain).
        options.ConfigureChannel?.Invoke(targetClusterId, channelOptions);
        var createdChannel = GrpcChannel.ForAddress(endpoint, channelOptions);
        // Cache the CallInvoker alongside the channel so SendAsync
        // does not pay a per-call DefaultCallInvoker allocation; the
        // invoker is a stateless wrapper that closes over the channel.
        var created = new PeerChannel(createdChannel, createdChannel.CreateCallInvoker());

        // Race-safe insert: if another thread won, dispose ours and
        // reuse theirs. If a concurrent Dispose() cleared the
        // dictionary between TryAdd and the follow-up TryGetValue, we
        // surface that as ObjectDisposedException rather than a
        // KeyNotFoundException leaking from the indexer.
        if (_channels.TryAdd(targetClusterId, created))
        {
            return created;
        }

        createdChannel.Dispose();
        if (_channels.TryGetValue(targetClusterId, out var winner))
        {
            return winner;
        }

        throw new ObjectDisposedException(nameof(GrpcPushTransport));
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

    private readonly struct ValueStopwatch
    {
        private static readonly double TimestampToMilliseconds = 1000.0 / Stopwatch.Frequency;

        private readonly long _startTimestamp;

        private ValueStopwatch(long startTimestamp) => _startTimestamp = startTimestamp;

        public static ValueStopwatch StartNew() => new(Stopwatch.GetTimestamp());

        public double GetElapsedMilliseconds()
        {
            var delta = Stopwatch.GetTimestamp() - _startTimestamp;
            return delta * TimestampToMilliseconds;
        }
    }
}

