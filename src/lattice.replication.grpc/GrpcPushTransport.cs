using Orleans.Lattice.BPlusTree.Grains;
using System.Collections.Concurrent;
using System.Diagnostics;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
/// <c>GrpcChannel.ForAddress(Uri, GrpcChannelOptions)</c>, and
/// subsequent calls reuse it. HTTP/2 multiplexes concurrent batches
/// over the underlying TCP connection, and gRPC retry policies
/// (configured via <see cref="GrpcChannelOptions.ServiceConfig"/>) are
/// the supported mechanism for transient-error backoff.
/// </para>
/// <para>
/// On each <c>SendAsync</c> the transport prefers the pre-built
/// <see cref="ReplicationBatch.Envelope"/> when the caller supplied
/// one and hands it straight to the gRPC marshaller. The marshaller
/// re-encodes via the same <see cref="IReplicationBatchEncoder"/>
/// directly into the gRPC stream's
/// <see cref="System.Buffers.IBufferWriter{T}"/>, so the outbound
/// bytes never allocate a managed buffer beyond the encoded length the
/// encoder needs. When the typed slot is absent (legacy call sites,
/// bytes-only tests), the transport falls back to decoding
/// <see cref="ReplicationBatch.Payload"/> through the canonical
/// encoder; that fallback allocates one <c>WalRecord[]</c> per send
/// and is the cost of the bytes-only seam shape.
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
internal sealed class GrpcPushTransport : IReplicationTransport, IReplicationDigestProbeTransport, IDisposable
{
    private readonly LatticeReplicationGrpcMethod _method;
    private readonly IReplicationBatchEncoder _encoder;
    private readonly IOptionsMonitor<GrpcPushTransportOptions> _options;
    private readonly IReplicationSecretProvider _secrets;
    private readonly IOptionsMonitor<LatticeReplicationOptions> _replicationOptions;
    private readonly ILogger _logger;
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
        IOptionsMonitor<LatticeReplicationOptions> replicationOptions,
        ILogger<GrpcPushTransport>? logger = null)
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
        _logger = logger ?? NullLogger<GrpcPushTransport>.Instance;
    }

    /// <inheritdoc />
    public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        => SendCoreAsync(batch, cancellationToken);

    /// <inheritdoc />
    public async Task<DigestProbeResponse> ProbeDigestAsync(
        string targetClusterId,
        DigestProbeRequest request,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        if (string.IsNullOrEmpty(targetClusterId))
        {
            throw new ArgumentException(
                "targetClusterId must be non-empty.",
                nameof(targetClusterId));
        }

        if (string.IsNullOrEmpty(request.TreeName))
        {
            throw new ArgumentException(
                "DigestProbeRequest.TreeName must be non-empty.",
                nameof(request));
        }

        var channel = ResolvePeerChannel(targetClusterId);
        using var call = channel.Invoker.AsyncUnaryCall(
            _method.ProbeDigest,
            host: null,
            options: new CallOptions(cancellationToken: cancellationToken),
            request: new DigestProbeRequestBox { Value = request });

        var responseBox = await call.ResponseAsync.ConfigureAwait(false);
        return responseBox.Value;
    }

    /// <inheritdoc />
    public async Task<ContentManifestResponse> ExchangeContentManifestAsync(
        string targetClusterId,
        ContentManifestRequest request,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        if (string.IsNullOrEmpty(targetClusterId))
        {
            throw new ArgumentException(
                "targetClusterId must be non-empty.",
                nameof(targetClusterId));
        }

        if (string.IsNullOrEmpty(request.TreeName))
        {
            throw new ArgumentException(
                "ContentManifestRequest.TreeName must be non-empty.",
                nameof(request));
        }

        if (string.IsNullOrEmpty(request.OriginClusterId))
        {
            throw new ArgumentException(
                "ContentManifestRequest.OriginClusterId must be non-empty.",
                nameof(request));
        }

        var channel = ResolvePeerChannel(targetClusterId);
        try
        {
            using var call = channel.Invoker.AsyncUnaryCall(
                _method.ExchangeContentManifest,
                host: null,
                options: new CallOptions(cancellationToken: cancellationToken),
                request: new ContentManifestRequestBox { Value = request });

            var responseBox = await call.ResponseAsync.ConfigureAwait(false);
            return responseBox.Value;
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.Unimplemented or StatusCode.Unavailable)
        {
            // An un-upgraded peer that has not bound the
            // ExchangeContentManifest RPC answers Unimplemented; a peer
            // that is momentarily unreachable answers Unavailable. In
            // both cases the sender must fall back to shipping the full
            // batch verbatim, which is exactly what
            // ContentManifestResponse.NotSupported signals - so the
            // exchange is rolling-upgrade safe without a wire-version
            // pre-check on this hop.
            return ContentManifestResponse.NotSupported;
        }
    }

    /// <inheritdoc />
    public async Task<CompressionDictionaryPullResponse> PullCompressionDictionaryAsync(
        string targetClusterId,
        CompressionDictionaryPullRequest request,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        if (string.IsNullOrEmpty(targetClusterId))
        {
            throw new ArgumentException(
                "targetClusterId must be non-empty.",
                nameof(targetClusterId));
        }

        var channel = ResolvePeerChannel(targetClusterId);
        try
        {
            using var call = channel.Invoker.AsyncUnaryCall(
                _method.PullCompressionDictionary,
                host: null,
                options: new CallOptions(cancellationToken: cancellationToken),
                request: new CompressionDictionaryPullRequestBox { Value = request });

            var responseBox = await call.ResponseAsync.ConfigureAwait(false);
            return responseBox.Value;
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.Unimplemented or StatusCode.Unavailable)
        {
            // An un-upgraded peer that has not bound the
            // PullCompressionDictionary RPC answers Unimplemented; a peer
            // that is momentarily unreachable answers Unavailable. In
            // both cases the caller leaves the dictionary uninstalled and
            // retries on a later tick, which is exactly what
            // CompressionDictionaryPullResponse.NotSupported signals - so
            // the pull is rolling-upgrade safe without a wire-version
            // pre-check on this hop.
            return CompressionDictionaryPullResponse.NotSupported;
        }
    }

    /// <inheritdoc />
    public async Task<MerkleWalkProbeResponse> ProbeMerkleWalkAsync(
        string targetClusterId,
        MerkleWalkProbeRequest request,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        if (string.IsNullOrEmpty(targetClusterId))
        {
            throw new ArgumentException(
                "targetClusterId must be non-empty.",
                nameof(targetClusterId));
        }

        if (string.IsNullOrEmpty(request.TreeName))
        {
            throw new ArgumentException(
                "MerkleWalkProbeRequest.TreeName must be non-empty.",
                nameof(request));
        }

        var channel = ResolvePeerChannel(targetClusterId);
        try
        {
            using var call = channel.Invoker.AsyncUnaryCall(
                _method.ProbeMerkleWalk,
                host: null,
                options: new CallOptions(cancellationToken: cancellationToken),
                request: new MerkleWalkProbeRequestBox { Value = request });

            var responseBox = await call.ResponseAsync.ConfigureAwait(false);
            return responseBox.Value;
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.Unimplemented or StatusCode.Unavailable)
        {
            // An un-upgraded peer that has not bound the ProbeMerkleWalk RPC
            // answers Unimplemented; a momentarily-unreachable peer answers
            // Unavailable. In both cases the Merkle-walk localisation pass
            // aborts cleanly with the remote-unavailable reason, which is
            // exactly what MerkleWalkProbeResponse.Unavailable signals - so
            // the walk is rolling-upgrade safe without a wire-version
            // pre-check on this hop.
            return MerkleWalkProbeResponse.Unavailable;
        }
    }

    /// <inheritdoc />
    public async Task<Orleans.Lattice.HybridLogicalClock> GetPeerHighWaterMarkAsync(
        string targetClusterId,
        string treeName,
        string originClusterId,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        if (string.IsNullOrEmpty(targetClusterId))
        {
            throw new ArgumentException(
                "targetClusterId must be non-empty.",
                nameof(targetClusterId));
        }

        if (string.IsNullOrEmpty(treeName))
        {
            throw new ArgumentException(
                "treeName must be non-empty.",
                nameof(treeName));
        }

        if (string.IsNullOrEmpty(originClusterId))
        {
            throw new ArgumentException(
                "originClusterId must be non-empty.",
                nameof(originClusterId));
        }

        var channel = ResolvePeerChannel(targetClusterId);
        try
        {
            using var call = channel.Invoker.AsyncUnaryCall(
                _method.GetPeerHighWaterMark,
                host: null,
                options: new CallOptions(cancellationToken: cancellationToken),
                request: new PeerHighWaterMarkRequestBox
                {
                    Value = new PeerHighWaterMarkRequest
                    {
                        TreeName = treeName,
                        OriginClusterId = originClusterId,
                    },
                });

            var responseBox = await call.ResponseAsync.ConfigureAwait(false);
            return responseBox.Value.Clock;
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.Unimplemented or StatusCode.Unavailable)
        {
            // An un-upgraded peer that has not bound the GetPeerHighWaterMark
            // RPC answers Unimplemented; a momentarily-unreachable peer
            // answers Unavailable. In both cases the re-replay stage falls
            // back to the conservative HybridLogicalClock.Zero bound - which
            // re-ships every in-range retained entry and relies on the
            // receiver's per-origin idempotent dedup - so the probe is
            // rolling-upgrade safe without a wire-version pre-check on this
            // hop.
            return Orleans.Lattice.HybridLogicalClock.Zero;
        }
    }

    private async Task<ReplicationAck> SendCoreAsync(ReplicationBatch batch, CancellationToken cancellationToken)
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
        var envelopeBox = BuildEnvelopeBox(batch);
        // Captured for metric tagging only; the framing-only path
        // does not materialise a typed envelope and therefore reads
        // the entry count off the framing header instead.
        var entryCount = envelopeBox.Framing is { } framing
            ? framing.Header.EntryCount
            : envelopeBox.Value.Entries?.Count ?? 0;

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

            // Count successfully shipped entries on ack. An empty
            // (heartbeat / keep-alive) batch contributes zero. This is
            // the producer-side ship-rate signal; the log-tailing
            // shipper is the sole driver of outbound entries.
            if (entryCount > 0)
            {
                LatticeReplicationMetrics.WalEntriesShipped.Add(
                    entryCount,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, batch.TreeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, batch.TargetClusterId),
                    LatticeTenantLabel.ForTree(batch.TreeName));
            }

            return ackBox.Value;
        }
        finally
        {
            LatticeReplicationMetrics.ShipDuration.Record(
                stopwatch.GetElapsedMilliseconds(),
                new System.Diagnostics.TagList
                {
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, batch.TreeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, batch.TargetClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, outcome),
                    LatticeTenantLabel.ForTree(batch.TreeName),
                });
        }
    }

    private ReplicationBatchEnvelopeBox BuildEnvelopeBox(ReplicationBatch batch)
    {
        // Framing-only fast path: the shipper supplied a pre-encoded
        // entry-segment list and a fixed-shape header. Hand the bytes
        // straight to the marshaller via the framing slot; no typed
        // envelope is materialised on the producer side.
        if (batch.EncodedEnvelope is { } encoded)
        {
            return new ReplicationBatchEnvelopeBox
            {
                Framing = new ReplicationBatchEnvelopeBox.FramingPayload(
                    encoded.Header,
                    batch.TreeName,
                    batch.OriginClusterId,
                    encoded.EncodedEntries),
            };
        }

        return new ReplicationBatchEnvelopeBox { Value = BuildEnvelope(batch) };
    }

    private ReplicationBatchEnvelope BuildEnvelope(ReplicationBatch batch)
    {
        // Typed-envelope fast path: when the shipper supplied the
        // pre-built envelope on the batch, ship it verbatim. Skips a
        // per-send `_encoder.Decode(batch.Payload)` call that would
        // otherwise allocate one `WalRecord[]` per send purely to
        // satisfy the gRPC marshaller (which then re-encodes via the
        // same canonical encoder directly into the gRPC stream's
        // buffer writer). The shipper populates this slot on every
        // batch it sends, so the legacy decode path below is
        // exercised only by call sites that predate the typed slot
        // (or by tests that exercise the bytes-only seam directly).
        if (batch.Envelope is { } envelope)
        {
            return envelope;
        }

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

        // Legacy bytes-shaped path: callers that did not supply the
        // typed envelope decode through the canonical encoder so the
        // gRPC marshaller can re-encode into the stream buffer. The
        // shipper hot path no longer takes this branch.
        return _encoder.Decode(batch.Payload);
    }

    /// <summary>
    /// Test-only seam exposing the private
    /// <c>BuildEnvelope</c> branch logic so the typed-envelope fast
    /// path, the heartbeat / empty-payload shortcut, and the legacy
    /// bytes-shaped decode fallback can be pinned without standing up
    /// a real gRPC server. Not part of the public API.
    /// </summary>
    internal ReplicationBatchEnvelope BuildEnvelopeForTesting(ReplicationBatch batch) => BuildEnvelope(batch);

    /// <summary>
    /// Test-only seam exposing the private
    /// <c>BuildEnvelopeBox</c> dispatch so the framing-only path and
    /// the typed fallback can be pinned without standing up a real
    /// gRPC server. Not part of the public API.
    /// </summary>
    internal ReplicationBatchEnvelopeBox BuildEnvelopeBoxForTesting(ReplicationBatch batch) => BuildEnvelopeBox(batch);

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
        // receiver-side interceptor can still authenticate the call -
        // and the helper emits a warning + metric so that insecure
        // path is never silent.
        GrpcChannelHardening.ApplyCallCredentials(
            channelOptions, endpoint, options.AllowPlaintextEndpoints, _secrets, targetClusterId, localClusterId, _logger, "push");

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

