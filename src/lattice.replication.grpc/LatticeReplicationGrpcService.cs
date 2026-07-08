using Orleans.Lattice.BPlusTree.Grains;
using System.Diagnostics;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Abstract base class for the gRPC <c>Push</c> RPC. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c>
/// reflects against to discover and register the unary
/// <c>Push(ReplicationBatchEnvelopeBox) -&gt; ReplicationAckBox</c>
/// route.
/// </summary>
/// <remarks>
/// The base/derived split mirrors the pattern <c>Grpc.Tools</c>'s
/// codegen produces for a <c>.proto</c>-defined service: the base
/// class is the metadata-bearing type the binder discovers, and the
/// derived class is the concrete implementation resolved from DI per
/// request. <c>Grpc.AspNetCore</c> calls <see cref="BindService"/>
/// once at startup with a <see langword="null"/> service instance to
/// record method metadata, then resolves the actual instance from DI
/// at request time.
/// </remarks>
[BindServiceMethod(typeof(LatticeReplicationGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeReplicationGrpcServiceBase
{
    /// <summary>
    /// Handles a single push batch. Implemented in
    /// <see cref="LatticeReplicationGrpcService"/>.
    /// </summary>
    public abstract Task<ReplicationAckBox> Push(ReplicationBatchEnvelopeBox request, ServerCallContext context);

    /// <summary>
    /// Handles a single anti-entropy digest probe. Implemented in
    /// <see cref="LatticeReplicationGrpcService"/>.
    /// </summary>
    public abstract Task<DigestProbeResponseBox> ProbeDigest(DigestProbeRequestBox request, ServerCallContext context);

    /// <summary>
    /// Handles a single content-hash payload-elision manifest exchange.
    /// Implemented in <see cref="LatticeReplicationGrpcService"/>.
    /// </summary>
    public abstract Task<ContentManifestResponseBox> ExchangeContentManifest(ContentManifestRequestBox request, ServerCallContext context);

    /// <summary>
    /// Handles a single self-distributing shared-dictionary pull.
    /// Implemented in <see cref="LatticeReplicationGrpcService"/>.
    /// </summary>
    public abstract Task<CompressionDictionaryPullResponseBox> PullCompressionDictionary(CompressionDictionaryPullRequestBox request, ServerCallContext context);

    /// <summary>
    /// Handles a single anti-entropy Merkle-walk drift-localisation probe.
    /// Implemented in <see cref="LatticeReplicationGrpcService"/>.
    /// </summary>
    public abstract Task<MerkleWalkProbeResponseBox> ProbeMerkleWalk(MerkleWalkProbeRequestBox request, ServerCallContext context);

    /// <summary>
    /// Handles a single anti-entropy peer high-water-mark probe.
    /// Implemented in <see cref="LatticeReplicationGrpcService"/>.
    /// </summary>
    public abstract Task<PeerHighWaterMarkResponseBox> GetPeerHighWaterMark(PeerHighWaterMarkRequestBox request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called
    /// once at startup with <paramref name="serviceImpl"/> set to
    /// <see langword="null"/> to record method metadata; the actual
    /// service instance is resolved per request from DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeReplicationGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var method = LatticeReplicationGrpcMethodHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeReplicationGrpcMethodHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpc)} "
                + "ran and that "
                + $"{nameof(LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpc)} "
                + "pre-resolved LatticeReplicationGrpcMethod before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            // Startup metadata pass - register the method shape with a
            // null handler. Grpc.AspNetCore replaces the handler with
            // the real per-request invoker resolved through DI.
            binder.AddMethod(method.Push, (UnaryServerMethod<ReplicationBatchEnvelopeBox, ReplicationAckBox>?)null);
            binder.AddMethod(method.ProbeDigest, (UnaryServerMethod<DigestProbeRequestBox, DigestProbeResponseBox>?)null);
            binder.AddMethod(method.ExchangeContentManifest, (UnaryServerMethod<ContentManifestRequestBox, ContentManifestResponseBox>?)null);
            binder.AddMethod(method.PullCompressionDictionary, (UnaryServerMethod<CompressionDictionaryPullRequestBox, CompressionDictionaryPullResponseBox>?)null);
            binder.AddMethod(method.ProbeMerkleWalk, (UnaryServerMethod<MerkleWalkProbeRequestBox, MerkleWalkProbeResponseBox>?)null);
            binder.AddMethod(method.GetPeerHighWaterMark, (UnaryServerMethod<PeerHighWaterMarkRequestBox, PeerHighWaterMarkResponseBox>?)null);
            return;
        }

        binder.AddMethod(method.Push, new UnaryServerMethod<ReplicationBatchEnvelopeBox, ReplicationAckBox>(serviceImpl.Push));
        binder.AddMethod(method.ProbeDigest, new UnaryServerMethod<DigestProbeRequestBox, DigestProbeResponseBox>(serviceImpl.ProbeDigest));
        binder.AddMethod(method.ExchangeContentManifest, new UnaryServerMethod<ContentManifestRequestBox, ContentManifestResponseBox>(serviceImpl.ExchangeContentManifest));
        binder.AddMethod(method.PullCompressionDictionary, new UnaryServerMethod<CompressionDictionaryPullRequestBox, CompressionDictionaryPullResponseBox>(serviceImpl.PullCompressionDictionary));
        binder.AddMethod(method.ProbeMerkleWalk, new UnaryServerMethod<MerkleWalkProbeRequestBox, MerkleWalkProbeResponseBox>(serviceImpl.ProbeMerkleWalk));
        binder.AddMethod(method.GetPeerHighWaterMark, new UnaryServerMethod<PeerHighWaterMarkRequestBox, PeerHighWaterMarkResponseBox>(serviceImpl.GetPeerHighWaterMark));
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeReplicationGrpcMethod"/>.
/// Populated by <see cref="LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpc"/>.
/// </summary>
/// <remarks>
/// gRPC's static <c>BindService</c> binding hook cannot accept DI
/// dependencies directly; this holder is the bridge from the DI graph
/// to the static binding callback. Setting it more than once is
/// allowed - subsequent registrations replace the prior method
/// instance, which matches the "last-host-wins" semantics most
/// integration test fixtures rely on when standing up an in-process
/// receiver.
/// </remarks>
internal static class LatticeReplicationGrpcMethodHolder
{
    /// <summary>
    /// The current resolved <see cref="LatticeReplicationGrpcMethod"/>,
    /// or <see langword="null"/> if registration has not yet occurred.
    /// </summary>
    public static LatticeReplicationGrpcMethod? Current { get; set; }
}

/// <summary>
/// Server-side gRPC service that receives <see cref="ReplicationBatchEnvelopeBox"/>
/// payloads pushed by remote <see cref="GrpcPushTransport"/> instances,
/// decodes the contained <see cref="WalRecord"/> records, and routes
/// each entry through the receiver-side <see cref="IReplicationApplier"/>
/// seam. Returns a single <see cref="ReplicationAck"/> per batch whose
/// <see cref="ReplicationAck.HighestAppliedHlc"/> is the maximum
/// high-water-mark advanced by any apply call.
/// </summary>
internal sealed class LatticeReplicationGrpcService : LatticeReplicationGrpcServiceBase
{
    /// <summary>
    /// Bounded backoff (milliseconds) stamped onto a not-accepted ack when the
    /// applier defers a batch behind the durable inbound receive fence (issue
    /// #1173). Backs the sender off the fenced tree while the fence is held
    /// instead of hot-looping the same rejected batch, while staying small
    /// enough that delivery resumes promptly once the fence lifts.
    /// </summary>
    private const int ReceiveFenceDeferPauseMs = 500;

    private readonly IReplicationApplier _applier;
    private readonly IWalCursorRegistry _cursorRegistry;
    private readonly IReceiverFlowControlPolicy _flowControlPolicy;
    private readonly IGrainFactory _grainFactory;
    private readonly ReceiverAppliedContentIndex _appliedContentIndex;
    private readonly ILogger<LatticeReplicationGrpcService> _logger;
    private readonly ILatticeCompressionDictionaryProvider? _dictionaryProvider;

    /// <summary>
    /// Initialises the service with its dependencies. The
    /// <paramref name="method"/> parameter is unused inside the
    /// service body but its presence on the constructor is
    /// load-bearing: it forces the DI container to resolve the
    /// <see cref="LatticeReplicationGrpcMethod"/> singleton (whose
    /// factory populates
    /// <see cref="LatticeReplicationGrpcMethodHolder.Current"/>)
    /// before this service resolves, so the static
    /// <see cref="LatticeReplicationGrpcServiceBase.BindService"/>
    /// hook always observes a populated holder. The
    /// <paramref name="grainFactory"/> resolves the local
    /// <see cref="ILattice"/> grain when answering an inbound digest
    /// probe and the per-tree high-water-mark grain when answering an
    /// inbound content-manifest exchange. The
    /// <paramref name="appliedContentIndex"/> answers the
    /// "which hashes do I already hold?" lookup the content-manifest
    /// exchange depends on. The optional <paramref name="dictionaryProvider"/>
    /// lets the receiver advertise which shared compression dictionaries it
    /// can resolve (when the provider implements
    /// <see cref="ILatticeCompressionDictionaryCatalog"/>) so an opted-in
    /// sender only compresses with a dictionary this peer can decode; the
    /// default registration is the always-resolvable empty operator-supplied
    /// provider, which advertises no dictionaries.
    /// </summary>
    public LatticeReplicationGrpcService(
        LatticeReplicationGrpcMethod method,
        IReplicationApplier applier,
        IWalCursorRegistry cursorRegistry,
        IReceiverFlowControlPolicy flowControlPolicy,
        IGrainFactory grainFactory,
        ReceiverAppliedContentIndex appliedContentIndex,
        ILogger<LatticeReplicationGrpcService> logger,
        ILatticeCompressionDictionaryProvider? dictionaryProvider = null)
    {
        ArgumentNullException.ThrowIfNull(method);
        ArgumentNullException.ThrowIfNull(applier);
        ArgumentNullException.ThrowIfNull(cursorRegistry);
        ArgumentNullException.ThrowIfNull(flowControlPolicy);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(appliedContentIndex);
        ArgumentNullException.ThrowIfNull(logger);

        _applier = applier;
        _cursorRegistry = cursorRegistry;
        _flowControlPolicy = flowControlPolicy;
        _grainFactory = grainFactory;
        _appliedContentIndex = appliedContentIndex;
        _logger = logger;
        _dictionaryProvider = dictionaryProvider;
    }

    /// <inheritdoc />
    public override async Task<ReplicationAckBox> Push(ReplicationBatchEnvelopeBox requestBox, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestBox);
        ArgumentNullException.ThrowIfNull(context);

        var request = requestBox.Value;

        if (string.IsNullOrEmpty(request.TreeName))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "ReplicationBatchEnvelope.TreeName must be non-empty."));
        }

        if (string.IsNullOrEmpty(request.OriginClusterId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "ReplicationBatchEnvelope.OriginClusterId must be non-empty."));
        }

        var entries = request.Entries;

        // Time the apply call so the flow-control policy can shape
        // its hint against the real receiver-side cost of the just-
        // applied batch. Stopwatch.GetTimestamp is allocation-free;
        // Stopwatch.GetElapsedTime materialises a TimeSpan only on
        // the success path where we actually need the ms value.
        ApplyResult result;
        var applyStart = Stopwatch.GetTimestamp();
        try
        {
            result = await _applier.ApplyBatchAsync(entries, context.CancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Best-effort context for the failure: surface the tree id
            // and batch size in the structured log so operators can
            // correlate the gRPC exception to a specific inbound push.
            // Per-entry detail is owned by the applier (it logs / parks
            // / records-metrics inside the batch loop), so we do not
            // re-inflate it here.
            _logger.LogError(ex,
                "Replication apply failed for tree {Tree} on a {EntryCount}-entry batch from origin {Origin}.",
                request.TreeName, entries.Count, request.OriginClusterId);

            throw new RpcException(
                new Status(StatusCode.Internal,
                    $"Replication apply failed on tree '{request.TreeName}' "
                    + $"({entries.Count} entries from origin '{request.OriginClusterId}'); "
                    + "see server logs for the underlying exception."),
                ex.Message);
        }

        var applyDurationMs = Stopwatch.GetElapsedTime(applyStart).TotalMilliseconds;

        // Stamp the receiver-side blocked-floor pin (the lowest
        // staged HLC across every partially-buffered atomic batch on
        // this tree) onto the ack so the producer-side WAL GC AND-s
        // a strict-less entry.Timestamp < blockedFloor clause into
        // its trim predicate. Failure is swallowed: the receiver
        // already applied / buffered the batch, the WAL still holds
        // the canonical mutation, and a subsequent batch's ack will
        // re-stamp the pin. Surfacing a registry-side exception out
        // of a successful apply path would convert a diagnostic
        // outage into a transport failure, which is the wrong
        // trade-off.
        HybridLogicalClock? blockedAtHlc = null;
        try
        {
            blockedAtHlc = await _cursorRegistry
                .GetBlockedFloorAsync(request.TreeName, context.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Reading receiver-side blocked-floor pin failed for tree {Tree}; ack will omit the slot.",
                request.TreeName);
        }

        // Evaluate the receiver-side flow-control policy and stamp
        // any returned hint onto the ack. Failure is swallowed: a
        // policy outage must not unwind the successful apply, and a
        // subsequent push's ack will re-evaluate the policy. The
        // default registration is NoOpReceiverFlowControlPolicy,
        // which always returns ReceiverFlowControlHint.None - so the
        // ack carries SuggestedBatchSize = null / PauseForMs = null
        // and the sender resumes at its configured ShipBatchSize on
        // the next pump tick (the canonical re-acceleration shape).
        var hint = ReceiverFlowControlHint.None;
        try
        {
            hint = await _flowControlPolicy.EvaluateAsync(
                new ReceiverFlowControlContext
                {
                    TreeName = request.TreeName,
                    OriginClusterId = request.OriginClusterId,
                    EntryCount = entries.Count,
                    ApplyDurationMs = applyDurationMs,
                },
                context.CancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Receiver flow-control policy threw for tree {Tree}; ack will omit hint slots.",
                request.TreeName);
        }

        // Advertise the shared compression dictionaries this receiver can
        // resolve, so a sender that has opted into shared-dictionary
        // negotiation only compresses a batch with a dictionary id this
        // peer can decode. Null when the provider exposes no catalog (a
        // build predating dictionary negotiation, or a provider without a
        // catalog) or holds no dictionaries; otherwise a sorted snapshot of
        // the registered ids for a deterministic advertisement order.
        uint[]? advertisedDictionaryIds = null;
        if (_dictionaryProvider is ILatticeCompressionDictionaryCatalog catalog)
        {
            var ids = catalog.AvailableDictionaryIds;
            if (ids.Count > 0)
            {
                var snapshot = new uint[ids.Count];
                var i = 0;
                foreach (var id in ids)
                {
                    snapshot[i++] = id;
                }
                Array.Sort(snapshot);
                advertisedDictionaryIds = snapshot;
            }
        }

        // DURABLE RECEIVE FENCE (issue #1173). When the applier deferred the
        // batch because this tree's inbound receive fence is engaged by an
        // in-flight restore saga, the entries were NOT applied. Return a
        // not-accepted ack so the sender takes its transient-retry path: it
        // keeps its per-peer cursor (does not advance past the deferred
        // entries) and re-ships the same batch after a backoff, so the entries
        // are delivered once the fence lifts on global completion. A modest,
        // bounded PauseForMs backs the sender off the fenced tree instead of
        // hot-looping while the fence is held. Every non-deferred result (apply,
        // dedup, local-origin rejection) keeps Accepted = true so the sender
        // makes normal cursor progress.
        if (result.Deferred)
        {
            return new ReplicationAckBox
            {
                Value = new ReplicationAck
                {
                    Accepted = false,
                    HighestAppliedHlc = result.HighWaterMark,
                    BlockedAtHlc = blockedAtHlc,
                    PauseForMs = ReceiveFenceDeferPauseMs,
                    SupportedWireVersion = EncodedBatchHeader.CurrentWireVersion,
                    AdvertisedDictionaryIds = advertisedDictionaryIds,
                    AdvertisedDictionaries = CompressionDictionaryAdvertisement.Build(_dictionaryProvider),
                },
            };
        }

        return new ReplicationAckBox
        {
            Value = new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = result.HighWaterMark,
                BlockedAtHlc = blockedAtHlc,
                SuggestedBatchSize = hint.SuggestedBatchSize,
                PauseForMs = hint.PauseForMs,
                // Advertise the maximum framing wire version this
                // receiver can decode so a sender that has opted into
                // wire-version negotiation can observe this peer's
                // capability (for its negotiation telemetry and the
                // minimum-floor guard).
                SupportedWireVersion = EncodedBatchHeader.CurrentWireVersion,
                AdvertisedDictionaryIds = advertisedDictionaryIds,
                // Advertise the same dictionaries carrying a content
                // fingerprint per id so a sender that has opted into
                // fingerprint-gated negotiation only compresses with a
                // dictionary whose bytes byte-match on both sides; a
                // same-id/different-bytes peer falls back to dictionary-less
                // compression instead of hard-failing decode. Null on the
                // same conditions as the id-only slot above (no catalog or no
                // dictionaries), so an older sender keeps negotiating on the
                // id-only slot.
                AdvertisedDictionaries = CompressionDictionaryAdvertisement.Build(_dictionaryProvider),
            },
        };
    }

    /// <inheritdoc />
    public override async Task<DigestProbeResponseBox> ProbeDigest(DigestProbeRequestBox requestBox, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestBox);
        ArgumentNullException.ThrowIfNull(context);

        var request = requestBox.Value;

        if (string.IsNullOrEmpty(request.TreeName))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "DigestProbeRequest.TreeName must be non-empty."));
        }

        var lattice = _grainFactory.GetGrain<ILattice>(request.TreeName);
        try
        {
            var digest = await lattice
                .GetLeafProjectionDigestAsync(request.ShardIndex, context.CancellationToken)
                .ConfigureAwait(false);

            return new DigestProbeResponseBox
            {
                Value = new DigestProbeResponse
                {
                    DigestAvailable = true,
                    Digest = digest,
                },
            };
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (InvalidOperationException)
        {
            // Projection-digest maintenance is disabled (or latched off)
            // for this tree locally. Report the digest as unavailable so
            // the probing peer records a non-comparable outcome rather
            // than treating the absence as a failure.
            return new DigestProbeResponseBox
            {
                Value = new DigestProbeResponse { DigestAvailable = false },
            };
        }
    }

    /// <inheritdoc />
    public override async Task<ContentManifestResponseBox> ExchangeContentManifest(
        ContentManifestRequestBox requestBox,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestBox);
        ArgumentNullException.ThrowIfNull(context);

        var request = requestBox.Value;

        if (string.IsNullOrEmpty(request.TreeName))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "ContentManifestRequest.TreeName must be non-empty."));
        }

        if (string.IsNullOrEmpty(request.OriginClusterId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "ContentManifestRequest.OriginClusterId must be non-empty."));
        }

        var entries = request.Entries ?? (IReadOnlyList<ContentManifestEntry>)Array.Empty<ContentManifestEntry>();

        // Resolve the durable per-origin high-water-mark so the
        // identical-content-newer-clock decision is taken against the
        // receiver's authoritative recorded clock rather than the
        // best-effort applied-content index. The index answers only
        // "do I hold byte-identical content for this key?"; the clock
        // comparison that drives the metadata-only advance is anchored
        // on the high-water-mark grain.
        var hwmGrain = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(request.TreeName);
        var hwm = await hwmGrain
            .GetAsync(request.OriginClusterId, context.CancellationToken)
            .ConfigureAwait(false);

        // Project the applied-content index onto the manifest's keys. A
        // key absent from the index (cold / never-applied / evicted) is
        // simply omitted, so the planner reports it as missing and the
        // sender ships it - always safe. The held clock is stamped at
        // the durable high-water-mark so the planner's advance is the
        // max manifest clock strictly newer than the recorded
        // high-water-mark among content-matching entries.
        Dictionary<string, (ulong ContentHash, HybridLogicalClock Hlc)>? held = null;
        for (var i = 0; i < entries.Count; i++)
        {
            var key = entries[i].Key ?? string.Empty;
            if (_appliedContentIndex.TryGetContentHash(request.TreeName, key, out var contentHash))
            {
                (held ??= new Dictionary<string, (ulong, HybridLogicalClock)>(StringComparer.Ordinal))[key] =
                    (contentHash, hwm);
            }
        }

        var response = ContentManifestPlanner.ComputeMissingSet(
            in request,
            held ?? (IReadOnlyDictionary<string, (ulong, HybridLogicalClock)>)EmptyHeld);

        // Durably advance the per-origin high-water-mark for the
        // identical-content entries the receiver elided whose clock was
        // newer than its recorded clock (the idempotent re-set). The
        // advance is metadata-only - no payload travelled - and is
        // strictly-greater-only inside the grain, so re-running the
        // exchange is idempotent. Surface the candidate clock on the
        // response so the sender can observe the advance.
        var advanced = false;
        if (response.AdvancedHlc != HybridLogicalClock.Zero)
        {
            advanced = await hwmGrain
                .TryAdvanceAsync(request.OriginClusterId, response.AdvancedHlc, context.CancellationToken)
                .ConfigureAwait(false);
        }

        var missingCount = response.MissingEntryIndices?.Count ?? 0;
        var elidedCount = entries.Count - missingCount;

        var treeTag = new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, request.TreeName);
        var peerTag = new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, request.OriginClusterId);
        LatticeReplicationMetrics.ReceiverContentManifestExchanges.Add(1, treeTag, peerTag);
        if (elidedCount > 0)
        {
            LatticeReplicationMetrics.ReceiverContentEntriesElided.Add(elidedCount, treeTag, peerTag);
        }
        if (advanced)
        {
            LatticeReplicationMetrics.ReceiverContentHwmAdvances.Add(1, treeTag, peerTag);
        }

        return new ContentManifestResponseBox { Value = response };
    }

    /// <inheritdoc />
    public override Task<CompressionDictionaryPullResponseBox> PullCompressionDictionary(
        CompressionDictionaryPullRequestBox requestBox,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestBox);
        ArgumentNullException.ThrowIfNull(context);

        var dictionaryId = requestBox.Value.DictionaryId;

        // The reserved id 0 ("no dictionary") is never served, and a
        // receiver with no provider (or one that cannot resolve the id)
        // answers "supported but not held" so the puller stops asking this
        // hop for this tick rather than treating the peer as un-upgraded.
        if (dictionaryId != 0u
            && _dictionaryProvider is { } provider
            && provider.TryGetDictionary(dictionaryId, out var bytes))
        {
            var response = new CompressionDictionaryPullResponse
            {
                ExchangeSupported = true,
                Found = true,
                DictionaryId = dictionaryId,
                Fingerprint = CompressionDictionaryFingerprint.Compute(bytes.Span),
                Dictionary = bytes,
            };
            return Task.FromResult(new CompressionDictionaryPullResponseBox { Value = response });
        }

        return Task.FromResult(new CompressionDictionaryPullResponseBox
        {
            Value = CompressionDictionaryPullResponse.NotHeld,
        });
    }

    /// <inheritdoc />
    public override async Task<MerkleWalkProbeResponseBox> ProbeMerkleWalk(
        MerkleWalkProbeRequestBox requestBox,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestBox);
        ArgumentNullException.ThrowIfNull(context);

        var request = requestBox.Value;

        if (string.IsNullOrEmpty(request.TreeName))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "MerkleWalkProbeRequest.TreeName must be non-empty."));
        }

        var lattice = _grainFactory.GetGrain<ILattice>(request.TreeName);
        try
        {
            var digest = await lattice
                .GetLeafProjectionDigestForRangeAsync(
                    request.ShardIndex,
                    request.RangeStartKey,
                    request.RangeEndKey,
                    context.CancellationToken)
                .ConfigureAwait(false);

            return new MerkleWalkProbeResponseBox
            {
                Value = new MerkleWalkProbeResponse
                {
                    Available = true,
                    Digest = digest,
                },
            };
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (InvalidOperationException)
        {
            // Projection-digest maintenance is disabled (or latched off)
            // for this tree locally. Report the range digest as unavailable
            // so the walking peer aborts cleanly rather than treating the
            // absence as a hard failure.
            return new MerkleWalkProbeResponseBox { Value = MerkleWalkProbeResponse.Unavailable };
        }
        catch (ArgumentOutOfRangeException)
        {
            // The shard index does not exist on this peer (divergent shard
            // map). Report unavailable rather than faulting the walk.
            return new MerkleWalkProbeResponseBox { Value = MerkleWalkProbeResponse.Unavailable };
        }
    }

    /// <inheritdoc />
    public override async Task<PeerHighWaterMarkResponseBox> GetPeerHighWaterMark(
        PeerHighWaterMarkRequestBox requestBox,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestBox);
        ArgumentNullException.ThrowIfNull(context);

        var request = requestBox.Value;

        if (string.IsNullOrEmpty(request.TreeName))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "PeerHighWaterMarkRequest.TreeName must be non-empty."));
        }

        if (string.IsNullOrEmpty(request.OriginClusterId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "PeerHighWaterMarkRequest.OriginClusterId must be non-empty."));
        }

        // Resolve the receiver's durable per-origin high-water-mark for the
        // (tree, origin) stream. An origin the receiver has never applied
        // returns HybridLogicalClock.Zero from the grain, which the walking
        // peer treats as "re-ship the whole in-range retained set".
        var hwmGrain = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(request.TreeName);
        var clock = await hwmGrain
            .GetAsync(request.OriginClusterId, context.CancellationToken)
            .ConfigureAwait(false);

        return new PeerHighWaterMarkResponseBox
        {
            Value = new PeerHighWaterMarkResponse { Clock = clock },
        };
    }

    /// <summary>
    /// Shared empty held-content view for an exchange whose manifest
    /// keys are all absent from the applied-content index. Avoids
    /// allocating a per-call empty dictionary on the cold-index path.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, (ulong ContentHash, HybridLogicalClock Hlc)> EmptyHeld =
        new Dictionary<string, (ulong, HybridLogicalClock)>(StringComparer.Ordinal);
}

