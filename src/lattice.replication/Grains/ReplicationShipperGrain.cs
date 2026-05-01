using System.Buffers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Default <see cref="IReplicationShipperGrain"/> implementation.
/// Hosts the per-<c>(tree, peer)</c> outbound ship loop using the
/// shared <see cref="CoordinatorGrain{TSelf}"/> reminder + phase-timer
/// scaffold.
/// </summary>
internal sealed class ReplicationShipperGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    ILogger<ReplicationShipperGrain> logger,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    IChangeFeed changeFeed,
    IReplicationTransport transport,
    IReplicationBatchEncoder encoder,
    ILatticeReplicationCursorRegistry cursorRegistry,
    IGrainFactory grainFactory,
    [PersistentState("replication-shipper", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationShipperState> state)
    : CoordinatorGrain<ReplicationShipperGrain>(context, reminderRegistry, logger),
      IReplicationShipperGrain
{
    private readonly IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor =
        optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly IChangeFeed _changeFeed =
        changeFeed ?? throw new ArgumentNullException(nameof(changeFeed));
    private readonly IReplicationTransport _transport =
        transport ?? throw new ArgumentNullException(nameof(transport));
    private readonly IReplicationBatchEncoder _encoder =
        encoder ?? throw new ArgumentNullException(nameof(encoder));
    private readonly ILatticeReplicationCursorRegistry _cursorRegistry =
        cursorRegistry ?? throw new ArgumentNullException(nameof(cursorRegistry));
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    private string _treeName = "";
    private string _peerClusterId = "";
    private bool _keyParsed;

    /// <summary>
    /// Wall-clock instant at or after which the next phase tick is
    /// allowed to attempt a send. Set to a future value on transient
    /// transport failure to apply backoff. <see cref="DateTime.MinValue"/>
    /// (the default) means "no backoff in effect".
    /// </summary>
    private DateTime _nextRetryAtUtc = DateTime.MinValue;

    /// <summary>Re-entrancy guard. Orleans serialises grain turns, so the field is for clarity rather than concurrency.</summary>
    private bool _pumpInFlight;

    /// <summary>
    /// Random source for backoff jitter. Aliased to the process-wide
    /// thread-safe singleton (<see cref="Random.Shared"/>) — shared
    /// across every shipper activation on this silo. Sufficient for
    /// jitter purposes; not cryptographically random.
    /// </summary>
    private readonly Random _jitterRandom = Random.Shared;

    /// <summary>
    /// Activation-scoped drain buffer reused across pump ticks. Cleared
    /// at the start of every <see cref="PumpOnceAsync"/>; the encoder
    /// consumes the list synchronously inside <see cref="IReplicationBatchEncoder.Encode"/>
    /// so reuse is safe (no aliasing past the call). Bounded in size by
    /// <see cref="LatticeReplicationOptions.ShipBatchSize"/>.
    /// </summary>
    private readonly List<ReplogEntry> _drainBuffer = new();

    /// <summary>
    /// Activation-scoped framing buffer reused across pump ticks. Reset
    /// via <c>ResetWrittenCount</c> when the previous batch fits within
    /// the soft budget (<see cref="LargeWriteBufferThreshold"/>); a
    /// one-time spike past the budget recreates the writer so a single
    /// outlier batch does not pin a large array on the heap forever.
    /// </summary>
    private ArrayBufferWriter<byte> _writeBuffer = new();

    /// <summary>
    /// 4 MB soft cap above which the framing buffer is recreated rather
    /// than reset. Sized to match the WAL's per-batch byte budget so
    /// the typical steady-state path always reuses the buffer.
    /// </summary>
    private const int LargeWriteBufferThreshold = 4 * 1024 * 1024;

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "shipper-keepalive";

    /// <inheritdoc />
    protected override TimeSpan KeepaliveReminderPeriod => TimeSpan.FromSeconds(90);

    /// <inheritdoc />
    protected override TimeSpan PhaseTimerPeriod => TimeSpan.FromMilliseconds(200);

    /// <inheritdoc />
    protected override bool InProgress => true; // The shipper is steady-state — always running.

    /// <inheritdoc />
    protected override string LogContext => $"shipper {_treeName}/{_peerClusterId}";

    /// <inheritdoc />
    public async Task EnsureActiveAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ParseGrainKey();
        // RegisterOrUpdateReminder is idempotent; StartPhaseTimer's
        // _phaseTimer ??= guard makes the second call a no-op. Safe
        // for repeated invocation.
        await StartCoordinatorAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    public Task OnDoorbellAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ParseGrainKey();
        // Drive an immediate pump on the same Orleans turn rather
        // than waiting for the next 200ms timer tick. Best-effort:
        // any thrown exception is logged by the base class's tick
        // handler and does not propagate to the doorbell caller
        // (the producer-side commit path).
        return ProcessNextPhaseAsync();
    }

    /// <inheritdoc />
    protected internal override async Task ProcessNextPhaseAsync()
    {
        ParseGrainKey();

        // Honour the backoff budget set by the previous failed pump.
        if (_nextRetryAtUtc > DateTime.UtcNow)
        {
            return;
        }

        if (_pumpInFlight)
        {
            return;
        }

        _pumpInFlight = true;
        try
        {
            await PumpOnceAsync(CancellationToken.None).ConfigureAwait(true);
        }
        finally
        {
            _pumpInFlight = false;
        }
    }

    /// <summary>
    /// Drains one batch from the change feed, applies producer-side
    /// filters, calls the transport, advances the cursor on positive
    /// ack, and applies backoff on transient failure. Schema-shaped
    /// failures during encode park every offending entry on the
    /// per-tree dead-letter queue (reason
    /// <see cref="LatticeReplicationMetrics.ReasonSchema"/>) and then
    /// advance the cursor past the batch so a single poison entry
    /// never stalls the stream forever; operators inspect / replay /
    /// discard via <see cref="ILatticeReplicationDeadLetters"/>.
    /// </summary>
    private async Task PumpOnceAsync(CancellationToken cancellationToken)
    {
        var options = _optionsMonitor.Get(_treeName);

        // Drain a batch up to ShipBatchSize entries past the cursor,
        // applying KeyFilter / KeyPrefixes / cycle-break inline. The
        // drain buffer is activation-scoped and reused across pump
        // ticks; Orleans serialises grain turns and the _pumpInFlight
        // guard prevents re-entry, so clearing in place is safe.
        _drainBuffer.Clear();
        var maxPerBatch = Math.Max(1, options.ShipBatchSize);
        try
        {
            await foreach (var entry in _changeFeed
                .Subscribe(_treeName, state.State.Cursor, includeLocalOrigin: true, cancellationToken)
                .ConfigureAwait(true))
            {
                if (!ShouldShip(entry, options))
                {
                    continue;
                }

                _drainBuffer.Add(entry);
                if (_drainBuffer.Count >= maxPerBatch)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Drain failure is transient by definition — back off and retry.
            ApplyBackoff(options, ex, "drain");
            return;
        }

        if (_drainBuffer.Count == 0)
        {
            // No work this tick; preserve any accumulated backoff.
            return;
        }

        var sourceHlc = _drainBuffer[^1].Timestamp;

        // Encode the batch into a buffer; the gRPC transport hands
        // the gRPC stream's IBufferWriter through directly so the
        // encoded bytes never round-trip through a managed array.
        // The opaque-bytes-shaped IReplicationTransport seam still
        // requires us to land bytes into a buffer here (the typed-envelope transport widens
        // the seam to remove this round-trip).
        //
        // The framing buffer is activation-scoped and reused across
        // ticks: in the steady state we ResetWrittenCount() (which
        // keeps the underlying array, just rewinds the write index),
        // and a one-time spike at-or-past the soft cap recreates the
        // writer so a single outlier batch does not pin a multi-MB
        // array on the heap forever. The encoder consumes _drainBuffer
        // synchronously inside Encode, so reuse is safe (no aliasing
        // past the call).
        if (_writeBuffer.Capacity >= LargeWriteBufferThreshold)
        {
            _writeBuffer = new ArrayBufferWriter<byte>();
        }
        else
        {
            _writeBuffer.ResetWrittenCount();
        }
        try
        {
            var envelope = new ReplicationBatchEnvelope
            {
                WireVersion = _encoder.CurrentWireVersion,
                TreeName = _treeName,
                OriginClusterId = options.ClusterId,
                Entries = _drainBuffer,
            };
            _encoder.Encode(envelope, _writeBuffer);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            // Schema-shaped failure during encode: the entries can
            // never be shipped in their current form. Park every
            // entry in the offending batch on the per-tree DLQ
            // tagged ReasonSchema and advance the cursor past the
            // batch so the stream makes progress. Operators inspect
            // / replay / discard via ILatticeReplicationDeadLetters.
            Logger.LogWarning(ex,
                "Encode failed for {EntryCount}-entry batch on {Context}; routing to DLQ and advancing cursor to {Hlc}",
                _drainBuffer.Count, LogContext, sourceHlc);
            await RouteBatchToDeadLetterAsync(ex, cancellationToken).ConfigureAwait(true);
            await AdvanceCursorAsync(sourceHlc, options, cancellationToken).ConfigureAwait(true);
            return;
        }

        ReplicationAck ack;
        try
        {
            var batch = new ReplicationBatch
            {
                TargetClusterId = _peerClusterId,
                TreeName = _treeName,
                OriginClusterId = options.ClusterId,
                Payload = _writeBuffer.WrittenMemory,
            };
            ack = await _transport.SendAsync(batch, cancellationToken).ConfigureAwait(true);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ApplyBackoff(options, ex, "transport");
            return;
        }

        if (!ack.Accepted)
        {
            // Receiver rejected the batch; treat as transient (the
            // sender's cursor stays put and we retry after backoff).
            ApplyBackoff(options, exception: null, reason: "ack-rejected");
            return;
        }

        // Pick the higher of (last entry HLC, ack HighestAppliedHlc).
        // A receiver that fully applied the batch returns the highest
        // entry HLC; a receiver that partially applied returns the
        // partial frontier. Either way we trust the ack.
        var advancedTo = ack.HighestAppliedHlc;
        if (sourceHlc.CompareTo(advancedTo) > 0)
        {
            // Receiver returned a lower frontier than the batch's
            // last entry — partial apply or HWM dedupe. Use the
            // ack's frontier so we don't claim more progress than
            // the receiver acknowledged.
            advancedTo = ack.HighestAppliedHlc;
        }
        if (advancedTo <= state.State.Cursor)
        {
            // Receiver acknowledged a frontier at or below ours
            // (e.g. every entry was deduped). Still resets the
            // backoff because the round-trip succeeded; advance the
            // cursor to the last shipped entry's HLC so we don't
            // re-ship the same batch next tick.
            advancedTo = sourceHlc;
        }

        await AdvanceCursorAsync(advancedTo, options, cancellationToken).ConfigureAwait(true);
        // Successful round-trip resets the backoff counter.
        state.State.ConsecutiveFailures = 0;
        _nextRetryAtUtc = DateTime.MinValue;
    }

    /// <summary>
    /// Producer-side filter: applies <see cref="LatticeReplicationOptions.KeyFilter"/> /
    /// <see cref="LatticeReplicationOptions.KeyPrefixes"/> and the
    /// durable origin-based cycle-break (skip entries whose
    /// <see cref="ReplogEntry.OriginClusterId"/> matches the peer's
    /// own cluster id).
    /// </summary>
    private bool ShouldShip(ReplogEntry entry, LatticeReplicationOptions options)
    {
        // Cycle-break: don't ship a peer its own writes back.
        if (entry.OriginClusterId is { } origin
            && string.Equals(origin, _peerClusterId, StringComparison.Ordinal))
        {
            return false;
        }

        if (options.KeyFilter is { } filter)
        {
            if (entry.Key is null || !filter(entry.Key))
            {
                return false;
            }
        }

        if (options.KeyPrefixes is { } prefixes && prefixes.Count > 0)
        {
            if (entry.Key is null)
            {
                return false;
            }
            var matched = false;
            foreach (var prefix in prefixes)
            {
                if (entry.Key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    matched = true;
                    break;
                }
            }
            if (!matched)
            {
                return false;
            }
        }

        return true;
    }

    private async Task AdvanceCursorAsync(
        HybridLogicalClock newCursor,
        LatticeReplicationOptions options,
        CancellationToken cancellationToken)
    {
        if (newCursor.CompareTo(state.State.Cursor) <= 0)
        {
            return;
        }

        state.State.Cursor = newCursor;
        await state.WriteStateAsync().ConfigureAwait(true);

        // Best-effort registry report: the GC consumes this to
        // compute the trim frontier. A registry-side failure does
        // not unwind the durable cursor advance.
        try
        {
            await _cursorRegistry
                .ReportCursorAsync(_treeName, _peerClusterId, newCursor, cancellationToken)
                .ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cursor registry report failed for {Context}; persisted cursor remains {Cursor}",
                LogContext, newCursor);
        }
        _ = options; // Reserved for future per-tree report flavours.
    }

    private void ApplyBackoff(LatticeReplicationOptions options, Exception? exception, string reason)
    {
        state.State.ConsecutiveFailures = checked(state.State.ConsecutiveFailures + 1);
        var failures = state.State.ConsecutiveFailures;
        var initialMs = options.ShipBackoffInitial.TotalMilliseconds;
        var maxMs = options.ShipBackoffMax.TotalMilliseconds;
        var multiplier = Math.Pow(2, Math.Max(0, failures - 1));
        var delayMs = Math.Min(maxMs, initialMs * multiplier);

        var jitter = options.ShipBackoffJitter;
        if (jitter > 0.0)
        {
            // Symmetric: [1 - jitter, 1 + jitter]
            var spread = (_jitterRandom.NextDouble() * 2.0 - 1.0) * jitter;
            delayMs = Math.Max(0.0, delayMs * (1.0 + spread));
        }

        var delay = TimeSpan.FromMilliseconds(delayMs);
        _nextRetryAtUtc = DateTime.UtcNow.Add(delay);
        if (exception is not null)
        {
            Logger.LogWarning(exception,
                "Shipper {Context} {Reason} failed (consecutive={Failures}); backing off {Delay}",
                LogContext, reason, failures, delay);
        }
        else
        {
            Logger.LogDebug(
                "Shipper {Context} {Reason} (consecutive={Failures}); backing off {Delay}",
                LogContext, reason, failures, delay);
        }
    }

    /// <summary>
    /// Routes every entry in the current drain buffer to the per-tree
    /// dead-letter queue, tagged with
    /// <see cref="LatticeReplicationMetrics.ReasonSchema"/>. A
    /// best-effort enqueue failure is logged and swallowed — the
    /// cursor still advances past the batch so a deterministically-
    /// failing DLQ does not pin the ship loop forever; the WAL
    /// retains the originals until the GC pass trims them, so an
    /// operator can still recover off the WAL even when the DLQ is
    /// unavailable.
    /// </summary>
    private async Task RouteBatchToDeadLetterAsync(Exception encodeFailure, CancellationToken cancellationToken)
    {
        var failureReason = encodeFailure.Message ?? "<no message>";
        var dlq = _grainFactory.GetGrain<IReplicationDeadLetterGrain>(_treeName);
        foreach (var entry in _drainBuffer)
        {
            try
            {
                await dlq.EnqueueAsync(
                    entry,
                    failureReason,
                    retryCount: 0,
                    LatticeReplicationMetrics.ReasonSchema,
                    cancellationToken).ConfigureAwait(true);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Logger.LogWarning(ex,
                    "Failed to park entry on DLQ for {Context} (key={Key}, hlc={Hlc}); proceeding with cursor advance",
                    LogContext, entry.Key, entry.Timestamp);
            }
        }
    }

    private void ParseGrainKey()
    {
        if (_keyParsed)
        {
            return;
        }

        var key = Context.GrainId.Key.ToString() ?? "";
        if (string.IsNullOrEmpty(key))
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationShipperGrain)} activation key is empty; expected '{{treeName}}/{{peerClusterId}}'.");
        }

        var slash = key.LastIndexOf('/');
        if (slash <= 0 || slash >= key.Length - 1)
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationShipperGrain)} activation key '{key}' is not in the expected '{{treeName}}/{{peerClusterId}}' format.");
        }

        _treeName = key[..slash];
        _peerClusterId = key[(slash + 1)..];
        _keyParsed = true;
    }

    /// <summary>
    /// Test seam: bypasses Orleans activation and key parsing for
    /// direct unit tests against a fake Orleans context.
    /// </summary>
    internal void InitializeForTesting(string treeName, string peerClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentException.ThrowIfNullOrEmpty(peerClusterId);
        _treeName = treeName;
        _peerClusterId = peerClusterId;
        _keyParsed = true;
    }
}
