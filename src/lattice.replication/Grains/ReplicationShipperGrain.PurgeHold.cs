using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Decision-purge holds (issue #4534). When the WAL GC's retention ceiling
/// trims a partition past this shipper's durable read position, it first
/// records a hold for this shipper in the log's <see cref="IWalPurgeHoldGrain"/>,
/// and the transaction registry purges no saga decision while any hold is
/// outstanding: the peer may need a re-seed, and a re-seed can only settle a
/// saga whose decision is still stored. This partial releases the hold.
/// <list type="bullet">
/// <item>Once the shipper has shown it lost nothing - no re-seed is
/// outstanding and its durable position is past every trimmed offset, as when
/// the trimmed records were already in flight - or once a re-seed completes
/// and every partition re-ships from its lowest retained entry.</item>
/// <item>When the peer is removed from the topology (<see cref="DetachFromLogAsync"/>):
/// a removed peer must not hold the log or the tree's decisions indefinitely.</item>
/// </list>
/// The release is checked on the phase timer at most every
/// <see cref="PurgeHoldCheckInterval"/>, and is conditional in the hold grain,
/// so a hold a concurrent trim widened is never released by an older read.
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    /// <summary>
    /// How often the phase timer checks whether this shipper's purge hold can
    /// be released. Thirty seconds; settable so in-process tests need not wait.
    /// </summary>
    internal static TimeSpan PurgeHoldCheckInterval { get; set; } = TimeSpan.FromSeconds(30);

    private DateTimeOffset _nextPurgeHoldCheckUtc = DateTimeOffset.MinValue;

    /// <summary><see langword="true"/> once the peer was removed from the topology and this shipper stopped holding the log.</summary>
    internal bool DetachedFromLog => state.State.DetachedFromLog;

    /// <inheritdoc />
    public async Task DetachFromLogAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ParseGrainKey();
        PrepareOutsidePump();
        if (!state.State.DetachedFromLog)
        {
            // Durable first: from here this shipper answers no read position, so
            // no later GC pass waits for it or records a hold for it. The GC can
            // then trim a prepare the shipper has not read without a hold, so
            // the same write takes the peer off the log: every saga record is
            // withheld until it is re-attached and re-seeded.
            state.State.DetachedFromLog = true;
            if (ReseedRequired)
            {
                await state.WriteStateAsync();
            }
            else
            {
                await TakePeerOffLogAsync();
            }
        }

        var log = _registeredReadLog
            ?? (string.IsNullOrEmpty(state.State.BoundPhysicalTreeId) ? _walTreeId : state.State.BoundPhysicalTreeId);
        _registeredReadLog = null;
        try
        {
            await _grainFactory.GetGrain<IWalOffsetConsumerRegistryGrain>(log).UnregisterAsync(Context.GrainId);
        }
        catch (Exception ex)
        {
            // Harmless to leave: a detached shipper answers no read position.
            Logger.LogWarning(ex,
                "{Context}: withdrawing from the offset consumers of log {Log} after the peer's removal failed; "
                + "the shipper no longer holds that log.",
                LogContext, log);
        }

        await ReleasePurgeHoldAsync(log, positions: null);

        // The replay hold too: a removed peer must not hold the tree's decision
        // purges. Re-attaching takes it again and re-marks the re-seed (#4533).
        if (state.State.ReplayHoldLog is { } replayLog && await RemoveReplayHoldAsync(replayLog))
        {
            state.State.ReplayHoldLog = null;
            await state.WriteStateAsync();
        }

        Logger.LogInformation(
            "{Context}: the peer was removed from the replication topology; the shipper no longer holds log {Log} "
            + "or the tree's saga decisions, ships only plain writes, and withholds saga records until the peer "
            + "returns and is re-seeded.",
            LogContext, log);
    }

    /// <summary>
    /// Re-attaches a shipper whose peer was added back, so its next read
    /// registers with the log again. Called from <see cref="EnsureActiveAsync"/>.
    /// </summary>
    private async Task ReattachToLogAsync()
    {
        if (!state.State.DetachedFromLog)
        {
            return;
        }

        // An export taken while detached predates the replay hold, so the peer
        // must be re-seeded from one taken after it: take the hold again and
        // re-mark at the current export epoch, in the same write (#4533).
        PrepareOutsidePump();
        await TakePeerOffLogStateAsync();
        state.State.DetachedFromLog = false;
        await state.WriteStateAsync();
        ReportReseedState();
        _nextPurgeHoldCheckUtc = DateTimeOffset.MinValue;
    }

    /// <summary>
    /// Releases this shipper's purge hold when it is covered, at most every
    /// <see cref="PurgeHoldCheckInterval"/> unless <paramref name="force"/>.
    /// A detached shipper releases unconditionally (a GC pass that read its
    /// position before it detached may still have recorded one). Never throws.
    /// </summary>
    private async Task MaybeReleasePurgeHoldAsync(bool force)
    {
        var now = _cursorFlushClock.GetUtcNow();
        if (!force && now < _nextPurgeHoldCheckUtc)
        {
            return;
        }

        _nextPurgeHoldCheckUtc = now + PurgeHoldCheckInterval;
        if (state.State.DetachedFromLog)
        {
            var log = string.IsNullOrEmpty(state.State.BoundPhysicalTreeId) ? _walTreeId : state.State.BoundPhysicalTreeId;
            await ReleasePurgeHoldAsync(log, positions: null);
            if (state.State.ReplayHoldLog is { } replayLog && await RemoveReplayHoldAsync(replayLog))
            {
                state.State.ReplayHoldLog = null;
                await state.WriteStateAsync();
            }

            return;
        }

        // A shipper awaiting a re-seed lost records; only the re-seed releases it.
        // Before this activation's first read there are no published positions
        // for a registered log yet; the next tick has them.
        if (ReseedRequired
            || _registeredReadLog is not { } registered
            || _readPositions is not { } published
            || (published.WalTreeId is { } bound && !string.Equals(bound, registered, StringComparison.Ordinal)))
        {
            return;
        }

        await ReleasePurgeHoldAsync(registered, published.Positions);
    }

    /// <summary>
    /// Sizes the partition scratch and binds the log for a call that takes the
    /// peer off the log outside the pump (a detach or a re-attach), whose first
    /// tick may not have run on this activation yet.
    /// </summary>
    private void PrepareOutsidePump()
    {
        EnsureScratchSized(Math.Max(1, _optionsMonitor.Get(_treeName).ReplogPartitions));
        if (!_sourceIdentityResolved && !string.IsNullOrEmpty(state.State.BoundPhysicalTreeId))
        {
            _walTreeId = state.State.BoundPhysicalTreeId;
        }
    }

    private async Task ReleasePurgeHoldAsync(string log, long[]? positions)
    {
        try
        {
            var released = await _grainFactory.GetGrain<IWalPurgeHoldGrain>(log)
                .ReleaseIfCoveredAsync(Context.GrainId.ToString(), positions);
            if (released)
            {
                Logger.LogInformation(
                    "{Context}: released the saga decision-purge hold on log {Log}.",
                    LogContext, log);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "{Context}: releasing the saga decision-purge hold on log {Log} failed; the next check retries.",
                LogContext, log);
        }
    }
}
