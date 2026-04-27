using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Decorator over the canonical <see cref="ReplicationApplier"/> that
/// tracks consecutive failed apply attempts for the same
/// <c>(treeId, originClusterId, timestamp, key, op)</c> tuple in a
/// process-local <see cref="ConcurrentDictionary{TKey, TValue}"/>.
/// When the failure count reaches
/// <see cref="LatticeReplicationOptions.MaxApplyRetries"/> the entry
/// is parked on the per-tree dead-letter queue, the per-origin
/// high-water-mark is advanced past the entry so subsequent apply
/// attempts dedupe it, and a non-applied <see cref="ApplyResult"/> is
/// returned to the caller. A successful apply clears the counter for
/// that tuple so later transient failures get a fresh budget.
/// <para>
/// The retry counter is intentionally in-memory: the decorator is
/// registered as a singleton, so all apply paths share the same
/// counter within a silo. A silo restart resets the counters,
/// effectively giving every entry another <c>MaxApplyRetries</c>
/// attempts after a failover — this is the desired behaviour because
/// silo restart usually correlates with the very transient failure
/// the retry budget is meant to absorb.
/// </para>
/// </summary>
internal sealed class DeadLetterTrackingReplicationApplier(
    IReplicationApplier inner,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options) : IReplicationApplier
{
    private readonly ConcurrentDictionary<RetryKey, int> _failures = new();

    /// <inheritdoc />
    public async Task<ApplyResult> ApplyAsync(ReplogEntry entry, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        ApplyResult result;
        try
        {
            result = await inner.ApplyAsync(entry, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cancellation is not a poison-entry signal — surface it
            // to the caller without touching the failure counter.
            throw;
        }
        catch (Exception ex)
        {
            return await OnFailureAsync(entry, ex, cancellationToken).ConfigureAwait(false);
        }

        // Successful apply (or filtered re-delivery) clears any
        // accumulated failure state for the tuple.
        _failures.TryRemove(KeyFor(entry), out _);
        return result;
    }

    private async Task<ApplyResult> OnFailureAsync(
        ReplogEntry entry,
        Exception failure,
        CancellationToken cancellationToken)
    {
        var key = KeyFor(entry);
        var attempts = _failures.AddOrUpdate(key, 1, static (_, current) => current + 1);

        var max = options.Get(entry.TreeId).MaxApplyRetries;
        if (attempts < max)
        {
            // Below the threshold — surface the original failure to
            // the caller so the transport can apply its own
            // backoff/redelivery policy on top of our local counter.
            throw failure;
        }

        // Threshold reached: park the entry, advance the HWM past it
        // so subsequent re-deliveries from the transport are deduped
        // by the canonical applier, and clear the counter so a future
        // entry against the same tuple gets a fresh budget.
        var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(entry.TreeId);
        var reasonTag = ClassifyFailure(failure);
        await dlq.EnqueueAsync(entry, failure.Message ?? "<no message>", attempts, reasonTag, cancellationToken).ConfigureAwait(false);

        // Advance HWM only for point-applied entries; range deletes do
        // not consult the HWM (see ReplicationApplier) so advancing it
        // would be misleading. Local-origin entries cannot reach this
        // path because the canonical applier returns Applied=false
        // synchronously without throwing.
        if (entry.Op != ReplogOp.DeleteRange)
        {
            var hwm = grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(
                entry.TreeId + "/" + entry.OriginClusterId);
            await hwm.TryAdvanceAsync(entry.Timestamp, cancellationToken).ConfigureAwait(false);
        }

        _failures.TryRemove(key, out _);
        return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
    }

    private static RetryKey KeyFor(ReplogEntry entry) =>
        new(
            entry.TreeId ?? string.Empty,
            entry.OriginClusterId ?? string.Empty,
            entry.Timestamp,
            entry.Key ?? string.Empty,
            entry.Op);

    /// <summary>
    /// Composite key identifying a single replicated entry across retry
    /// attempts. Equality is structural so the dictionary collapses
    /// repeated apply attempts of the same logical entry onto the same
    /// counter.
    /// </summary>
    private readonly record struct RetryKey(
        string TreeId,
        string OriginClusterId,
        HybridLogicalClock Timestamp,
        string Key,
        ReplogOp Op);

    /// <summary>
    /// Classifies the terminal apply failure into a stable
    /// <c>reason</c> tag value for the
    /// <c>orleans.lattice.replication.dead_letter.enqueued</c>
    /// counter. The mapping is intentionally conservative: only
    /// failure shapes the canonical <see cref="ReplicationApplier"/>
    /// (or another decorator under our control) is known to emit are
    /// matched explicitly; everything else lands on
    /// <see cref="LatticeReplicationMetrics.ReasonUnknown"/> rather
    /// than guessing from message-text patterns.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    ///   <item>
    ///     <see cref="ArgumentException"/> — surfaced by
    ///     <see cref="ReplicationApplier"/> for malformed entries
    ///     (null <see cref="ReplogEntry.Value"/> on a
    ///     <see cref="ReplogOp.Set"/>, missing <see cref="ReplogEntry.EndExclusiveKey"/>,
    ///     empty required fields). Tagged
    ///     <see cref="LatticeReplicationMetrics.ReasonSchema"/>.
    ///   </item>
    ///   <item>
    ///     <see cref="InvalidOperationException"/> — surfaced for
    ///     unrecognised <see cref="ReplicationMode"/> dispatch and
    ///     CAS-budget exhaustion on state-merge applies. Both are
    ///     payload-shape faults from the receiver's perspective and
    ///     are tagged
    ///     <see cref="LatticeReplicationMetrics.ReasonSchema"/>.
    ///   </item>
    ///   <item>
    ///     Anything else — tagged
    ///     <see cref="LatticeReplicationMetrics.ReasonUnknown"/>.
    ///     Future iterations may decorate the applier to surface
    ///     <see cref="LatticeReplicationMetrics.ReasonOversized"/> /
    ///     <see cref="LatticeReplicationMetrics.ReasonHlcSkew"/>
    ///     classifications when the size/skew validation seams land.
    ///   </item>
    /// </list>
    /// </remarks>
    private static string ClassifyFailure(Exception failure) => failure switch
    {
        ArgumentException => LatticeReplicationMetrics.ReasonSchema,
        InvalidOperationException => LatticeReplicationMetrics.ReasonSchema,
        _ => LatticeReplicationMetrics.ReasonUnknown,
    };
}

