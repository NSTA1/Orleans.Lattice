using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Implementation of <see cref="ILatticeCursorGrain"/> - a stateful cursor
/// grain that checkpoints scan progress server-side. Each
/// <c>NextAsync</c> / <c>DeleteRangeStepAsync</c> call runs a bounded
/// sub-scan through the tree's public <see cref="ILattice"/> API using the
/// persisted <see cref="LatticeCursorState.LastYieldedKey"/> as a
/// continuation, then persists the advanced position atomically before
/// returning the page.
/// <para>
/// Because each step goes through the normal <see cref="ILattice.KeysAsync"/>
/// / <see cref="ILattice.EntriesAsync"/> path, topology-change reconciliation
/// is automatic within each step. Global ordering is preserved
/// across steps because the continuation bounds strictly exclude every
/// previously-yielded key.
/// </para>
/// <para>
/// <b>Self-cleanup.</b> The grain registers an idle-TTL reminder
/// (<c>cursor-ttl</c>) on every successful call. If the reminder fires
/// without any intervening activity, the grain clears its persisted state,
/// unregisters the reminder, and deactivates - protecting against cursor
/// leaks from clients that forget to call
/// <see cref="ILattice.CloseCursorAsync"/>. The interval is configured by
/// <see cref="LatticeOptions.CursorIdleTtl"/> (default 48h); set to
/// <see cref="Timeout.InfiniteTimeSpan"/> to disable.
/// </para>
/// </summary>
internal sealed partial class LatticeCursorGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    IServiceProvider services,
    ILogger<LatticeCursorGrain> logger,
    [PersistentState("lattice-cursor", LatticeOptions.StorageProviderName)]
    IPersistentState<LatticeCursorState> state)
    : TtlGrain<LatticeCursorGrain>(context, reminderRegistry, logger), ILatticeCursorGrain
{
    private const string IdleReminderName = "cursor-ttl";

    /// <summary>
    /// Stable consumer-id prefix for snapshot WAL pins. Pairs with the
    /// per-cursor id so a snapshot consumer is uniquely identifiable in
    /// <see cref="IWalCursorRegistry"/> snapshots and so a tree-wide
    /// unregister can target only this cursor's pin.
    /// </summary>
    internal const string SnapshotConsumerIdPrefix = "_lattice_snapshot_cursor_";

    /// <summary>Composite cursor grain key (<c>{treeId}/{cursorId}</c>).</summary>
    private string CursorKey => GrainContext.GrainId.Key.ToString()!;

    /// <summary>
    /// Optional WAL cursor registry. <see langword="null"/> when the host
    /// did not opt into <c>AddWalCursorRegistry(...)</c>; in that case
    /// snapshot cursors degrade to "best-effort retention" (the WAL GC
    /// remains free to trim under its TTL / cursor / blocked-floor
    /// predicates) and any subsequent rebuild that observes a trimmed
    /// prefix surfaces as the coordinator's underlying failure.
    /// </summary>
    private IWalCursorRegistry? WalCursorRegistry => services.GetService<IWalCursorRegistry>();

    /// <summary>
    /// Stable consumer id this cursor reports under. Includes the
    /// cursor key so concurrent snapshot cursors on the same tree
    /// register distinct pins.
    /// </summary>
    private string SnapshotConsumerId => SnapshotConsumerIdPrefix + CursorKey;

    /// <inheritdoc />
    protected override string TtlReminderName => IdleReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl()
    {
        var treeId = state.State.TreeId;
        var options = string.IsNullOrEmpty(treeId)
            ? optionsMonitor.CurrentValue
            : optionsMonitor.Get(treeId);
        return options.CursorIdleTtl;
    }

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        Logger.LogInformation(
            "Cursor {CursorKey}: idle TTL expired; clearing persisted state.",
            CursorKey);

        // Best-effort pin release on idle eviction so the registry
        // does not retain a snapshot for a cursor that has gone
        // silent. A failure here only delays pin release until its
        // own TTL elapses on the registry side.
        await ReleasePointInTimePinAsync(rethrow: false);
        await TryUnregisterSnapshotPinAsync();

        // Delete the per-shard frozen baselines captured for this cursor so
        // they do not outlive it. Best-effort; mirrors the WAL-pin release.
        await TryDeleteSnapshotBaselinesAsync();

        await state.ClearStateAsync();
    }

    /// <inheritdoc />
    public async Task OpenAsync(string treeId, LatticeCursorSpec spec)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        if (spec.Kind == LatticeCursorKind.DeleteRange)
        {
            if (spec.StartInclusive is null || spec.EndExclusive is null)
            {
                throw new ArgumentException(
                    "DeleteRange cursors require both StartInclusive and EndExclusive bounds.",
                    nameof(spec));
            }
            if (spec.Reverse)
            {
                throw new ArgumentException(
                    "DeleteRange cursors cannot be reverse.", nameof(spec));
            }
            if (spec.PointInTime)
            {
                throw new ArgumentException(
                    "DeleteRange cursors cannot run in point-in-time mode: range " +
                    "deletes are themselves mutations, not snapshot reads.",
                    nameof(spec));
            }
        }

        if (state.State.Phase == LatticeCursorPhase.NotStarted)
        {
            var prevTreeId = state.State.TreeId;
            var prevSpec = state.State.Spec;
            var prevPhase = state.State.Phase;
            var prevSnapshot = state.State.PointInTimeSnapshot;
            var prevPinId = state.State.SnapshotPinId;

            Dictionary<Guid, TxStatus>? snapshot = null;
            Guid pinId = Guid.Empty;
            if (spec.PointInTime)
            {
                var registry = grainFactory.GetGrain<ITxRegistryGrain>(treeId);
                snapshot = await registry.SnapshotAsync();
                if (snapshot is { Count: > 0 })
                {
                    // Pin every txid whose decision the snapshot
                    // captured. InFlight entries are excluded because
                    // they don't yet have a tombstone to protect - if
                    // they later commit or abort, the cursor falls
                    // back to the snapshot's InFlight reading anyway,
                    // which masks the post-snapshot transition.
                    var pinned = new List<Guid>(snapshot.Count);
                    foreach (var (txid, status) in snapshot)
                    {
                        if (status != TxStatus.InFlight) pinned.Add(txid);
                    }
                    if (pinned.Count > 0)
                    {
                        pinId = Guid.NewGuid();
                        var ttl = optionsMonitor.Get(treeId).MaxCursorSnapshotPinTtl;
                        await registry.PinSnapshotAsync(pinId, pinned, ttl);
                    }
                }
            }

            state.State.TreeId = treeId;
            state.State.Spec = spec;
            state.State.Phase = LatticeCursorPhase.Open;
            state.State.PointInTimeSnapshot = snapshot;
            state.State.SnapshotPinId = pinId;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                // Restore in-memory state so the Phase == NotStarted guard
                // above remains entered on retry and the spec-mismatch check
                // below does not reject a valid second OpenAsync.
                state.State.TreeId = prevTreeId;
                state.State.Spec = prevSpec;
                state.State.Phase = prevPhase;
                state.State.PointInTimeSnapshot = prevSnapshot;
                state.State.SnapshotPinId = prevPinId;

                // Also drop the pin we just installed - persisting
                // the cursor failed, so the pin would leak retention
                // for a cursor that does not exist.
                if (pinId != Guid.Empty)
                {
                    try
                    {
                        var registry = grainFactory.GetGrain<ITxRegistryGrain>(treeId);
                        await registry.UnpinSnapshotAsync(pinId);
                    }
                    catch (Exception unpinEx)
                    {
                        Logger.LogWarning(unpinEx,
                            "Cursor {CursorKey}: failed to unpin snapshot {PinId} after open " +
                            "persist failure; pin will expire via TTL.",
                            CursorKey, pinId);
                    }
                }
                throw;
            }
            await SlideTtlAsync();
            return;
        }

        // Idempotent re-open: only tolerate the same spec/tree. Differences
        // would silently corrupt an in-flight scan.
        if (state.State.TreeId != treeId || !state.State.Spec.Equals(spec))
        {
            throw new InvalidOperationException(
                $"Cursor '{CursorKey}' is already open with a different specification.");
        }

        // Refresh the pin on re-open of a still-open point-in-time
        // cursor. A re-open is one of the natural touchpoints (the
        // client decided to keep paging after a pause), so it should
        // slide the registry-side TTL alongside the local idle TTL.
        await RefreshPointInTimePinAsync();

        await SlideTtlAsync();
    }

    /// <inheritdoc />
    public async Task<LatticeCursorKeysPage> NextKeysAsync(int pageSize)
    {
        EnsureOpenFor(LatticeCursorKind.Keys);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        if (state.State.Phase == LatticeCursorPhase.Exhausted)
            return new LatticeCursorKeysPage { Keys = Array.Empty<string>(), HasMore = false };

        // Snapshot cursors (ZeroObservableWrites) route through the
        // snapshot-leaf fan-out partial; they do not touch the live
        // shard projection at all.
        if (state.State.Spec.ZeroObservableWrites)
        {
            return await NextSnapshotKeysAsync(pageSize);
        }

        await RefreshPointInTimePinAsync();

        var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);
        var (effStart, effEnd) = ComputeEffectiveRange();

        var collected = new List<string>(pageSize);
        var predicate = state.State.Spec.Predicate;
        using (BeginPointInTimeScopeIfNeeded())
        {
            var keys = predicate is { } pred
                ? lattice.KeysWherePredicateAsync(pred, effStart, effEnd, state.State.Spec.Reverse)
                : lattice.KeysAsync(effStart, effEnd, state.State.Spec.Reverse);
            await foreach (var key in keys)
            {
                collected.Add(key);
                if (collected.Count >= pageSize) break;
            }
        }

        var hasMore = collected.Count >= pageSize;
        var prevLastYieldedKey = state.State.LastYieldedKey;
        var prevPhase = state.State.Phase;
        if (collected.Count > 0)
        {
            state.State.LastYieldedKey = collected[^1];
        }
        if (!hasMore)
        {
            state.State.Phase = LatticeCursorPhase.Exhausted;
        }
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            // Restore so the Phase == Exhausted short-circuit at the top of
            // NextKeysAsync does not return empty pages forever after a
            // single failed persist, and so the next retry resumes from the
            // same continuation key.
            state.State.LastYieldedKey = prevLastYieldedKey;
            state.State.Phase = prevPhase;
            throw;
        }
        await SlideTtlAsync();

        return new LatticeCursorKeysPage { Keys = collected, HasMore = hasMore };
    }

    /// <inheritdoc />
    public async Task<LatticeCursorEntriesPage> NextEntriesAsync(int pageSize)
    {
        EnsureOpenFor(LatticeCursorKind.Entries);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        if (state.State.Phase == LatticeCursorPhase.Exhausted)
        {
            return new LatticeCursorEntriesPage
            {
                Entries = Array.Empty<KeyValuePair<string, byte[]>>(),
                HasMore = false,
            };
        }

        // Snapshot cursors (ZeroObservableWrites) route through the
        // snapshot-leaf fan-out partial; they do not touch the live
        // shard projection at all.
        if (state.State.Spec.ZeroObservableWrites)
        {
            return await NextSnapshotEntriesAsync(pageSize);
        }

        await RefreshPointInTimePinAsync();

        var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);
        var (effStart, effEnd) = ComputeEffectiveRange();

        var collected = new List<KeyValuePair<string, byte[]>>(pageSize);
        var predicate = state.State.Spec.Predicate;
        using (BeginPointInTimeScopeIfNeeded())
        {
            var entries = predicate is { } pred
                ? lattice.EntriesWherePredicateAsync(pred, effStart, effEnd, state.State.Spec.Reverse)
                : lattice.EntriesAsync(effStart, effEnd, state.State.Spec.Reverse);
            await foreach (var entry in entries)
            {
                collected.Add(entry);
                if (collected.Count >= pageSize) break;
            }
        }

        var hasMore = collected.Count >= pageSize;
        var prevEntriesLastYieldedKey = state.State.LastYieldedKey;
        var prevEntriesPhase = state.State.Phase;
        if (collected.Count > 0)
        {
            state.State.LastYieldedKey = collected[^1].Key;
        }
        if (!hasMore)
        {
            state.State.Phase = LatticeCursorPhase.Exhausted;
        }
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            // Restore so the Phase == Exhausted short-circuit at the top of
            // NextEntriesAsync does not return empty entry pages forever
            // after a single failed persist.
            state.State.LastYieldedKey = prevEntriesLastYieldedKey;
            state.State.Phase = prevEntriesPhase;
            throw;
        }
        await SlideTtlAsync();

        return new LatticeCursorEntriesPage { Entries = collected, HasMore = hasMore };
    }

    /// <inheritdoc />
    public async Task<LatticeCursorDeleteProgress> DeleteRangeStepAsync(int maxToDelete)
    {
        EnsureOpenFor(LatticeCursorKind.DeleteRange);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxToDelete);

        if (state.State.Phase == LatticeCursorPhase.Exhausted)
        {
            return new LatticeCursorDeleteProgress
            {
                DeletedThisStep = 0,
                DeletedTotal = state.State.DeletedTotal,
                IsComplete = true,
            };
        }

        var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);
        var (effStart, effEnd) = ComputeEffectiveRange();
        var predicate = state.State.Spec.Predicate;

        // Probe the range: collect up to maxToDelete+1 keys so we can tell
        // whether this step exhausts the range. One-past-budget lets us pick
        // a correct sub-range end without an extra round-trip.
        // Forward-only: OpenAsync rejects reverse DeleteRange specs, so the
        // default KeysAsync direction is correct here.
        //
        // When the cursor carries a predicate the probe filters to matching
        // keys only (re-applied from the persisted spec on every step, so a
        // post-failover resume sees the identical filter), so the step budget
        // counts and bounds the keys actually tombstoned.
        var probe = new List<string>(maxToDelete + 1);
        var probeKeys = predicate is { } pred
            ? lattice.KeysWherePredicateAsync(pred, effStart, effEnd)
            : lattice.KeysAsync(effStart, effEnd);
        await foreach (var key in probeKeys)
        {
            probe.Add(key);
            if (probe.Count > maxToDelete) break;
        }

        if (probe.Count == 0)
        {
            var prevEmptyPhase = state.State.Phase;
            state.State.Phase = LatticeCursorPhase.Exhausted;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                // Restore so the Phase == Exhausted short-circuit at the top
                // of DeleteRangeStepAsync does not report IsComplete=true for
                // every retry without ever persisting the completion.
                state.State.Phase = prevEmptyPhase;
                throw;
            }
            await SlideTtlAsync();
            return new LatticeCursorDeleteProgress
            {
                DeletedThisStep = 0,
                DeletedTotal = state.State.DeletedTotal,
                IsComplete = true,
            };
        }

        int deletedThisStep;
        bool isComplete;
        var prevDeleteLastYieldedKey = state.State.LastYieldedKey;
        var prevDeletePhase = state.State.Phase;
        var prevDeletedTotal = state.State.DeletedTotal;
        if (probe.Count <= maxToDelete)
        {
            // Final step: delete everything remaining in one call.
            deletedThisStep = predicate is { } finalPred
                ? await lattice.DeleteRangeWherePredicateAsync(finalPred, effStart!, effEnd!)
                : await lattice.DeleteRangeAsync(effStart!, effEnd!);
            state.State.LastYieldedKey = probe[^1];
            state.State.Phase = LatticeCursorPhase.Exhausted;
            isComplete = true;
        }
        else
        {
            // Partial step: delete [effStart, stopKey + "\0") so stopKey is
            // included. The next step resumes from stopKey + "\0".
            var stopKey = probe[maxToDelete - 1];
            var subEnd = stopKey + "\0";
            deletedThisStep = predicate is { } partialPred
                ? await lattice.DeleteRangeWherePredicateAsync(partialPred, effStart!, subEnd)
                : await lattice.DeleteRangeAsync(effStart!, subEnd);
            state.State.LastYieldedKey = stopKey;
            isComplete = false;
        }

        state.State.DeletedTotal += deletedThisStep;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            // Restore the cursor's in-memory checkpoint so a retry resumes
            // from the same continuation key and the Exhausted short-circuit
            // does not lock the activation into reporting IsComplete=true
            // with a stale DeletedTotal. The keys deleted by the cross-grain
            // DeleteRangeAsync above remain deleted on the target tree;
            // their count is forfeited from DeletedTotal because reverting
            // here is preferable to double-counting on retry (the probe on
            // retry will skip the already-deleted prefix anyway).
            state.State.LastYieldedKey = prevDeleteLastYieldedKey;
            state.State.Phase = prevDeletePhase;
            state.State.DeletedTotal = prevDeletedTotal;
            throw;
        }
        await SlideTtlAsync();

        return new LatticeCursorDeleteProgress
        {
            DeletedThisStep = deletedThisStep,
            DeletedTotal = state.State.DeletedTotal,
            IsComplete = isComplete,
        };
    }

    /// <inheritdoc />
    public async Task CloseAsync()
    {
        // Always attempt to drop the idle-TTL reminder so a closed cursor
        // never fires a redundant cleanup tick.
        await UnregisterTtlAsync();

        // Release the registry-side snapshot pin (if any) ahead of the
        // state clear, so a CloseAsync that races with a registry
        // outage at least drops the local cursor cleanly. A failed
        // unpin only leaks until the pin's TTL elapses on the
        // registry side; it does not block close.
        await ReleasePointInTimePinAsync(rethrow: false);

        // Release the WAL retention pin held by a snapshot cursor (if
        // any). Mirrors the registry-side pin release above so a
        // closed snapshot cursor does not retain WAL prefix that the
        // GC would otherwise be free to trim.
        await TryUnregisterSnapshotPinAsync();

        // Delete the per-shard frozen baselines captured for this cursor so
        // they do not outlive it. Best-effort; mirrors the WAL-pin release.
        await TryDeleteSnapshotBaselinesAsync();

        if (state.State.Phase == LatticeCursorPhase.NotStarted
            || state.State.Phase == LatticeCursorPhase.Closed)
        {
            // No persisted state to clear, or already closed; deactivate so
            // we don't accumulate idle closed cursors.
            this.DeactivateOnIdle();
            return;
        }

        try
        {
            await state.ClearStateAsync();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cursor {CursorKey}: failed to clear state on close; marking closed in-memory only.",
                CursorKey);
            state.State.Phase = LatticeCursorPhase.Closed;
        }
        this.DeactivateOnIdle();
    }

    /// <inheritdoc />
    public Task<bool> IsOpenAsync() =>
        Task.FromResult(state.State.Phase == LatticeCursorPhase.Open);

    /// <summary>
    /// Verifies the cursor is open and was opened for the expected kind.
    /// Throws <see cref="InvalidOperationException"/> otherwise so callers see
    /// a clean error rather than silently reading stale state.
    /// </summary>
    private void EnsureOpenFor(LatticeCursorKind expectedKind)
    {
        if (state.State.Phase == LatticeCursorPhase.NotStarted)
        {
            throw new InvalidOperationException(
                $"Cursor '{CursorKey}' has not been opened. Call OpenAsync first.");
        }
        if (state.State.Phase == LatticeCursorPhase.Closed)
        {
            throw new InvalidOperationException(
                $"Cursor '{CursorKey}' has been closed.");
        }
        if (state.State.Spec.Kind != expectedKind)
        {
            throw new InvalidOperationException(
                $"Cursor '{CursorKey}' was opened for {state.State.Spec.Kind}, not {expectedKind}.");
        }
    }

    /// <summary>
    /// Computes the effective scan range for the next step by clamping the
    /// persisted spec with the last-yielded key. Forward scans advance the
    /// lower bound to <c>LastYieldedKey + "\0"</c>; reverse scans pull the
    /// upper bound down to <c>LastYieldedKey</c>.
    /// </summary>
    private (string? start, string? end) ComputeEffectiveRange()
    {
        var spec = state.State.Spec;
        var last = state.State.LastYieldedKey;
        if (last is null) return (spec.StartInclusive, spec.EndExclusive);

        if (spec.Reverse)
        {
            // endExclusive <- last (never widen past the original end).
            var newEnd = spec.EndExclusive is null
                ? last
                : (string.Compare(last, spec.EndExclusive, StringComparison.Ordinal) < 0
                    ? last : spec.EndExclusive);
            return (spec.StartInclusive, newEnd);
        }

        // Forward: startInclusive <- last + "\0" (never retreat past original start).
        var afterLast = last + "\0";
        var newStart = spec.StartInclusive is null
            ? afterLast
            : (string.Compare(afterLast, spec.StartInclusive, StringComparison.Ordinal) > 0
                ? afterLast : spec.StartInclusive);
        return (newStart, spec.EndExclusive);
    }

    /// <summary>
    /// Wraps the current step's tree-read fan-out in a
    /// <see cref="LatticeRegistrySnapshotContext"/> scope when the
    /// cursor was opened with
    /// <see cref="LatticeCursorSpec.PointInTime"/> set; returns a
    /// no-op disposable otherwise. Centralising the conditional keeps
    /// the page-fetch loops branch-free.
    /// </summary>
    private IDisposable BeginPointInTimeScopeIfNeeded()
    {
        if (!state.State.Spec.PointInTime) return NoopScope.Instance;
        return LatticeRegistrySnapshotContext.BeginScope(state.State.PointInTimeSnapshot);
    }

    /// <summary>
    /// Slides the registry-side pin's <c>ExpiresAt</c> forward when
    /// the cursor was opened in point-in-time mode and actually pinned
    /// at least one decision. A <see langword="false"/> result from
    /// <see cref="ITxRegistryGrain.RefreshPinAsync"/> means the pin
    /// has been evicted (either by an out-of-band
    /// <c>UnpinSnapshotAsync</c> or by expiry past
    /// <see cref="LatticeOptions.MaxCursorSnapshotPinTtl"/>) - the
    /// cursor surfaces this as
    /// <see cref="LatticeCursorSnapshotExpiredException"/> on the
    /// current step and self-closes so the local activation does not
    /// keep serving a snapshot whose retention has lapsed.
    /// </summary>
    private async Task RefreshPointInTimePinAsync()
    {
        if (!state.State.Spec.PointInTime) return;
        if (state.State.SnapshotPinId == Guid.Empty) return; // nothing to refresh

        var registry = grainFactory.GetGrain<ITxRegistryGrain>(state.State.TreeId);
        var ttl = optionsMonitor.Get(state.State.TreeId).MaxCursorSnapshotPinTtl;
        var refreshed = await registry.RefreshPinAsync(state.State.SnapshotPinId, ttl);
        if (refreshed) return;

        // Pin has been evicted. Mark the cursor closed so a follow-up
        // call sees a deterministic Closed phase rather than a state
        // mismatch, and forward a typed exception to the caller. The
        // captured snapshot is also cleared because it no longer has
        // retention behind it.
        state.State.Phase = LatticeCursorPhase.Closed;
        state.State.PointInTimeSnapshot = null;
        state.State.SnapshotPinId = Guid.Empty;
        try
        {
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cursor {CursorKey}: failed to persist Closed phase after snapshot pin expiry; " +
                "in-memory state still reflects closure.",
                CursorKey);
        }

        throw new LatticeCursorSnapshotExpiredException(
            $"Cursor '{CursorKey}': point-in-time snapshot pin has been evicted from the " +
            "TxRegistry (TTL elapsed or out-of-band unpin). Open a fresh point-in-time " +
            "cursor to resume the scan against a new snapshot.");
    }

    /// <summary>
    /// Best-effort release of the registry-side pin held by this
    /// cursor (if any). Called from <see cref="CloseAsync"/>,
    /// <see cref="OnTtlExpiredAsync"/>, and the snapshot-expiry
    /// fallback so a cursor never leaks tombstone-retention beyond
    /// its own lifetime. A failure on the registry side is logged
    /// and swallowed when <paramref name="rethrow"/> is
    /// <see langword="false"/> - the pin will fall out on its own
    /// TTL.
    /// </summary>
    private async Task ReleasePointInTimePinAsync(bool rethrow)
    {
        var pinId = state.State.SnapshotPinId;
        if (pinId == Guid.Empty) return;

        try
        {
            var registry = grainFactory.GetGrain<ITxRegistryGrain>(state.State.TreeId);
            await registry.UnpinSnapshotAsync(pinId);
        }
        catch (Exception ex)
        {
            if (rethrow) throw;
            Logger.LogWarning(ex,
                "Cursor {CursorKey}: failed to release point-in-time snapshot pin {PinId}; " +
                "pin will expire via its own TTL.",
                CursorKey, pinId);
        }
    }

    /// <summary>
    /// Reusable no-op disposable for the non-point-in-time cursor
    /// path so the <c>using</c> block in the page-fetch loops does
    /// not allocate when point-in-time mode is off.
    /// </summary>
    private sealed class NoopScope : IDisposable
    {
        public static readonly NoopScope Instance = new();
        private NoopScope() { }
        public void Dispose() { }
    }
}
