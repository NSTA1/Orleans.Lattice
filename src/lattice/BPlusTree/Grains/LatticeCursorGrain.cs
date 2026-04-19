using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Implementation of <see cref="ILatticeCursorGrain"/> — a stateful cursor
/// grain (F-033) that checkpoints scan progress server-side. Each
/// <c>NextAsync</c> / <c>DeleteRangeStepAsync</c> call runs a bounded
/// sub-scan through the tree's public <see cref="ILattice"/> API using the
/// persisted <see cref="LatticeCursorState.LastYieldedKey"/> as a
/// continuation, then persists the advanced position atomically before
/// returning the page.
/// <para>
/// Because each step goes through the normal <see cref="ILattice.KeysAsync"/>
/// / <see cref="ILattice.EntriesAsync"/> path, topology-change reconciliation
/// (F-032) is automatic within each step. Global ordering is preserved
/// across steps because the continuation bounds strictly exclude every
/// previously-yielded key.
/// </para>
/// <para>
/// <b>Self-cleanup.</b> The grain registers an idle-TTL reminder
/// (<c>cursor-ttl</c>) on every successful call. If the reminder fires
/// without any intervening activity, the grain clears its persisted state,
/// unregisters the reminder, and deactivates — protecting against cursor
/// leaks from clients that forget to call
/// <see cref="ILattice.CloseCursorAsync"/>. The interval is configured by
/// <see cref="LatticeOptions.CursorIdleTtl"/> (default 48h); set to
/// <see cref="Timeout.InfiniteTimeSpan"/> to disable.
/// </para>
/// </summary>
internal sealed class LatticeCursorGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<LatticeCursorGrain> logger,
    [PersistentState("lattice-cursor", LatticeOptions.StorageProviderName)]
    IPersistentState<LatticeCursorState> state)
    : TtlGrain(context, reminderRegistry, logger), ILatticeCursorGrain
{
    private const string IdleReminderName = "cursor-ttl";

    /// <summary>Composite cursor grain key (<c>{treeId}/{cursorId}</c>).</summary>
    private string CursorKey => GrainContext.GrainId.Key.ToString()!;

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
        logger.LogInformation(
            "Cursor {CursorKey}: idle TTL expired; clearing persisted state.",
            CursorKey);
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
        }

        if (state.State.Phase == LatticeCursorPhase.NotStarted)
        {
            state.State.TreeId = treeId;
            state.State.Spec = spec;
            state.State.Phase = LatticeCursorPhase.Open;
            await state.WriteStateAsync();
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

        await SlideTtlAsync();
    }

    /// <inheritdoc />
    public async Task<LatticeCursorKeysPage> NextKeysAsync(int pageSize)
    {
        EnsureOpenFor(LatticeCursorKind.Keys);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        if (state.State.Phase == LatticeCursorPhase.Exhausted)
            return new LatticeCursorKeysPage { Keys = Array.Empty<string>(), HasMore = false };

        var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);
        var (effStart, effEnd) = ComputeEffectiveRange();

        var collected = new List<string>(pageSize);
        await foreach (var key in lattice.KeysAsync(effStart, effEnd, state.State.Spec.Reverse))
        {
            collected.Add(key);
            if (collected.Count >= pageSize) break;
        }

        var hasMore = collected.Count >= pageSize;
        if (collected.Count > 0)
        {
            state.State.LastYieldedKey = collected[^1];
        }
        if (!hasMore)
        {
            state.State.Phase = LatticeCursorPhase.Exhausted;
        }
        await state.WriteStateAsync();
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

        var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);
        var (effStart, effEnd) = ComputeEffectiveRange();

        var collected = new List<KeyValuePair<string, byte[]>>(pageSize);
        await foreach (var entry in lattice.EntriesAsync(effStart, effEnd, state.State.Spec.Reverse))
        {
            collected.Add(entry);
            if (collected.Count >= pageSize) break;
        }

        var hasMore = collected.Count >= pageSize;
        if (collected.Count > 0)
        {
            state.State.LastYieldedKey = collected[^1].Key;
        }
        if (!hasMore)
        {
            state.State.Phase = LatticeCursorPhase.Exhausted;
        }
        await state.WriteStateAsync();
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

        // Probe the range: collect up to maxToDelete+1 keys so we can tell
        // whether this step exhausts the range. One-past-budget lets us pick
        // a correct sub-range end without an extra round-trip.
        // Forward-only: OpenAsync rejects reverse DeleteRange specs, so the
        // default KeysAsync direction is correct here.
        var probe = new List<string>(maxToDelete + 1);
        await foreach (var key in lattice.KeysAsync(effStart, effEnd))
        {
            probe.Add(key);
            if (probe.Count > maxToDelete) break;
        }

        if (probe.Count == 0)
        {
            state.State.Phase = LatticeCursorPhase.Exhausted;
            await state.WriteStateAsync();
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
        if (probe.Count <= maxToDelete)
        {
            // Final step: delete everything remaining in one call.
            deletedThisStep = await lattice.DeleteRangeAsync(effStart!, effEnd!);
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
            deletedThisStep = await lattice.DeleteRangeAsync(effStart!, subEnd);
            state.State.LastYieldedKey = stopKey;
            isComplete = false;
        }

        state.State.DeletedTotal += deletedThisStep;
        await state.WriteStateAsync();
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
            logger.LogWarning(ex,
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
}
