using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-tree saga decision registry. See <see cref="ITxRegistryGrain"/>
/// for the contract and the role this grain plays in delivering strict
/// per-tree atomic-write visibility.
/// <para>
/// Implementation notes:
/// </para>
/// <list type="bullet">
/// <item><description>The registry is the single tree-wide linearization
/// point. <c>MarkCommittedAsync</c> / <c>MarkAbortedAsync</c> persist
/// the decision before returning; the saga grain then begins the
/// terminal fan-out. Concurrent leaf reads observing a pending entry
/// dial back to <c>GetStatusAsync</c> and use the registry's
/// already-persisted decision to resolve the read.</description></item>
/// <item><description>The grain is single-threaded by Orleans turn semantics
/// and persisted via <see cref="LatticeOptions.StorageProviderName"/>,
/// so decision recording is a single atomic state-write per call.</description></item>
/// <item><description>Idempotency: repeated calls with the same outcome are
/// no-ops. Conflicting calls (commit-then-abort or abort-then-commit)
/// throw <see cref="InvalidOperationException"/> - they indicate a saga
/// implementation bug, not a recoverable transient.</description></item>
/// <item><description><c>ForgetAsync</c> tombstones the decision with a
/// TTL (<see cref="LatticeOptions.TxDecisionRetention"/>, default 60s)
/// rather than removing it immediately. A concurrent shard-split sweep
/// that installs an orphan pending bucket on a destination shard
/// <i>after</i> the saga's terminal fan-out completed can then still
/// resolve the saga's outcome via <see cref="GetStatusAsync"/> and
/// apply the terminal directly during its post-sweep cleanup. Setting
/// <c>TxDecisionRetention</c> to <see cref="TimeSpan.Zero"/> restores
/// the original "remove immediately" semantic for callers that don't
/// run online resharding.</description></item>
/// </list>
/// </summary>
internal sealed class TxRegistryGrain(
    IGrainContext context,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    [PersistentState("tx-registry", LatticeOptions.StorageProviderName)]
    IPersistentState<TxRegistryState> state) : ITxRegistryGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>
    /// Time source for tombstone-expiry checks. Defaults to
    /// <see cref="TimeProvider.System"/>. Tests substitute an
    /// alternative <see cref="TimeProvider"/> to drive deterministic
    /// expiry without real wall-clock waits.
    /// </summary>
    internal TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    /// <summary>
    /// Tree id derived from the grain key. Used to resolve the
    /// per-tree <see cref="LatticeOptions"/> snapshot for
    /// tombstone-retention configuration.
    /// </summary>
    private string TreeId => context.GrainId.Key.ToString()!;

    /// <summary>
    /// Current per-tree tombstone retention. Re-read on every call so
    /// runtime reconfiguration via <c>ConfigureLattice</c> takes effect
    /// without grain reactivation.
    /// </summary>
    private TimeSpan Retention => optionsMonitor.Get(TreeId).TxDecisionRetention;

    /// <inheritdoc />
    public async Task MarkCommittedAsync(Guid txid)
    {
        // A tombstoned decision is treated as absent: the saga has
        // already completed its post-fan-out cleanup, and a fresh Mark
        // call is therefore a new authoritative outcome. Clear the
        // tombstone AND the stale decision so the conflict-detection
        // guard below cannot block re-marking with an opposite outcome.
        if (state.State.ForgottenAt.Remove(txid))
        {
            state.State.Decisions.Remove(txid);
        }

        if (state.State.Decisions.TryGetValue(txid, out var existing))
        {
            if (existing == TxStatus.Committed) return;
            if (existing == TxStatus.Aborted)
            {
                throw new InvalidOperationException(
                    $"Cannot mark saga {txid:N} as committed: it was previously recorded as aborted.");
            }
        }

        // Snapshot prior in-memory state so a failing WriteStateAsync
        // can be unwound. Without this, the in-memory dictionary records
        // Committed while disk does not, and the next retry from the
        // same activation hits the `existing == TxStatus.Committed`
        // short-circuit and silently returns without re-persisting.
        var hadEntry = state.State.Decisions.TryGetValue(txid, out var prevStatus);
        state.State.Decisions[txid] = TxStatus.Committed;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (hadEntry) state.State.Decisions[txid] = prevStatus;
            else state.State.Decisions.Remove(txid);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task MarkAbortedAsync(Guid txid)
    {
        if (state.State.ForgottenAt.Remove(txid))
        {
            state.State.Decisions.Remove(txid);
        }

        if (state.State.Decisions.TryGetValue(txid, out var existing))
        {
            if (existing == TxStatus.Aborted) return;
            if (existing == TxStatus.Committed)
            {
                throw new InvalidOperationException(
                    $"Cannot mark saga {txid:N} as aborted: it was previously recorded as committed.");
            }
        }

        // Snapshot prior in-memory state so a failing WriteStateAsync
        // can be unwound (see MarkCommittedAsync for the same rationale).
        var hadEntry = state.State.Decisions.TryGetValue(txid, out var prevStatus);
        state.State.Decisions[txid] = TxStatus.Aborted;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (hadEntry) state.State.Decisions[txid] = prevStatus;
            else state.State.Decisions.Remove(txid);
            throw;
        }
    }

    /// <inheritdoc />
    public Task<TxStatus> GetStatusAsync(Guid txid)
    {
        if (IsTombstoneExpired(txid))
        {
            // Tombstone TTL elapsed: treat as absent. The decision is
            // not physically purged here (purging happens lazily inside
            // ForgetAsync via PruneExpired) so GetStatusAsync stays a
            // pure read with no state-write side effects.
            return Task.FromResult(TxStatus.InFlight);
        }
        return Task.FromResult(
            state.State.Decisions.TryGetValue(txid, out var status)
                ? status
                : TxStatus.InFlight);
    }

    /// <inheritdoc />
    public Task<Dictionary<Guid, TxStatus>> GetStatusManyAsync(IReadOnlyList<Guid> txids)
    {
        ArgumentNullException.ThrowIfNull(txids);
        var result = new Dictionary<Guid, TxStatus>(txids.Count);
        // Hoist UtcNow + retention out of the per-txid loop: every
        // expiry check uses the same instant and the same window, so
        // resolving them once amortises the TimeProvider / options
        // lookups across the whole batch.
        var now = TimeProvider.GetUtcNow();
        var retention = Retention;
        foreach (var txid in txids)
        {
            if (IsTombstoneExpiredAt(txid, now, retention))
            {
                result[txid] = TxStatus.InFlight;
                continue;
            }
            result[txid] = state.State.Decisions.TryGetValue(txid, out var status)
                ? status
                : TxStatus.InFlight;
        }
        return Task.FromResult(result);
    }

    /// <inheritdoc />
    public Task<Dictionary<Guid, TxStatus>> SnapshotAsync()
    {
        // Return a defensive copy so callers cannot mutate the
        // registry's persisted state through the returned reference.
        // Expired tombstones are filtered out so the snapshot reflects
        // observable status (consistent with GetStatusAsync), not the
        // raw persisted footprint. Active tombstones (within retention)
        // are included with their recorded outcome - they're still
        // queryable and the snapshot must agree with the per-txid API.
        var now = TimeProvider.GetUtcNow();
        var retention = Retention;
        var result = new Dictionary<Guid, TxStatus>(state.State.Decisions.Count);
        foreach (var (txid, status) in state.State.Decisions)
        {
            if (IsTombstoneExpiredAt(txid, now, retention)) continue;
            result[txid] = status;
        }
        return Task.FromResult(result);
    }

    /// <inheritdoc />
    public async Task ForgetAsync(Guid txid)
    {
        var now = TimeProvider.GetUtcNow();
        var retention = Retention;

        // Capture prior state so a failing WriteStateAsync can be fully
        // unwound. Without this, the in-memory dictionaries lose the
        // saga while disk still has it; a subsequent retry of
        // ForgetAsync from the same activation finds nothing to drop
        // and short-circuits without re-persisting.
        var hadDecision = state.State.Decisions.TryGetValue(txid, out var prevStatus);
        state.State.Participants.TryGetValue(txid, out var prevParticipants);
        var hadForgottenAt = state.State.ForgottenAt.ContainsKey(txid);

        var droppedDecision = false;
        var addedForgottenAt = false;
        if (hadDecision)
        {
            if (retention == TimeSpan.Zero)
            {
                // Legacy semantic: tombstoning disabled, drop the
                // decision immediately. Equivalent to the original
                // ForgetAsync behaviour before the tombstone feature.
                state.State.Decisions.Remove(txid);
                droppedDecision = true;
            }
            else if (!hadForgottenAt)
            {
                // Tombstone the decision. Re-tombstoning an already-
                // tombstoned txid is a no-op so repeated ForgetAsync
                // calls don't bump the ForgottenAt timestamp and stretch
                // the retention window - the test
                // ForgetAsync_is_idempotent_under_repeated_calls
                // depends on this short-circuit.
                state.State.ForgottenAt[txid] = now;
                addedForgottenAt = true;
            }
        }

        // Participants are always dropped immediately. They're a
        // broadcast-fan-out aid for the saga grain and are not needed
        // after the saga calls ForgetAsync; the orphan-resolution path
        // that depends on the tombstone uses Decisions only.
        var droppedParticipants = state.State.Participants.Remove(txid);

        // Receiver-side cross-cluster terminal-tally state is also
        // bounded by the saga lifetime, so drop it alongside the
        // participants. The tally is only consulted while the gate is
        // pending; once the decision flips it has done its job. The
        // legacy single-cluster path never populates these slots so
        // the Remove calls are cheap no-ops in that mode.
        state.State.TerminalArrivals.TryGetValue(txid, out var prevArrivals);
        var droppedArrivals = state.State.TerminalArrivals.Remove(txid);
        state.State.ExpectedTerminals.TryGetValue(txid, out var prevExpectedTotal);
        var hadExpectedTotal = state.State.ExpectedTerminals.ContainsKey(txid);
        var droppedExpected = state.State.ExpectedTerminals.Remove(txid);

        // Inline prune of expired tombstones from earlier ForgetAsync
        // calls. Folding the GC pass into the natural caller (saga
        // post-cleanup) means tombstones are pruned at roughly the
        // same cadence as new sagas land - no separate timer reminder
        // is required. PruneExpired returns the list of pruned entries
        // so a failing WriteStateAsync can restore them.
        var pruned = PruneExpired(now, retention);

        var changed = droppedDecision || addedForgottenAt || droppedParticipants
            || droppedArrivals || droppedExpected
            || (pruned is { Count: > 0 });

        if (changed)
        {
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                if (droppedDecision) state.State.Decisions[txid] = prevStatus;
                if (addedForgottenAt) state.State.ForgottenAt.Remove(txid);
                if (droppedParticipants && prevParticipants is not null)
                {
                    state.State.Participants[txid] = prevParticipants;
                }
                if (droppedArrivals && prevArrivals is not null)
                {
                    state.State.TerminalArrivals[txid] = prevArrivals;
                }
                if (droppedExpected && hadExpectedTotal)
                {
                    state.State.ExpectedTerminals[txid] = prevExpectedTotal;
                }
                if (pruned is not null)
                {
                    foreach (var entry in pruned)
                    {
                        // PruneExpired removes both the Decisions row
                        // and the ForgottenAt row in lockstep, so the
                        // revert restores both. A pruned tombstone
                        // without a recorded decision (entry.HadDecision
                        // == false) only restores the ForgottenAt entry
                        // - the legacy zero-retention path can leave
                        // entries in this shape transiently.
                        if (entry.HadDecision)
                            state.State.Decisions[entry.Txid] = entry.Decision;
                        state.State.ForgottenAt[entry.Txid] = entry.ForgottenAt;
                    }
                }
                throw;
            }
        }
    }

    /// <inheritdoc />
    public async Task RegisterParticipantAsync(Guid txid, int shardIndex)
    {
        var createdSet = false;
        if (!state.State.Participants.TryGetValue(txid, out var set))
        {
            set = [];
            state.State.Participants[txid] = set;
            createdSet = true;
        }

        if (!set.Add(shardIndex))
        {
            // Already recorded - no-op, no state write. The shard-root
            // dedup gate normally prevents this RPC entirely on a
            // stable activation; this branch only fires when the
            // shard-root deactivated and reactivated between two
            // prepare-phase writes for the same saga.
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            // Unwind the in-memory mutation so a retry from the same
            // activation does not hit the `!set.Add(shardIndex)`
            // short-circuit and silently no-op with disk still stale.
            set.Remove(shardIndex);
            if (createdSet) state.State.Participants.Remove(txid);
            throw;
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<int>> GetParticipantsAsync(Guid txid)
    {
        if (!state.State.Participants.TryGetValue(txid, out var set) || set.Count == 0)
        {
            return Task.FromResult<IReadOnlyList<int>>(Array.Empty<int>());
        }

        var sorted = new int[set.Count];
        set.CopyTo(sorted);
        Array.Sort(sorted);
        return Task.FromResult<IReadOnlyList<int>>(sorted);
    }

    /// <inheritdoc />
    public async Task<TerminalTallyResult> RecordTerminalArrivalAsync(
        Guid txid,
        int sourceShardIndex,
        bool committed,
        int expectedShardCount)
    {
        // Legacy-producer fast path: a 0 expected count means the
        // producer did not stamp the gate, so fall back to "mark on
        // first terminal" semantics. The caller treats IsFinal=true
        // as the signal to flip the per-tree linearization mark
        // immediately, matching pre-gate behaviour. We also do NOT
        // accumulate tally state in this branch - there is no
        // expected total to compare against, so the dedup set would
        // grow unbounded if cross-cluster delivery retries piled up.
        if (expectedShardCount <= 0)
        {
            return new TerminalTallyResult
            {
                IsFinal = true,
                FinalOutcome = committed ? TxStatus.Committed : TxStatus.Aborted,
                // Legacy fan-out semantic is "fan out the source shard
                // index just observed". Return a single-element list so
                // the caller's loop body is uniform between the legacy
                // fast path and the gated final-arrival path.
                ObservedSourceShards = new[] { sourceShardIndex },
            };
        }

        // Mixed-outcome guard: every per-source-shard terminal of a
        // saga must agree on commit/abort. A mixed sequence is a
        // protocol violation (the saga coordinator never broadcasts a
        // mixed terminal set); throwing here lets a malformed inbound
        // stream surface as a hard error rather than silently
        // corrupting the gate.
        if (state.State.Decisions.TryGetValue(txid, out var existing))
        {
            if (committed && existing == TxStatus.Aborted)
            {
                throw new InvalidOperationException(
                    $"Saga {txid:N} received a commit terminal after an abort was already recorded.");
            }
            if (!committed && existing == TxStatus.Committed)
            {
                throw new InvalidOperationException(
                    $"Saga {txid:N} received an abort terminal after a commit was already recorded.");
            }
        }

        // Snapshot prior state so a failing WriteStateAsync can unwind
        // every in-memory mutation - mirrors the
        // RegisterParticipantAsync / MarkCommittedAsync pattern.
        var arrivalsHadEntry = state.State.TerminalArrivals.TryGetValue(txid, out var arrivals);
        var arrivalsCreated = false;
        if (arrivals is null)
        {
            arrivals = [];
            state.State.TerminalArrivals[txid] = arrivals;
            arrivalsCreated = true;
        }

        // Idempotent add: a duplicate-delivery retry of the same
        // source-shard terminal is a safe no-op for the tally side.
        // The caller-facing decision still needs to re-evaluate so we
        // do not early-return; it just contributes no new state mutation.
        var arrivalAdded = arrivals.Add(sourceShardIndex);

        var expectedHadEntry = state.State.ExpectedTerminals.TryGetValue(txid, out var prevExpected);
        var newExpected = expectedHadEntry
            ? Math.Max(prevExpected, expectedShardCount)
            : expectedShardCount;
        var expectedChanged = !expectedHadEntry || newExpected != prevExpected;
        if (expectedChanged)
        {
            state.State.ExpectedTerminals[txid] = newExpected;
        }

        if (arrivalAdded || expectedChanged)
        {
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                if (arrivalAdded) arrivals.Remove(sourceShardIndex);
                if (arrivalsCreated || (!arrivalsHadEntry && arrivals.Count == 0))
                {
                    state.State.TerminalArrivals.Remove(txid);
                }
                if (expectedChanged)
                {
                    if (expectedHadEntry) state.State.ExpectedTerminals[txid] = prevExpected;
                    else state.State.ExpectedTerminals.Remove(txid);
                }
                throw;
            }
        }

        var isFinal = arrivals.Count >= newExpected;
        // Materialise the observed source-shard set only on the final
        // arrival - in-progress arrivals do not need to know the
        // interim list and shipping a fresh copy on every arrival
        // would inflate the wire size of the call linearly with saga
        // shard cardinality. Empty list (Array.Empty<int>()) on the
        // non-final path is a singleton, so the allocation is free.
        IReadOnlyList<int> observed;
        if (isFinal)
        {
            var sorted = new int[arrivals.Count];
            arrivals.CopyTo(sorted);
            Array.Sort(sorted);
            observed = sorted;
        }
        else
        {
            observed = Array.Empty<int>();
        }
        return new TerminalTallyResult
        {
            IsFinal = isFinal,
            FinalOutcome = committed ? TxStatus.Committed : TxStatus.Aborted,
            ObservedSourceShards = observed,
        };
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="txid"/> has
    /// a tombstone whose age exceeds the per-tree retention window.
    /// Used by the read-side APIs to mask expired-but-not-yet-purged
    /// tombstones from callers.
    /// </summary>
    private bool IsTombstoneExpired(Guid txid)
        => IsTombstoneExpiredAt(txid, TimeProvider.GetUtcNow(), Retention);

    /// <summary>
    /// Batched variant of <see cref="IsTombstoneExpired(Guid)"/> that
    /// reuses a single <paramref name="now"/> / <paramref name="retention"/>
    /// pair across a loop, avoiding per-iteration <see cref="TimeProvider"/>
    /// and options-monitor lookups.
    /// </summary>
    private bool IsTombstoneExpiredAt(Guid txid, DateTimeOffset now, TimeSpan retention)
    {
        if (!state.State.ForgottenAt.TryGetValue(txid, out var ts)) return false;
        // TimeSpan.Zero retention: any tombstone observed here is
        // expired (this branch is only reachable from GetStatus* /
        // SnapshotAsync; ForgetAsync's own zero-retention path drops
        // the decision directly without writing to ForgottenAt).
        if (retention == TimeSpan.Zero) return true;
        return now - ts > retention;
    }

    /// <summary>
    /// Physically drops every decision whose tombstone has elapsed,
    /// plus its <see cref="TxRegistryState.ForgottenAt"/> entry. Returns
    /// the list of pruned entries (or <see langword="null"/> when
    /// nothing was pruned) so the caller can persist the change AND
    /// restore the entries if the subsequent <c>WriteStateAsync</c>
    /// fails. Called inline from <see cref="ForgetAsync"/>;
    /// <see cref="TimeSpan.Zero"/> retention purges the entire
    /// tombstone map (covers the legacy path where a non-zero retention
    /// was downgraded to zero at runtime).
    /// </summary>
    private List<PrunedEntry>? PruneExpired(DateTimeOffset now, TimeSpan retention)
    {
        if (state.State.ForgottenAt.Count == 0) return null;

        if (retention == TimeSpan.Zero)
        {
            // Tombstoning is disabled - flush any residual tombstones
            // (and their decisions) accumulated under a previous
            // non-zero retention. This is rare in practice but the
            // option monitor allows runtime reconfiguration so the
            // path must terminate cleanly.
            var flushed = new List<PrunedEntry>(state.State.ForgottenAt.Count);
            foreach (var (txid, ts) in state.State.ForgottenAt)
            {
                var hadDecision = state.State.Decisions.TryGetValue(txid, out var decision);
                flushed.Add(new PrunedEntry(txid, hadDecision, decision, ts));
            }
            foreach (var entry in flushed)
            {
                state.State.Decisions.Remove(entry.Txid);
            }
            state.State.ForgottenAt.Clear();
            return flushed;
        }

        List<PrunedEntry>? expired = null;
        foreach (var (txid, ts) in state.State.ForgottenAt)
        {
            if (now - ts > retention)
            {
                var hadDecision = state.State.Decisions.TryGetValue(txid, out var decision);
                (expired ??= new List<PrunedEntry>()).Add(new PrunedEntry(txid, hadDecision, decision, ts));
            }
        }
        if (expired is null) return null;
        foreach (var entry in expired)
        {
            state.State.Decisions.Remove(entry.Txid);
            state.State.ForgottenAt.Remove(entry.Txid);
        }
        return expired;
    }

    /// <summary>
    /// Captures a tombstone entry pruned by <see cref="PruneExpired"/>
    /// so a failing <c>WriteStateAsync</c> can restore both the
    /// decision row and the <c>ForgottenAt</c> row in lockstep.
    /// </summary>
    private readonly record struct PrunedEntry(
        Guid Txid,
        bool HadDecision,
        TxStatus Decision,
        DateTimeOffset ForgottenAt);
}
