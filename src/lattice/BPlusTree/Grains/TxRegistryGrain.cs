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
    IGrainFactory grainFactory,
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

        // A locally-recorded decision supersedes any cross-tree delegation:
        // this sub-saga's finalize is the authoritative outcome for this tree.
        state.State.ExternalAuthorities.Remove(txid);
        state.State.ReceiverDecisionAuthorities.Remove(txid);

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
        var prevRevision = state.State.DecisionsRevision;
        state.State.DecisionsRevision = prevRevision + 1;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (hadEntry) state.State.Decisions[txid] = prevStatus;
            else state.State.Decisions.Remove(txid);
            state.State.DecisionsRevision = prevRevision;
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

        // A locally-recorded decision supersedes any cross-tree delegation.
        state.State.ExternalAuthorities.Remove(txid);
        state.State.ReceiverDecisionAuthorities.Remove(txid);

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
        var prevRevision = state.State.DecisionsRevision;
        state.State.DecisionsRevision = prevRevision + 1;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (hadEntry) state.State.Decisions[txid] = prevStatus;
            else state.State.Decisions.Remove(txid);
            state.State.DecisionsRevision = prevRevision;
            throw;
        }
    }

    /// <inheritdoc />
    public async Task RegisterExternalDecisionAuthorityAsync(Guid txid, string coordinatorKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(coordinatorKey);

        // A locally-recorded terminal decision already supersedes any
        // delegation - the sub-saga finalized before (or concurrently with)
        // this registration. Leave the local decision authoritative.
        if (state.State.Decisions.ContainsKey(txid))
        {
            return;
        }

        // Idempotent: re-registering the same coordinator is a no-op.
        if (state.State.ExternalAuthorities.TryGetValue(txid, out var existing)
            && string.Equals(existing, coordinatorKey, StringComparison.Ordinal))
        {
            return;
        }

        state.State.ExternalAuthorities[txid] = coordinatorKey;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (existing is not null) state.State.ExternalAuthorities[txid] = existing;
            else state.State.ExternalAuthorities.Remove(txid);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task RegisterReceiverDecisionAuthorityAsync(Guid txid, string receiverCoordinatorKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(receiverCoordinatorKey);

        // A locally-recorded terminal decision already supersedes any
        // delegation - the receiver coordinator's deferred materialization
        // finalized before (or concurrently with) this registration.
        if (state.State.Decisions.ContainsKey(txid))
        {
            return;
        }

        // Idempotent: re-registering the same receiver coordinator is a no-op.
        if (state.State.ReceiverDecisionAuthorities.TryGetValue(txid, out var existing)
            && string.Equals(existing, receiverCoordinatorKey, StringComparison.Ordinal))
        {
            return;
        }

        state.State.ReceiverDecisionAuthorities[txid] = receiverCoordinatorKey;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (existing is not null) state.State.ReceiverDecisionAuthorities[txid] = existing;
            else state.State.ReceiverDecisionAuthorities.Remove(txid);
            throw;
        }
    }

    /// <summary>
    /// Resolves a single receiver-delegated <paramref name="txid"/> against its
    /// receiver coordinator (<see cref="ILatticeCrossTreeReceiverGrain"/>),
    /// mirroring <see cref="ResolveDelegatedAsync"/> but for the receiver-side
    /// barrier. Returns <see cref="TxStatus.InFlight"/> while the receiver
    /// coordinator's wait set is incomplete; caches a terminal verdict into
    /// <see cref="TxRegistryState.Decisions"/> and drops the delegation entry
    /// once resolved. Dial failures surface as <c>InFlight</c> (conservative).
    /// </summary>
    private async Task<TxStatus> ResolveReceiverDelegatedAsync(Guid txid, string receiverCoordinatorKey)
    {
        TxStatus verdict;
        try
        {
            var coordinator = grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(receiverCoordinatorKey);
            verdict = await coordinator.GetDecisionAsync();
        }
        catch
        {
            return TxStatus.InFlight;
        }

        if (verdict == TxStatus.InFlight)
        {
            return TxStatus.InFlight;
        }

        if (!state.State.Decisions.ContainsKey(txid))
        {
            state.State.Decisions[txid] = verdict;
            state.State.ReceiverDecisionAuthorities.Remove(txid);
            var prevRevision = state.State.DecisionsRevision;
            state.State.DecisionsRevision = prevRevision + 1;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.Decisions.Remove(txid);
                state.State.ReceiverDecisionAuthorities[txid] = receiverCoordinatorKey;
                state.State.DecisionsRevision = prevRevision;
            }
        }
        return verdict;
    }

    /// <summary>
    /// Resolves a txid that has no local decision against whichever cross-tree
    /// delegation map (authoring-side <see cref="TxRegistryState.ExternalAuthorities"/>
    /// or receiver-side <see cref="TxRegistryState.ReceiverDecisionAuthorities"/>)
    /// carries it, else returns <see cref="TxStatus.InFlight"/>. A txid is never
    /// present in both maps.
    /// </summary>
    private async Task<TxStatus> ResolveAnyDelegatedAsync(Guid txid)
    {
        if (state.State.ExternalAuthorities.TryGetValue(txid, out var coordinatorKey))
        {
            return await ResolveDelegatedAsync(txid, coordinatorKey);
        }
        if (state.State.ReceiverDecisionAuthorities.TryGetValue(txid, out var receiverKey))
        {
            return await ResolveReceiverDelegatedAsync(txid, receiverKey);
        }
        return TxStatus.InFlight;
    }

    /// <summary>
    /// Resolves a single delegated <paramref name="txid"/> against its
    /// coordinator. While the coordinator is still preparing this returns
    /// <see cref="TxStatus.InFlight"/> without touching state. Once the
    /// coordinator's verdict is terminal it is cached into
    /// <see cref="TxRegistryState.Decisions"/> (bumping the revision) and the
    /// delegation entry is dropped, so later reads resolve locally. Coordinator
    /// dial failures are swallowed and surface as <c>InFlight</c> (conservative:
    /// the cross-tree batch stays invisible on this tree until it can be
    /// resolved).
    /// </summary>
    private async Task<TxStatus> ResolveDelegatedAsync(Guid txid, string coordinatorKey)
    {
        TxStatus verdict;
        try
        {
            var coordinator = grainFactory.GetGrain<ILatticeCrossTreeTxGrain>(coordinatorKey);
            verdict = await coordinator.GetDecisionAsync();
        }
        catch
        {
            return TxStatus.InFlight;
        }

        if (verdict == TxStatus.InFlight)
        {
            return TxStatus.InFlight;
        }

        // Terminal: cache locally so the global flip is durable on this tree
        // and future reads need no further coordinator round-trips.
        if (!state.State.Decisions.ContainsKey(txid))
        {
            state.State.Decisions[txid] = verdict;
            state.State.ExternalAuthorities.Remove(txid);
            var prevRevision = state.State.DecisionsRevision;
            state.State.DecisionsRevision = prevRevision + 1;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.Decisions.Remove(txid);
                state.State.ExternalAuthorities[txid] = coordinatorKey;
                state.State.DecisionsRevision = prevRevision;
                // Surface the resolved verdict for this read even though the
                // cache write failed; the next read re-dials and re-attempts.
            }
        }
        return verdict;
    }

    /// <summary>
    /// Resolves every active cross-tree delegation against its coordinator,
    /// caching terminal verdicts. Invoked before the snapshot read paths build
    /// their dictionaries so a coordinator-decided sub-saga is never omitted
    /// from a tree-wide snapshot (which would read as a partial cross-tree
    /// view). In-flight delegations are left in place to be retried on the next
    /// snapshot.
    /// </summary>
    private async Task ResolveAllDelegatedAsync()
    {
        if (state.State.ExternalAuthorities.Count > 0)
        {
            // Snapshot the pending delegations: ResolveDelegatedAsync mutates the
            // ExternalAuthorities map when a verdict turns terminal.
            var pending = new List<KeyValuePair<Guid, string>>(state.State.ExternalAuthorities);
            foreach (var (txid, coordinatorKey) in pending)
            {
                if (state.State.Decisions.ContainsKey(txid)) continue;
                await ResolveDelegatedAsync(txid, coordinatorKey);
            }
        }
        if (state.State.ReceiverDecisionAuthorities.Count > 0)
        {
            // Mirror the receiver-side delegation map (see above): a
            // coordinator-decided-but-not-yet-materialized receiver sub-saga
            // must not be omitted from a tree-wide snapshot.
            var pendingReceiver = new List<KeyValuePair<Guid, string>>(state.State.ReceiverDecisionAuthorities);
            foreach (var (txid, receiverKey) in pendingReceiver)
            {
                if (state.State.Decisions.ContainsKey(txid)) continue;
                await ResolveReceiverDelegatedAsync(txid, receiverKey);
            }
        }
    }

    /// <inheritdoc />
    public async Task<TxStatus> GetStatusAsync(Guid txid)
    {
        if (IsTombstoneExpired(txid))
        {
            // Tombstone TTL elapsed: treat as absent. The decision is
            // not physically purged here (purging happens lazily inside
            // ForgetAsync via PruneExpired) so GetStatusAsync stays a
            // pure read with no state-write side effects.
            return TxStatus.InFlight;
        }
        if (state.State.Decisions.TryGetValue(txid, out var status))
        {
            return status;
        }
        // No local decision: if the txid is a cross-tree sub-saga (authoring
        // or receiver side), resolve its visibility against the coordinator's
        // single global decision.
        return await ResolveAnyDelegatedAsync(txid);
    }

    /// <inheritdoc />
    public async Task<Dictionary<Guid, TxStatus>> GetStatusManyAsync(IReadOnlyList<Guid> txids)
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
            if (state.State.Decisions.TryGetValue(txid, out var status))
            {
                result[txid] = status;
                continue;
            }
            // No local decision: resolve any cross-tree delegation (authoring
            // or receiver side) against the coordinator so a bulk leaf read
            // honours the same global visibility flip as the per-txid path.
            result[txid] = await ResolveAnyDelegatedAsync(txid);
        }
        return result;
    }

    /// <inheritdoc />
    public async Task<Dictionary<Guid, TxStatus>> SnapshotAsync()
    {
        // Resolve any active cross-tree delegations against their coordinator
        // BEFORE the synchronous dict-build below. A coordinator-decided but
        // not-yet-finalized sub-saga would otherwise be omitted from the
        // snapshot (no local decision) and read as InFlight - invisible on
        // this tree while a sibling tree that already finalized shows the
        // value, a partial cross-tree view. Resolving here caches terminal
        // verdicts into Decisions so the snapshot reflects the global flip.
        await ResolveAllDelegatedAsync();

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
        return result;
    }

    /// <inheritdoc />
    public async Task<TxRegistrySnapshot> SnapshotWithRevisionAsync()
    {
        // Resolve cross-tree delegations first (see SnapshotAsync). The
        // dict + revision are then captured in one synchronous block with no
        // intervening await, so both reflect the exact same persisted state
        // (including any verdicts just cached by the resolution pass).
        await ResolveAllDelegatedAsync();

        // The revision captured inside the same synchronous block. Both
        // fields therefore reflect the exact same persisted state - no
        // inter-call skew is possible because the body below has no await
        // and reads the revision after the dict copy, so any concurrent
        // in-memory mutation (which would need its own turn token to reach
        // the synchronous mutation path in MarkCommittedAsync /
        // MarkAbortedAsync / ForgetAsync) is necessarily fully visible
        // in BOTH fields or neither.
        var now = TimeProvider.GetUtcNow();
        var retention = Retention;
        var dict = new Dictionary<Guid, TxStatus>(state.State.Decisions.Count);
        foreach (var (txid, status) in state.State.Decisions)
        {
            if (IsTombstoneExpiredAt(txid, now, retention)) continue;
            dict[txid] = status;
        }
        return new TxRegistrySnapshot
        {
            Decisions = dict,
            Revision = state.State.DecisionsRevision,
        };
    }

    /// <inheritdoc />
    public Task<long> GetDecisionsRevisionAsync()
    {
        // Cheap probe paired with SnapshotAsync's double-checked retry.
        // [AlwaysInterleave] on the interface lets this method bypass
        // the registry's turn token so heavy saga workloads do not
        // block reader-side probes. The writers (MarkCommittedAsync /
        // MarkAbortedAsync / ForgetAsync) perform their in-memory dict
        // mutation AND the revision bump synchronously before their
        // first await (state.WriteStateAsync), so an interleaved probe
        // observes a self-consistent (dict, revision) pair: a pre-bump
        // revision corresponds to a pre-mutation dict, a post-bump
        // revision to a post-mutation dict. The persisted long is
        // value-typed and aligned, so the read is JIT-atomic on every
        // supported runtime architecture; the surrounding Task
        // continuation establishes the memory barrier needed to see
        // the most recent committed write.
        return Task.FromResult(state.State.DecisionsRevision);
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

        // Drop any lingering cross-tree delegation. Normally cleared already
        // by the sub-saga's finalize (MarkCommitted/MarkAborted); this is a
        // belt-and-braces cleanup so the delegation map stays bounded.
        var hadAuthority = state.State.ExternalAuthorities.TryGetValue(txid, out var prevAuthority);
        var droppedAuthority = state.State.ExternalAuthorities.Remove(txid);
        var hadReceiverAuthority = state.State.ReceiverDecisionAuthorities.TryGetValue(txid, out var prevReceiverAuthority);
        var droppedReceiverAuthority = state.State.ReceiverDecisionAuthorities.Remove(txid);

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

        // Inline prune of expired tombstones and pins from earlier
        // ForgetAsync calls. Folding the GC pass into the natural
        // caller (saga post-cleanup) means tombstones are pruned at
        // roughly the same cadence as new sagas land - no separate
        // timer reminder is required. The returned PruneResult carries
        // the dropped tombstones AND expired pins so a failing
        // WriteStateAsync can restore them in lockstep.
        var pruned = PruneExpired(now, retention);

        var changed = droppedDecision || addedForgottenAt || droppedParticipants
            || droppedArrivals || droppedExpected || droppedAuthority
            || droppedReceiverAuthority
            || pruned.Any;

        if (changed)
        {
            // Bump the decisions revision whenever the Decisions map
            // itself mutated (legacy zero-retention drop OR a physical
            // tombstone prune). Other deltas in this method (pure
            // Participants/Arrivals/Expected removals or a
            // first-tombstone insert into ForgottenAt) do not change
            // the readable Decisions surface and do not need to invalidate
            // the reader-side snap1. The local also feeds the catch
            // block's rollback.
            var revisionBumped = droppedDecision
                || (pruned.Tombstones is { Count: > 0 });
            var prevRevision = state.State.DecisionsRevision;
            if (revisionBumped)
            {
                state.State.DecisionsRevision = prevRevision + 1;
            }
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
                if (droppedAuthority && hadAuthority)
                {
                    state.State.ExternalAuthorities[txid] = prevAuthority!;
                }
                if (droppedReceiverAuthority && hadReceiverAuthority)
                {
                    state.State.ReceiverDecisionAuthorities[txid] = prevReceiverAuthority!;
                }
                if (pruned.Tombstones is { } tombstones)
                {
                    foreach (var entry in tombstones)
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
                if (pruned.ExpiredPins is { } evicted)
                {
                    foreach (var (pinId, pin) in evicted)
                    {
                        state.State.SnapshotPins[pinId] = pin;
                    }
                }
                if (revisionBumped)
                {
                    state.State.DecisionsRevision = prevRevision;
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
    public async Task RegisterParticipantsAsync(Guid txid, IReadOnlyList<int> shardIndices)
    {
        ArgumentNullException.ThrowIfNull(shardIndices);
        if (shardIndices.Count == 0) return;

        var createdSet = false;
        if (!state.State.Participants.TryGetValue(txid, out var set))
        {
            set = [];
            state.State.Participants[txid] = set;
            createdSet = true;
        }

        // Track only the indices this call actually inserts so a
        // failed WriteStateAsync can unwind the in-memory mutation
        // without touching slots that pre-existed (e.g. from an
        // earlier per-shard RegisterParticipantAsync that already
        // persisted them, or a duplicate bulk replay).
        List<int>? added = null;
        foreach (var shardIndex in shardIndices)
        {
            if (set.Add(shardIndex))
            {
                (added ??= new List<int>(shardIndices.Count)).Add(shardIndex);
            }
        }

        if (added is null)
        {
            // Every requested index was already present - no state
            // mutation, so no WriteStateAsync. Also: if we created
            // the set above for a never-seen txid AND every supplied
            // index was a duplicate (impossible by construction, but
            // defensive), `createdSet` is meaningless because `set`
            // is currently empty; leave the empty entry rather than
            // remove it, matching the RegisterParticipantAsync
            // contract that a created-but-empty set is allowed.
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            // Unwind only the indices this call inserted so a retry
            // from the same activation re-issues the bulk insert
            // rather than silently no-oping with disk still stale.
            foreach (var idx in added)
            {
                set.Remove(idx);
            }
            if (createdSet && set.Count == 0)
            {
                state.State.Participants.Remove(txid);
            }
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

    /// <inheritdoc />
    public async Task PinSnapshotAsync(Guid pinId, IReadOnlyCollection<Guid> txids, TimeSpan ttl)
    {
        ArgumentNullException.ThrowIfNull(txids);

        var now = TimeProvider.GetUtcNow();
        var options = optionsMonitor.Get(TreeId);
        var effectiveTtl = ClampPinTtl(ttl, options);

        // Build the proposed pin set and assert the new union does not
        // exceed the per-tree footprint cap. The check ignores expired
        // pins (they're about to be pruned anyway) but does include
        // the existing entry under pinId so a refresh-via-replace
        // doesn't double-count the snapshot the cursor already paid
        // for on open.
        var proposed = new HashSet<Guid>(txids);

        var futureUnion = new HashSet<Guid>(proposed);
        foreach (var (existingPinId, existingPin) in state.State.SnapshotPins)
        {
            if (existingPinId == pinId) continue;
            if (existingPin.ExpiresAt <= now) continue;
            foreach (var t in existingPin.Txids) futureUnion.Add(t);
        }
        if (futureUnion.Count > options.MaxPinnedSagaDecisions)
        {
            throw new LatticeCursorRegistryPinExhaustedException(
                $"TxRegistry '{TreeId}' cannot accept pin {pinId:N}: " +
                $"the resulting union of {futureUnion.Count} pinned saga decisions " +
                $"would exceed MaxPinnedSagaDecisions={options.MaxPinnedSagaDecisions}. " +
                $"Reduce concurrent point-in-time cursor count or raise the cap.");
        }

        // Snapshot prior pin so a failing WriteStateAsync can be
        // unwound. A repeat call with the same pinId replaces the
        // prior pin wholesale (matches the OpenAsync contract: one
        // cursor, one pinId).
        var hadPrior = state.State.SnapshotPins.TryGetValue(pinId, out var prior);
        state.State.SnapshotPins[pinId] = new SnapshotPin
        {
            Txids = proposed,
            ExpiresAt = now + effectiveTtl,
        };
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (hadPrior && prior is not null) state.State.SnapshotPins[pinId] = prior;
            else state.State.SnapshotPins.Remove(pinId);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<bool> RefreshPinAsync(Guid pinId, TimeSpan ttl)
    {
        var now = TimeProvider.GetUtcNow();

        if (!state.State.SnapshotPins.TryGetValue(pinId, out var pin))
        {
            return false;
        }
        // A pin that has already expired (between the prior step's
        // refresh and this one) is treated as missing - the caller
        // surfaces this as LatticeCursorSnapshotExpiredException so
        // the cursor terminates rather than silently extending an
        // already-evicted pin.
        if (pin.ExpiresAt <= now)
        {
            // The prune pass in ForgetAsync drops expired pins on its
            // own cadence; we don't bother dropping it here because
            // returning false is enough to fail the cursor cleanly.
            return false;
        }

        var options = optionsMonitor.Get(TreeId);
        var effectiveTtl = ClampPinTtl(ttl, options);
        var newExpiresAt = now + effectiveTtl;
        if (newExpiresAt == pin.ExpiresAt)
        {
            // No-op: identical ttl was already recorded (e.g. two
            // refreshes within the same TimeProvider tick).
            return true;
        }

        var prior = pin.ExpiresAt;
        pin.ExpiresAt = newExpiresAt;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            pin.ExpiresAt = prior;
            throw;
        }
        return true;
    }

    /// <inheritdoc />
    public async Task UnpinSnapshotAsync(Guid pinId)
    {
        if (!state.State.SnapshotPins.TryGetValue(pinId, out var prior))
        {
            return;
        }
        state.State.SnapshotPins.Remove(pinId);
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.SnapshotPins[pinId] = prior;
            throw;
        }
    }

    /// <inheritdoc />
    public Task<int> GetPinnedDecisionCountAsync()
    {
        if (state.State.SnapshotPins.Count == 0)
        {
            return Task.FromResult(0);
        }
        // Honour the expiry semantic of the prune pass: an expired
        // pin contributes nothing to the diagnostics count even
        // though the prune pass has not yet physically removed it.
        var now = TimeProvider.GetUtcNow();
        var union = new HashSet<Guid>();
        foreach (var pin in state.State.SnapshotPins.Values)
        {
            if (pin.ExpiresAt <= now) continue;
            foreach (var txid in pin.Txids) union.Add(txid);
        }
        return Task.FromResult(union.Count);
    }

    /// <summary>
    /// Clamps a caller-supplied pin TTL against the per-tree hard cap
    /// (<see cref="LatticeOptions.MaxCursorSnapshotPinTtl"/>) and the
    /// tombstone-retention floor: a pin shorter than
    /// <see cref="LatticeOptions.TxDecisionRetention"/> is silently
    /// floored to the retention, because the registry's own tombstone
    /// prune pass already covers anything shorter.
    /// </summary>
    private static TimeSpan ClampPinTtl(TimeSpan requested, LatticeOptions options)
    {
        if (requested <= TimeSpan.Zero) requested = options.MaxCursorSnapshotPinTtl;
        if (options.MaxCursorSnapshotPinTtl > TimeSpan.Zero
            && requested > options.MaxCursorSnapshotPinTtl)
        {
            requested = options.MaxCursorSnapshotPinTtl;
        }
        if (options.TxDecisionRetention > TimeSpan.Zero
            && requested < options.TxDecisionRetention)
        {
            requested = options.TxDecisionRetention;
        }
        return requested;
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
    /// <para>
    /// Pin-aware: a tombstoned decision whose txid is in the union of
    /// every unexpired pin's <see cref="SnapshotPin.Txids"/> is held
    /// back from physical removal even when its tombstone TTL has
    /// elapsed - that's the whole point of the pin. Expired pins
    /// (<see cref="SnapshotPin.ExpiresAt"/> &lt;= <paramref name="now"/>)
    /// are dropped on the same pass, so a pin that has lapsed releases
    /// its retention on the next prune cycle without an explicit
    /// <c>UnpinSnapshotAsync</c>.
    /// </para>
    /// </summary>
    private PruneResult PruneExpired(DateTimeOffset now, TimeSpan retention)
    {
        // First sweep: drop expired pins. Their txids fall out of the
        // pin union and become candidates for the tombstone sweep
        // below. The dropped-pin list is folded into the parent caller's
        // write-state unwind via the returned PruneResult.
        Dictionary<Guid, SnapshotPin>? expiredPins = null;
        foreach (var (pinId, pin) in state.State.SnapshotPins)
        {
            if (pin.ExpiresAt <= now)
            {
                (expiredPins ??= new Dictionary<Guid, SnapshotPin>()).Add(pinId, pin);
            }
        }
        if (expiredPins is not null)
        {
            foreach (var pinId in expiredPins.Keys)
            {
                state.State.SnapshotPins.Remove(pinId);
            }
        }

        if (state.State.ForgottenAt.Count == 0)
        {
            return new PruneResult(null, expiredPins);
        }

        // Compute the union of pinned txids once per prune pass.
        var pinned = state.State.SnapshotPins.Count == 0 ? null : BuildPinnedUnion();

        if (retention == TimeSpan.Zero)
        {
            // Tombstoning is disabled - flush any residual tombstones
            // (and their decisions) accumulated under a previous
            // non-zero retention. Pinned tombstones are retained even
            // under zero retention so a cursor in flight never sees a
            // saga decision evaporate under a runtime reconfiguration.
            var flushed = new List<PrunedEntry>(state.State.ForgottenAt.Count);
            foreach (var (txid, ts) in state.State.ForgottenAt)
            {
                if (pinned is not null && pinned.Contains(txid)) continue;
                var hadDecision = state.State.Decisions.TryGetValue(txid, out var decision);
                flushed.Add(new PrunedEntry(txid, hadDecision, decision, ts));
            }
            foreach (var entry in flushed)
            {
                state.State.Decisions.Remove(entry.Txid);
                state.State.ForgottenAt.Remove(entry.Txid);
            }
            return new PruneResult(flushed.Count == 0 ? null : flushed, expiredPins);
        }

        List<PrunedEntry>? expired = null;
        foreach (var (txid, ts) in state.State.ForgottenAt)
        {
            if (pinned is not null && pinned.Contains(txid)) continue;
            if (now - ts > retention)
            {
                var hadDecision = state.State.Decisions.TryGetValue(txid, out var decision);
                (expired ??= new List<PrunedEntry>()).Add(new PrunedEntry(txid, hadDecision, decision, ts));
            }
        }
        if (expired is null)
        {
            return new PruneResult(null, expiredPins);
        }
        foreach (var entry in expired)
        {
            state.State.Decisions.Remove(entry.Txid);
            state.State.ForgottenAt.Remove(entry.Txid);
        }
        return new PruneResult(expired, expiredPins);
    }

    /// <summary>
    /// Computes the registry-wide union of pinned saga txids. Used by
    /// both the prune pass (the "do not remove" predicate) and the
    /// open-time footprint cap on
    /// <see cref="PinSnapshotAsync(Guid, IReadOnlyCollection{Guid}, TimeSpan)"/>.
    /// </summary>
    private HashSet<Guid> BuildPinnedUnion()
    {
        var pinned = new HashSet<Guid>();
        foreach (var pin in state.State.SnapshotPins.Values)
        {
            foreach (var txid in pin.Txids) pinned.Add(txid);
        }
        return pinned;
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

    /// <summary>
    /// Aggregated outcome of one <see cref="PruneExpired"/> pass:
    /// tombstones pruned plus pins evicted. Returned together so a
    /// failing <c>WriteStateAsync</c> can unwind both in lockstep.
    /// </summary>
    private readonly record struct PruneResult(
        List<PrunedEntry>? Tombstones,
        Dictionary<Guid, SnapshotPin>? ExpiredPins)
    {
        /// <summary>
        /// <see langword="true"/> when at least one tombstone or pin
        /// was removed - drives the <c>changed</c> guard in
        /// <see cref="ForgetAsync(Guid)"/>.
        /// </summary>
        public bool Any => (Tombstones is { Count: > 0 }) || ExpiredPins is { Count: > 0 };
    }
}
