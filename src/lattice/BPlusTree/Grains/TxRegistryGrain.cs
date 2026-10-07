using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
/// and persisted via <see cref="LatticeOptions.StorageProviderName"/>. Each
/// mutating call validates and mutates in one synchronous block and then
/// joins a group commit (see <c>TxRegistryGrain.GroupCommit.cs</c>): at most
/// one whole-state write is in flight, mutations arriving meanwhile ride the
/// next write, and every caller returns only once a write carrying its
/// mutation is durable. Readers are read-committed: they never return an
/// answer derived from a mutation that is not yet durable.</description></item>
/// <item><description>Idempotency: repeated calls with the same outcome are
/// no-ops. Conflicting calls (commit-then-abort or abort-then-commit)
/// throw <see cref="InvalidOperationException"/> - they indicate a saga
/// implementation bug, not a recoverable transient - unless the earlier
/// decision has already been tombstoned by <c>ForgetAsync</c>: a conflicting
/// mark then clears the tombstone and the stale decision and records the new
/// outcome.</description></item>
/// <item><description><c>ForgetAsync</c> tombstones the decision with a
/// TTL (<see cref="LatticeOptions.TxDecisionRetention"/>, default 60s)
/// rather than removing it immediately. A concurrent shard-split sweep
/// that installs an orphan pending bucket on a destination shard
/// <i>after</i> the saga's terminal fan-out completed can then still
/// resolve the saga's outcome via <see cref="GetStatusManyAsync(IReadOnlyList{Guid})"/> and
/// apply the terminal directly during its post-sweep cleanup. Setting
/// <c>TxDecisionRetention</c> to <see cref="TimeSpan.Zero"/> restores
/// the original "remove immediately" semantic for callers that don't
/// run online resharding.</description></item>
/// </list>
/// </summary>
internal sealed partial class TxRegistryGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<TxRegistryGrain> logger,
    [PersistentState("tx-registry", LatticeOptions.StorageProviderName)]
    IPersistentState<TxRegistryState> state) : ITxRegistryGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    private string? _metricTreeId;

    async Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken)
    {
        ResolveWalPurgeGuardServices();
        if (context.ActivationServices?.GetService<LatticeOptionsResolver>() is { } resolver)
            _metricTreeId = await resolver.ResolveMetricTreeIdAsync(TreeId);
    }

    /// <summary>
    /// Time source for tombstone-expiry checks. Defaults to
    /// <see cref="TimeProvider.System"/>. Tests substitute an
    /// alternative <see cref="TimeProvider"/> to drive deterministic
    /// expiry without real wall-clock waits.
    /// </summary>
    internal TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    /// <summary>
    /// Tree id derived from the grain key (the shard framing, if any, stripped
    /// by <see cref="TxRegistryRouting.TreeIdFromKey"/>). Used to resolve the
    /// per-tree <see cref="LatticeOptions"/> snapshot for
    /// tombstone-retention and admission configuration.
    /// </summary>
    private string TreeId => _treeId ??= TxRegistryRouting.TreeIdFromKey(GrainKey);

    private string? _treeId;

    /// <summary>
    /// The full grain key: the bare tree id for the legacy (unsharded)
    /// registry, or <c>_lattice_txshard_{n}_{treeId}</c> for a shard.
    /// </summary>
    private string GrainKey => context.GrainId.Key.ToString()!;

    /// <summary>
    /// Current per-tree tombstone retention. Re-read on every call so
    /// runtime reconfiguration via <c>ConfigureLattice</c> takes effect
    /// without grain reactivation.
    /// </summary>
    private TimeSpan Retention => optionsMonitor.Get(TreeId).TxDecisionRetention;

    /// <summary>
    /// Constructs a fresh <see cref="TxRegistryDecisionCore"/> wrapping the
    /// live persisted decision map and revision counter. Built per call
    /// rather than cached in a field because Orleans replaces the
    /// <c>IPersistentState&lt;TState&gt;.State</c> object (and hence its
    /// <see cref="TxRegistryState.Decisions"/> dictionary) on load, so a
    /// field-captured reference could dangle. The core wraps the dictionary
    /// by reference, so a mutation lands in the same map the grain persists.
    /// </summary>
    private TxRegistryDecisionCore DecisionCore() =>
        new(state.State.Decisions, state.State.DecisionsRevision);

    /// <inheritdoc />
    public async Task MarkCommittedAsync(Guid txid)
    {
        // Write-once terminal guard through the shared, dependency-free
        // TerminalDecisionGuard, so the rule is one model-checked function rather
        // than a hand-copied inline branch. What that buys is scoped, and the
        // scope is stated here because "model-checked" is the phrase that stops a
        // reviewer looking further (issue #2331): this method enforces "never
        // both commit and abort" only while the decision is LIVE. The
        // `Conflict when !tombstoned` arm below deliberately lets an
        // opposite-outcome mark through once the saga's cleanup has tombstoned
        // the row, and once a tombstone is purged there is no row for Classify to
        // see at all. Classify is correct over its inputs; the invariant across a
        // forget is not claimed.
        //
        // Evaluated against the maps as they stand, BEFORE the tombstone-clearing
        // prologue below. Clearing first hides the existing row from Classify, so
        // a same-outcome repeat is classified Record rather than Idempotent: it
        // resurrects a decision this tree had already forgotten, restarts its
        // retention window, and bumps the revision for a surface change that the
        // caller was promised would not happen. A duplicate terminal is the
        // ordinary case on the cross-cluster path (a replicated terminal can
        // arrive after the origin's own cleanup has tombstoned the saga), so the
        // ordering here is load-bearing, not a tidy-up.
        var hasExisting = state.State.Decisions.TryGetValue(txid, out var existing);
        var tombstoned = state.State.ForgottenAt.ContainsKey(txid);
        switch (TerminalDecisionGuard.Classify(hasExisting, existing, incomingCommitted: true))
        {
            case TerminalRecordAction.Idempotent:
                // The existing row may still be riding an un-durable group
                // commit. Acknowledge only once it is durable; if that write
                // failed the row was rolled back, so re-run against disk state.
                if (!await WhenDurableAsync(txid))
                {
                    await MarkCommittedAsync(txid);
                }
                return;
            case TerminalRecordAction.Conflict when !tombstoned:
                // A conflict against an un-durable verdict is only real if that
                // verdict persists; if its write failed, re-classify.
                if (!await WhenDurableAsync(txid))
                {
                    await MarkCommittedAsync(txid);
                    return;
                }
                throw new InvalidOperationException(
                    $"Cannot mark saga {txid:N} as committed: it was previously recorded as aborted.");
        }

        // Issue #4485: past the guard this call records a NEW decision (a repeat
        // of an existing one returned above). A capture holding the decision
        // gate refuses it; the caller retries once the gate is released.
        ThrowIfDecisionGated();

        // A tombstoned decision is treated as absent for the CONFLICTING-outcome
        // case: the saga has already completed its post-fan-out cleanup, so a
        // fresh Mark carrying a different verdict is a new authoritative outcome
        // rather than a write-once violation. Clear the tombstone AND the stale
        // decision so the record below is unobstructed. (A same-outcome repeat
        // never reaches here - it returned Idempotent above and leaves the
        // tombstone in place, which is what makes the no-op a real no-op.)
        var now = TimeProvider.GetUtcNow();
        var tombstoneClear = ClearTombstone(txid, now, Retention);

        // A locally-recorded decision supersedes any cross-tree delegation:
        // this sub-saga's finalize is the authoritative outcome for this tree.
        //
        // The drop sits BELOW the write-once guard deliberately. Above it, the
        // Idempotent and Conflict exits return without ever reaching a
        // WriteStateAsync, so the rows were gone from memory on a path that
        // persists nothing at all - an unconditional mutation on a no-failure
        // path. Below the guard, every path that reaches the drop also reaches
        // the write, so the drop is either persisted or unwound by the catch.
        var delegations = DropDelegations(txid);

        // Snapshot prior in-memory state so a failing WriteStateAsync
        // can be unwound. Without this, the in-memory dictionary records
        // Committed while disk does not, and the next retry from the
        // same activation hits the `existing == TxStatus.Committed`
        // short-circuit and silently returns without re-persisting.
        var core = DecisionCore();
        var mutation = core.Apply(txid, TxStatus.Committed);
        state.State.DecisionsRevision = core.Revision;
        await CommitAsync(PendingGroup(txid), () =>
        {
            core.Rollback(mutation);
            state.State.DecisionsRevision = core.Revision;
            // Restore EVERY map this call mutated, not just the decision and
            // its revision. A partial unwind leaves the delegation maps ahead
            // of disk, which aliases a still-preparing cross-tree saga as a
            // completed one for the snapshot export, the backup drain gate,
            // and the backup post-capture fence, and destroys the only
            // coordinator pointer GetStatusAsync has to resolve it with.
            RestoreDelegations(txid, delegations);
            RestoreTombstone(txid, tombstoneClear);
        });
    }

    /// <inheritdoc />
    public async Task MarkAbortedAsync(Guid txid)
    {
        // Guard first, clear second - see MarkCommittedAsync for why the
        // ordering is load-bearing, and for the scope of the write-once claim:
        // live decisions only, not across a forget. The defect is a pair, and a
        // remedy applied only to the committed path leaves the abort path
        // defective.
        var hasExisting = state.State.Decisions.TryGetValue(txid, out var existing);
        var tombstoned = state.State.ForgottenAt.ContainsKey(txid);
        switch (TerminalDecisionGuard.Classify(hasExisting, existing, incomingCommitted: false))
        {
            case TerminalRecordAction.Idempotent:
                // Durability barrier: see MarkCommittedAsync.
                if (!await WhenDurableAsync(txid))
                {
                    await MarkAbortedAsync(txid);
                }
                return;
            case TerminalRecordAction.Conflict when !tombstoned:
                if (!await WhenDurableAsync(txid))
                {
                    await MarkAbortedAsync(txid);
                    return;
                }
                throw new InvalidOperationException(
                    $"Cannot mark saga {txid:N} as aborted: it was previously recorded as committed.");
        }

        // Issue #4485: see MarkCommittedAsync.
        ThrowIfDecisionGated();

        var now = TimeProvider.GetUtcNow();
        var tombstoneClear = ClearTombstone(txid, now, Retention);

        // A locally-recorded decision supersedes any cross-tree delegation.
        // Dropped below the write-once guard for the reason spelled out in
        // MarkCommittedAsync.
        var delegations = DropDelegations(txid);

        // Snapshot prior in-memory state so a failing WriteStateAsync
        // can be unwound (see MarkCommittedAsync for the same rationale).
        var core = DecisionCore();
        var mutation = core.Apply(txid, TxStatus.Aborted);
        state.State.DecisionsRevision = core.Revision;
        await CommitAsync(PendingGroup(txid), () =>
        {
            core.Rollback(mutation);
            state.State.DecisionsRevision = core.Revision;
            RestoreDelegations(txid, delegations);
            RestoreTombstone(txid, tombstoneClear);
        });
    }

    /// <summary>
    /// Undo token for the tombstone-clearing step the <c>Mark*</c> paths run
    /// immediately <b>after</b> their write-once guard. The ordering is
    /// load-bearing and the guard's own comment explains why: clearing first
    /// hides the existing row from <c>Classify</c>, so a same-outcome repeat is
    /// classified <c>Record</c> rather than <c>Idempotent</c> and resurrects a
    /// decision the tree had already retired.
    /// Captures whether the clear actually removed
    /// a <see cref="TxRegistryState.ForgottenAt"/> row and a
    /// <see cref="TxRegistryState.Decisions"/> row, plus the values to put back,
    /// so a failing <c>WriteStateAsync</c> restores both rather than leaving the
    /// in-memory maps ahead of the persisted ones.
    /// </summary>
    private readonly record struct TombstoneClear(
        bool ClearedTombstone,
        DateTimeOffset PreviousForgottenAt,
        bool ClearedDecision,
        TxStatus PreviousDecision,
        bool RetiredExpired,
        long? PreviousWalGeneration);

    /// <summary>
    /// Undo token for the cross-tree delegation drop the <c>Mark*</c> paths run
    /// once their write-once guard has admitted the call. Captures each dropped
    /// row's prior coordinator key so a failing <c>WriteStateAsync</c> can put it
    /// back.
    /// </summary>
    private readonly record struct DelegationDrop(
        bool DroppedExternal,
        string? PreviousExternal,
        bool DroppedReceiver,
        string? PreviousReceiver);

    /// <summary>
    /// Clears a tombstone and its stale decision, returning the undo token that
    /// <see cref="RestoreTombstone(Guid, in TombstoneClear)"/> consumes. When the
    /// tombstone was already masked from readers at <paramref name="now"/>, the
    /// removal is also accounted into
    /// <see cref="TxRegistryState.TombstoneRetirementEpoch"/> so the effective
    /// revision does not fall as the live-expired count drops.
    /// </summary>
    private TombstoneClear ClearTombstone(Guid txid, DateTimeOffset now, TimeSpan retention)
    {
        // Read the expiry verdict BEFORE the removal, while the row is still
        // present - IsTombstoneExpiredAt answers false for an absent txid, so
        // probing afterwards would silently under-count every retirement.
        var wasExpired = IsTombstoneExpiredAt(txid, now, retention);

        // Remove(key, out value) is a single hash probe, where a TryGetValue
        // followed by Remove is two. This runs once per saga terminal, so the
        // saving is small, but the shape is the one the rest of the file uses.
        if (!state.State.ForgottenAt.Remove(txid, out var forgottenAt))
        {
            return default;
        }

        InvalidateExpiryMemo();
        if (wasExpired)
        {
            state.State.TombstoneRetirementEpoch++;
        }

        var clearedDecision = state.State.Decisions.Remove(txid, out var previousDecision);
        long? previousWalGeneration = state.State.ForgetWalGenerations.Remove(txid, out var walGeneration) ? walGeneration : null;
        return new TombstoneClear(true, forgottenAt, clearedDecision, previousDecision, wasExpired, previousWalGeneration);
    }

    /// <summary>
    /// Restores the rows a <see cref="ClearTombstone(Guid, DateTimeOffset, TimeSpan)"/>
    /// call removed, including the retirement accounting. Must run
    /// <b>after</b> the decision core's own rollback, which restores the
    /// decision map to its post-clear state.
    /// </summary>
    private void RestoreTombstone(Guid txid, in TombstoneClear clear)
    {
        if (!clear.ClearedTombstone)
        {
            return;
        }

        state.State.ForgottenAt[txid] = clear.PreviousForgottenAt;
        if (clear.PreviousWalGeneration is { } walGeneration)
        {
            state.State.ForgetWalGenerations[txid] = walGeneration;
        }
        InvalidateExpiryMemo();
        if (clear.RetiredExpired)
        {
            // Puts the row back into the live-expired population, so the
            // matching increment has to come back out. Non-decreasing is a
            // property of the token across *observable* states; an unwound
            // write leaves no observable state behind it, and restoring the
            // pair (map, epoch) together is what keeps the two consistent.
            state.State.TombstoneRetirementEpoch--;
        }
        if (clear.ClearedDecision)
        {
            state.State.Decisions[txid] = clear.PreviousDecision;
        }
    }

    /// <summary>
    /// Drops both cross-tree delegation rows for <paramref name="txid"/>,
    /// returning the undo token that
    /// <see cref="RestoreDelegations(Guid, in DelegationDrop)"/> consumes.
    /// </summary>
    private DelegationDrop DropDelegations(Guid txid)
    {
        var hadExternal = state.State.ExternalAuthorities.Remove(txid, out var previousExternal);
        var hadReceiver = state.State.ReceiverDecisionAuthorities.Remove(txid, out var previousReceiver);
        return new DelegationDrop(hadExternal, previousExternal, hadReceiver, previousReceiver);
    }

    /// <summary>
    /// Restores the delegation rows a <see cref="DropDelegations(Guid)"/> removed,
    /// following the save-and-restore-or-remove precedent already used by
    /// <see cref="RegisterExternalDecisionAuthorityAsync(Guid, string)"/>.
    /// </summary>
    private void RestoreDelegations(Guid txid, in DelegationDrop drop)
    {
        if (drop.DroppedExternal && drop.PreviousExternal is not null)
        {
            state.State.ExternalAuthorities[txid] = drop.PreviousExternal;
        }
        if (drop.DroppedReceiver && drop.PreviousReceiver is not null)
        {
            state.State.ReceiverDecisionAuthorities[txid] = drop.PreviousReceiver;
        }
    }

    /// <summary>
    /// Enforces the disjointness premise the two cross-tree delegation maps rest
    /// on: a txid may be delegated to an authoring coordinator or to a receiver
    /// coordinator, never to both. Throws before any mutation, so a rejected
    /// registration leaves the registry exactly as it found it and needs no
    /// unwind.
    /// <para>
    /// The check is on the <b>consequence</b> - two rows coexisting - and
    /// deliberately not on the cause, a terminal arriving with a foreign origin.
    /// The cause is not expressible here: the core holds both maps but holds no
    /// local cluster identity to compare an origin against.
    /// <see cref="ILatticeOriginClusterIdResolver"/> looks like the missing
    /// operand and is not, because its core default resolves to
    /// <see cref="string.Empty"/>; a self-origin comparison built on it would
    /// pass vacuously in precisely the deployment that has no replication
    /// package and therefore most needs the seam guarded. A missing operand
    /// stops an implementer; a present-but-vacuous one lets them ship.
    /// </para>
    /// <para>
    /// Reaching this throw means an invariant several consumers read as given
    /// has already been violated upstream - most plausibly by a caller of the
    /// public replication-apply seam supplying a foreign origin, which
    /// <c>EnsureInternalOrigin</c> does not constrain (it gates who may call,
    /// never what origin is claimed, and is itself a no-op unless
    /// <c>AddLatticeAuth</c> was registered). Failing the registration is the
    /// conservative outcome: the alternative is a registry in which
    /// <c>ResolveAnyDelegatedAsync</c> silently answers from whichever map it
    /// probes first, which is undetectable at every call site.
    /// </para>
    /// </summary>
    private static void ThrowIfWouldCoexist(
        Guid txid,
        Dictionary<Guid, string> otherMap,
        string registering,
        string occupied)
    {
        if (otherMap.ContainsKey(txid))
        {
            throw new InvalidOperationException(
                $"Transaction {txid} is already delegated through {occupied}; "
                + $"registering it in {registering} as well would leave the registry "
                + "with two coordinators for one decision. The two cross-tree "
                + "delegation maps are required to be disjoint per transaction id.");
        }
    }

    /// <inheritdoc />
    public async Task RegisterExternalDecisionAuthorityAsync(Guid txid, string coordinatorKey)
    {        ArgumentException.ThrowIfNullOrEmpty(coordinatorKey);

        // A locally-recorded terminal decision already supersedes any
        // delegation - the sub-saga finalized before (or concurrently with)
        // this registration. Leave the local decision authoritative.
        if (state.State.Decisions.ContainsKey(txid))
        {
            // The local decision may still be riding an un-durable group
            // commit; if that write fails the decision is rolled back and this
            // registration must be applied after all.
            if (!await WhenDurableAsync(txid))
            {
                await RegisterExternalDecisionAuthorityAsync(txid, coordinatorKey);
            }
            return;
        }

        // Idempotent: re-registering the same coordinator is a no-op.
        if (state.State.ExternalAuthorities.TryGetValue(txid, out var existing)
            && string.Equals(existing, coordinatorKey, StringComparison.Ordinal))
        {
            if (!await WhenDurableAsync(txid))
            {
                await RegisterExternalDecisionAuthorityAsync(txid, coordinatorKey);
            }
            return;
        }

        ThrowIfRegistrationFenced();

        ThrowIfWouldCoexist(
            txid,
            state.State.ReceiverDecisionAuthorities,
            registering: nameof(TxRegistryState.ExternalAuthorities),
            occupied: nameof(TxRegistryState.ReceiverDecisionAuthorities));

        state.State.ExternalAuthorities[txid] = coordinatorKey;
        // First registration of this txid: advance the monotonic cross-tree
        // registration epoch so the backup fence can detect a saga that both
        // registers and completes inside a capture window.
        var isNewRegistration = existing is null;
        var prevEpoch = state.State.CrossTreeRegistrationEpoch;
        if (isNewRegistration)
        {
            state.State.CrossTreeRegistrationEpoch = prevEpoch + 1;
        }
        await CommitAsync(PendingGroup(txid), () =>
        {
            if (existing is not null) state.State.ExternalAuthorities[txid] = existing;
            else state.State.ExternalAuthorities.Remove(txid);
            state.State.CrossTreeRegistrationEpoch = prevEpoch;
        });
    }

    /// <inheritdoc />
    public async Task RecordCrossTreeMembershipAsync(Guid txid, string operationId, IReadOnlyList<string> participants)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(participants);
        if (txid == Guid.Empty)
        {
            throw new ArgumentException("A cross-tree membership needs a non-empty transaction id.", nameof(txid));
        }

        if (state.State.CrossTreeMemberships.ContainsKey(txid))
        {
            if (!await WhenDurableAsync(txid))
            {
                await RecordCrossTreeMembershipAsync(txid, operationId, participants);
            }

            return;
        }

        state.State.CrossTreeMemberships[txid] = new CrossTreeMembership
        {
            OperationId = operationId,
            Participants = [.. CanonicalStringSet.SortedDistinct(participants)],
        };
        await CommitAsync(PendingGroup(txid), () => state.State.CrossTreeMemberships.Remove(txid));
    }

    /// <inheritdoc />
    public async Task RecordCrossTreeDecisionStampsAsync(
        Guid txid, IReadOnlyDictionary<string, long> stamps, IReadOnlyDictionary<string, long>? sequences = null)
    {
        ArgumentNullException.ThrowIfNull(stamps);
        if (!state.State.CrossTreeMemberships.TryGetValue(txid, out var membership))
        {
            return;
        }

        if (membership.DecisionStamps is not null && (sequences is null || membership.DecisionSequences is not null))
        {
            if (!await WhenDurableAsync(txid))
            {
                await RecordCrossTreeDecisionStampsAsync(txid, stamps, sequences);
            }

            return;
        }

        state.State.CrossTreeMemberships[txid] = membership with
        {
            DecisionStamps = membership.DecisionStamps ?? stamps.ToImmutableDictionary(StringComparer.Ordinal),
            DecisionSequences = membership.DecisionSequences ?? sequences?.ToImmutableDictionary(StringComparer.Ordinal),
        };
        await CommitAsync(PendingGroup(txid), () => state.State.CrossTreeMemberships[txid] = membership);
    }

    /// <inheritdoc />
    public Task<long?> GetCrossTreeSequenceFloorAsync()
    {
        long? floor = null;
        foreach (var (txid, membership) in state.State.CrossTreeMemberships)
        {
            if (!state.State.Decisions.ContainsKey(txid))
            {
                continue;
            }

            var sequence = membership.DecisionSequences is { } sequences && sequences.TryGetValue(TreeId, out var own) ? own : 0L;
            if (floor is not { } current || sequence < current)
            {
                floor = sequence;
            }
        }

        return Task.FromResult(floor);
    }

    /// <inheritdoc />
    public Task<Dictionary<Guid, CrossTreeMembership>> GetCrossTreeMembershipsAsync(IReadOnlyList<Guid> txids)
    {
        ArgumentNullException.ThrowIfNull(txids);
        var found = new Dictionary<Guid, CrossTreeMembership>();
        foreach (var txid in txids)
        {
            if (state.State.CrossTreeMemberships.TryGetValue(txid, out var membership))
            {
                found[txid] = membership;
            }
        }

        return Task.FromResult(found);
    }

    /// <summary>
    /// Drops the cross-tree membership of every saga whose decision and
    /// delegation are both gone (issue #4683): the saga was purged, so no export
    /// can ship its decision row. In memory only; the caller's write persists
    /// it, and losing it to a failed write keeps a few stale rows a later pass
    /// drops.
    /// </summary>
    private void DropPurgedCrossTreeMemberships()
    {
        if (state.State.CrossTreeMemberships.Count == 0)
        {
            return;
        }

        List<Guid>? purged = null;
        foreach (var txid in state.State.CrossTreeMemberships.Keys)
        {
            if (!state.State.Decisions.ContainsKey(txid) && !state.State.ExternalAuthorities.ContainsKey(txid))
            {
                (purged ??= []).Add(txid);
            }
        }

        foreach (var txid in purged ?? [])
        {
            state.State.CrossTreeMemberships.Remove(txid);
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
            // Durability barrier: see RegisterExternalDecisionAuthorityAsync.
            if (!await WhenDurableAsync(txid))
            {
                await RegisterReceiverDecisionAuthorityAsync(txid, receiverCoordinatorKey);
            }
            return;
        }

        // Idempotent: re-registering the same receiver coordinator is a no-op.
        if (state.State.ReceiverDecisionAuthorities.TryGetValue(txid, out var existing)
            && string.Equals(existing, receiverCoordinatorKey, StringComparison.Ordinal))
        {
            if (!await WhenDurableAsync(txid))
            {
                await RegisterReceiverDecisionAuthorityAsync(txid, receiverCoordinatorKey);
            }
            return;
        }

        ThrowIfRegistrationFenced();

        ThrowIfWouldCoexist(
            txid,
            state.State.ExternalAuthorities,
            registering: nameof(TxRegistryState.ReceiverDecisionAuthorities),
            occupied: nameof(TxRegistryState.ExternalAuthorities));

        state.State.ReceiverDecisionAuthorities[txid] = receiverCoordinatorKey;
        // First registration of this txid: advance the monotonic cross-tree
        // registration epoch (see RegisterExternalDecisionAuthorityAsync).
        var isNewRegistration = existing is null;
        var prevEpoch = state.State.CrossTreeRegistrationEpoch;
        if (isNewRegistration)
        {
            state.State.CrossTreeRegistrationEpoch = prevEpoch + 1;
        }
        await CommitAsync(PendingGroup(txid), () =>
        {
            if (existing is not null) state.State.ReceiverDecisionAuthorities[txid] = existing;
            else state.State.ReceiverDecisionAuthorities.Remove(txid);
            state.State.CrossTreeRegistrationEpoch = prevEpoch;
        });
    }

    /// <summary>
    /// Resolves a single receiver-delegated <paramref name="txid"/> against its
    /// receiver coordinator (<see cref="ILatticeCrossTreeReceiverGrain"/>),
    /// mirroring <see cref="ResolveDelegatedAsync"/> but for the receiver-side
    /// barrier. Returns <see cref="TxStatus.InFlight"/> while the receiver
    /// coordinator's wait set is incomplete; caches a terminal verdict into
    /// <see cref="TxRegistryState.Decisions"/> and drops the delegation entry
    /// once resolved. A failed coordinator dial surfaces as
    /// <see cref="TxStatus.Indeterminate"/>, which hides the saga's prepared
    /// keys rather than disclosing their pre-saga values.
    /// <para>
    /// Issue #4485. With <paramref name="terminalIntent"/> the caller is about to
    /// apply a terminal on the answer, so a verdict is returned only once it is
    /// durably cached here (a failed cache write reads
    /// <see cref="TxStatus.InFlight"/>), and nothing is dialled while a capture
    /// holds the decision gate. Without it (a reader), a gated registry still
    /// returns the coordinator's verdict but does not cache it.
    /// </para>
    /// </summary>
    private async Task<TxStatus> ResolveReceiverDelegatedAsync(Guid txid, string receiverCoordinatorKey, bool terminalIntent = false)
    {
        if (terminalIntent && IsDecisionGated(out _))
        {
            return TxStatus.InFlight;
        }

        TxStatus verdict;
        try
        {
            var coordinator = grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(receiverCoordinatorKey);
            verdict = await coordinator.GetDecisionAsync();
        }
        catch (Exception ex)
        {
            // We could not reach the only authority for this saga, so we do not
            // know its outcome. That is Indeterminate, not InFlight: InFlight is
            // a positive claim that the saga has not yet decided, and the
            // visibility gate acts on it by serving the pre-saga value. One
            // failed grain call - a rolling restart, a rebalance - is enough to
            // reach here, so the disclosure flapped rather than persisting,
            // which is why it presented as an intermittent read anomaly.
            //
            // Note the state-write catch further down deliberately does the
            // opposite and returns the true verdict: there we HAVE the answer
            // and merely failed to cache it, so suppressing it would discard
            // knowledge we hold. The two catches differ because what is unknown
            // differs, not because one of them is conservative and the other is
            // not.
            logger.LogWarning(
                ex,
                "Registry {TreeId} could not reach receiver coordinator {Coordinator} for saga {TxId}; reporting the outcome as indeterminate.",
                this.GetPrimaryKeyString(),
                receiverCoordinatorKey,
                txid);
            return TxStatus.Indeterminate;
        }

        if (verdict == TxStatus.InFlight)
        {
            return TxStatus.InFlight;
        }

        if (!state.State.Decisions.ContainsKey(txid))
        {
            // Issue #4485: a capture holding the decision gate admits no new
            // local decision, and a cached verdict is one.
            if (IsDecisionGated(out _))
            {
                return terminalIntent ? TxStatus.InFlight : verdict;
            }

            var core = DecisionCore();
            var mutation = core.Apply(txid, verdict);
            var removed = state.State.ReceiverDecisionAuthorities.Remove(txid);
            state.State.DecisionsRevision = core.Revision;
            try
            {
                await CommitAsync(PendingGroup(txid), () =>
                {
                    core.Rollback(mutation);
                    if (removed) state.State.ReceiverDecisionAuthorities[txid] = receiverCoordinatorKey;
                    state.State.DecisionsRevision = core.Revision;
                });
            }
            catch
            {
                // The group commit already rolled the cache back; the verdict
                // itself is durable at the coordinator, so it is still served
                // to a reader. A terminal-applying caller must not act on a
                // verdict that is not recorded here (issue #4485).
                if (terminalIntent)
                {
                    return TxStatus.InFlight;
                }
            }
        }
        return verdict;
    }

    /// <summary>
    /// Resolves a txid that has no local decision against whichever cross-tree
    /// delegation map (authoring-side <see cref="TxRegistryState.ExternalAuthorities"/>
    /// or receiver-side <see cref="TxRegistryState.ReceiverDecisionAuthorities"/>)
    /// carries it, else returns <see cref="TxStatus.InFlight"/>.
    /// <para>
    /// The probe order is authoring-side first, and it is only sound because a
    /// txid is never present in both maps - a premise stated on both members of
    /// <see cref="TxRegistryState"/> and enforced at the two registration sites
    /// by <c>ThrowIfWouldCoexist</c>. It is not self-evident and is not
    /// established by the coordinator-placement argument on the receiver map,
    /// which is a different claim. Were it to fail, this method would answer
    /// from the authoring coordinator and every other consumer would answer from
    /// whichever map it probed first, with no call site able to observe the
    /// divergence.
    /// </para>
    /// </summary>
    private async Task<TxStatus> ResolveAnyDelegatedAsync(Guid txid, bool terminalIntent = false)
    {
        if (state.State.ExternalAuthorities.TryGetValue(txid, out var coordinatorKey))
        {
            return await ResolveDelegatedAsync(txid, coordinatorKey, terminalIntent);
        }
        if (state.State.ReceiverDecisionAuthorities.TryGetValue(txid, out var receiverKey))
        {
            return await ResolveReceiverDelegatedAsync(txid, receiverKey, terminalIntent);
        }
        return TxStatus.InFlight;
    }

    /// <summary>
    /// Resolves a single delegated <paramref name="txid"/> against its
    /// coordinator. While the coordinator is still preparing this returns
    /// <see cref="TxStatus.InFlight"/> without touching state. Once the
    /// coordinator's verdict is terminal it is cached into
    /// <see cref="TxRegistryState.Decisions"/> (bumping the revision) and the
    /// delegation entry is dropped, so later reads resolve locally. A failed
    /// coordinator dial surfaces as <see cref="TxStatus.Indeterminate"/>, which
    /// keeps the cross-tree batch hidden on this tree until it can be resolved
    /// rather than disclosing the participating keys' pre-saga values.
    /// <para>
    /// Issue #4485: <paramref name="terminalIntent"/> and the decision gate act
    /// exactly as on <see cref="ResolveReceiverDelegatedAsync"/>.
    /// </para>
    /// </summary>
    private async Task<TxStatus> ResolveDelegatedAsync(Guid txid, string coordinatorKey, bool terminalIntent = false)
    {
        if (terminalIntent && IsDecisionGated(out _))
        {
            return TxStatus.InFlight;
        }

        TxStatus verdict;
        try
        {
            var coordinator = grainFactory.GetGrain<ILatticeCrossTreeTxGrain>(coordinatorKey);
            verdict = await coordinator.GetDecisionAsync();
        }
        catch (Exception ex)
        {
            // Authoring-side twin of the receiver-side catch above, and fixed
            // for the same reason: an unreachable coordinator means we do not
            // know the outcome, which is Indeterminate. Patching only one of
            // the two looks complete while leaving the other live, because
            // ResolveAnyDelegatedAsync reaches both.
            logger.LogWarning(
                ex,
                "Registry {TreeId} could not reach cross-tree coordinator {Coordinator} for saga {TxId}; reporting the outcome as indeterminate.",
                this.GetPrimaryKeyString(),
                coordinatorKey,
                txid);
            return TxStatus.Indeterminate;
        }

        if (verdict == TxStatus.InFlight)
        {
            return TxStatus.InFlight;
        }

        // Terminal: cache locally so the global flip is durable on this tree
        // and future reads need no further coordinator round-trips.
        if (!state.State.Decisions.ContainsKey(txid))
        {
            // Issue #4485: a capture holding the decision gate admits no new
            // local decision, and a cached verdict is one.
            if (IsDecisionGated(out _))
            {
                return terminalIntent ? TxStatus.InFlight : verdict;
            }

            var core = DecisionCore();
            var mutation = core.Apply(txid, verdict);
            var removed = state.State.ExternalAuthorities.Remove(txid);
            state.State.DecisionsRevision = core.Revision;
            try
            {
                await CommitAsync(PendingGroup(txid), () =>
                {
                    core.Rollback(mutation);
                    if (removed) state.State.ExternalAuthorities[txid] = coordinatorKey;
                    state.State.DecisionsRevision = core.Revision;
                });
            }
            catch
            {
                // Surface the resolved verdict for this read even though the
                // cache write failed (the group commit already rolled the cache
                // back); the next read re-dials and re-attempts. A
                // terminal-applying caller must not act on a verdict that is not
                // recorded here (issue #4485).
                if (terminalIntent)
                {
                    return TxStatus.InFlight;
                }
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
    /// <para>
    /// While a capture holds the decision gate (issue #4485) a terminal verdict
    /// is not cached; it is returned in <c>Uncached</c> instead, so a reader's
    /// snapshot still reflects the coordinator's decision without recording a
    /// new local decision.
    /// </para>
    /// </summary>
    /// <returns>
    /// The number of delegations whose coordinator could not be reached, so a
    /// caller can tell "still preparing" apart from "could not find out". Every
    /// such delegation is also still counted among the remaining entries, which
    /// is the correct conservative accounting; this figure names the subset of
    /// that count which is unreachable rather than pending. Also returns the
    /// terminal verdicts the gate kept from being cached, or
    /// <see langword="null"/> when there are none.
    /// </returns>
    /// <param name="unresolvable">
    /// When supplied, receives the txid of every delegation whose coordinator
    /// could not be reached, so a snapshot can report it as
    /// <see cref="TxStatus.Indeterminate"/> rather than omit it (issue #4448).
    /// </param>
    private async Task<(int Unresolvable, Dictionary<Guid, TxStatus>? Uncached)> ResolveAllDelegatedAsync(List<Guid>? unresolvable = null)
    {
        var unresolvableCount = 0;
        Dictionary<Guid, TxStatus>? uncached = null;
        if (state.State.ExternalAuthorities.Count > 0)
        {
            // Snapshot the pending delegations: ResolveDelegatedAsync mutates the
            // ExternalAuthorities map when a verdict turns terminal.
            var pending = new List<KeyValuePair<Guid, string>>(state.State.ExternalAuthorities);
            foreach (var (txid, coordinatorKey) in pending)
            {
                if (state.State.Decisions.ContainsKey(txid)) continue;
                var verdict = await ResolveDelegatedAsync(txid, coordinatorKey);
                if (verdict == TxStatus.Indeterminate)
                {
                    unresolvableCount++;
                    unresolvable?.Add(txid);
                }
                else if (verdict is (TxStatus.Committed or TxStatus.Aborted) && !state.State.Decisions.ContainsKey(txid))
                {
                    (uncached ??= new Dictionary<Guid, TxStatus>())[txid] = verdict;
                }
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
                var verdict = await ResolveReceiverDelegatedAsync(txid, receiverKey);
                if (verdict == TxStatus.Indeterminate)
                {
                    unresolvableCount++;
                    unresolvable?.Add(txid);
                }
                else if (verdict is (TxStatus.Committed or TxStatus.Aborted) && !state.State.Decisions.ContainsKey(txid))
                {
                    (uncached ??= new Dictionary<Guid, TxStatus>())[txid] = verdict;
                }
            }
        }
        return (unresolvableCount, uncached);
    }

    /// <summary>
    /// Whether any cross-tree delegation is active, so the snapshot paths can
    /// skip allocating an unresolvable-txid list in the common case of none.
    /// </summary>
    private bool HasDelegations =>
        state.State.ExternalAuthorities.Count > 0 || state.State.ReceiverDecisionAuthorities.Count > 0;

    /// <summary>
    /// Adds an <see cref="TxStatus.Indeterminate"/> entry to a snapshot for
    /// every delegation in <paramref name="unresolvable"/> that is still
    /// delegated and still has no local decision, matching what
    /// <see cref="GetStatusAsync(Guid)"/> answers for the same txid (issue
    /// #4448). Omitting it would read as <see cref="TxStatus.InFlight"/>, which
    /// the visibility gate acts on by serving the saga's pre-saga values: an
    /// affirmative claim that a saga whose coordinator could not be reached did
    /// not commit, while sibling trees may already show it committed.
    /// <para>
    /// A txid the resolution pass could not settle may have been settled since
    /// by an interleaved call, so it is re-checked here, synchronously, inside
    /// the caller's snapshot-building block. Carrying the mask does not move
    /// the revision token: a reader holding this snapshot keeps the key hidden
    /// until the next decision mutation moves it, which the coordinator's own
    /// finalisation of this tree produces once it decides.
    /// </para>
    /// </summary>
    private void MaskUnresolvableDelegations(Dictionary<Guid, TxStatus> snapshot, List<Guid>? unresolvable)
    {
        if (unresolvable is null)
            return;

        foreach (var txid in unresolvable)
        {
            if (snapshot.ContainsKey(txid) || state.State.Decisions.ContainsKey(txid))
                continue;

            if (state.State.ExternalAuthorities.ContainsKey(txid)
                || state.State.ReceiverDecisionAuthorities.ContainsKey(txid))
            {
                snapshot[txid] = TxStatus.Indeterminate;
            }
        }
    }

    /// <inheritdoc />
    public async Task<CrossTreeInFlightObservation> ObserveCrossTreeInFlightAsync()
    {
        while (true)
        {
            var observation = await ObserveCrossTreeInFlightCoreAsync();
            // Read-committed: the counts are tree-wide, so they may only be
            // reported once every mutation they could reflect is durable.
            if (await WhenAllDurableAsync())
            {
                return observation;
            }
        }
    }

    private async Task<CrossTreeInFlightObservation> ObserveCrossTreeInFlightCoreAsync()
    {
        // Resolve every active delegation against its coordinator first, so a
        // saga whose coordinator has already decided is caches-and-dropped and
        // no longer counts as in-flight.
        //
        // What remains delegated afterwards is NOT uniformly "genuinely still
        // preparing". A resolve whose coordinator dial fails returns early
        // without dropping the entry, so the saga stays counted here. That is
        // the right conservative accounting - an unreachable coordinator is not
        // evidence of a decision - but it is a different fact, and folding it
        // into the in-flight count made a connectivity fault report as healthy
        // pipelining. UnresolvableCount below reports the unreachable subset
        // separately so a fence reading this observation can distinguish
        // "sagas are still running" from "I could not find out".
        var (unresolvable, _) = await ResolveAllDelegatedAsync();

        var inFlight = state.State.ExternalAuthorities.Count
            + state.State.ReceiverDecisionAuthorities.Count;

        return new CrossTreeInFlightObservation(
            inFlight,
            state.State.CrossTreeRegistrationEpoch,
            unresolvable);
    }

    /// <inheritdoc />
    public async Task<TxStatus> GetStatusAsync(Guid txid)
    {
        while (true)
        {
            var status = await ReadStatusAsync(txid);
            // Read-committed: never report a verdict (or an absence) that a
            // not-yet-durable mutation produced. Completes synchronously when
            // nothing un-durable touches this txid.
            if (await WhenDurableAsync(txid))
            {
                return status;
            }
        }
    }

    /// <summary>
    /// The unguarded body of <see cref="GetStatusAsync(Guid)"/>: the answer the
    /// in-memory state gives, before the read-committed barrier.
    /// </summary>
    private async ValueTask<TxStatus> ReadStatusAsync(Guid txid)
    {
        if (IsTombstoneExpired(txid))
        {
            // Tombstone TTL elapsed. The decision is not physically purged here
            // (purging happens lazily inside ForgetAsync via PruneExpired) so
            // GetStatusAsync stays a pure read with no state-write side effects.
            //
            // If a row is still stored we know a decision was made and know we
            // are no longer entitled to report it, which is Indeterminate - NOT
            // InFlight. Reporting InFlight here was the defect: the visibility
            // gate reads InFlight as "fall through to the pre-saga value", which
            // is an affirmative claim that the saga did not commit, and it also
            // told the leaf sweep the saga was still running so the stranded
            // prepare it would have resolved was left in place. One line both
            // corrupted the read and disabled the heal.
            //
            // A txid with no stored row is genuinely absent and stays InFlight.
            return state.State.Decisions.ContainsKey(txid)
                ? TxStatus.Indeterminate
                : TxStatus.InFlight;
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
    public async Task<TxStatus> GetRecordedStatusAsync(Guid txid)
    {
        while (true)
        {
            var status = state.State.Decisions.TryGetValue(txid, out var recorded)
                ? recorded
                : TxStatus.InFlight;
            if (await WhenDurableAsync(txid))
            {
                return status;
            }
        }
    }

    /// <inheritdoc />
    public async Task<Dictionary<Guid, TxStatus>> GetStatusManyAsync(IReadOnlyList<Guid> txids)
    {
        ArgumentNullException.ThrowIfNull(txids);
        while (true)
        {
            var result = await ReadStatusManyAsync(txids);
            if (await WhenDurableAsync(txids))
            {
                return result;
            }
        }
    }

    /// <summary>
    /// The unguarded body of <see cref="GetStatusManyAsync(IReadOnlyList{Guid})"/>.
    /// </summary>
    private async Task<Dictionary<Guid, TxStatus>> ReadStatusManyAsync(IReadOnlyList<Guid> txids)
    {
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
                // Same rule as GetStatusAsync: a stored row we may no longer
                // report is Indeterminate, a txid we have never heard of is
                // InFlight. Kept textually parallel so the two cannot drift.
                result[txid] = state.State.Decisions.ContainsKey(txid)
                    ? TxStatus.Indeterminate
                    : TxStatus.InFlight;
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
        while (true)
        {
            var snapshot = await ReadSnapshotAsync();
            // Read-committed: a tree-wide snapshot is only handed out once every
            // mutation it could reflect is durable.
            if (await WhenAllDurableAsync())
            {
                return snapshot;
            }
        }
    }

    private async Task<Dictionary<Guid, TxStatus>> ReadSnapshotAsync()
    {
        // Resolve any active cross-tree delegations against their coordinator
        // BEFORE the synchronous dict-build below. A coordinator-decided but
        // not-yet-finalized sub-saga would otherwise be omitted from the
        // snapshot (no local decision) and read as InFlight - invisible on
        // this tree while a sibling tree that already finalized shows the
        // value, a partial cross-tree view. Resolving here caches terminal
        // verdicts into Decisions so the snapshot reflects the global flip.
        // A delegation whose coordinator could not be reached is collected
        // and carried as Indeterminate below, as the point path reports it.
        var unresolvable = HasDelegations ? new List<Guid>() : null;
        var (_, uncachedVerdicts) = await ResolveAllDelegatedAsync(unresolvable);

        // Return a defensive copy so callers cannot mutate the
        // registry's persisted state through the returned reference.
        //
        // An expired tombstone is reported as Indeterminate rather than
        // omitted. Omission was the bug: this dictionary is the payload a
        // bootstrapping cluster's snapshot export is built from, and the
        // receiver's contract reads an absent txid as "InFlight or absent".
        // So an aged-out COMMITTED saga arrived at the receiver
        // indistinguishable from one still preparing - and because the
        // terminal is outside the incremental stream that follows the
        // snapshot, nothing later corrected it. Carrying the row with an
        // explicit "cannot determine" value gives the receiver (and every
        // local reader resolving against an ambient snapshot) the vocabulary
        // to hide the key instead of falling through to its pre-saga value.
        //
        // Active tombstones (within retention) are still included with their
        // recorded outcome - they are queryable and the snapshot must agree
        // with the per-txid API, which reports them the same way.
        var now = TimeProvider.GetUtcNow();
        var retention = Retention;
        var result = new Dictionary<Guid, TxStatus>(state.State.Decisions.Count);
        foreach (var (txid, status) in state.State.Decisions)
        {
            result[txid] = IsTombstoneExpiredAt(txid, now, retention)
                ? TxStatus.Indeterminate
                : status;
        }
        MaskUnresolvableDelegations(result, unresolvable);
        MergeUncachedVerdicts(result, uncachedVerdicts);
        return result;
    }

    /// <inheritdoc />
    public async Task<TxRegistrySnapshot> SnapshotWithRevisionAsync()
    {
        while (true)
        {
            var snapshot = await ReadSnapshotWithRevisionAsync();
            // Read-committed: the (dict, revision) pair must name a durable
            // state. Were an un-durable pair handed out and its write then
            // failed, the rollback could later reuse the same revision for a
            // different decision map, and a reader's revision probe would take
            // its stale snapshot as current.
            if (await WhenAllDurableAsync())
            {
                return snapshot;
            }
        }
    }

    private async Task<TxRegistrySnapshot> ReadSnapshotWithRevisionAsync()
    {
        // Resolve cross-tree delegations first (see SnapshotAsync). The
        // dict + revision are then captured in one synchronous block with no
        // intervening await, so both reflect the exact same persisted state
        // (including any verdicts just cached by the resolution pass).
        // Unreachable delegations are carried as Indeterminate (#4448).
        var unresolvable = HasDelegations ? new List<Guid>() : null;
        var (_, uncachedVerdicts) = await ResolveAllDelegatedAsync(unresolvable);

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
            // Expired rows are carried as Indeterminate, not dropped - see
            // SnapshotAsync for why omission was unsound. The revision term
            // below counts exactly these rows, so the token still moves when a
            // row crosses into the masked state and a reader holding the older
            // snapshot is still invalidated on the fast path.
            dict[txid] = IsTombstoneExpiredAt(txid, now, retention)
                ? TxStatus.Indeterminate
                : status;
        }
        MaskUnresolvableDelegations(dict, unresolvable);
        MergeUncachedVerdicts(dict, uncachedVerdicts);
        return new TxRegistrySnapshot
        {
            Decisions = dict,
            // Stamped from the SAME `now` and `retention` the mask above used.
            // The token's third term counts the rows that mask filtered out, so
            // taking a second clock reading here could stamp a revision that
            // reflects one more expiry than the dictionary does - a snapshot
            // whose own token already disagrees with it.
            Revision = EffectiveDecisionsRevision(now, retention),
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
        //
        // The value is the composite token, not the bare counter - see
        // EffectiveDecisionsRevision. The interleaving argument above is
        // unchanged by that: the two extra terms are likewise read
        // synchronously inside this turn, and the expiry term is a pure
        // function of ForgottenAt and the clock, which the writers mutate in
        // the same pre-await block as the decision map.
        //
        // Group commit (#3475) and durability. Unlike every other reader, this
        // probe deliberately does NOT wait for the pending group commit, and
        // may return a token that includes mutations not yet durable. That is
        // safe, and group commit does not make it less so, because of how the
        // one consumer uses it: LatticeGrain.ClassifySnap2Async compares the
        // probe for EQUALITY with the token of a snapshot it already holds, and
        // snapshots are now read-committed, so that token always names a
        // durable state. Every un-durable decision-view mutation advances the
        // token past the durable value, so an un-durable probe either equals
        // the durable token (nothing pending changes the view: the snapshot is
        // current) or exceeds it (the reader falls back to a fresh, barriered
        // snapshot - a conservative extra fetch, never a stale accept). A
        // failed write rolls the token back to the durable value, which again
        // names the snapshot's state. Waiting here would put the probe back
        // behind the registry's writes, which is the queueing this change
        // exists to remove.
        return Task.FromResult(EffectiveDecisionsRevision(TimeProvider.GetUtcNow(), Retention));
    }

    /// <inheritdoc />
    public async Task ForgetAsync(Guid txid)
    {
        // Advance the decision-purge guard before this call mutates anything,
        // so the prune below sees its latest cleared generation (#4508).
        await RefreshWalPurgeGuardAsync();
        await RefreshWalPurgeHoldAsync();
        await RefreshCrossTreeHoldAsync();

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
        var hadWalStamp = state.State.ForgetWalGenerations.TryGetValue(txid, out var prevWalStamp);
        var stampedWal = false;
        if (hadDecision)
        {
            if (retention == TimeSpan.Zero && !WalPurgeGuardApplies)
            {
                // Legacy semantic: tombstoning disabled, drop the
                // decision immediately. A tree the decision-purge guard
                // covers tombstones anyway (#4508): dropping here would purge
                // a decision while the WAL can still retain its prepares. Equivalent to the original
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
                InvalidateExpiryMemo();
                if (CurrentWalStamp() is { } walStamp)
                {
                    state.State.ForgetWalGenerations[txid] = walStamp;
                    stampedWal = true;
                }
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

        // Every row PruneExpired removes was already masked from readers at
        // `now` (it prunes on exactly the IsTombstoneExpiredAt predicate, and
        // the zero-retention flush path masks unconditionally), so retiring
        // them drops the effective revision's live-expired term by the same
        // count. Account for it here so the token stays non-decreasing: the
        // revision advance below fires once per batch, not once per row, so
        // for a batch of k > 1 the counter alone cannot cover the loss.
        var retired = pruned.Tombstones?.Count ?? 0;
        state.State.TombstoneRetirementEpoch += retired;

        var changed = droppedDecision || addedForgottenAt || droppedParticipants
            || droppedArrivals || droppedExpected || droppedAuthority
            || droppedReceiverAuthority
            || pruned.Any;

        if (changed)
        {
            // Bump the decisions revision whenever the Decisions map
            // itself mutated (legacy zero-retention drop OR a physical
            // tombstone prune). Pure Participants/Arrivals/Expected removals
            // are invisible to readers and need no bump. The local also feeds
            // the catch block's rollback.
            //
            // A first-tombstone insert into ForgottenAt needs none either, but
            // NOT for the reason this comment used to give. It is not that the
            // insert leaves the readable surface unchanged and the matter ends
            // there: the insert is precisely what ARMS a later unannounced
            // change, because the row it adds will silently drop out of the
            // readable surface the instant it crosses its retention boundary,
            // with no write anywhere to bump a counter. That transition is
            // covered by the live-expired term of the effective revision (see
            // EffectiveDecisionsRevision), which is why no bump is needed here
            // - the insert is accounted for continuously rather than once.
            var revisionBumped = droppedDecision
                || (pruned.Tombstones is { Count: > 0 });
            // The core is the sole revision authority: the intricate map
            // mutations above are left inline (the core does not model the
            // participant / delegation / pin maps), and only the single
            // per-batch revision advance is routed through it so the counter
            // stays owned by one place. Constructed unconditionally so the
            // catch block can reach it; this is the cold Forget cleanup path,
            // not the reader hot path, so the per-call allocation is fine.
            var core = DecisionCore();
            var prevRevision = state.State.DecisionsRevision;
            if (revisionBumped)
            {
                core.AdvanceRevision();
                state.State.DecisionsRevision = core.Revision;
            }

            // The prune retires OTHER txids' expired rows (their reads flip
            // from Indeterminate to InFlight) and may evict pins (which change
            // every txid's read mask), so the group is marked as touching them
            // too: a reader of any of those must wait for this write.
            var group = PendingGroup(txid);
            if (pruned.Tombstones is { } prunedTombstones)
            {
                foreach (var entry in prunedTombstones)
                {
                    group.AddTxid(entry.Txid);
                }
            }
            if (pruned.ExpiredPins is not null)
            {
                group.TouchesAllTxids = true;
            }

            await CommitAsync(group, () =>
            {
                if (droppedDecision) state.State.Decisions[txid] = prevStatus;
                if (addedForgottenAt)
                {
                    state.State.ForgottenAt.Remove(txid);
                    InvalidateExpiryMemo();
                }
                if (stampedWal)
                {
                    if (hadWalStamp) state.State.ForgetWalGenerations[txid] = prevWalStamp;
                    else state.State.ForgetWalGenerations.Remove(txid);
                }
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
                        if (entry.WalGeneration is { } walGeneration)
                            state.State.ForgetWalGenerations[entry.Txid] = walGeneration;
                    }

                    // The rows are back in the live-expired population, so the
                    // matching retirement accounting has to come back out.
                    state.State.TombstoneRetirementEpoch -= retired;
                    InvalidateExpiryMemo();
                }
                if (pruned.ExpiredPins is { } evicted)
                {
                    foreach (var (pinId, pin) in evicted)
                    {
                        state.State.SnapshotPins[pinId] = pin;
                    }
                    InvalidatePinMemo();
                }
                if (revisionBumped)
                {
                    core.RollbackRevision(prevRevision);
                    state.State.DecisionsRevision = core.Revision;
                }
            });
        }
        else if (!await WhenDurableAsync(txid))
        {
            // Nothing to do against the in-memory state, but that state may
            // itself be un-durable (a Forget of this txid riding a pending
            // write). Acknowledge only once it is durable; if that write failed
            // it was rolled back, so this call must do the work itself.
            await ForgetAsync(txid);
        }
    }

    /// <summary>
    /// Whether the participant registration being served is a forwarded prepare's
    /// (<see cref="LatticeForwardedPrepareContext"/>) for a saga this cluster
    /// authored (no <see cref="LatticeOriginContext"/>) whose decision this
    /// registry records. Such a registration joins an existing row but never
    /// creates one (issue #4632): the saga's coordinator holds the row from
    /// before its first prepare until <see cref="ForgetAsync"/>, so an absent row
    /// means the saga was forgotten, and a late forward that recreated it would
    /// make the forgotten saga read as live to the destination leaf's refusal.
    /// A replicated prepare's saga is never forgotten here, and its row may be
    /// held by another registry, so its registration is unchanged.
    /// </summary>
    private bool JoinsOnly() =>
        LatticeOriginContext.Current is null
        && LatticeForwardedPrepareContext.RegistryTreeId is { } registryTreeId
        && string.Equals(registryTreeId, TreeId, StringComparison.Ordinal);

    /// <inheritdoc />
    public async Task RegisterParticipantAsync(Guid txid, int shardIndex)
    {
        var createdSet = false;
        if (!state.State.Participants.TryGetValue(txid, out var set))
        {
            if (JoinsOnly())
            {
                // A forwarded prepare of a locally authored saga only joins the
                // row its coordinator holds from before its first prepare until
                // ForgetAsync. With no row the saga is forgotten, and recreating
                // the row would make it read as live again (issue #4632).
                if (!await WhenDurableAsync(txid))
                {
                    await RegisterParticipantAsync(txid, shardIndex);
                }
                return;
            }

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
            //
            // The existing slot is normally long durable (the saga's bulk
            // RegisterParticipantsAsync landed before its first prepare), in
            // which case this completes synchronously and never waits behind
            // another saga's write. Only if the slot is still riding an
            // un-durable group does it wait, and re-register if that failed.
            if (!await WhenDurableAsync(txid))
            {
                await RegisterParticipantAsync(txid, shardIndex);
            }
            return;
        }

        await CommitAsync(PendingGroup(txid), () =>
        {
            // Unwind the in-memory mutation so a retry from the same
            // activation does not hit the `!set.Add(shardIndex)`
            // short-circuit and silently no-op with disk still stale.
            set.Remove(shardIndex);
            if (createdSet) state.State.Participants.Remove(txid);
        });
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
            if (!await WhenDurableAsync(txid))
            {
                await RegisterParticipantsAsync(txid, shardIndices);
            }
            return;
        }

        await CommitAsync(PendingGroup(txid), () =>
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
        });
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<int>> GetParticipantsAsync(Guid txid)
    {
        while (true)
        {
            IReadOnlyList<int> participants;
            if (!state.State.Participants.TryGetValue(txid, out var set) || set.Count == 0)
            {
                participants = Array.Empty<int>();
            }
            else
            {
                var sorted = new int[set.Count];
                set.CopyTo(sorted);
                Array.Sort(sorted);
                participants = sorted;
            }

            if (await WhenDurableAsync(txid))
            {
                return participants;
            }
        }
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
        if (TerminalArrivalTally.IsUngated(expectedShardCount))
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
        // corrupting the gate. Routed through the shared write-once
        // TerminalDecisionGuard so it is the same rule the Mark* paths use.
        //
        // The same rule, over a different scope (issue #2331 asked for this
        // call site to be audited): hasExisting is "a row is stored", so a
        // conflict is rejected against any row still held - tombstoned or
        // not, unlike the Mark* paths, which re-record over a tombstone - and
        // a row that has been purged classifies as Record. Rejecting is the
        // conservative choice on a receiver, which has no business replacing
        // an outcome it was told; the purge case is the retention window
        // TerminalDecisionGuard documents, not a gap special to this path.
        var hasExisting = state.State.Decisions.TryGetValue(txid, out var existing);
        if (TerminalDecisionGuard.Classify(hasExisting, existing, committed) == TerminalRecordAction.Conflict)
        {
            // A conflict against an un-durable verdict is only real if that
            // verdict persists; if its write failed, re-evaluate.
            if (!await WhenDurableAsync(txid))
            {
                return await RecordTerminalArrivalAsync(txid, sourceShardIndex, committed, expectedShardCount);
            }
            throw new InvalidOperationException(committed
                ? $"Saga {txid:N} received a commit terminal after an abort was already recorded."
                : $"Saga {txid:N} received an abort terminal after a commit was already recorded.");
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
        var newExpected = TerminalArrivalTally.MergeExpected(expectedHadEntry, prevExpected, expectedShardCount);
        var expectedChanged = !expectedHadEntry || newExpected != prevExpected;
        if (expectedChanged)
        {
            state.State.ExpectedTerminals[txid] = newExpected;
        }

        // The tally verdict is taken NOW, in the same synchronous block as this
        // arrival's own mutation, and not after the write below. The write is a
        // group commit and this method interleaves, so other arrivals for the
        // same saga can land in the same group while it is outstanding; reading
        // the count after the await would let every one of them see the full
        // tally and each report itself as the final arrival.
        var isFinal = TerminalArrivalTally.IsFinalArrival(arrivals.Count, newExpected);
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

        if (arrivalAdded || expectedChanged)
        {
            await CommitAsync(PendingGroup(txid), () =>
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
            });
        }
        else if (!await WhenDurableAsync(txid))
        {
            // A duplicate delivery re-evaluated against a tally that was still
            // riding an un-durable write, and that write failed: the tally was
            // rolled back, so record this arrival afresh.
            return await RecordTerminalArrivalAsync(txid, sourceShardIndex, committed, expectedShardCount);
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
        var expiresAt = ResolvePinExpiry(now, ttl, options);

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

        // Count the rows this pin is about to un-mask: tombstoned decisions that
        // are masked from readers right now and will not be once the pin lands.
        // The read mask is pin-aware, so those rows re-enter the readable
        // surface, and the live-expired term of the revision token drops by
        // exactly this count. Compensate it (see TombstonePinUnmaskEpoch) so the
        // token cannot fall, and add one more so it moves strictly across a
        // mutation that genuinely changed what readers can see.
        var newlyUnmasked = 0;
        if (state.State.ForgottenAt.Count > 0)
        {
            foreach (var txid in proposed)
            {
                if (IsTombstoneExpiredAt(txid, now, options.TxDecisionRetention)) newlyUnmasked++;
            }
        }

        var priorUnmaskEpoch = state.State.TombstonePinUnmaskEpoch;
        if (newlyUnmasked > 0)
        {
            state.State.TombstonePinUnmaskEpoch += newlyUnmasked + 1;
        }

        state.State.SnapshotPins[pinId] = new SnapshotPin
        {
            Txids = proposed,
            ExpiresAt = expiresAt,
        };
        InvalidatePinMemo();
        // A pin changes the read mask of every txid it covers, and the
        // expired-tombstone mask is what every per-txid reader consults, so the
        // group is marked as touching them all.
        var group = PendingGroup();
        group.TouchesAllTxids = true;
        await CommitAsync(group, () =>
        {
            if (hadPrior && prior is not null) state.State.SnapshotPins[pinId] = prior;
            else state.State.SnapshotPins.Remove(pinId);
            state.State.TombstonePinUnmaskEpoch = priorUnmaskEpoch;
            InvalidatePinMemo();
        });
    }

    /// <inheritdoc />
    public async Task<bool> RefreshPinAsync(Guid pinId, TimeSpan ttl)
    {
        var now = TimeProvider.GetUtcNow();

        if (!state.State.SnapshotPins.TryGetValue(pinId, out var pin))
        {
            // Read-committed: the absence may be an un-durable unpin or prune.
            return await WhenAllDurableAsync() ? false : await RefreshPinAsync(pinId, ttl);
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
            return await WhenAllDurableAsync() ? false : await RefreshPinAsync(pinId, ttl);
        }

        var options = optionsMonitor.Get(TreeId);
        var newExpiresAt = ResolvePinExpiry(now, ttl, options);
        if (newExpiresAt == pin.ExpiresAt)
        {
            // No-op: identical ttl was already recorded (e.g. two
            // refreshes within the same TimeProvider tick).
            return await WhenAllDurableAsync() || await RefreshPinAsync(pinId, ttl);
        }

        var prior = pin.ExpiresAt;
        pin.ExpiresAt = newExpiresAt;
        // The txid membership is unchanged but the union's validity horizon is
        // derived from pin expiries, so extending one moves the horizon.
        InvalidatePinMemo();
        var group = PendingGroup();
        group.TouchesAllTxids = true;
        await CommitAsync(group, () =>
        {
            pin.ExpiresAt = prior;
            InvalidatePinMemo();
        });
        return true;
    }

    /// <inheritdoc />
    public async Task UnpinSnapshotAsync(Guid pinId)
    {
        if (!state.State.SnapshotPins.TryGetValue(pinId, out var prior))
        {
            // The pin may be missing because an un-durable unpin or prune
            // removed it; if that write fails the pin is restored, so retry.
            if (!await WhenAllDurableAsync())
            {
                await UnpinSnapshotAsync(pinId);
            }
            return;
        }
        state.State.SnapshotPins.Remove(pinId);
        InvalidatePinMemo();
        var group = PendingGroup();
        group.TouchesAllTxids = true;
        await CommitAsync(group, () =>
        {
            state.State.SnapshotPins[pinId] = prior;
            InvalidatePinMemo();
        });
    }

    /// <inheritdoc />
    public async Task<int> GetPinnedDecisionCountAsync()
    {
        while (true)
        {
            var count = CountPinnedDecisions();
            if (await WhenAllDurableAsync())
            {
                return count;
            }
        }
    }

    private int CountPinnedDecisions()
    {
        if (state.State.SnapshotPins.Count == 0)
        {
            return 0;
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
        return union.Count;
    }

    /// <summary>
    /// Clamps a caller-supplied pin TTL against the per-tree hard cap
    /// (<see cref="LatticeOptions.MaxCursorSnapshotPinTtl"/>) and the
    /// tombstone-retention floor: a pin shorter than
    /// <see cref="LatticeOptions.TxDecisionRetention"/> is silently
    /// floored to the retention, because the registry's own tombstone
    /// prune pass already covers anything shorter. A non-positive cap (for
    /// example <see cref="Timeout.InfiniteTimeSpan"/>) disables the cap; when
    /// the cap is disabled and no positive TTL was requested the pin has no
    /// expiry and <see cref="Timeout.InfiniteTimeSpan"/> is returned, rather
    /// than an unbounded pin collapsing to the retention floor (or, with
    /// retention disabled, expiring the instant it is recorded).
    /// </summary>
    private static TimeSpan ClampPinTtl(TimeSpan requested, LatticeOptions options)
    {
        var cap = options.MaxCursorSnapshotPinTtl;
        if (requested <= TimeSpan.Zero) requested = cap;
        if (cap > TimeSpan.Zero && requested > cap)
        {
            requested = cap;
        }
        if (requested <= TimeSpan.Zero)
        {
            return Timeout.InfiniteTimeSpan;
        }
        if (options.TxDecisionRetention > TimeSpan.Zero
            && requested < options.TxDecisionRetention)
        {
            requested = options.TxDecisionRetention;
        }
        return requested;
    }

    /// <summary>
    /// Resolves the absolute expiry of a pin recorded at <paramref name="now"/>
    /// for the caller-supplied <paramref name="requested"/> TTL, applying
    /// <see cref="ClampPinTtl"/>. An unbounded pin, or a TTL that would carry
    /// the expiry past <see cref="DateTimeOffset.MaxValue"/>, saturates at
    /// <see cref="DateTimeOffset.MaxValue"/> instead of overflowing.
    /// </summary>
    private static DateTimeOffset ResolvePinExpiry(DateTimeOffset now, TimeSpan requested, LatticeOptions options)
    {
        var ttl = ClampPinTtl(requested, options);
        if (ttl == Timeout.InfiniteTimeSpan || ttl >= DateTimeOffset.MaxValue - now)
        {
            return DateTimeOffset.MaxValue;
        }
        return now + ttl;
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="txid"/> has
    /// a tombstone whose age exceeds the per-tree retention window and
    /// is not held by a live snapshot pin.
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
        // A pinned txid is not expired. PruneExpired has always skipped pinned
        // entries, so the row is guaranteed to still be here; the read mask used
        // to ignore pins and hide it anyway, which meant a cursor that had taken
        // a pin precisely to keep reading a decision across a long walk was
        // refused the very row its pin was protecting. Pin-awareness is checked
        // second because it is the rarer condition and the dictionary probe
        // above already filtered out every txid with no tombstone at all.
        if (IsPinnedAt(txid, now)) return false;
        // TimeSpan.Zero retention: any tombstone observed here is
        // expired (this branch is only reachable from GetStatus* /
        // SnapshotAsync; ForgetAsync's own zero-retention path drops
        // the decision directly without writing to ForgottenAt).
        if (retention == TimeSpan.Zero) return true;
        return now - ts > retention;
    }

    /// <summary>
    /// Memoised union of every live snapshot pin's txid set, together with the
    /// earliest instant at which that union could change by a pin lapsing.
    /// Rebuilt on demand and dropped by <see cref="InvalidatePinMemo"/> at every
    /// mutation of <see cref="TxRegistryState.SnapshotPins"/>.
    /// <para>
    /// The union is consulted from the read-side expiry mask, which sits on the
    /// <c>[AlwaysInterleave]</c> reader hot path, so rebuilding it per call would
    /// put an allocation and a walk of every pin on every status read. Like the
    /// expiry memo below it is a pure function of persisted state and the clock,
    /// so losing it costs a rebuild and can never change an answer.
    /// </para>
    /// </summary>
    private HashSet<Guid>? _pinnedMemo;
    private long _pinnedMemoValidBeforeTicks;

    /// <summary>
    /// Drops the memoised pin union, and with it the memoised expiry scan.
    /// Called from every site that mutates
    /// <see cref="TxRegistryState.SnapshotPins"/>.
    /// <para>
    /// Both memos have to go, because the expired-tombstone count is pin-aware
    /// (a live pin un-masks the rows it holds) and so is a function of the pin
    /// map as well as of <see cref="TxRegistryState.ForgottenAt"/>. The expiry
    /// memo's validity horizon covers the clock-driven half of that dependency -
    /// it is clamped to the first pin lapse - but an explicit pin, unpin, or
    /// refresh changes the answer with no clock advance at all, so the horizon
    /// cannot see it. Invalidating only <c>_pinnedMemo</c> would leave the count
    /// serving a pre-mutation answer, the token would not move across a real
    /// surface change, and the reader's revision fast path would accept a stale
    /// snapshot - the precise defect the composite token exists to close.
    /// </para>
    /// </summary>
    private void InvalidatePinMemo()
    {
        _pinnedMemo = null;
        InvalidateExpiryMemo();
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="txid"/> is held by a
    /// snapshot pin that has not itself lapsed at <paramref name="now"/>. Uses
    /// exactly the liveness rule <see cref="PruneExpired"/> applies, so the read
    /// mask and the prune pass can never disagree about which rows survive.
    /// </summary>
    private bool IsPinnedAt(Guid txid, DateTimeOffset now)
    {
        // Overwhelmingly the common case: no cursor holds a point-in-time pin on
        // this tree, so the hot path costs one int comparison and no allocation.
        if (state.State.SnapshotPins.Count == 0) return false;

        if (_pinnedMemo is null || now.UtcTicks >= _pinnedMemoValidBeforeTicks)
        {
            var union = new HashSet<Guid>();
            var nextExpiry = long.MaxValue;
            foreach (var pin in state.State.SnapshotPins.Values)
            {
                if (pin.ExpiresAt <= now) continue;
                foreach (var pinned in pin.Txids) union.Add(pinned);
                if (pin.ExpiresAt.UtcTicks < nextExpiry) nextExpiry = pin.ExpiresAt.UtcTicks;
            }
            _pinnedMemo = union;
            _pinnedMemoValidBeforeTicks = nextExpiry;
        }

        return _pinnedMemo.Contains(txid);
    }

    /// <summary>
    /// Cached result of the last <see cref="CountExpiredTombstones"/> scan: the
    /// retention it was computed under, the first tick at which the answer could
    /// change, and the count itself. Purely an in-memory accelerator - the value
    /// it caches is a pure function of persisted state and the clock, so a lost
    /// cache (reactivation, invalidation) only costs a rescan and can never
    /// change an answer.
    /// </summary>
    private TimeSpan _expiryMemoRetention = TimeSpan.MinValue;
    private long _expiryMemoValidBeforeTicks;
    private int _expiryMemoCount;

    /// <summary>
    /// Drops the memoised expiry scan. Called from every site that mutates
    /// <see cref="TxRegistryState.ForgottenAt"/>, and - through
    /// <see cref="InvalidatePinMemo"/> - from every site that mutates
    /// <see cref="TxRegistryState.SnapshotPins"/>, because the cached count and
    /// its validity horizon are derived from both maps: the tombstone rows
    /// supply the candidates and the pin set decides which of them are masked.
    /// </summary>
    private void InvalidateExpiryMemo() => _expiryMemoRetention = TimeSpan.MinValue;

    /// <summary>
    /// Number of <see cref="TxRegistryState.ForgottenAt"/> rows that are already
    /// masked from readers at <paramref name="now"/> under
    /// <paramref name="retention"/>. Uses exactly the predicate
    /// <see cref="IsTombstoneExpiredAt"/> applies, so the count and the mask can
    /// never disagree.
    /// <para>
    /// Memoised on the reader hot path: a full scan is O(|ForgottenAt|) and this
    /// runs on every revision probe, so the scan also records the earliest tick
    /// at which any still-live tombstone becomes expired. Until the clock
    /// reaches that tick the cached count is provably still correct and the
    /// probe costs two comparisons. Mutating either input map invalidates the
    /// memo: <see cref="TxRegistryState.ForgottenAt"/> through
    /// <see cref="InvalidateExpiryMemo"/> and
    /// <see cref="TxRegistryState.SnapshotPins"/> through
    /// <see cref="InvalidatePinMemo"/>.
    /// </para>
    /// </summary>
    private int CountExpiredTombstones(DateTimeOffset now, TimeSpan retention)
    {
        if (retention == _expiryMemoRetention && now.UtcTicks < _expiryMemoValidBeforeTicks)
        {
            return _expiryMemoCount;
        }

        var forgotten = state.State.ForgottenAt;
        // A live pin un-expires the rows it holds, so the count has to apply the
        // same pin-aware predicate the mask does, and its validity horizon has
        // to end no later than the first pin lapse - a lapsing pin turns rows
        // expired with no mutation to invalidate the memo, exactly as a
        // retention crossing does.
        var pinned = state.State.SnapshotPins.Count > 0;
        int expired;
        long validBefore;
        if (forgotten.Count == 0)
        {
            expired = 0;
            // Nothing can expire out of an empty map; the next insert
            // invalidates the memo, so an unbounded horizon is safe.
            validBefore = long.MaxValue;
        }
        else if (retention == TimeSpan.Zero && !pinned)
        {
            // Every row is masked the instant it is observed, and no clock
            // advance can change that, so the horizon is unbounded too.
            expired = forgotten.Count;
            validBefore = long.MaxValue;
        }
        else
        {
            expired = 0;
            validBefore = long.MaxValue;
            foreach (var (txid, ts) in forgotten)
            {
                if (pinned && IsPinnedAt(txid, now))
                {
                    // Held by a live pin. It becomes countable when that pin
                    // lapses, which the horizon clamp below covers.
                    continue;
                }

                if (retention == TimeSpan.Zero || now - ts > retention)
                {
                    expired++;
                    continue;
                }

                // First tick at which `now - ts > retention` turns true.
                // Saturating, because a caller-supplied retention can be large
                // enough to overflow the sum and a throwing probe on the reader
                // path would be a far worse outcome than a conservative horizon.
                var expiresAtTicks = TickArithmetic.SaturatingAdd(ts.UtcTicks, retention.Ticks);
                var becomesExpiredAt = expiresAtTicks == long.MaxValue
                    ? long.MaxValue
                    : expiresAtTicks + 1;
                if (becomesExpiredAt < validBefore)
                {
                    validBefore = becomesExpiredAt;
                }
            }

            if (pinned && _pinnedMemoValidBeforeTicks < validBefore)
            {
                validBefore = _pinnedMemoValidBeforeTicks;
            }
        }

        _expiryMemoRetention = retention;
        _expiryMemoValidBeforeTicks = validBefore;
        _expiryMemoCount = expired;
        return expired;
    }

    /// <summary>
    /// The token readers compare across a fan-out:
    /// <c>DecisionsRevision + TombstoneRetirementEpoch + TombstonePinUnmaskEpoch
    /// + liveExpiredTombstones(now)</c>.
    /// <para>
    /// <see cref="TxRegistryState.DecisionsRevision"/> alone tracks the
    /// <see cref="TxRegistryState.Decisions"/> map, but the surface a reader can
    /// observe is that map <i>masked by</i>
    /// <see cref="TxRegistryState.ForgottenAt"/> at the current instant. A
    /// tombstone crossing its retention boundary removes a row from the readable
    /// surface with no write anywhere to hang a bump on, so the bare counter
    /// reports "unchanged" across a real change and the reader-side fast path
    /// (which short-circuits on revision equality and never consults
    /// <c>ClassifySnapshot</c>) accepts a stale view. Folding the live-expired
    /// count into the token makes that transition announce itself.
    /// </para>
    /// <para>
    /// The sum is non-decreasing because each of the terms only ever loses
    /// value to another: physically retiring an expired tombstone drops the
    /// count by one and raises
    /// <see cref="TxRegistryState.TombstoneRetirementEpoch"/> by one, and the
    /// decision row that leaves with it bumps
    /// <see cref="TxRegistryState.DecisionsRevision"/>. Without the epoch term a
    /// batch prune of <c>k &gt; 1</c> tombstones would drop the sum by
    /// <c>k - 1</c> (the prune advances the revision once per batch, not once
    /// per row), letting the token revisit a value it previously carried under a
    /// different surface.
    /// </para>
    /// <para>
    /// <see cref="TxRegistryState.TombstonePinUnmaskEpoch"/> is the same
    /// argument applied to the other input that can push the count down. The
    /// count is pin-aware because the read mask is, so a pin taking cover of
    /// <c>m</c> already-masked rows un-masks all <c>m</c> at once; the pin path
    /// adds <c>m + 1</c> here, which both restores monotonicity and makes the
    /// token move strictly across a mutation that really did change what readers
    /// can see. A pin <i>lapsing</i> needs no term of its own: it re-masks its
    /// rows, so the count rises on its own and the memo horizon is clamped to
    /// the first lapse so the rise is observed on time.
    /// </para>
    /// <para>
    /// It stays a <see cref="long"/> deliberately.
    /// <c>GetDecisionsRevisionAsync</c> and <c>TxRegistrySnapshot.Revision</c>
    /// keep their existing signatures, so a mixed-version cluster mid-rolling-
    /// upgrade exchanges the same wire shape it always did; the token is opaque
    /// and compared only for equality, so a peer that predates this change reads
    /// the composite value correctly without knowing it is composite.
    /// </para>
    /// </summary>
    private long EffectiveDecisionsRevision(DateTimeOffset now, TimeSpan retention)
        => state.State.DecisionsRevision
            + state.State.TombstoneRetirementEpoch
            + state.State.TombstonePinUnmaskEpoch
            + CountExpiredTombstones(now, retention);

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
        // Memberships of sagas an earlier pass purged (#4683); persisted by
        // the caller's write with whatever this pass removes.
        DropPurgedCrossTreeMemberships();

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
            InvalidatePinMemo();
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
                if (!IsWalPurgeCleared(txid)) continue;
                var hadDecision = state.State.Decisions.TryGetValue(txid, out var decision);
                flushed.Add(new PrunedEntry(txid, hadDecision, decision, ts, WalGenerationOf(txid)));
            }
            foreach (var entry in flushed)
            {
                state.State.Decisions.Remove(entry.Txid);
                state.State.ForgottenAt.Remove(entry.Txid);
                state.State.ForgetWalGenerations.Remove(entry.Txid);
            }
            if (flushed.Count > 0) InvalidateExpiryMemo();
            return new PruneResult(flushed.Count == 0 ? null : flushed, expiredPins);
        }

        List<PrunedEntry>? expired = null;
        foreach (var (txid, ts) in state.State.ForgottenAt)
        {
            if (pinned is not null && pinned.Contains(txid)) continue;
            // Held past its retention while the WAL may still retain a prepare
            // of the saga (#4508). It stays masked from readers meanwhile.
            if (now - ts > retention && IsWalPurgeCleared(txid))
            {
                var hadDecision = state.State.Decisions.TryGetValue(txid, out var decision);
                (expired ??= new List<PrunedEntry>()).Add(new PrunedEntry(txid, hadDecision, decision, ts, WalGenerationOf(txid)));
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
            state.State.ForgetWalGenerations.Remove(entry.Txid);
        }
        InvalidateExpiryMemo();
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
        DateTimeOffset ForgottenAt,
        long? WalGeneration);

    private long? WalGenerationOf(Guid txid) =>
        state.State.ForgetWalGenerations.TryGetValue(txid, out var g) ? g : null;

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
