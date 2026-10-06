using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Receiver-side coordinator for a replicated cross-tree atomic write. See
/// <see cref="ILatticeCrossTreeReceiverGrain"/> for the contract and rationale.
/// One activation per <c>(originClusterId, operationId)</c> (this grain's
/// compound key). Purely reactive: it is driven entirely by the per-tree
/// terminals that arrive over replication - it never runs a saga, and the only
/// grain it calls is its trees' <see cref="ICrossTreeBarrierIndexGrain"/>,
/// which calls nothing, so it cannot participate in a circular wait.
/// <para>
/// Crash recovery rides on replication's own at-least-once redelivery: every
/// <see cref="NotifyTerminalAsync"/> persists before returning and returns the
/// full finalize set whenever decided, so a redelivered terminal re-heals
/// materialization idempotently. A one-shot retention reminder clears the
/// persisted state once the configured <see cref="LatticeOptions.AtomicWriteRetention"/>
/// elapses after the decision.
/// </para>
/// </summary>
internal sealed class LatticeCrossTreeReceiverGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILatticeOriginClusterIdResolver originClusterIdResolver,
    ILogger<LatticeCrossTreeReceiverGrain> logger,
    [PersistentState("cross-tree-receiver", LatticeOptions.StorageProviderName)]
    IPersistentState<CrossTreeReceiverState> state)
    : TtlGrain<LatticeCrossTreeReceiverGrain>(context, reminderRegistry, logger), ILatticeCrossTreeReceiverGrain
{
    private const string RetentionReminderName = "cross-tree-receiver-retention";

    /// <summary>
    /// In-memory marker, <c>true</c> only while a freshly-computed decision is
    /// mid-flight in <see cref="NotifyTerminalAsync"/> and therefore not yet
    /// durable (the in-memory <see cref="CrossTreeReceiverState.Decided"/> has
    /// been set but <c>WriteStateAsync</c> has not yet
    /// completed). The <c>[AlwaysInterleave]</c> <see cref="GetDecisionAsync"/>
    /// and the redelivery early-return must <b>not</b> publish a decision in
    /// this window: the caller (<c>TxRegistryGrain.ResolveReceiverDelegatedAsync</c>)
    /// durably caches the returned verdict and drops its delegation, so a
    /// decision that a crash then loses before the write lands would become a
    /// permanent partial cross-tree view. On a fresh activation this is
    /// <c>false</c>, so a decision loaded from durable storage is published
    /// immediately - it is durable by construction. A failed write leaves it
    /// <c>true</c>, so the next (redelivered) terminal re-drives the persist.
    /// </summary>
    private bool _decisionAwaitingPersist;

    /// <inheritdoc />
    protected override string TtlReminderName => RetentionReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl() => optionsMonitor.CurrentValue.AtomicWriteRetention;

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        Logger.LogInformation(
            "Cross-tree receiver {Key}: retention window expired; clearing state.",
            GrainContext.GrainId.Key);
        await state.ClearStateAsync();
    }

    /// <summary>
    /// Builds the compound grain key for a receiver coordinator from the source
    /// cluster id and the cross-tree operation id.
    /// <para>
    /// The key is length-prefixed - the decimal character length of
    /// <paramref name="originClusterId"/>, an underscore, then the two halves
    /// concatenated - rather than joined by a delimiter character. A
    /// length prefix keeps the two halves unambiguous regardless of the
    /// characters either contains (the property the old ASCII Unit Separator
    /// delimiter was chosen for), while producing a key that is free of any
    /// control character. That matters because this grain persists via a
    /// storage provider: Azure Table grain storage carries the primary key into
    /// the request URL and the Partition/Row key columns, both of which reject
    /// control chars 0x00-0x1F, so a raw Unit Separator (0x1F) in the key made
    /// the receiver fail to activate with HTTP 400 on a real Azure deployment.
    /// </para>
    /// <para>
    /// The encoding is a pure deterministic function of its two inputs so every
    /// region derives an identical key for the same operation. Changing the
    /// encoding changes this grain's identity; that is acceptable here because
    /// the previous (control-char) key was non-functional on the Azure Table
    /// path, so there is no live durable state to stay compatible with.
    /// </para>
    /// </summary>
    [GrainKeyBuilder]
    public static string ComputeKey(string originClusterId, string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return string.Concat(
            originClusterId.Length.ToString(CultureInfo.InvariantCulture),
            "_",
            originClusterId,
            operationId);
    }

    /// <inheritdoc />
    public async Task<CrossTreeReceiverDecision> NotifyTerminalAsync(CrossTreeReceiverTerminal terminal)
    {
        ArgumentNullException.ThrowIfNull(terminal);
        ArgumentException.ThrowIfNullOrEmpty(terminal.TreeId);
        ArgumentNullException.ThrowIfNull(terminal.WaitSet);
        ArgumentNullException.ThrowIfNull(terminal.ObservedSourceShards);

        // This entry point carries the caller-supplied commit/abort verdict for a
        // whole cross-tree batch, so it is the receiver-side twin of the single-tree
        // saga's IAtomicWriteGrain.FinalizeAsync and is guarded identically. The
        // first terminal also freezes the wait set, origin cluster, and operation id,
        // so an unguarded external call could poison a barrier that has not started
        // yet as well as force a premature verdict on one in flight. Legitimate
        // callers are the replication apply path inside the cluster trust boundary
        // and therefore carry the internal-origin marker; the assertion is inert
        // unless internal-origin enforcement is registered. Placed ahead of the
        // decided/redelivery short-circuit so a settled verdict is never disclosed
        // to an external caller either.
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            GrainContext.ActivationServices, terminal.TreeId, LatticeOperation.Replication);

        // Already decided AND durable: a redelivered terminal re-heals
        // materialization. Return the full finalize set without re-persisting.
        // While a decision is awaiting persistence (a prior write failed mid
        // flight) we must NOT short-circuit here - fall through and re-drive
        // the persist so the verdict only escapes once durable.
        if (state.State.Decided && !_decisionAwaitingPersist)
        {
            // A tree that joined after the barrier decided - it was not
            // replicated here when the wait set froze (issue #4692) - is
            // recorded so the returned finalize set materializes it with the
            // operation's verdict, which every participant's terminal shares.
            if (!state.State.Arrived.TryGetValue(terminal.TreeId, out var recorded))
            {
                // The same premise as the undecided join: one verdict crosses the
                // tree boundary only between trees that agree on cluster identity.
                ThrowIfWaitSetClusterIdsDisagree(
                    CanonicalStringSet.SortedDistinct(state.State.WaitSet.Append(terminal.TreeId)));
                state.State.Arrived[terminal.TreeId] = terminal;
                await state.WriteStateAsync();
            }
            else if (recorded.TransactionId == Guid.Empty && terminal.TransactionId != Guid.Empty)
            {
                // An arrival an import recorded (#4684) carries no transaction id
                // and finalizes nothing; a real terminal of the tree replaces it,
                // so its pending bucket is finalized with the verdict.
                state.State.Arrived[terminal.TreeId] = terminal;
                await state.WriteStateAsync();
            }

            return BuildDecision();
        }

        if (state.State.WaitSet.Count == 0)
        {
            // First terminal: freeze the wait set and identity. The wait set is
            // canonicalized (ordinal-sorted, de-duplicated) so the exact-match
            // validation below is order-insensitive.
            var frozen = CanonicalStringSet.SortedDistinct(terminal.WaitSet);

            // The receiver-side twin of the coordinator's admission check. The
            // barrier carries one global verdict across every tree in the wait
            // set, which is the same tree-boundary transitivity step, so it
            // rests on the same premise: the trees must agree on cluster
            // identity. Checked at the freeze because that is the only moment
            // the receiver holds the whole participant set and has not yet
            // recorded anything. A tree that joins the wait set later is checked
            // when it joins.
            ThrowIfWaitSetClusterIdsDisagree(frozen);

            await IndexAsync(frozen);
            state.State.WaitSet = frozen;
            state.State.OriginClusterId = terminal.OriginClusterId;
            state.State.OperationId = terminal.OperationId;
            state.State.StartedAtTicks = DateTime.UtcNow.Ticks;
        }
        else if (!CrossTreeReceiverBarrier.WaitSetMatches(state.State.WaitSet, terminal.WaitSet))
        {
            // Later terminal whose wait set differs (issue #4692). The sender
            // computes each terminal's wait set from the receiver's live
            // replicated-tree configuration, so a configuration change between
            // two terminals of one operation is ordinary, and rejecting the
            // terminal would fail it on every retry. The frozen wait set is
            // authoritative and is never recomputed: the differing set is
            // ignored, and only the arriving tree itself may join it, below.
            Logger.LogWarning(
                "Cross-tree receiver {Key}: the terminal for tree '{TreeId}' carries a wait set that differs from the "
                + "frozen one; the receiver's replicated trees changed mid-operation, so the frozen wait set stands.",
                GrainContext.GrainId.Key, terminal.TreeId);
        }

        if (!state.State.WaitSet.Contains(terminal.TreeId))
        {
            // A tree that was not replicated here when the wait set froze has
            // since been, and its terminal arrived (issue #4692). It joins the
            // wait set: it is arriving now, so it adds nothing to wait for, and
            // only its own arrival - never a recomputed set - grows the barrier.
            var grown = CanonicalStringSet.SortedDistinct(state.State.WaitSet.Append(terminal.TreeId));
            ThrowIfWaitSetClusterIdsDisagree(grown);
            if (!state.State.Decided)
            {
                await IndexAsync([terminal.TreeId]);
            }

            state.State.WaitSet = grown;
        }

        // Record (or idempotently overwrite) this tree's terminal.
        state.State.Arrived[terminal.TreeId] = terminal;

        // A tree whose latest import settled its part of the operation without
        // naming it arrives with this verdict (#4684).
        await FillImportedArrivalsAsync();

        // The barrier completes when every wait-set tree has arrived, and the
        // global verdict is commit iff every arrived terminal voted commit. Both
        // rules are the shared, dependency-free CrossTreeReceiverBarrier core,
        // so the grain runs the rule the cross-cluster model checks.
        if (CrossTreeReceiverBarrier.IsComplete(state.State.WaitSet, state.State.Arrived))
        {
            state.State.Decided = true;
            state.State.Committed = CrossTreeReceiverBarrier.CommitsAll(state.State.Arrived);
            // Mark the decision non-durable until the write below completes, so
            // an interleaving GetDecisionAsync cannot publish it early.
            _decisionAwaitingPersist = true;
        }

        // Persist BEFORE returning so the decision (and the arrival that may yet
        // complete it on a later terminal) is durable - the registration that
        // preceded this call is thereby linearized against durable state.
        await state.WriteStateAsync();

        // The write landed: the decision (if any) is now durable and may be
        // published to readers.
        _decisionAwaitingPersist = false;

        if (!state.State.Decided)
        {
            return CrossTreeReceiverDecision.InFlight;
        }

        // Arm one-shot retention cleanup now that the decision is terminal.
        await SlideTtlAsync();
        await UnindexAsync();
        return BuildDecision();
    }

    /// <inheritdoc />
    public async Task<CrossTreeReceiverDecision> NotifyParticipantAbsentAsync(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            GrainContext.ActivationServices, treeId, LatticeOperation.Replication);

        if (state.State.Decided)
        {
            // A decision still awaiting its persist reads as in flight; the next
            // terminal or notification re-drives the write.
            return _decisionAwaitingPersist ? CrossTreeReceiverDecision.InFlight : BuildDecision();
        }

        // A barrier that has not opened has nothing to wait for and must not
        // persist anything: the tree id and operation are peer-supplied.
        if (state.State.WaitSet.Count == 0
            || !state.State.WaitSet.Contains(treeId)
            || state.State.Arrived.ContainsKey(treeId))
        {
            return CrossTreeReceiverDecision.InFlight;
        }

        Logger.LogWarning(
            "Cross-tree receiver {Key}: tree '{TreeId}' is no longer replicated here, so the barrier stops waiting for it "
            + "and decides on the trees that remain.",
            GrainContext.GrainId.Key, treeId);

        state.State.WaitSet = state.State.WaitSet.Where(t => !string.Equals(t, treeId, StringComparison.Ordinal)).ToList();
        await FillImportedArrivalsAsync();
        if (CrossTreeReceiverBarrier.IsComplete(state.State.WaitSet, state.State.Arrived))
        {
            state.State.Decided = true;
            state.State.Committed = CrossTreeReceiverBarrier.CommitsAll(state.State.Arrived);
            _decisionAwaitingPersist = true;
        }

        await state.WriteStateAsync();
        _decisionAwaitingPersist = false;

        if (!state.State.Decided)
        {
            return CrossTreeReceiverDecision.InFlight;
        }

        await SlideTtlAsync();
        await UnindexAsync();
        return BuildDecision();
    }

    /// <inheritdoc />
    public async Task<CrossTreeReceiverDecision> RecordDecisionStampsAsync(IReadOnlyDictionary<string, long> stamps)
    {
        ArgumentNullException.ThrowIfNull(stamps);
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            GrainContext.ActivationServices, state.State.WaitSet.FirstOrDefault() ?? string.Empty, LatticeOperation.Replication);

        if (state.State.DecisionStamps is null && stamps.Count > 0)
        {
            // Persisted even before the barrier opens: the terminal or decision
            // row that carried the stamps opens it next, and a barrier that lost
            // them would take the operation for one decided before stamping.
            state.State.DecisionStamps = new Dictionary<string, long>(stamps, StringComparer.Ordinal);
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.DecisionStamps = null;
                throw;
            }
        }

        return await ReevaluateAsync();
    }

    /// <inheritdoc />
    public async Task<CrossTreeReceiverDecision> ReevaluateAsync()
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            GrainContext.ActivationServices, state.State.WaitSet.FirstOrDefault() ?? string.Empty, LatticeOperation.Replication);

        if (state.State.Decided)
        {
            return _decisionAwaitingPersist ? CrossTreeReceiverDecision.InFlight : BuildDecision();
        }

        if (state.State.WaitSet.Count == 0 || !await FillImportedArrivalsAsync())
        {
            return CrossTreeReceiverDecision.InFlight;
        }

        if (CrossTreeReceiverBarrier.IsComplete(state.State.WaitSet, state.State.Arrived))
        {
            state.State.Decided = true;
            state.State.Committed = CrossTreeReceiverBarrier.CommitsAll(state.State.Arrived);
            _decisionAwaitingPersist = true;
        }

        await state.WriteStateAsync();
        _decisionAwaitingPersist = false;

        if (!state.State.Decided)
        {
            return CrossTreeReceiverDecision.InFlight;
        }

        await SlideTtlAsync();
        await UnindexAsync();
        return BuildDecision();
    }

    /// <summary>
    /// Records, in memory, the arrival of every wait-set tree that has not
    /// arrived and whose latest import from the origin settled its part of the
    /// operation (issue #4684): the import's export opened after the
    /// operation's decision and named the operation on no row. The tree takes
    /// the verdict the arrived trees carry and no transaction id, so the
    /// decision finalizes nothing on it. A barrier with no arrival has no
    /// verdict to give. Returns whether anything was recorded; the caller
    /// persists.
    /// </summary>
    private async Task<bool> FillImportedArrivalsAsync()
    {
        if (state.State.Arrived.Count == 0
            || string.IsNullOrEmpty(state.State.OriginClusterId)
            || GrainContext.ActivationServices?.GetService<IGrainFactory>() is not { } grainFactory)
        {
            return false;
        }

        var filled = false;
        foreach (var tree in state.State.WaitSet)
        {
            if (state.State.Arrived.ContainsKey(tree))
            {
                continue;
            }

            var import = await grainFactory.GetGrain<ICrossTreeBarrierIndexGrain>(tree).GetImportAsync(state.State.OriginClusterId);
            if (import is null
                || import.NamedOperations.Contains(state.State.OperationId)
                || !ImportOpenedAfterDecision(tree, import.ExportEpoch))
            {
                continue;
            }

            state.State.Arrived[tree] = new CrossTreeReceiverTerminal
            {
                OriginClusterId = state.State.OriginClusterId,
                OperationId = state.State.OperationId,
                TreeId = tree,
                TransactionId = Guid.Empty,
                Committed = CrossTreeReceiverBarrier.CommitsAll(state.State.Arrived),
                WaitSet = state.State.WaitSet,
                ObservedSourceShards = [],
                TerminalHlc = HybridLogicalClock.Zero,
            };
            filled = true;
            Logger.LogInformation(
                "Cross-tree receiver {Key}: tree '{TreeId}' was imported from an export that opened after the operation's "
                + "decision and named it nowhere, so its sub-saga was purged at the origin; it arrives with its siblings' verdict.",
                GrainContext.GrainId.Key, tree);
        }

        return filled;
    }

    /// <summary>
    /// Whether an export of <paramref name="tree"/> numbered
    /// <paramref name="exportEpoch"/> opened after the operation's decision:
    /// its epoch is greater than the tree's decision stamp. An operation with no
    /// recorded stamps was decided by a silo that predates stamping, which the
    /// origin serves no export alongside, so every import qualifies.
    /// </summary>
    private bool ImportOpenedAfterDecision(string tree, long exportEpoch)
    {
        if (state.State.DecisionStamps is null)
        {
            return true;
        }

        return state.State.DecisionStamps.TryGetValue(tree, out var stamp) && exportEpoch > stamp;
    }

    /// <inheritdoc />
    public Task<CrossTreeReceiverStatus> GetStatusAsync() =>
        Task.FromResult(new CrossTreeReceiverStatus
        {
            Opened = state.State.WaitSet.Count > 0,
            Decided = state.State.Decided && !_decisionAwaitingPersist,
            OriginClusterId = state.State.OriginClusterId,
            OperationId = state.State.OperationId,
            WaitSet = [.. state.State.WaitSet],
            ArrivedTrees = [.. state.State.Arrived.Keys],
        });

    /// <summary>
    /// Registers this barrier under every tree of <paramref name="trees"/>
    /// (issue #4684), before the wait set that names them is persisted, so an
    /// import of any of them finds it. Fails the call on a failed registration.
    /// </summary>
    private async Task IndexAsync(IEnumerable<string> trees)
    {
        if (GrainContext.ActivationServices?.GetService<IGrainFactory>() is not { } grainFactory)
        {
            return;
        }

        var key = GrainContext.GrainId.Key.ToString()!;
        foreach (var tree in trees)
        {
            await grainFactory.GetGrain<ICrossTreeBarrierIndexGrain>(tree).AddAsync(key);
        }
    }

    /// <summary>
    /// Withdraws the decided barrier from its trees' indexes. Best effort: an
    /// entry left behind costs a reader one status read.
    /// </summary>
    private async Task UnindexAsync()
    {
        if (GrainContext.ActivationServices?.GetService<IGrainFactory>() is not { } grainFactory)
        {
            return;
        }

        var key = GrainContext.GrainId.Key.ToString()!;
        foreach (var tree in state.State.WaitSet)
        {
            try
            {
                await grainFactory.GetGrain<ICrossTreeBarrierIndexGrain>(tree).RemoveAsync(key);
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex,
                    "Cross-tree receiver {Key}: withdrawing from the barrier index of tree '{TreeId}' failed; the stale entry is skipped by readers.",
                    key, tree);
            }
        }
    }

    /// <inheritdoc />
    public Task<TxStatus> GetDecisionAsync()
    {
        // Only publish a decision that is durable: a decision still awaiting its
        // persist (set in memory by an in-flight NotifyTerminalAsync) reads as
        // InFlight so the caller never durably caches a verdict a crash could
        // lose. A decision loaded from storage on activation has the marker
        // false and is published immediately.
        var status = state.State.Decided && !_decisionAwaitingPersist
            ? (state.State.Committed ? TxStatus.Committed : TxStatus.Aborted)
            : TxStatus.InFlight;
        return Task.FromResult(status);
    }

    /// <summary>
    /// Builds the decided <see cref="CrossTreeReceiverDecision"/> from the
    /// persisted arrivals. Returns the full per-tree finalize set so a
    /// redelivered terminal re-heals every tree's materialization.
    /// </summary>
    private CrossTreeReceiverDecision BuildDecision()
    {
        var finalize = new List<CrossTreeReceiverTreeFinalize>(state.State.Arrived.Count);
        foreach (var t in state.State.Arrived.Values)
        {
            if (t.TransactionId == Guid.Empty)
            {
                // An imported arrival (#4684): the import settled the tree.
                continue;
            }

            finalize.Add(new CrossTreeReceiverTreeFinalize
            {
                TreeId = t.TreeId,
                TransactionId = t.TransactionId,
                ObservedSourceShards = t.ObservedSourceShards,
                TerminalHlc = t.TerminalHlc,
                OriginClusterId = t.OriginClusterId,
            });
        }
        return new CrossTreeReceiverDecision
        {
            Decided = true,
            Committed = state.State.Committed,
            TreesToFinalize = finalize,
        };
    }

    /// <summary>
    /// Rejects a replicated cross-tree barrier whose participating trees do not
    /// all resolve the same origin cluster id. The receiver-side twin of
    /// <c>LatticeCrossTreeTxGrain.ThrowIfParticipantClusterIdsDisagree</c>; see
    /// that member for why the premise exists and why no per-tree options
    /// validator can discharge it. Runs at the wait-set freeze, before anything
    /// is recorded, so a rejected first terminal leaves the barrier unstarted.
    /// <para>
    /// A uniform resolver - the core default, which returns
    /// <see cref="string.Empty"/> for every tree - satisfies this for any
    /// single-cluster host, so the check costs one resolver call per
    /// participant and changes no existing deployment's behaviour.
    /// </para>
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Two trees in the wait set resolve different cluster ids.
    /// </exception>
    private void ThrowIfWaitSetClusterIdsDisagree(List<string> waitSet)
    {
        if (waitSet.Count < 2) return;

        var expected = originClusterIdResolver.Resolve(waitSet[0]);
        for (var i = 1; i < waitSet.Count; i++)
        {
            var actual = originClusterIdResolver.Resolve(waitSet[i]);
            if (string.Equals(expected, actual, StringComparison.Ordinal)) continue;

            throw new InvalidOperationException(
                $"Cross-tree receiver '{GrainContext.GrainId.Key}' was handed a wait set spanning trees "
                + $"that resolve different origin cluster ids: tree '{waitSet[0]}' resolves '{expected}' "
                + $"but tree '{waitSet[i]}' resolves '{actual}'. Every tree barriered by one cross-tree "
                + "operation must resolve the same cluster id, because the barrier carries a single global "
                + "verdict across the tree boundary and that step is sound only when the trees agree on "
                + "cluster identity. This is a configuration fault on the receiving cluster: "
                + "LatticeReplicationOptions.ClusterId is a per-tree option, so a named override for one "
                + "participating tree silently defeats the cluster-wide value the others inherit.");
        }
    }
}
