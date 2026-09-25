using Orleans.Serialization.Invocation;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Optimistic, interleavable point reads (issue #3474).
/// <para>
/// The serial <see cref="IShardRootGrain.GetAsync"/> path serves one read per full
/// leaf round trip on each shard root, which caps point-read throughput at roughly
/// <c>shardCount / leafRoundTrip</c>. <see cref="TryGetOptimisticAsync"/> lets reads
/// overlap by validating them against a routing epoch instead of serialising them.
/// </para>
/// <para>
/// <b>The guard is generic, not a per-writer audit.</b> The U9h-C violation came from
/// a read observing a routing mutation part-way (a promotion or move-away publish
/// landing between the reader's non-atomic reads of <c>RootNodeId</c> /
/// <c>RootIsLeaf</c> / <c>MovedAwaySlots</c>, or a moved-away seal fanned out across
/// only part of the leaf chain). Rather than relying on every such writer remembering
/// to bump the epoch, this grain implements <see cref="IIncomingGrainCallFilter"/> and
/// treats incoming calls except the allowlisted reads, diagnostics and point Sets as a potential
/// routing mutation: it is counted in <see cref="_routingMutationsInFlight"/> for its
/// whole duration and bumps <see cref="_routingEpoch"/> as it starts and finishes. The
/// exempt calls change routing only through self-bracketing prepare, split-link
/// and retirement-retry paths. Reads overlapping point Sets and absent-key reads
/// additionally validate leaf ownership against an activation-scoped generation.
/// Timer callbacks bypass the filter; the only shard-root timers (dirty-leaf and
/// leaf-access flushes) do not touch routing state.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain : IIncomingGrainCallFilter
{
    /// <summary>
    /// Monotonic in-memory routing epoch. Bumped whenever a potentially
    /// routing-mutating call starts or finishes. Not persisted: a reactivation starts a
    /// fresh activation whose reads cannot straddle the previous one.
    /// </summary>
    private long _routingEpoch;

    /// <summary>
    /// Number of potentially routing-mutating calls (or bracketed prepare slow paths)
    /// currently in flight on this activation. An optimistic read is refused while it
    /// is non-zero.
    /// </summary>
    private int _routingMutationsInFlight;

    // Separate from the write-side routing cache: only serial reads publish these
    // ownership proofs, and stale entries can only cause an optimistic retry.
    private readonly Dictionary<GrainId, (Guid Epoch, long Generation)> _leafRoutingStamps = new();

    private RoutingMutationScope EnterRoutingMutation()
    {
        BeginRoutingMutation();
        return new RoutingMutationScope(this);
    }

    private readonly struct RoutingMutationScope(ShardRootGrain root) : IDisposable
    {
        public void Dispose() => root.EndRoutingMutation();
    }

    /// <summary>Current routing epoch. Exposed for unit tests.</summary>
    internal long RoutingEpoch => _routingEpoch;

    /// <summary>
    /// Opens a routing-mutation bracket: bumps the epoch and marks a mutation in
    /// flight. Every call must be paired with <see cref="EndRoutingMutation"/>.
    /// </summary>
    internal void BeginRoutingMutation()
    {
        _routingMutationsInFlight++;
        _routingEpoch++;
    }

    /// <summary>Closes a bracket opened by <see cref="BeginRoutingMutation"/>.</summary>
    internal void EndRoutingMutation()
    {
        _routingMutationsInFlight--;
        _routingEpoch++;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Dispatches through the point-write quiesce guard first (see
    /// <c>ShardRootGrain.PointWriteQuiesce.cs</c>), then applies the routing-mutation
    /// bracket in <see cref="InvokeRoutingFiltered"/>.
    /// </remarks>
    Task IIncomingGrainCallFilter.Invoke(IIncomingGrainCallContext context) =>
        ClassifyIncomingTurn(context.Request) switch
        {
            IncomingTurnKind.PointWrite => InvokePointWriteAsync(context),
            IncomingTurnKind.Serial => InvokeSerialTurnAsync(context),
            _ => InvokeRoutingFiltered(context),
        };

    /// <summary>
    /// Applies the routing-mutation bracket to an admitted call.
    /// </summary>
    private Task InvokeRoutingFiltered(IIncomingGrainCallContext context)
    {
        if (IsRoutingNeutralCall(context.Request))
        {
            return context.Invoke();
        }

        return InvokeAsRoutingMutationAsync(context);
    }

    private Task InvokeAsRoutingMutationAsync(IIncomingGrainCallContext context)
    {
        BeginRoutingMutation();
        Task invocation;
        try
        {
            invocation = context.Invoke();
        }
        catch
        {
            EndRoutingMutation();
            throw;
        }

        if (invocation.IsCompleted)
        {
            EndRoutingMutation();
            return invocation;
        }

        // Intentional per-call state machine for a call that genuinely suspends; it
        // is dwarfed by the storage / leaf round trip that made the call suspend.
        return AwaitRoutingMutationAsync(invocation);
    }

    private async Task AwaitRoutingMutationAsync(Task invocation)
    {
        try
        {
            await invocation;
        }
        finally
        {
            EndRoutingMutation();
        }
    }

    /// <summary>
    /// Returns <c>true</c> for the incoming calls that cannot change shard-root
    /// routing state (other than through the self-bracketing
    /// prepare, split-link and retirement-retry paths) and so do not invalidate an
    /// in-flight optimistic read. Everything else is conservatively treated as a
    /// routing mutation.
    /// </summary>
    internal static bool IsRoutingNeutralCall(IInvokable request)
    {
        if (request.GetInterfaceType() != typeof(IShardRootGrain))
        {
            return false;
        }

        return IsRoutingNeutralMethod(request.GetMethodName());
    }

    /// <summary>
    /// The <see cref="IShardRootGrain"/> method names exempt from the
    /// routing-mutation bracket. Kept deliberately small: point reads, the batch read,
    /// two activation-scoped diagnostics, and point Sets whose routing changes
    /// are covered by the self-bracketing prepare, split-link and retry seams.
    /// </summary>
    internal static bool IsRoutingNeutralMethod(string? methodName) => methodName switch
    {
        nameof(IShardRootGrain.TryGetOptimisticAsync) => true,
        nameof(IShardRootGrain.GetAsync) => true,
        nameof(IShardRootGrain.GetWithVersionAsync) => true,
        nameof(IShardRootGrain.ExistsAsync) => true,
        nameof(IShardRootGrain.GetManyAsync) => true,
        nameof(IShardRootGrain.GetHotnessAsync) => true,
        nameof(IShardRootGrain.PublishLeafByteFootprintAsync) => true,
        // Both Set overloads change values, not ownership, unless they split.
        // LinkSplitAsync brackets every non-null split; leaf stamps cover the
        // earlier donor transfer and divisions absorbed below the shard root.
        // No other non-read method is newly exempted.
        nameof(IShardRootGrain.SetAsync) => true,
        _ => false,
    };

    /// <inheritdoc />
    public async Task<OptimisticReadResult> TryGetOptimisticAsync(string key)
    {
        EnsureInternalOrigin(LatticeOperation.Read);

        // Resolve the per-tree options once per activation, BEFORE the snapshot
        // block (the await here may yield; nothing after it relies on state read
        // before it). A resolution fault is not the read's to report: defer to the
        // serial path, which does not depend on option resolution.
        var options = _cachedOptions;
        if (options is null)
        {
            try
            {
                options = await GetOptionsAsync();
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return SerialRetry(LatticeMetrics.OutcomeOptimisticReadDisabledTag);
            }
        }

        if (!options.OptimisticShardRootPointReads)
        {
            return SerialRetry(LatticeMetrics.OutcomeOptimisticReadDisabledTag);
        }

        // Snapshot block: everything from here to the leaf call runs in one
        // synchronous turn slice, so no other turn can mutate routing state between
        // these checks and the leaf resolution. The leaf is resolved from the
        // already-cached routing tables only: an optimistic read never fetches a
        // routing table and never publishes into _routingTableCache. A fetch here
        // would run interleaved with serial writes and could cache a routing table
        // that a concurrent split / fold had already superseded, misrouting
        // subsequent WRITES into a leaf that no longer owns the key (lost writes,
        // not just a false-null read). A cache miss defers to the serial path.
        if (!CanServeOptimisticRead())
        {
            return SerialRetry(LatticeMetrics.OutcomeOptimisticReadBusyTag);
        }

        if (!IsOptimisticReadGateOpen(key))
        {
            return SerialRetry(LatticeMetrics.OutcomeOptimisticReadGateClosedTag);
        }

        var epoch = _routingEpoch;
        if (!TryResolveReadLeafFromCache(key, out var leafId))
        {
            return SerialRetry(LatticeMetrics.OutcomeOptimisticReadRoutingCacheMissTag);
        }

        // Preserve the byte[]-only baseline RPC for uncontended present reads.
        // The admission epoch closes the start-and-finish-during-await race;
        // a raw miss or an overlapping write must instead obtain ownership proof.
        var writeEpoch = _pointWriteAdmissionEpoch;
        if (_interleavedPointWritesInFlight == 0)
        {
            byte[]? value;
            try
            {
                value = await ResolveLeafGrain(leafId).GetAsync(key);
            }
            catch when (_routingEpoch != epoch || _pointWriteAdmissionEpoch != writeEpoch)
            {
                return SerialRetry(_routingEpoch != epoch
                    ? LatticeMetrics.OutcomeOptimisticReadEpochChangedTag
                    : LatticeMetrics.OutcomeOptimisticReadLeafGenerationChangedTag);
            }

            if (_routingEpoch != epoch)
                return SerialRetry(LatticeMetrics.OutcomeOptimisticReadEpochChangedTag);

            if (value is not null && _pointWriteAdmissionEpoch == writeEpoch
                && _interleavedPointWritesInFlight == 0)
                return ValidatedRead(leafId, value);
        }

        return await TryGetProvedOptimisticAsync(key, leafId, epoch);
    }

    // Keep the ownership tuple and versioned awaiter out of the raw read's
    // state machine; only reads that need proof allocate this continuation.
    private async Task<OptimisticReadResult> TryGetProvedOptimisticAsync(string key, GrainId leafId, long epoch)
    {
        if (!_leafRoutingStamps.TryGetValue(leafId, out var expectedStamp))
        {
            return SerialRetry(LatticeMetrics.OutcomeOptimisticReadLeafGenerationChangedTag);
        }

        // The optimistic read goes to the PRIMARY leaf, never through the
        // stateless-worker LeafCacheGrain the serial path uses. A cache replica
        // activated or refreshed from an interleaved read during a fold / split
        // window can hold moved-away seal state and pruned rows that diverge from
        // this shard root's routing, and a later serial read served by that replica
        // can then miss a key it owns (lost writes and a stale-routing retry loop
        // observed under the consolidation chaos suite). The primary leaf is the
        // authority for its own moved-away seal and pending-transaction state, so
        // its answer needs both root-epoch and leaf-ownership validation below.
        VersionedValue result;
        try
        {
            // Resolved through the per-activation reference cache: a bare
            // GetGrain per read re-resolves the interface type and rebuilds the
            // proxy (reflection-built copiers included) on every call, which is
            // a measurable share of the point-read CPU cost.
            result = await ResolveLeafGrain(leafId).GetWithVersionAsync(key);
        }
        catch when (_routingEpoch != epoch)
        {
            // A fault raised while routing moved under the read is not attributable
            // to the key; the serial path re-evaluates it against settled state.
            return SerialRetry(LatticeMetrics.OutcomeOptimisticReadEpochChangedTag);
        }

        // Validation: any routing mutation that started (or finished) after the
        // snapshot bumped the epoch, so an unchanged epoch proves the read observed
        // routing state no other call touched while it was in flight. The read is
        // counted for hotness only when validated, so a serial retry does not
        // double-count it.
        if (_routingEpoch != epoch)
        {
            return SerialRetry(LatticeMetrics.OutcomeOptimisticReadEpochChangedTag);
        }

        // Old-wire/default stamps never match, even if a default was cached.
        // The leaf stamps only a synchronous value/absence observation whose
        // key range it owns, outside splitting, sealing and retirement windows.
        if (result.LeafRoutingEpoch == Guid.Empty || result.LeafRoutingGeneration <= 0
            || expectedStamp != (result.LeafRoutingEpoch, result.LeafRoutingGeneration))
        {
            _leafRoutingStamps.Remove(leafId);
            return SerialRetry(result.Value is null
                && (result.LeafRoutingEpoch == Guid.Empty || result.LeafRoutingGeneration <= 0)
                ? LatticeMetrics.OutcomeOptimisticReadAbsentTag
                : LatticeMetrics.OutcomeOptimisticReadLeafGenerationChangedTag);
        }

        return ValidatedRead(leafId, result.Value);
    }

    private OptimisticReadResult ValidatedRead(GrainId leafId, byte[]? value)
    {
        RecordRead();
        RecordLeafAccess(leafId);
        RecordOptimisticReadOutcome(1, LatticeMetrics.OutcomeOptimisticReadValidatedTag);
        return OptimisticReadResult.FromValue(value);
    }

    private OptimisticReadResult SerialRetry(KeyValuePair<string, object?> outcome)
    {
        RecordOptimisticReadOutcome(1, outcome);
        return OptimisticReadResult.SerialRetry;
    }

    private KeyValuePair<string, object?> _optimisticReadTreeTag;
    private KeyValuePair<string, object?> _optimisticReadTenantTag;
    private bool _optimisticReadTagsResolved;

    /// <summary>
    /// Records one optimistic-read outcome. The tree and tenant tags are resolved
    /// once per activation because this runs on every point read.
    /// </summary>
    private void RecordOptimisticReadOutcome(long delta, KeyValuePair<string, object?> outcome)
    {
        if (!_optimisticReadTagsResolved)
        {
            _optimisticReadTreeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId);
            _optimisticReadTenantTag = LatticeTenantLabel.ForTree(TreeId);
            _optimisticReadTagsResolved = true;
        }

        LatticeMetrics.ShardRootOptimisticReadOutcomes.Add(delta, _optimisticReadTreeTag, outcome, _optimisticReadTenantTag);
    }

    /// <summary>
    /// Publishes every optimistic-read outcome arm at zero from activation, so an
    /// absent arm reads as a measured zero rather than an unwired instrument.
    /// </summary>
    private void PrimeOptimisticReadOutcomes()
    {
        RecordOptimisticReadOutcome(0, LatticeMetrics.OutcomeOptimisticReadValidatedTag);
        RecordOptimisticReadOutcome(0, LatticeMetrics.OutcomeOptimisticReadDisabledTag);
        RecordOptimisticReadOutcome(0, LatticeMetrics.OutcomeOptimisticReadBusyTag);
        RecordOptimisticReadOutcome(0, LatticeMetrics.OutcomeOptimisticReadGateClosedTag);
        RecordOptimisticReadOutcome(0, LatticeMetrics.OutcomeOptimisticReadRoutingCacheMissTag);
        RecordOptimisticReadOutcome(0, LatticeMetrics.OutcomeOptimisticReadEpochChangedTag);
        RecordOptimisticReadOutcome(0, LatticeMetrics.OutcomeOptimisticReadAbsentTag);
        RecordOptimisticReadOutcome(0, LatticeMetrics.OutcomeOptimisticReadLeafGenerationChangedTag);
    }

    /// <summary>
    /// Returns <c>true</c> when none of the serial read's gates (tree rejecting,
    /// retained redirect, deleted, moved-away slot) would reject <paramref name="key"/>.
    /// <para>
    /// The optimistic read never raises those rejections itself. It defers to the
    /// serial read, which runs <see cref="PrepareForOperationAsync"/> first and then
    /// raises (or repairs) them against settled state. Raising them here instead
    /// would let the caller's stale-routing retry loop re-enter the optimistic read
    /// indefinitely without ever reaching the serial path that settles the
    /// condition, which is a livelock observed under interleaved folds and splits.
    /// Every gate runs synchronously, so this stays inside the snapshot block.
    /// </para>
    /// </summary>
    private bool IsOptimisticReadGateOpen(string key)
    {
        try
        {
            ThrowIfTreeRejecting();
            ThrowIfRetainedRedirect();
            ThrowIfDeleted();
            ThrowIfMovedAwayForReadKey(key);
            return true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Rare by construction (a closing tree, a reshard window): the throw
            // cost is paid only on reads the serial path must adjudicate anyway.
            return false;
        }
    }

    /// <summary>
    /// Returns <c>true</c> when an optimistic read may proceed: no routing mutation
    /// is in flight, <see cref="PrepareForOperationAsync"/> owes no work (so the
    /// serial prepare would be a no-op), and no split / move-away is in progress.
    /// </summary>
    private bool CanServeOptimisticRead()
    {
        var s = state.State;
        return _routingMutationsInFlight == 0
            && s.RootNodeId is not null
            && s.PendingPromotion is null
            && s.PendingBulkGraft is null
            && s.PendingChildLinks.Count == 0
            && s.SplitInProgress is null;
    }
}
