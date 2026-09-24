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
/// treats <em>every</em> incoming call except the pure point reads as a potential
/// routing mutation: it is counted in <see cref="_routingMutationsInFlight"/> for its
/// whole duration and bumps <see cref="_routingEpoch"/> as it starts and finishes. The
/// pure reads that are exempt can only mutate routing state through
/// <see cref="PrepareForOperationSlowAsync"/>, which brackets itself the same way.
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
    Task IIncomingGrainCallFilter.Invoke(IIncomingGrainCallContext context)
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
    /// <see cref="PrepareForOperationSlowAsync"/>) and so do not invalidate an
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
    /// and two activation-scoped diagnostics publishers that never touch routing.
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
                return OptimisticReadResult.SerialRetry;
            }
        }

        if (!options.OptimisticShardRootPointReads)
        {
            return OptimisticReadResult.SerialRetry;
        }

        // Snapshot block: everything up to the traversal's first await runs in one
        // synchronous turn slice, so no other turn can mutate routing state between
        // these checks and the reads of RootNodeId / RootIsLeaf the traversal makes
        // before it first yields.
        ThrowIfTreeRejecting();
        ThrowIfRetainedRedirect();
        ThrowIfDeleted();
        if (!CanServeOptimisticRead())
        {
            return OptimisticReadResult.SerialRetry;
        }

        var epoch = _routingEpoch;
        ThrowIfMovedAwayForReadKey(key);

        byte[]? value;
        try
        {
            value = await TraverseForReadAsync(key);
        }
        catch when (_routingEpoch != epoch)
        {
            // A fault raised while routing moved under the read is not attributable
            // to the key; the serial path re-evaluates it against settled state.
            return OptimisticReadResult.SerialRetry;
        }

        // Validation: any routing mutation that started (or finished) after the
        // snapshot bumped the epoch, so an unchanged epoch proves the read observed
        // routing state no other call touched while it was in flight. The read is
        // counted for hotness only when validated, so a serial retry does not
        // double-count it.
        if (_routingEpoch != epoch)
        {
            return OptimisticReadResult.SerialRetry;
        }

        RecordRead();
        return OptimisticReadResult.FromValue(value);
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
