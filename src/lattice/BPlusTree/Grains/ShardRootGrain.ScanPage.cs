using Microsoft.Extensions.ObjectPool;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// How far a shard range-scan page fill had progressed, so a stall is
/// attributable to a phase rather than only to a duration.
/// </summary>
internal enum ScanPagePhase
{
    /// <summary>Preparing the shard for the operation, before any descent.</summary>
    Prologue,

    /// <summary>Traversing down to the start leaf.</summary>
    Descent,

    /// <summary>Reading the leaf chain.</summary>
    LeafWalk,

    /// <summary>
    /// Folding each frozen leaf's WAL tail back onto its frozen cache during a
    /// snapshot baseline capture. Distinct from <see cref="LeafWalk"/> because
    /// the fold pass is fanned out, so "the read in flight is leaf N + 1" - true
    /// of the serial chain walk - does not describe it.
    /// </summary>
    BaselineFold,
}

internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Per-call state for one range-scan page fill: the work budget the walk
    /// runs under, the phase probe that makes a stall attributable, and the
    /// hard end-to-end deadline that releases the shard when the page fill
    /// stops making progress.
    /// <para>
    /// It is a class rather than a struct because the budget has to be mutated
    /// in place by the core method while the wrapper holds the same instance,
    /// and it is pooled because a page fill is a hot path: one rented instance
    /// per call, reused, is the difference between zero steady-state
    /// allocations and one object plus one
    /// <see cref="CancellationTokenSource"/> per read.
    /// </para>
    /// </summary>
    private sealed class ScanPageWalk
    {
        /// <summary>
        /// The cooperative work budget the leaf loop samples between reads.
        /// Public field, not a property, so the core method can mutate the
        /// struct in place without copying it back.
        /// </summary>
        internal LeafWalkBudget Budget;

        /// <summary>How far the page fill has got. Written by the core method.</summary>
        internal ScanPagePhase Phase;

        /// <summary>The grain method being bounded, for the stall message.</summary>
        internal string Operation = string.Empty;

        /// <summary>The hard ceiling in force, or <see cref="Timeout.InfiniteTimeSpan"/>.</summary>
        internal TimeSpan StallDuration = Timeout.InfiniteTimeSpan;

        /// <summary>
        /// The leaf whose read this walk most recently issued, or
        /// <see langword="null"/> before the walk has issued one.
        /// </summary>
        internal GrainId? LeafInFlightId;

        /// <summary>
        /// <see cref="LeafWalkBudget.LeavesVisited"/> as it stood when
        /// <see cref="LeafInFlightId"/> was recorded, so a stall can tell a
        /// read that is genuinely outstanding from one that has since
        /// completed. See <see cref="LeafInFlight"/>.
        /// </summary>
        internal int LeafInFlightOrdinal;

        /// <summary>
        /// The leaf whose read is genuinely still outstanding, or
        /// <see langword="null"/> when the walk is between reads.
        /// <para>
        /// A leaf read is outstanding exactly when no completion has been
        /// recorded since it was issued, which is why the ordinal is captured
        /// alongside the identity: every leaf-walk site records its visit
        /// immediately after the await returns, so an unchanged
        /// <see cref="LeafWalkBudget.LeavesVisited"/> is precisely the
        /// condition "the read we issued has not come back". Without that
        /// comparison the identity would be ambiguous - the ceiling can fire
        /// either while a read is parked (the case this exists to attribute)
        /// or at the stand-down between two reads, and in the second the last
        /// identity recorded names a leaf that already answered.
        /// </para>
        /// </summary>
        internal GrainId? LeafInFlight =>
            LeafInFlightId is { } id && LeafInFlightOrdinal == Budget.LeavesVisited ? id : null;

        private CancellationTokenSource? _deadline;

        /// <summary>Whether the hard stall ceiling is armed for this call.</summary>
        internal bool IsStallGuarded => _deadline is not null;

        /// <summary>The token that fires when the hard ceiling elapses.</summary>
        internal CancellationToken DeadlineToken =>
            _deadline?.Token ?? CancellationToken.None;

        /// <summary>Whether the hard ceiling is what ended the wait.</summary>
        internal bool DeadlineFired => _deadline is { IsCancellationRequested: true };

        /// <summary>
        /// Arms the walk for one page fill. Deliberately synchronous and
        /// allocation-free on a pooled instance: it is called as the first
        /// statement of the grain call so the clock it starts covers
        /// everything the call subsequently does.
        /// </summary>
        internal void Begin(in ScanPageBounds bounds, string operation)
        {
            Budget = LeafWalkBudget.ForScanPage(bounds, LeafWalkBudget.StartClock());
            Phase = ScanPagePhase.Prologue;
            Operation = operation;
            StallDuration = bounds.StallDuration;
            LeafInFlightId = null;
            LeafInFlightOrdinal = 0;
            if (!bounds.IsStallGuarded)
            {
                return;
            }

            _deadline ??= new CancellationTokenSource();
            _deadline.CancelAfter(bounds.StallDuration);
        }

        /// <summary>
        /// Clears the walk for reuse, cancelling the armed timer. Returns
        /// <see langword="false"/> only when the instance must not be pooled.
        /// </summary>
        internal bool TryReset()
        {
            if (_deadline is not null && !_deadline.TryReset())
            {
                // A cancelled source cannot be reused. Drop it rather than the
                // whole pooled instance; the next Begin allocates a fresh one.
                _deadline.Dispose();
                _deadline = null;
            }

            Budget = default;
            Phase = ScanPagePhase.Prologue;
            Operation = string.Empty;
            StallDuration = Timeout.InfiniteTimeSpan;
            LeafInFlightId = null;
            LeafInFlightOrdinal = 0;
            return true;
        }
    }

    private static readonly ObjectPool<ScanPageWalk> ScanPageWalkPool =
        new DefaultObjectPoolProvider().Create(new ScanPageWalkPolicy());

    private sealed class ScanPageWalkPolicy : PooledObjectPolicy<ScanPageWalk>
    {
        public override ScanPageWalk Create() => new();

        public override bool Return(ScanPageWalk obj) => obj.TryReset();
    }

    /// <summary>
    /// Opens a bounded range-scan page fill. Call this as the <em>first</em>
    /// statement of the public grain method, before any <c>await</c>.
    /// <para>
    /// Both bounds a page fill runs under start their clock here, which is the
    /// whole point of resolving them synchronously
    /// (<see cref="LatticeOptionsResolver.GetScanPageBounds(string)"/>): the
    /// quantity that head-of-line-blocks a deliberately non-reentrant shard is
    /// the <em>whole</em> hold, so anything the call does before the clock
    /// starts - preparing the shard, resolving options, descending to the start
    /// leaf - is time the bounds cannot see. Issue 1992 moved the clock from
    /// the leaf loop up to the top of the method but left an
    /// <c>await GetOptionsAsync()</c> in front of it; issue 2002 closes that
    /// last gap and removes the round trip with it.
    /// </para>
    /// </summary>
    private ScanPageWalk BeginScanPage(string operation)
    {
        var walk = ScanPageWalkPool.Get();
        walk.Begin(optionsResolver.GetScanPageBounds(TreeId), operation);
        return walk;
    }

    /// <summary>
    /// Applies the hard end-to-end stall ceiling to a page fill already in
    /// flight, and returns the walk to the pool once it settles.
    /// <para>
    /// <see cref="LatticeOptions.MaxScanPageDuration"/> is cooperative: the
    /// leaf loop samples it between reads, so it can only stop the walk
    /// somewhere it can resume from - and it is therefore structurally unable
    /// to bound the two cases issue 2002 reports, a prologue that parks and a
    /// single leaf read that never returns. This ceiling is the outer
    /// guarantee that neither can hold the shard indefinitely: when it fires
    /// the call stops waiting, so the shard's queue drains and the caller
    /// retries from its last continuation token.
    /// </para>
    /// <para>
    /// Abandoning the in-flight page fill is safe and is established practice
    /// here (see <see cref="EnsureRootSlowWithDeadlineAsync"/> and the shadow
    /// forward): a page fill reads a key range, so nothing is half-applied,
    /// and Orleans runs the stray continuation on this activation's
    /// single-threaded scheduler, so it interleaves between turns rather than
    /// racing them. The abandoned walk is deliberately <em>not</em> pooled -
    /// the stray continuation keeps writing its phase and leaf counter, and
    /// reusing it would corrupt a later call's diagnostics.
    /// </para>
    /// <para>
    /// What abandoning must <em>not</em> mean is that the walk carries on
    /// working (issue 2233). <see cref="Task.WaitAsync(CancellationToken)"/>
    /// ends the wait, never the task: left alone, the core walk keeps its
    /// place in the leaf chain and, the moment the read it was parked on
    /// returns, walks on - a read, a key-range and a sibling call per leaf for
    /// the remainder of the chain. That work is charged to nobody. It is not a
    /// request, so it is invisible to the non-reentrancy queue depth and to
    /// the running-call count; its caller was answered with a stall, so it
    /// logs no timeout; its page is discarded, so it produces no result. It
    /// forces leaf activations, and each of those takes a permit from the
    /// process-wide replay gate that every other shard's leaves also draw on -
    /// so the damage is not even confined to this shard. And because the
    /// caller has already been told to retry, the retry contends with the
    /// wreckage of its own predecessor, which makes the next stall likelier
    /// still. Hence <see cref="StandDownIfCeilingFired"/>: every bounded leaf
    /// walk checks the same deadline the ceiling is watching, so a stall costs
    /// at most the one read already in flight rather than the length of the
    /// chain.
    /// </para>
    /// </summary>
    private Task<T> GuardScanPageAsync<T>(ScanPageWalk walk, Task<T> page)
    {
        if (page.IsCompletedSuccessfully)
        {
            ScanPageWalkPool.Return(walk);
            return page;
        }

        return walk.IsStallGuarded
            ? AwaitGuardedScanPageAsync(walk, page)
            : AwaitScanPageAsync(walk, page);
    }

    private async Task<T> AwaitGuardedScanPageAsync<T>(ScanPageWalk walk, Task<T> page)
    {
        T result;
        try
        {
            result = await page.WaitAsync(walk.DeadlineToken)
                .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        }
        catch (OperationCanceledException oce) when (walk.DeadlineFired)
        {
            ObserveAbandonedScanPage(page);
            throw ScanPageStalled(walk, oce);
        }
        catch
        {
            ScanPageWalkPool.Return(walk);
            throw;
        }

        ScanPageWalkPool.Return(walk);
        return result;
    }

    private async Task<T> AwaitScanPageAsync<T>(ScanPageWalk walk, Task<T> page)
    {
        try
        {
            var result = await page.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            ScanPageWalkPool.Return(walk);
            return result;
        }
        catch
        {
            ScanPageWalkPool.Return(walk);
            throw;
        }
    }

    /// <summary>
    /// The stand-down every bounded leaf walk takes at the top of each
    /// iteration, so that the hard page-fill ceiling stops the walk and not
    /// merely the wait on it (issue 2233).
    /// <para>
    /// This is the same deadline
    /// <see cref="AwaitGuardedScanPageAsync{T}"/> is watching, read from
    /// inside the walk rather than from outside it. Reading it here is what
    /// makes <see cref="LatticeOptions.MaxScanPageStallDuration"/> mean what
    /// it says: without it the ceiling ends the caller's wait and leaves the
    /// walk running, so the bound applies to how long a caller waits rather
    /// than to how much work a stalled page fill costs the silo.
    /// </para>
    /// <para>
    /// It throws rather than returning a truncated page on purpose. The page
    /// this walk would build is discarded either way - the guard has already
    /// answered the caller - so returning one would only oblige all sixteen
    /// core methods to name a resume key they have no caller for. Throwing
    /// unwinds each of them identically, and the
    /// <see cref="OperationCanceledException"/> it raises is the same fault
    /// the guard already converts to a
    /// <see cref="ScanPageStalledException"/> when the cancellation beats the
    /// walk to it, so the caller cannot tell which of the two raced. When the
    /// guard has already answered, the throw lands on an abandoned task and is
    /// observed by <see cref="ObserveAbandonedScanPage"/>.
    /// </para>
    /// <para>
    /// Deliberately <em>not</em> a work or volume predicate. The walk this
    /// stops has not overrun any leaf, row or byte bound - it is stopped
    /// because the wall clock the ceiling set has elapsed, which is the only
    /// quantity that moves when the fault is a read that will not return.
    /// </para>
    /// </summary>
    private static void StandDownIfCeilingFired(ScanPageWalk scan) =>
        scan.DeadlineToken.ThrowIfCancellationRequested();

    /// <summary>
    /// The same stand-down, additionally recording the leaf whose read the
    /// walk is about to issue so that a stall names it (issue 2278).
    /// <para>
    /// Before this, a leaf-walk stall reported only an <em>ordinal</em> - "the
    /// read in flight was leaf 1" - which cannot be joined to anything else in
    /// the log. That is the difference between knowing a page fill stopped and
    /// being able to ask why: a stall at zero leaves has several candidate
    /// causes (the leaf is replaying its whole WAL window from cold, its
    /// activation is queued behind another call, its storage read is
    /// contended), and every one of them is distinguishable from the others
    /// only by looking at what that specific leaf was doing. The wrapped
    /// <see cref="OperationCanceledException"/> cannot help: the guard raises
    /// it itself when the ceiling fires, so it reports the ceiling rather than
    /// the cause.
    /// </para>
    /// <para>
    /// The identity is recorded <em>after</em> the stand-down, never before,
    /// so a walk that stands down here does not name a leaf it never read.
    /// </para>
    /// </summary>
    private static void StandDownIfCeilingFired(ScanPageWalk scan, GrainId leafId)
    {
        scan.DeadlineToken.ThrowIfCancellationRequested();
        scan.LeafInFlightId = leafId;
        scan.LeafInFlightOrdinal = scan.Budget.LeavesVisited;
    }

    /// <summary>
    /// Observes the outcome of a page fill the ceiling has abandoned, so that
    /// the stand-down it is about to take cannot surface as an unobserved task
    /// exception.
    /// <para>
    /// The continuation runs on <see cref="TaskScheduler.Default"/> rather
    /// than on the captured activation scheduler: it exists only to read
    /// <see cref="Task.Exception"/>, and the activation's single thread is the
    /// resource this whole guard is trying to protect.
    /// </para>
    /// </summary>
    private static void ObserveAbandonedScanPage(Task page)
    {
        if (page.IsCompleted)
        {
            _ = page.Exception;
            return;
        }

        _ = page.ContinueWith(
            static abandoned => _ = abandoned.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// Builds the typed stall fault and records it, tagged with the phase that
    /// makes the next occurrence self-diagnosing rather than a bare duration.
    /// </summary>
    private ScanPageStalledException ScanPageStalled(ScanPageWalk walk, OperationCanceledException cause)
    {
        var phase = walk.Phase;
        var leaves = walk.Budget.LeavesVisited;
        var leafInFlight = walk.LeafInFlight;
        LatticeMetrics.ScanPageStalls.Add(
            1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, MyShardIndex),
            PhaseTag(phase),
            LatticeTenantLabel.ForTree(TreeId));

        // Deliberately in the message and the typed slot only, never a metric
        // tag: leaf identity is unbounded cardinality, and the counter above is
        // already attributable by tree, shard and phase.
        var which = leafInFlight is { } id ? $", leaf {id}" : string.Empty;

        var where = phase switch
        {
            ScanPagePhase.Prologue =>
                "while preparing the shard for the operation, before any leaf was read",
            ScanPagePhase.Descent =>
                "while traversing down to the start leaf, before any leaf was read",
            ScanPagePhase.BaselineFold =>
                $"while folding the frozen leaves' WAL tails, over a chain of {leaves} leaf/leaves; "
                + "the fold pass is fanned out, so several leaf folds may have been in flight",
            _ => $"while reading the leaf chain, after {leaves} leaf/leaves; the read in flight was "
                + $"leaf {leaves + 1}{which}",
        };

        return new ScanPageStalledException(
            $"{walk.Operation} on shard {MyShardIndex} of tree '{TreeId}' exceeded the "
            + $"{walk.StallDuration} page-fill ceiling "
            + $"({nameof(LatticeOptions.MaxScanPageStallDuration)}) {where}. "
            + $"{nameof(LatticeOptions.MaxScanPageDuration)} is sampled between leaf reads, so it "
            + "cannot stop a single await that never returns; the page fill is abandoned so the "
            + "shard stops being held and the operation can be retried from its last continuation "
            + "token.", cause)
        {
            TreeId = TreeId ?? string.Empty,
            ShardIndex = MyShardIndex,
            Operation = walk.Operation,
            Phase = PhaseLabel(phase),
            LeavesVisited = leaves,
            LeafInFlight = leafInFlight?.ToString(),
            TimeoutSeconds = walk.StallDuration.TotalSeconds,
        };
    }

    private static KeyValuePair<string, object?> PhaseTag(ScanPagePhase phase) => phase switch
    {
        ScanPagePhase.Prologue => LatticeMetrics.PhaseScanPagePrologueTag,
        ScanPagePhase.Descent => LatticeMetrics.PhaseScanPageDescentTag,
        ScanPagePhase.BaselineFold => LatticeMetrics.PhaseScanPageBaselineFoldTag,
        _ => LatticeMetrics.PhaseScanPageLeafWalkTag,
    };

    private static string PhaseLabel(ScanPagePhase phase) => phase switch
    {
        ScanPagePhase.Prologue => "prologue",
        ScanPagePhase.Descent => "descent",
        ScanPagePhase.BaselineFold => "baseline-fold",
        _ => "leaf-walk",
    };
}
