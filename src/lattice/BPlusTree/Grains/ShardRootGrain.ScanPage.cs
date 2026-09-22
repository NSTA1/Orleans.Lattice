using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// How far a shard range-scan page fill had progressed, so a stall is
/// attributable to a phase rather than only to a duration.
/// </summary>
[InstrumentedEnum(
    typeof(ShardRootGrain),
    "orleans.lattice.shard_root.scan_page.stalls",
    LatticeMetrics.TagPhase)]
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
        /// <see langword="null"/> before the walk has issued one. Written only
        /// by <see cref="StandDownIfCeilingFired(ScanPageWalk, GrainId)"/>, so
        /// it is recorded on every leaf-walk path and on none of the fold pass.
        /// Read through <see cref="LeafInFlight"/>, never directly: on its own
        /// this is "last issued", not "still outstanding".
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
        /// <see langword="null"/> when no leaf read is outstanding.
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
        /// <para>
        /// <see langword="null"/> has exactly three meanings, and issue 2365
        /// removed a fourth. In <see cref="ScanPagePhase.Prologue"/> and
        /// <see cref="ScanPagePhase.Descent"/> no leaf read has been issued at
        /// all; in <see cref="ScanPagePhase.LeafWalk"/> it means the walk is
        /// between reads, and that reading is now trustworthy on every
        /// leaf-walk path because all of them record through
        /// <see cref="StandDownIfCeilingFired(ScanPageWalk, GrainId)"/>; and in
        /// <see cref="ScanPagePhase.BaselineFold"/> it means no identity is
        /// recorded by design - see that overload's sibling for why the fold
        /// pass cannot use this mechanism. The fourth meaning was "this call
        /// site never records an identity", which held on five of the six
        /// leaf-walk stand-downs and rendered identically to "between reads",
        /// so a stall from a projection-admin or diagnostics walk looked like a
        /// measured negative when the diagnostic had simply never been applied.
        /// </para>
        /// </summary>
        internal GrainId? LeafInFlight =>
            LeafInFlightId is { } id && LeafInFlightOrdinal == Budget.LeavesVisited ? id : null;

        /// <summary>
        /// The rows the walk has collected so far - a
        /// <c>List&lt;KeyValuePair&lt;string, byte[]&gt;&gt;</c> or a
        /// <c>List&lt;string&gt;</c> - published by the core method so that a
        /// ceiling fire can bank them rather than discard them (issue 2585).
        /// <para>
        /// It is the walk's own live list rather than a copy, so the guard must
        /// copy before handing it out: the abandoned walk keeps appending until
        /// its next stand-down, and Orleans would otherwise serialise a list
        /// that is being mutated. Reading it is nonetheless safe without a
        /// lock, and for a structural reason rather than a timing one - the
        /// activation scheduler is single-threaded, so the guard's continuation
        /// can only run while the walk is parked at an <c>await</c>, and no
        /// leaf-walk site awaits anything inside its append loop. The list a
        /// ceiling fire observes is therefore always at a leaf boundary, never
        /// mid-leaf.
        /// </para>
        /// <para>
        /// Typed as <see cref="object"/> because the two page shapes collect
        /// different element types through the same pooled walk; the guard
        /// discriminates on the page type it was asked for, which is a
        /// once-per-stall cost on a path that has already lost a wall-clock
        /// ceiling.
        /// </para>
        /// </summary>
        internal object? Accumulated;

        /// <summary>
        /// The moved-away virtual slots the walk has filtered out so far, or
        /// <see langword="null"/> when it has filtered none.
        /// <para>
        /// A banked page has to carry these or it loses rows silently. A
        /// strongly consistent scan reads
        /// <see cref="EntriesPage.MovedAwaySlots"/> to re-ask the split's new
        /// owner for the keys the old owner filtered; a page that banks the
        /// rows but drops the slots reports success, raises nothing, and simply
        /// omits every key in those slots from the caller's result.
        /// </para>
        /// </summary>
        internal HashSet<int>? MovedAwaySlots;

        /// <summary>
        /// A complete, immutable partial result the core method has published
        /// for the guard to hand back verbatim if the ceiling fires, or
        /// <see langword="null"/> when the walk has reached no bankable
        /// checkpoint (issue 2807).
        /// <para>
        /// This is the counterpart of <see cref="Accumulated"/> for the walks
        /// whose result is an <em>aggregate</em> rather than a list of rows - a
        /// count, an emptiness probe, a rollup. Those cannot be banked from raw
        /// rows, because their partial-result contract expresses progress as a
        /// <c>ResumeFromInclusive</c> key rather than as rows the caller can
        /// derive a continuation from, and that key is a leaf boundary the
        /// guard has no way to obtain: resolving one is an <c>await</c> on the
        /// very shard whose unresponsiveness is the reason the ceiling fired.
        /// </para>
        /// <para>
        /// So the core method publishes the whole page instead, at each leaf
        /// boundary where it holds both its running aggregate and a usable
        /// resume key - which it obtains from the
        /// <c>GetKeyRangeAsync</c> the walk now makes on every leaf. The
        /// guard then needs no per-operation knowledge at all: it type-checks
        /// what was published and returns it.
        /// </para>
        /// <para>
        /// Publishing a <em>value</em> rather than a live accumulator is what
        /// makes this safe against the abandoned walk, which keeps running
        /// until its own stand-down. Each published page is an immutable
        /// snapshot, so the one the guard reads is whichever checkpoint the
        /// walk had last completed and cannot be mutated under serialization -
        /// the copy <see cref="Accumulated"/> needs is unnecessary here.
        /// </para>
        /// </summary>
        internal object? BankedPartial;

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
            Accumulated = null;
            MovedAwaySlots = null;
            BankedPartial = null;
            if (!bounds.IsStallGuarded)
            {
                // Drop any source inherited from a previous call on this POOLED
                // instance, or this call reports a ceiling it does not have
                // (issue #2809). TryReset disarms the timer but deliberately
                // keeps the source for reuse, and IsStallGuarded is defined as
                // "_deadline is not null", so an unguarded call renting an
                // instance that last served a guarded one would answer true -
                // and be sent down the coalescing path that the ceiling is what
                // justifies. It also made the unguarded early return in
                // ReadLeafAsync unreachable after the first guarded walk in the
                // process, which is why the priming defect below it could not
                // be pinned by a test until this was corrected.
                //
                // Disposing is safe and is the same disposal TryReset already
                // performs on its own failure path: the previous call has stood
                // down, and a successful TryReset has already invalidated every
                // token it handed out.
                _deadline?.Dispose();
                _deadline = null;
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
            Accumulated = null;
            MovedAwaySlots = null;
            BankedPartial = null;
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
        // Above everything this method does, and above every caller's own early
        // returns, for the reason set out on PrimeScanPageStallPhases. The
        // activation hook is the primary site; this one keeps the invariant "no
        // stall is ever recorded on an unprimed phase arm" true on its own for
        // an activation that somehow reached a page fill without running it. It
        // is a latched bool read on every subsequent call.
        PrimeScanPageStallPhases();

        // Same reasoning, and the same pair of seams, for the chain-regression
        // counter that replaced the unbounded warning stream (issue 3341).
        PrimeScanChainRegressions();

        var walk = ScanPageWalkPool.Get();
        walk.Begin(optionsResolver.GetScanPageBounds(TreeId), operation);
        return walk;
    }

    private bool _scanPageStallPhasesPrimed;

    /// <summary>
    /// Publishes all four <see cref="ScanPagePhase"/> arms of
    /// <see cref="LatticeMetrics.ScanPageStalls"/> at zero, so that an absent
    /// phase is a measured zero rather than an absent measurement (issue #2952).
    /// <para>
    /// Before this, the only write to the counter was the increment on the stall
    /// path, so exactly the arm that had already fired existed. A live scrape
    /// carried <c>leaf-walk</c> on 321 series and <b>nothing at all</b> for
    /// <c>prologue</c>, <c>descent</c> and <c>baseline-fold</c> - and an absent
    /// arm is byte-identical to an arm that was never reached, which is
    /// byte-identical to an arm whose call site does not exist. That cost the
    /// epic a written-down discriminator: a pre-registered acceptance predicate
    /// claimed a stall surfacing under <c>prologue</c> or <c>descent</c> would
    /// prove the fault had moved off the leaf read, and the clause had to be
    /// withdrawn in its two-sided form because the <em>continued absence</em> of
    /// such a stall proved nothing whatsoever.
    /// </para>
    /// <para>
    /// <b>Why activation, and not the page-fill entry the issue proposed.</b>
    /// Issue #2952 suggested binding the prime to the point a shard first begins
    /// a scan page, on the reasoning that it primes only the shards that
    /// demonstrably scan. That bound is sound on cardinality and is still taken
    /// as a second call site, but on its own it is strictly weaker than the one
    /// this grain already established for
    /// <see cref="LatticeMetrics.ScanPageLeafReadOutcomes"/> under issue #2809: a
    /// workload-gated prime leaves an absent series meaning either "the build
    /// does not carry the instrument" <em>or</em> "it does and no page fill ever
    /// ran", which is the same two-reading ambiguity one layer out. Priming from
    /// the lifecycle hook collapses it, and costs the same four series per
    /// <c>(tree, shard)</c> that activates - the identical population that
    /// already carries three primed leaf-read arms from that earlier fix, so the
    /// cardinality precedent is set rather than newly taken.
    /// </para>
    /// <para>
    /// <b>Priming is correct here because this is a counter.</b> Adding zero to a
    /// counter is the identity, so it creates the series and changes no reading
    /// of it. A zero <em>recorded</em> on a <c>Histogram&lt;T&gt;</c> is a
    /// fabricated sample, so this pattern must not be carried across to one.
    /// </para>
    /// </summary>
    private void PrimeScanPageStallPhases()
    {
        if (_scanPageStallPhasesPrimed)
        {
            return;
        }

        _scanPageStallPhasesPrimed = true;
        RecordScanPageStall(0, LatticeMetrics.PhaseScanPagePrologueTag);
        RecordScanPageStall(0, LatticeMetrics.PhaseScanPageDescentTag);
        RecordScanPageStall(0, LatticeMetrics.PhaseScanPageLeafWalkTag);
        RecordScanPageStall(0, LatticeMetrics.PhaseScanPageBaselineFoldTag);
    }

    /// <summary>
    /// The single write seam for <see cref="LatticeMetrics.ScanPageStalls"/>, so
    /// that a primed arm and an armed one are the same series by construction
    /// rather than by two call sites agreeing on a tag list.
    /// </summary>
    private void RecordScanPageStall(long delta, KeyValuePair<string, object?> phase) =>
        LatticeMetrics.ScanPageStalls.Add(
            delta,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, MyShardIndex),
            phase,
            LatticeTenantLabel.ForTree(TreeId));

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
    /// What abandoning must not mean is that the work is thrown away
    /// (issues 2585, 2807). The rows the walk had already read are banked as an
    /// ordinary short page by <see cref="TryBankPartialScanPage{T}"/> whenever
    /// there is at least one of them; a walk that accumulates an aggregate
    /// rather than rows publishes a finished partial page at each leaf boundary
    /// instead, and that is banked the same way. Only a fire that caught the
    /// walk with nothing to show still faults. Without that, the ceiling is a
    /// livelock rather than a bound: the retry it invites re-walks the same
    /// leaves, hits the same ceiling and discards the same work, so a page that
    /// cannot fill in one attempt cannot fill in any number of them.
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
            NoteScanPageProgress();
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

            // Issue 2585: bank what the walk had already read. Without this the
            // ceiling is not a bound but a livelock - the retry it invites
            // re-walks the same leaves, hits the same ceiling and discards the
            // same work, so a page that cannot fill in one attempt cannot fill
            // in any number of them.
            var banked = TryBankPartialScanPage<T>(walk, out var partial);
            LatticeMetrics.ScanPageCeilingOutcomes.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagShard, MyShardIndex),
                banked
                    ? LatticeMetrics.OutcomeScanPageBankedTag
                    : LatticeMetrics.OutcomeScanPageDiscardedTag,
                LatticeTenantLabel.ForTree(TreeId));

            if (banked)
            {
                // A banked page carries at least one row, so at least one leaf
                // completed: the run of zero-progress fires is broken and the
                // next fire starts a fresh count (issue #3016).
                NoteScanPageProgress();
                return partial;
            }

            var stall = ScanPageStalled(walk, oce);

            // Awaited before the throw rather than fired and forgotten, so the
            // durable record the fault reports is committed by the time the
            // caller sees it. Best-effort inside: a storage failure here must
            // not replace the fault that names the wedge (issue #3016).
            await FlushStrandedLeafRecoveryAsync();

            throw stall;
        }
        catch
        {
            ScanPageWalkPool.Return(walk);
            throw;
        }

        NoteScanPageProgress();
        ScanPageWalkPool.Return(walk);
        return result;
    }

    private async Task<T> AwaitScanPageAsync<T>(ScanPageWalk walk, Task<T> page)
    {
        try
        {
            var result = await page.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            NoteScanPageProgress();
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
    /// Turns the work an abandoned walk had already done into an ordinary
    /// short page, so that a ceiling fire costs the caller a page boundary
    /// rather than the whole attempt (issues 2585, 2807).
    /// <para>
    /// Two carriers reach it, and neither is a new wire shape. A page fill
    /// publishes its row accumulator through
    /// <see cref="BeginScanPageRows{TRow}"/> and this method assembles the page
    /// around it; an aggregate walk has no rows to accumulate, so it publishes
    /// a finished, immutable page through
    /// <see cref="PublishScanPagePartial{T}"/> at each leaf boundary and this
    /// method hands the most recent one back verbatim. The second carrier
    /// exists because the first cannot be generalised: this method would
    /// otherwise have to know how to construct every guarded operation's page,
    /// which is precisely why banking reached only the six page fills and left
    /// the other ten guarded operations discarding unconditionally.
    /// </para>
    /// <para>
    /// For the row carrier, a page carrying rows, <c>HasMore = true</c> and no
    /// <c>ResumeFromKey</c> is exactly what the cooperative
    /// <see cref="LatticeOptions.MaxScanPageDuration"/> budget already emits
    /// when a leaf declares no usable boundary, and every cursor in
    /// <c>LatticeGrain</c> already advances past it by taking the last row's
    /// key as its next continuation token. That is why the fix needs no wire
    /// format change and no caller change: it reuses a contract the callers
    /// have always had to honour.
    /// </para>
    /// <para>
    /// No <c>ResumeFromKey</c> is computed here, deliberately. The resume key
    /// is a leaf <em>boundary</em>, and this method cannot <c>await</c> the
    /// further <c>GetKeyRangeAsync</c> that would yield one - the shard whose
    /// unresponsiveness brought us here is the shard it would have to ask. It
    /// is also an inclusive lower bound, so handing back the last banked key as
    /// one would re-serve that row. The caller's exclusive last-key
    /// continuation is both correct and already implemented. The aggregate
    /// walks have no rows and so no such continuation to fall back on, which is
    /// why they resolve their boundary <em>in the walk</em>, where the leaf is
    /// already activated and its range is a <c>Task.FromResult</c> off state
    /// the walk has in hand.
    /// </para>
    /// <para>
    /// <b>Returning <see langword="false"/> for an empty accumulator is
    /// load-bearing, not an optimisation.</b> A page with no rows and no resume
    /// key carries nothing a caller can advance past, and every cursor reads
    /// that combination as the end of the scan. Banking one would convert a
    /// loud, retriable <see cref="ScanPageStalledException"/> into a silently
    /// truncated result set - a strictly worse failure, and one no test of the
    /// caller would catch. It is also what keeps the fix from trading one
    /// livelock for another: because a banked page always carries at least one
    /// row, the caller's continuation token strictly advances on every
    /// attempt, so a finite tree still terminates. The published carrier obeys
    /// the same rule from the other end - a partial is only ever published at a
    /// boundary the walk has passed, and never with a null resume key - so its
    /// caller's cursor strictly advances too.
    /// </para>
    /// <para>
    /// Two guarded operations publish nothing and still fault on a ceiling
    /// fire, both deliberately.
    /// <c>CaptureSnapshotBaselineAsync</c> has no meaningful partial: a
    /// baseline covering some of the chain is not a baseline.
    /// <c>DeleteRangeBoundedAsync</c> has one it must not bank, because its
    /// replication notification is published after the loop: a resume key past
    /// a prefix whose tombstones were applied but never published would orphan
    /// that closure permanently, where today's fault has the caller retry from
    /// the range start and re-publish it. Both exclusions are about side
    /// effects and shape, not about the boundary being unavailable - the
    /// distinction the survey behind issue 2807 did not draw.
    /// </para>
    /// <para>
    /// Copying the accumulator is likewise required. The abandoned walk keeps
    /// appending until its own stand-down observes the same deadline, so
    /// handing out the live list would let Orleans serialise a collection while
    /// it is being mutated. The published carrier needs no copy for the mirror
    /// image of that reason: each publication is a finished, immutable page
    /// that the walk replaces rather than mutates.
    /// </para>
    /// </summary>
    private bool TryBankPartialScanPage<T>(ScanPageWalk walk, out T banked)
    {
        banked = default!;

        // Issue 2807: the aggregate walks publish a finished partial page at
        // each leaf boundary they reach, so the guard hands it back without
        // knowing anything about the operation that built it. Checked first
        // because a walk publishes either a partial or an accumulator, never
        // both, and the type test is the cheaper of the two.
        if (walk.BankedPartial is T published)
        {
            banked = published;
            return true;
        }

        if (walk.Accumulated is null)
        {
            return false;
        }

        if (typeof(T) == typeof(EntriesPage)
            && walk.Accumulated is List<KeyValuePair<string, byte[]>> { Count: > 0 } entries)
        {
            banked = (T)(object)new EntriesPage
            {
                Entries = new List<KeyValuePair<string, byte[]>>(entries),
                HasMore = true,
                MovedAwaySlots = BankedMovedAwaySlots(walk),
            };
            return true;
        }

        if (typeof(T) == typeof(KeysPage)
            && walk.Accumulated is List<string> { Count: > 0 } keys)
        {
            banked = (T)(object)new KeysPage
            {
                Keys = new List<string>(keys),
                HasMore = true,
                MovedAwaySlots = BankedMovedAwaySlots(walk),
            };
            return true;
        }

        return false;
    }

    private static int[]? BankedMovedAwaySlots(ScanPageWalk walk) =>
        walk.MovedAwaySlots is { Count: > 0 } moved ? SortedSlotsArray(moved) : null;

    /// <summary>
    /// Publishes the list a page fill is about to collect into, so a ceiling
    /// fire can bank it (issue 2585). Call it in place of allocating the list
    /// directly; a core method that allocates its own list without publishing
    /// it reverts to discarding its work, silently and only under stall.
    /// </summary>
    private static List<TRow> BeginScanPageRows<TRow>(ScanPageWalk scan, int pageSize)
    {
        var rows = new List<TRow>(pageSize);
        scan.Accumulated = rows;
        return rows;
    }

    /// <summary>
    /// Tracks how far along the keyspace a paged range-scan sibling walk has
    /// already consumed, so a leaf that is not reachable by descent cannot
    /// contribute to the page (issue 3271).
    /// <para>
    /// The defect this closes: the sibling walks follow next/prev pointers with
    /// no descent-reachability check. A leaf that is spliced into the chain but
    /// unreachable from the root (an orphan - see issue 3265 for how the split
    /// seam creates them) is therefore read and emitted <em>in addition to</em>
    /// the live leaf that legitimately owns the same range. Its rows are not
    /// empty placeholders: leaf rows are not durable state, they are
    /// materialised at activation by replaying the shard WAL through a
    /// predicate keyed on (ShardIndex, LowKeyInclusive, HighKeyExclusive) and
    /// never on leaf identity, so an orphan sharing a live leaf's shard and
    /// bounds materialises a full shadow copy of its range. Nothing on the
    /// page-assembly path de-duplicates.
    /// </para>
    /// <para>
    /// <b>Why a watermark rather than a reachability probe or a set.</b>
    /// Proving descent-reachability per leaf means re-descending from the root
    /// for every leaf, which turns the O(n) sibling walk into O(n log n) grain
    /// calls and so gives up the entire reason the chain exists. A hash set at
    /// the accumulator de-duplicates rows but leaves the two other halves of
    /// the invariant unfixed: an orphan's declared bounds would still steer
    /// termination and resume, and for an entries page it raises an
    /// unanswerable question about which of two values for one key wins. The
    /// watermark instead exploits the one property the walk already depends on
    /// and can check for free: a correct chain visits the keyspace
    /// monotonically. Any key at or behind the furthest key already consumed
    /// proves the walk has re-entered territory it has left, whoever produced
    /// it, which catches the orphan case and also transient split races where
    /// a leaf is read before a split and its new sibling after.
    /// </para>
    /// <para>
    /// <b>The detection is per leaf and latching, deliberately.</b> Leaf rows
    /// arrive sorted, so the first regressed key condemns the whole leaf: every
    /// later key on it comes from the same untrusted source, including keys
    /// that happen to fall beyond the watermark because the orphan holds a
    /// stale key the live leaf no longer has. The latch clears at the next leaf
    /// because a single bad splice says nothing about the leaves past it.
    /// </para>
    /// <para>
    /// <b>The walk still follows the chain past a regressed leaf.</b> An orphan
    /// is spliced <em>between</em> live leaves, so stopping at one would
    /// truncate the page and lose live rows - a worse defect than the one being
    /// fixed. Only the orphan's rows and its declared bounds are discarded.
    /// </para>
    /// <para>
    /// <b>What this deliberately does not do:</b> it does not advance the
    /// leaf-level <c>afterExclusive</c> filter to the watermark. Doing so would
    /// suppress the duplicates one hop earlier and more cheaply, but it would
    /// also destroy the evidence that anything was wrong - the filter that
    /// hides the symptom would hide the cause - and it would make the leaf-read
    /// coalescing key depend on the contents of previously visited leaves.
    /// Rows are read as before and judged here, where the condition can be
    /// reported.
    /// </para>
    /// <para>
    /// Cost on a healthy chain is one ordinal comparison per emitted row and no
    /// allocation; the regression branch is never taken.
    /// </para>
    /// </summary>
    private struct ScanChainCursor
    {
        private readonly bool _reverse;
        private string? _watermark;
        private string? _leafEntryWatermark;
        private bool _leafRegressed;
        private bool _leafWarned;

        private ScanChainCursor(bool reverse)
        {
            _reverse = reverse;
            _watermark = null;
            _leafEntryWatermark = null;
            _leafRegressed = false;
            _leafWarned = false;
        }

        /// <summary>A cursor for a walk that consumes the keyspace ascending.</summary>
        internal static ScanChainCursor Forward() => new(reverse: false);

        /// <summary>A cursor for a walk that consumes the keyspace descending.</summary>
        internal static ScanChainCursor Reverse() => new(reverse: true);

        /// <summary>
        /// Whether the leaf currently being read regressed the chain, and so
        /// must not contribute rows, termination, or a resume position.
        /// </summary>
        internal readonly bool LeafRegressed => _leafRegressed;

        /// <summary>The furthest key consumed from a trusted leaf so far.</summary>
        internal readonly string? Watermark => _watermark;

        /// <summary>
        /// Clears the per-leaf regression latch and records where the walk had
        /// reached before this leaf contributed anything. Call once per leaf,
        /// before admitting any of its rows.
        /// </summary>
        internal void BeginLeaf()
        {
            _leafRegressed = false;
            _leafWarned = false;
            _leafEntryWatermark = _watermark;
        }

        /// <summary>
        /// Returns <see langword="true"/> at most once per leaf, and only for a
        /// leaf that regressed, so a leaf detected by both its rows and its
        /// declared bounds is reported once rather than twice.
        /// </summary>
        internal bool TryClaimWarning()
        {
            if (!_leafRegressed || _leafWarned)
                return false;
            _leafWarned = true;
            return true;
        }

        /// <summary>
        /// Judges one row's key against the watermark. Returns
        /// <see langword="true"/> when the row may be emitted, advancing the
        /// watermark to it; returns <see langword="false"/> when the key
        /// regresses the chain, latching the current leaf as untrusted for the
        /// remainder of its rows.
        /// </summary>
        /// <remarks>
        /// A key equal to the watermark is a regression, not a tie: a key lives
        /// on exactly one leaf, so seeing it twice in one walk is itself the
        /// proof that two leaves claim the same range.
        /// </remarks>
        internal bool Admit(string key)
        {
            if (_leafRegressed)
                return false;

            if (_watermark is { } mark)
            {
                var order = string.CompareOrdinal(key, mark);
                if (_reverse ? order >= 0 : order <= 0)
                {
                    _leafRegressed = true;
                    return false;
                }
            }

            _watermark = key;
            return true;
        }

        /// <summary>
        /// Judges the leaf's own declared <see cref="LeafKeyRange"/> against the
        /// watermark as it stood <em>before</em> this leaf contributed, and
        /// returns whether those bounds may be trusted to terminate the walk or
        /// to produce a resume key.
        /// </summary>
        /// <remarks>
        /// This is the half of the check <see cref="Admit"/> cannot reach. A
        /// leaf that yields no rows at all - because it is empty, or because
        /// the range or predicate filtered everything it holds - never trips
        /// the row watermark, yet its declared bounds are still consulted for
        /// termination and resume. An orphan claiming a range wider than the
        /// live leaf's would therefore end the page early and silently drop the
        /// live leaves beyond it. Comparing the leaf's leading edge against the
        /// pre-leaf watermark catches exactly that: in a correct chain the
        /// keyspace a leaf claims begins past everything already consumed, so a
        /// leading edge at or behind the pre-leaf watermark is a leaf claiming
        /// territory the walk has already left. A leaf with no bounds recorded
        /// at all is not judged, preserving the existing legacy fallback; but a
        /// leaf that declares a real trailing edge while leaving its leading
        /// edge unset is claiming the keyspace from the unbounded end, which is
        /// behind any watermark and so is judged like any other regression.
        /// </remarks>
        internal bool TrustsBounds(LeafKeyRange bounds)
        {
            if (_leafRegressed)
                return false;

            if (_leafEntryWatermark is not { } mark)
                return true;

            var edge = _reverse ? bounds.HighKeyExclusive : bounds.LowKeyInclusive;
            if (edge is null)
            {
                var trailing = _reverse ? bounds.LowKeyInclusive : bounds.HighKeyExclusive;
                if (trailing is null)
                    return true;

                _leafRegressed = true;
                return false;
            }

            var order = string.CompareOrdinal(edge, mark);
            if (_reverse ? order > 0 : order <= 0)
            {
                _leafRegressed = true;
                return false;
            }

            return true;
        }
    }

    /// <summary>
    /// Reports a leaf whose keys regressed the scan's chain watermark, which
    /// proves it is not reachable by descent for the range it claims to own
    /// (issue 3271). Called once per offending leaf per page.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is a warning rather than a throw on purpose. Throwing would convert
    /// a bounded read-correctness defect into a total read outage for exactly
    /// the trees that carry an orphan, and issue 3269 establishes that no
    /// operator repair path exists yet - so the failure would be unrecoverable
    /// rather than merely loud. The page is served correctly and the tree's
    /// need for repair is reported.
    /// </para>
    /// <para>
    /// <b>Why the log is bounded and the counter is not.</b> Reporting the
    /// suppression <em>only</em> as a log line made the report itself the
    /// outage: one field burst emitted 110,322 warnings in about eight minutes,
    /// roughly 2,354 lines a second, which is 99.3% of all warning output. That
    /// filled half of a 100 MB container log ring and collapsed the host's log
    /// retention to about 108 seconds, so nothing on that box could be diagnosed
    /// after the fact - a "we checked the logs and saw nothing" conclusion was
    /// drawn and had to be retracted. The volume was not new damage: it was
    /// 2,886 distinct leaves re-encountered about 43 times each, once per page
    /// fill, because <see cref="ScanChainCursor.TryClaimWarning"/> only dedupes
    /// within a single page. So every line after the first sighting of a leaf
    /// restated a fact already on the record (issue 3341).
    /// </para>
    /// <para>
    /// <b>Novelty-throttled, not rate-throttled, and that is deliberate.</b> The
    /// standing repository guidance on log throttling argues for a time floor
    /// rather than a count cap, because a pure count cap degrades pathologically
    /// with cadence - it silences a slow, long-running condition forever after N
    /// lines while still permitting a fast one to flood. That argument is about
    /// throttling on the <em>rate</em> axis. This throttles on the <em>fact</em>
    /// axis: output is bounded by the number of distinct damaged leaves, not by
    /// elapsed time or by a line budget, so a leaf that has never been reported
    /// is always reported and a leaf already on the record never is. The set of
    /// damaged leaves is what an operator acts on, and it does not change with
    /// how often the shard is scanned. Liveness is not lost to the throttle
    /// because it moves to
    /// <see cref="LatticeMetrics.ScanChainRegressions"/>, which is incremented on
    /// <em>every</em> suppression before any throttling decision is taken and is
    /// primed at zero so an absent series cannot be misread as a clean shard. A
    /// wall-clock floor was also rejected on mechanism: this grain takes no
    /// <see cref="TimeProvider"/>, so a time-based bound could not be tested
    /// without inventing a seam for the test alone.
    /// </para>
    /// <para>
    /// The bound is one detailed line per distinct leaf up to
    /// <see cref="ChainRegressionWarnDetailCap"/>, then exactly one summary line
    /// naming the counter, then silence for the life of the activation: about
    /// eleven lines where the field burst produced 110,322.
    /// </para>
    /// </remarks>
    private void WarnScanChainRegression(ScanPageWalk scan, GrainId leafId, string? watermark)
    {
        // Allocated lazily, so a shard with an intact chain - the overwhelming
        // majority - carries no set at all. Its size is bounded by the number
        // of damaged leaves in this shard, which is bounded by the shard's own
        // leaf population.
        var firstSighting = (_chainRegressionLeaves ??= []).Add(leafId);

        // Recorded before any throttling decision, and on every suppression, so
        // the counter carries the true magnitude the log no longer does.
        RecordScanChainRegression(1, LatticeMetrics.OutcomeScanChainRegressionSuppressionTag);

        if (!firstSighting)
        {
            return;
        }

        RecordScanChainRegression(1, LatticeMetrics.OutcomeScanChainRegressionDistinctLeafTag);

        var distinctLeaves = _chainRegressionLeaves.Count;
        if (distinctLeaves <= ChainRegressionWarnDetailCap)
        {
            logger.LogWarning(
                "Range scan {Operation} on tree {TreeId} shard {ShardIndex} suppressed rows from leaf {LeafId}: " +
                "its keys are at or behind the chain watermark {Watermark}, so the leaf is not reachable by descent " +
                "for the range it claims and its rows duplicate a live leaf (issue 3271). The shard's leaf chain " +
                "needs repair (issue 3269).",
                scan.Operation,
                TreeId,
                ShardIndex,
                leafId,
                watermark);
            return;
        }

        if (distinctLeaves == ChainRegressionWarnDetailCap + 1)
        {
            logger.LogWarning(
                "Range scan {Operation} on tree {TreeId} shard {ShardIndex} has now suppressed rows from more than " +
                "{DetailCap} distinct leaves whose keys regressed the chain watermark (issue 3271). Further leaves " +
                "will not be named in the log for the life of this activation, because restating them once per page " +
                "fill is what collapsed log retention on the host that first met this (issue 3341). Read " +
                "{CounterName} instead: its distinct-leaf arm is the number of damaged leaves and its suppression " +
                "arm is the rate. The shard's leaf chain needs repair (issue 3269).",
                scan.Operation,
                TreeId,
                ShardIndex,
                ChainRegressionWarnDetailCap,
                LatticeMetrics.ScanChainRegressions.Name);
        }
    }

    /// <summary>
    /// How many distinct chain-regressed leaves a single shard-root activation
    /// names in the log before it falls back to the counter. Ten is enough for
    /// an operator to recognise the shape of the damage - which leaves, which
    /// watermarks - without the log becoming the outage.
    /// </summary>
    internal const int ChainRegressionWarnDetailCap = 10;

    /// <summary>
    /// The distinct leaves this activation has already reported as chain
    /// regressions. <see langword="null"/> until the first regression, so an
    /// intact shard allocates nothing.
    /// </summary>
    private HashSet<GrainId>? _chainRegressionLeaves;

    private bool _scanChainRegressionsPrimed;

    /// <summary>
    /// Publishes both arms of <see cref="LatticeMetrics.ScanChainRegressions"/>
    /// at zero, so that a shard whose chain is intact reports a measured zero
    /// rather than no series at all.
    /// <para>
    /// This matters more here than on most counters, because the whole point of
    /// issue 3341 is that the suppression stopped being visible in the log. If
    /// the counter that replaced the log only existed after the first
    /// suppression, an absent series would read as "the feature is off" or "this
    /// build does not carry it" exactly as often as it read "this shard is
    /// clean", and the fix would have traded one blindness for another.
    /// </para>
    /// <para>
    /// Priming is correct here because this is a counter: adding zero is the
    /// identity, so it creates the series and changes no reading of it. The same
    /// pattern must not be carried across to a <c>Histogram&lt;T&gt;</c>, where a
    /// recorded zero is a fabricated sample.
    /// </para>
    /// </summary>
    private void PrimeScanChainRegressions()
    {
        if (_scanChainRegressionsPrimed)
        {
            return;
        }

        _scanChainRegressionsPrimed = true;
        RecordScanChainRegression(0, LatticeMetrics.OutcomeScanChainRegressionSuppressionTag);
        RecordScanChainRegression(0, LatticeMetrics.OutcomeScanChainRegressionDistinctLeafTag);
    }

    /// <summary>
    /// The single write seam for
    /// <see cref="LatticeMetrics.ScanChainRegressions"/>, so that a primed arm
    /// and an armed one are the same series by construction rather than by two
    /// call sites agreeing on a tag list.
    /// </summary>
    private void RecordScanChainRegression(long delta, KeyValuePair<string, object?> outcome) =>
        LatticeMetrics.ScanChainRegressions.Add(
            delta,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, MyShardIndex),
            outcome,
            LatticeTenantLabel.ForTree(TreeId));

    /// <summary>
    /// Publishes a finished partial page for the guard to bank if the ceiling
    /// fires (issue 2807), replacing any earlier checkpoint from this walk.
    /// <para>
    /// Call it at a leaf boundary, with the aggregate the walk has accumulated
    /// up to and including the leaf just completed, and a
    /// <c>ResumeFromInclusive</c> that is that leaf's exclusive high bound. The
    /// two must be consistent or the banked answer is wrong in the one way this
    /// whole mechanism must not be: a resume key naming a leaf already folded
    /// into the aggregate makes the caller's next batch count it twice, which
    /// is precisely the hazard <see cref="ShardCountPage"/>'s own contract
    /// calls out. Never publish against the leaf whose read is in flight.
    /// </para>
    /// <para>
    /// A page must never be published with a null resume key. For these
    /// operations "no resume key" is the wire signal for <em>complete</em>, so
    /// banking one would convert a loud, retriable
    /// <see cref="ScanPageStalledException"/> into a silently wrong answer -
    /// an undercount or a populated shard reported empty. It is the same rule
    /// that makes <see cref="TryBankPartialScanPage{T}"/> refuse an empty row
    /// accumulator, for the same reason.
    /// </para>
    /// </summary>
    private static void PublishScanPagePartial<T>(ScanPageWalk scan, T partial) =>
        scan.BankedPartial = partial;

    /// <summary>
    /// The key a walk may resume from once it has finished with the leaf whose
    /// <paramref name="bounds"/> these are, or <see langword="null"/> when the
    /// leaf declares no usable boundary.
    /// <para>
    /// The leaf's exclusive high bound is exactly where the next leaf begins.
    /// A high bound outside the walk's own <c>[lowerBound, upperBound)</c> is
    /// not a position this walk can resume from, and when there is no safe key
    /// the caller must keep walking rather than stop, because stopping without
    /// a resume position would silently truncate - the "only stop where you can
    /// resume" rule the range-delete and page-fill bounds also follow.
    /// </para>
    /// </summary>
    private static string? ResumeKeyFrom(
        in LeafKeyRange bounds, string? lowerBound, string? upperBound)
    {
        if (bounds.HighKeyExclusive is { } high
            && (lowerBound is null || string.CompareOrdinal(high, lowerBound) > 0)
            && (upperBound is null || string.CompareOrdinal(high, upperBound) < 0))
        {
            return high;
        }

        return null;
    }

    /// <summary>
    /// Records a virtual slot the walk filtered out as moved away, into both
    /// the core method's own set and the walk, so a banked page reports it.
    /// </summary>
    private static void RecordMovedAwaySlot(ScanPageWalk scan, ref HashSet<int>? movedSet, int slot)
    {
        scan.MovedAwaySlots = movedSet ??= [];
        movedSet.Add(slot);
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
    /// It throws rather than returning a truncated page on purpose, and that
    /// stays true after issue 2585 made the <em>outer</em> guard bank the rows
    /// this walk had already collected. The two are not in tension: the guard
    /// has already answered the caller by the time this fires, so a page built
    /// here would have nobody to return it to, and the rows it would have
    /// carried are precisely the ones the guard read out of
    /// <see cref="ScanPageWalk.Accumulated"/> before abandoning the walk.
    /// Throwing unwinds all sixteen core methods identically without obliging
    /// any of them to name a resume key, and the
    /// <see cref="OperationCanceledException"/> it raises is the same fault
    /// the guard already handles when the cancellation beats the walk to it,
    /// so the caller cannot tell which of the two raced. When the guard has
    /// already answered, the throw lands on an abandoned task and is observed
    /// by <see cref="ObserveAbandonedScanPage"/>.
    /// </para>
    /// <para>
    /// Deliberately <em>not</em> a work or volume predicate. The walk this
    /// stops has not overrun any leaf, row or byte bound - it is stopped
    /// because the wall clock the ceiling set has elapsed, which is the only
    /// quantity that moves when the fault is a read that will not return.
    /// </para>
    /// <para>
    /// This overload records no leaf identity, so a leaf-walk site must call
    /// <see cref="StandDownIfCeilingFired(ScanPageWalk, GrainId)"/> instead
    /// (issue 2365): a stand-down that records nothing leaves
    /// <see cref="ScanPageWalk.LeafInFlight"/> permanently
    /// <see langword="null"/>, which a stall reader cannot distinguish from
    /// the documented "between reads". The one legitimate caller is the
    /// <see cref="ScanPagePhase.BaselineFold"/> pass, and the reason is
    /// structural rather than a matter of effort: the freshness test that
    /// makes a recorded identity trustworthy is
    /// <c>LeafInFlightOrdinal == Budget.LeavesVisited</c>, and the fold pass
    /// never calls <see cref="LeafWalkBudget.RecordLeafVisited"/>, so an
    /// identity recorded there would compare equal forever and go on naming a
    /// leaf that had already answered. That is strictly worse than naming
    /// none, which is why the fold pass reports its fan-out through the phase
    /// instead.
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
        RecordScanPageStall(1, PhaseTag(phase));

        // Issue #3016. Classified here and nowhere else, because this is the
        // one site that both knows the fire completed no leaf and knows which
        // leaf it was parked on - and because a fire that banked its rows
        // returns before reaching here, so no page that made progress can be
        // mistaken for a wedge.
        var progress = ClassifyScanPageStall(walk);
        var stranded = progress == ScanPageLeafProgress.Stranded;
        var consecutive = _consecutiveZeroProgressStalls;
        var applications = stranded ? state.State.StrandedScanRecoveries : 0;

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

        // The wedge clause. A stall that is one of a run is a categorically
        // different report from a stall that is the first of its kind, and the
        // two were indistinguishable for the 307 attempts behind issue #3016.
        var run = stranded
            ? $" This is the {consecutive}th consecutive ceiling fire on this shard that completed "
                + "no leaf and named this same leaf, so the leaf is classified UNREADABLE rather "
                + "than slow: retrying it unchanged has already been tried and did not differ. The "
                + "coalesced read the retries were attaching to has been abandoned so the next "
                + "attempt issues a fresh one."
            : consecutive > 1
                ? $" This is the {consecutive}th consecutive ceiling fire on this shard that "
                    + "completed no leaf and named this same leaf."
                : string.Empty;

        // The did-the-remedy-take clause. The run above is bounded by this
        // activation, and a freshly activated shard root holds no coalesced
        // reads - so its eviction is a no-op and its stall reads identically to
        // a first-ever stall however long the leaf has been unreadable. This
        // count is durable precisely so that reading cannot recur (issue #3016).
        var took = stranded && applications > 1
            ? $" The recovery has now been applied to this leaf {applications} times across every "
                + "activation of this shard root, so a fresh read was already issued on an earlier "
                + "occasion and the leaf still did not answer: the fault is inside that leaf "
                + "activation rather than in this shard root's read coalescing, and no further "
                + "scan attempt will converge without the leaf being made readable."
            : string.Empty;

        return new ScanPageStalledException(
            $"{walk.Operation} on shard {MyShardIndex} of tree '{TreeId}' exceeded the "
            + $"{walk.StallDuration} page-fill ceiling "
            + $"({nameof(LatticeOptions.MaxScanPageStallDuration)}) {where}. "
            + $"{nameof(LatticeOptions.MaxScanPageDuration)} is sampled between leaf reads, so it "
            + "cannot stop a single await that never returns; the page fill is abandoned so the "
            + "shard stops being held and the operation can be retried from its last continuation "
            + $"token.{run}{took}", cause)
        {
            TreeId = TreeId ?? string.Empty,
            ShardIndex = MyShardIndex,
            Operation = walk.Operation,
            Phase = PhaseLabel(phase),
            LeavesVisited = leaves,
            LeafInFlight = leafInFlight?.ToString(),
            ConsecutiveZeroProgressStalls = consecutive,
            LeafStranded = stranded,
            StrandedRecoveryApplications = applications,
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
