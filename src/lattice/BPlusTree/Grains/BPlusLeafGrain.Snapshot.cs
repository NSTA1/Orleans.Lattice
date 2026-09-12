using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Snapshot-capture partial for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>. Adds the
/// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.CaptureSnapshotAsync"/> seam that copies
/// the per-activation entry cache into a canonical byte-row
/// <see cref="LeafSnapshotBlob"/> and persists it through the dedicated
/// <see cref="ILeafSnapshotStorageGrain"/> keyed by this leaf's grain
/// id. The capture is read-only on the leaf side - it stamps the blob
/// with the already-persisted <c>ProjectionCheckpointOffset</c> and
/// does not mutate any leaf state.
/// <para>
/// Capture is driven by the leaf itself (not by the maintenance
/// grain): when the fall-off-log detector raises the
/// <see cref="Orleans.Lattice.BPlusTree.Grains.FallOffLogDecision.SnapshotPending"/> advisory at
/// activation time, the leaf latches <see cref="_activationSnapshotPending"/>
/// and captures once the tail replay has completed. While the leaf
/// stays active, every
/// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/>
/// successful checkpoint persist re-classifies and (on advisory) drives
/// another capture. A single-flight guard
/// (<see cref="_snapshotCaptureInFlight"/>) suppresses overlapping
/// captures so a slow <c>SaveAsync</c> cannot pin a follow-on capture
/// behind it - the follow-on is dropped and the next cadence tick
/// re-evaluates.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Latched at activation when the fall-off-log detector returns
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.FallOffLogDecision.SnapshotPending"/>. The activation
    /// hook reads-and-clears the flag after the tail replay so a
    /// proactive capture fires exactly once per advisory-firing
    /// activation.
    /// </summary>
    private bool _activationSnapshotPending;

    /// <summary>
    /// Single-flight guard for the snapshot-capture seam. Set on
    /// entry to <see cref="CaptureSnapshotAsync"/> and the periodic
    /// recheck path, cleared on completion. Concurrent capture
    /// invocations observe a <c>true</c> value and return immediately;
    /// the next cadence tick re-evaluates.
    /// </summary>
    private bool _snapshotCaptureInFlight;

    /// <summary>
    /// Number of successful checkpoint persists since this
    /// activation last ran the periodic snapshot recheck.
    /// <see cref="FlushPendingCheckpointAsync"/> increments this on
    /// every successful persist; when it reaches
    /// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/>
    /// the leaf re-runs the fall-off-log detector and, on advisory,
    /// drives a capture.
    /// </summary>
    private int _checkpointPersistCountSinceRecheck;

    /// <summary>
    /// Set once this activation genuinely advances a per-partition projection
    /// checkpoint over cache-resident applies - i.e. the pending-advance branch
    /// of <see cref="FlushPendingCheckpointAsync"/> runs, which only happens
    /// after foreground writes or a WAL tail replay have folded new entries
    /// into the in-memory cache and moved the checkpoint forward. It is the
    /// safety precondition for the graceful-deactivation snapshot capture
    /// (<see cref="TryCaptureSnapshotOnDeactivateAsync"/>): a checkpoint that
    /// advanced this way is, by construction, backed by data the cache holds,
    /// so capturing that cache and stamping the new checkpoint truthfully
    /// records coverage. A leaf that merely reactivated cold - its persisted
    /// checkpoint restored from state or a rehydrate that reset uncovered
    /// partitions to <c>-1</c>, with no forward apply this activation - never
    /// sets this, so the deactivation hook does not capture an empty/partial
    /// cache and falsely claim coverage of a prefix the WAL alone still holds
    /// (the #1535 no-loss invariant). Reset to <c>false</c> on every fresh
    /// activation because it is a plain instance field, never persisted.
    /// </summary>
    private bool _checkpointAdvancedThisActivation;

    /// <summary>
    /// Set once this activation cold-rebuilt the in-memory cache from the start
    /// of the readable WAL over a <b>pre-existing durable checkpoint</b> - the
    /// residual-liveness path in <c>ReplayWalSinceCheckpointAsync</c>, latched only
    /// for a partition whose persisted <c>ProjectionCheckpointOffset</c> was
    /// already greater than zero, whose cold-cache override (<c>-1</c>) drove the
    /// replay from the absolute WAL start, and whose durable-frontier fall-off
    /// guard (#945) passed - so the checkpointed prefix provably still survives in
    /// the readable WAL and the rebuild reconstructs the entire readable window.
    /// Like <see cref="_checkpointAdvancedThisActivation"/> this is a safety
    /// precondition for the graceful-deactivation snapshot capture
    /// (<see cref="TryCaptureSnapshotOnDeactivateAsync"/>): a faithful full rebuild
    /// leaves the cache holding a superset of every checkpointed prefix, so
    /// capturing that cache and stamping each partition's checkpoint truthfully
    /// records coverage. It closes the residual liveness gap #1537 leaves open -
    /// an already-converged leaf (its checkpoint already at head) cold-reactivates,
    /// rebuilds its full cache, but advances no checkpoint, so
    /// <see cref="_checkpointAdvancedThisActivation"/> alone stays <c>false</c> and
    /// the block pin would never lift. A brand-new leaf has no pre-existing
    /// checkpoint (persisted offset <c>0</c>), never satisfies the guard condition,
    /// and so its foreground writes are never auto-covered - preserving the #1535
    /// no-loss invariant and leaving an uncovered checkpoint pinned at the Zero
    /// block. Reset to <c>false</c> on every fresh activation because it is a plain
    /// instance field, never persisted.
    /// </summary>
    private bool _cacheRebuiltFromWalStartThisActivation;

    /// <summary>
    /// Set once this activation's snapshot rehydrate <b>lowered</b> a
    /// per-partition projection checkpoint below the durable value it held on
    /// entry - i.e. the leaf activated with a durable checkpoint the loaded
    /// snapshot does not cover (<c>durable[p] &gt; snapshotOffsets[p]</c>). This
    /// is the frozen-leaf signature (#2220): a leaf whose replay gap exceeded
    /// <c>MaxLeafReplayEntries</c> was torn down before it could capture a fresh
    /// snapshot, so its durable snapshot froze two days behind the advancing
    /// checkpoint. Every reactivation then reloads that stale snapshot, the
    /// per-partition rehydrate loop rolls the advanced partition back to the
    /// snapshot offset (lowering is REQUIRED for cache coherence after the
    /// whole-cache <c>Cache.Clear()</c>, so the tail replay rebuilds the
    /// dropped rows), and the leaf re-replays the same window forever while its
    /// WAL pin stays frozen at the stale covered offset and its WAL grows
    /// unbounded. The livelock only breaks if the leaf banks a fresh snapshot
    /// covering the re-advanced checkpoint DURING an activation, off the
    /// deactivation deadline; the periodic recheck cannot do it because its
    /// per-activation persist counter resets every activation and a short
    /// over-budget activation never reaches
    /// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/>.
    /// This latch drives the off-cadence coverage-deficit capture in
    /// <see cref="MaybeRunPeriodicSnapshotRecheckAsync"/> exactly once, after
    /// the tail replay re-advances the partition over cache-resident applies
    /// (the same <see cref="_checkpointAdvancedThisActivation"/> /
    /// <see cref="_cacheRebuiltFromWalStartThisActivation"/> no-loss precondition
    /// the graceful-deactivation capture already trusts). Reset to <c>false</c>
    /// on every fresh activation because it is a plain instance field, never
    /// persisted.
    /// </summary>
    private bool _snapshotCoverageDeficitAtActivation;

    /// <summary>
    /// Per-activation budget for the zero-coverage repair path (issue #2692).
    /// The repair is self-extinguishing on success - a capture stamps coverage
    /// for every checkpointed partition, and coverage is monotone-max, so the
    /// trigger predicate is false forever afterwards - which means this budget
    /// is only ever consumed by captures that FAIL. Eight attempts absorbs a
    /// transient snapshot-store fault without letting a persistently failing
    /// store turn every checkpoint persist into a capture attempt.
    /// </summary>
    private const int MaxZeroCoverageRepairAttempts = 8;

    /// <summary>
    /// Number of zero-coverage repair captures attempted on this activation.
    /// Reset implicitly on every activation because it is a plain instance
    /// field, never persisted - which is correct, since a fresh activation
    /// re-reads the durable snapshot and so re-derives the coverage the budget
    /// is spent chasing.
    /// </summary>
    private int _zeroCoverageRepairAttempts;

    /// <summary>
    /// Whether this activation has already reported budget exhaustion on
    /// <see cref="LatticeMetrics.LeafSnapshotCoverageRepairs"/>. Keeps the
    /// exhaustion series a count of stuck ACTIVATIONS rather than of persists,
    /// which would otherwise scale with write rate and say nothing about how
    /// many leaves are stuck.
    /// </summary>
    private bool _zeroCoverageRepairExhaustionReported;

    /// <summary>
    /// Byte-accurate footprint of the most recently persisted snapshot
    /// for this leaf, or <c>0</c> when no snapshot has been captured this
    /// activation. Mirrors the value written into
    /// <see cref="LeafSnapshotBlob.SnapshotBytes"/> at capture time and
    /// is consumed by the per-persist byte-footprint publish path in
    /// <c>BPlusLeafGrain.Metrics.PersistAsync</c> so the shard root's
    /// running snapshot-bytes total stays current without a snapshot
    /// storage read on the persist hot path.
    /// </summary>
    private long _lastCapturedSnapshotBytes;

    /// <summary>
    /// Per-partition WAL offset that a durable leaf snapshot is known to
    /// cover for this activation, or <see langword="null"/> when no snapshot
    /// coverage is known. Slot <c>p</c> holds the highest offset a durable
    /// snapshot covers for partition <c>p</c>; <c>-1</c> (or an absent slot)
    /// means "no durable snapshot covers this partition", so the WAL prefix
    /// is the only durable copy and must not be trimmed.
    /// <para>
    /// Set from a loaded <see cref="LeafSnapshotBlob"/> at activation
    /// (rehydrate, both accept and decline paths - a loaded blob is durable
    /// regardless of whether it repopulates the cache) and advanced after a
    /// successful <see cref="CaptureSnapshotAsync"/>. Read by
    /// <c>ResolveDurablePinForPartition</c> to gate the durable materialiser
    /// pin at <c>min(checkpoint, coveredOffset)</c> so the WAL GC never
    /// authorises trimming a checkpointed prefix that no snapshot covers.
    /// </para>
    /// </summary>
    private long[]? _durableSnapshotOffsetsByPartition;

    /// <summary>
    /// Per-partition RE-READ frontier of the cold rebuild in progress on this
    /// activation: the highest offset this activation has actually re-read from
    /// the WAL start and applied into the cache, or <c>-1</c> for a partition it
    /// has not reached. <see langword="null"/> when no cold rebuild is running.
    /// <para>
    /// This is a DIFFERENT quantity from the projection checkpoint, and the
    /// distinction is the whole of issue #2280. The checkpoint is the APPLIED
    /// frontier - what the projection has durably absorbed - and it is strictly
    /// monotonic because #1492 requires it to be. A cold rebuild re-reads from
    /// offset 0 while that checkpoint still sits at its persisted value
    /// <c>C_p</c>, so every offset it re-reads below <c>C_p</c> is real progress
    /// that no monotonic scalar can express. Collapsing the two onto one number
    /// is what makes a cancelled cold replay bank nothing: see the short-circuit
    /// in <c>TryFlushRecoveredCeilingAsync</c>, which correctly refuses to lower
    /// the checkpoint and thereby also refuses to record the re-read.
    /// </para>
    /// <para>
    /// Recorded from the CLAMPED ceiling that
    /// <c>TryFlushRecoveredCeilingAsync</c> already computes - <c>maxApplied</c>
    /// bounded below the lowest unresolved deferred terminal and below any
    /// unresolved saga prepare - and never from raw <c>maxApplied</c>. That
    /// choice is load-bearing TWICE over, and a later reader must not
    /// "simplify" it away:
    /// </para>
    /// <para>
    /// (1) It makes over-claiming unrepresentable. The claim is the same
    /// quantity already trusted to advance the durable checkpoint, so it
    /// inherits a tested property instead of adding a new one.
    /// </para>
    /// <para>
    /// (2) It keeps the un-restored pending-transaction set safe. Rehydrate
    /// loads cache rows only and never repopulates <c>_pendingTx</c>, so a
    /// banked frontier ABOVE an unresolved prepare would resume past a prepare
    /// the next activation cannot reconstruct. Because the ceiling is clamped
    /// below the earliest unresolved prepare, no unresolved prepare can ever lie
    /// below a banked frontier and the re-read necessarily re-reads it.
    /// </para>
    /// </summary>
    private long[]? _coldReplayFrontierByPartition;

    /// <summary>
    /// Records that the cold rebuild on this activation has re-read and applied
    /// <paramref name="ceiling"/> for <paramref name="partition"/>. Monotonic
    /// per partition within the activation; discarded when the activation ends.
    /// </summary>
    private void RecordColdReplayFrontier(int partition, long ceiling, int partitionCount)
    {
        if (partition < 0 || ceiling < 0)
            return;

        var slots = Math.Max(partitionCount, partition + 1);
        var arr = _coldReplayFrontierByPartition;
        if (arr is null || arr.Length < slots)
        {
            var grown = new long[slots];
            for (var i = 0; i < grown.Length; i++)
                grown[i] = arr is not null && i < arr.Length ? arr[i] : -1L;
            _coldReplayFrontierByPartition = arr = grown;
        }

        if (ceiling > arr[partition])
            arr[partition] = ceiling;
    }

    /// <summary>
    /// Returns the cold-rebuild re-read frontier for <paramref name="partition"/>,
    /// or <c>-1</c> when this activation has not re-read it.
    /// </summary>
    internal long ColdReplayFrontierForPartition(int partition)
    {
        var arr = _coldReplayFrontierByPartition;
        if (arr is null || partition < 0 || partition >= arr.Length)
            return -1L;
        return arr[partition];
    }

    /// <summary>
    /// Builds the ordinary per-partition coverage claim for a capture: each
    /// partition's current checkpoint, with slot 0 mirroring the scalar.
    /// </summary>
    private long[] BuildCheckpointCoverage(int partitionCount, long checkpoint)
    {
        var offsets = new long[partitionCount];
        offsets[0] = checkpoint;
        for (var p = 1; p < partitionCount; p++)
            offsets[p] = GetCurrentCheckpointForPartition(p);
        return offsets;
    }

    /// <summary>
    /// Banks the progress of an IN-FLIGHT cold rebuild as a durable snapshot
    /// whose coverage claim is the re-read frontier rather than the checkpoint,
    /// so that a cold activation torn down before it converges leaves an anchor
    /// the next activation can resume from (issue #2280). Returns whether a blob
    /// was written.
    /// <para>
    /// Banking is INLINE during replay and never on the way out. Orleans does
    /// not run <c>OnDeactivateAsync</c> when <c>OnActivateAsync</c> throws, and
    /// a cancelled cold replay leaves activation BY throwing, so a deactivation
    /// hook is unreachable on exactly the path that needs it.
    /// </para>
    /// <para>
    /// The claim is per-partition and is refused outright - fail closed - if ANY
    /// partition would be claimed below coverage a durable snapshot already
    /// holds. Declining a partial capture is always safe, because a partial
    /// capture is a bonus and never a correctness requirement, whereas a
    /// regressing claim would drive <c>LeafSnapshotStorageGrain.MergeMonotone</c>
    /// off its fast path onto the element-wise merge whose row union retains a
    /// key present in the stored blob and ABSENT from the incoming one with no
    /// comparison at all - the resurrection shape flagged by issue #2436. This
    /// keeps that hazard exactly as reachable as it is today and no more.
    /// </para>
    /// <para>
    /// This is NOT the durable-offset design ruled unsound in the #2089
    /// follow-up. That design extended a frontier ABOVE the clamped ceiling,
    /// which would have authorised trimming past unapplied holes. This one
    /// records a frontier strictly BELOW it - the opposite direction, and
    /// strictly safer: the pin is <c>min(checkpoint, covered)</c>, so a claim
    /// below the checkpoint can only ever LOWER the trim floor.
    /// </para>
    /// </summary>
    private async Task<bool> TryBankColdReplayProgressAsync(CancellationToken cancellationToken)
    {
        if (!_cacheRebuiltFromWalStartThisActivation || state.State.TreeId is null)
            return false;

        var frontier = _coldReplayFrontierByPartition;
        if (frontier is null)
            return false;

        var resolved = await GetOptionsAsync();
        var partitionCount = Math.Max(1, resolved.WalPartitions);

        var claim = new long[partitionCount];
        var anyProgress = false;
        for (var p = 0; p < partitionCount; p++)
        {
            var reRead = p < frontier.Length ? frontier[p] : -1L;
            if (reRead < DurableSnapshotCoverageForPartition(p))
                return false;

            claim[p] = reRead;
            if (reRead >= 0)
                anyProgress = true;
        }

        if (!anyProgress)
            return false;

        await CaptureSnapshotCoreAsync(cancellationToken, claim);
        return true;
    }

    /// <summary>
    /// Returns the highest WAL offset a durable snapshot is known to cover
    /// for <paramref name="partition"/>, or <c>-1</c> when no
    /// durable snapshot covers it. Consumed by the coverage-gated durable-pin
    /// resolution.
    /// </summary>
    internal long DurableSnapshotCoverageForPartition(int partition)
    {
        var arr = _durableSnapshotOffsetsByPartition;
        if (arr is null || partition < 0 || partition >= arr.Length)
            return -1L;
        return arr[partition];
    }

    /// <summary>
    /// Reports whether any partition holds a durable projection checkpoint that
    /// no durable snapshot covers - the leaf-local form of the tree-wide WAL
    /// retention stall of issue #2692.
    /// <para>
    /// <b>Why this state is not merely suboptimal.</b>
    /// <c>ResolveDurablePinForPartition</c> computes
    /// <c>min(checkpoint, covered)</c> and returns the Zero block pin whenever
    /// that is negative. A partition matching this predicate therefore reports
    /// the block value on every pin flush for the life of the activation, and
    /// <c>ApplyDurableMaterialiserFloorAsync</c> abandons the cursor branch for
    /// the WHOLE TREE on the first Zero pin it folds. One leaf in this state
    /// retains every other leaf's WAL, without bound, for as long as it stays
    /// in it.
    /// </para>
    /// <para>
    /// <b>Both conjuncts are load-bearing; neither may be dropped.</b>
    /// Requiring <c>covered &lt; 0</c> rather than <c>checkpoint &gt; covered</c>
    /// is what separates this from the ordinary cadence debounce further down
    /// <see cref="MaybeRunPeriodicSnapshotRecheckAsync"/>: coverage that merely
    /// LAGS an advancing checkpoint still yields a usable (non-blocking) pin, so
    /// it is a cadence concern and not an outage, and triggering on it would
    /// capture on essentially every persist. Requiring <c>checkpoint &gt;= 0</c>
    /// is what makes the repair TERMINATE: capture stamps coverage from
    /// <see cref="BuildCheckpointCoverage"/>, which derives each partition's
    /// coverage FROM its checkpoint, so a checkpointed partition necessarily
    /// lands at a non-negative covered offset and (coverage being monotone-max)
    /// can never return to -1. Drop that conjunct and a never-checkpointed
    /// partition would be stamped -1 by its own capture, leaving the predicate
    /// true and re-firing on every subsequent persist forever.
    /// </para>
    /// <para>
    /// A partition holding live data but no checkpoint is deliberately NOT
    /// matched here. That is the population Half A (#2692) already routes to
    /// capture through <see cref="CaptureSnapshotCoreAsync"/>'s live-data
    /// fall-through, and its coverage correctly stays at -1 so its Zero block
    /// pin is RETAINED - there is no WAL offset it could honestly claim. This
    /// predicate governs when to capture and never what to claim.
    /// </para>
    /// </summary>
    internal bool HasCheckpointedPartitionWithoutCoverage(int partitionCount)
    {
        for (var p = 0; p < partitionCount; p++)
        {
            if (IsPartitionProvenCheckpointed(p)
                && DurableSnapshotCoverageForPartition(p) < 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Whether a partition carries POSITIVE evidence that it was actually
    /// checkpointed, as opposed to merely reporting a non-negative offset.
    /// <para>
    /// Every partition reports the <c>-1</c> "nothing applied" sentinel until it
    /// is genuinely checkpointed, so a non-negative offset IS the positive
    /// evidence and this predicate is a thin, intention-revealing alias. That
    /// holds for partition 0 only because
    /// <c>GetPersistedCheckpointForPartition</c> resolves the born-<c>0</c>
    /// scalar ambiguity described in issue #2703 at source; before that fix
    /// partition 0 reported <c>0</c> for a leaf that had never checkpointed it,
    /// and this predicate carried a compensating <c>&gt; 0</c> clamp of its own.
    /// </para>
    /// <para>
    /// The clamp was deliberately removed rather than kept as defence in depth.
    /// Two independent mechanisms enforcing one invariant means neither can be
    /// shown to be load-bearing: mutating either leaves the tests green, so the
    /// suite silently stops covering the property it was written for. One
    /// mechanism, mutation-testable, is the stronger arrangement - and a second
    /// clamp here would additionally be WRONG once the marker exists, rejecting
    /// a leaf legitimately assigned offset 0.
    /// </para>
    /// <para>
    /// What this predicate is for is unchanged, and is the reason it has a name
    /// at all. Reading an unproven partition as checkpointed would let the
    /// repair stamp a durable coverage offset for a partition that never
    /// applied anything, turning the Zero block pin - correct, and deliberately
    /// retained by <c>CaptureSnapshotAsync</c>'s live-data fall-through - into a
    /// published trim entitlement. That is the silent-data-loss-on-upgrade shape
    /// this fix must not introduce, arriving through a type default rather than
    /// through advancing a pin.
    /// </para>
    /// </summary>
    private bool IsPartitionProvenCheckpointed(int partition)
        => GetCurrentCheckpointForPartition(partition) >= 0;

    /// <summary>
    /// Runs the zero-coverage repair capture (issue #2692) when this leaf holds
    /// a checkpointed partition with no durable snapshot coverage, and reports
    /// the outcome on
    /// <see cref="LatticeMetrics.LeafSnapshotCoverageRepairs"/>. Returns whether
    /// a capture was attempted.
    /// <para>
    /// Shared by the two drivers that must both be able to reach it: the
    /// activation-time hook, which covers a leaf that entered the activation
    /// already uncovered (including a tree that has stopped taking writes
    /// entirely, and so will never reach the persist-driven hook again), and the
    /// post-persist hook, which covers a leaf that becomes uncovered DURING an
    /// activation - a newly split sibling being the case that matters, since it
    /// is created mid-activation and would otherwise wait for a deactivation
    /// that may never come. Between them a leaf cannot occupy this state
    /// unobserved: it either holds it at activation, or acquires it by
    /// persisting a checkpoint, and those are precisely the two call sites.
    /// </para>
    /// <para>
    /// <b>Bound on concurrent uncancellable persists.</b> The post-persist
    /// driver passes no cancellation token, because none exists anywhere on
    /// that path - <c>CompleteCheckpointFlushTailAsync</c> takes none and
    /// neither do its callers in <c>FlushPendingCheckpointAsync</c>. Since
    /// issue #1965 is precisely about an uncancellable capture outrunning a
    /// deactivation deadline, the exposure this adds is bounded as follows.
    /// </para>
    /// <para>
    /// Per leaf the bound is ONE concurrent capture, and it holds even though
    /// the leaf mutation surface is <c>[AlwaysInterleave]</c> so several write
    /// turns run on one activation. <c>CaptureSnapshotCoreAsync</c> tests and
    /// sets <c>_snapshotCaptureInFlight</c> in adjacent statements with no
    /// await between them, and an Orleans activation yields only at an await,
    /// so the check-and-set cannot be torn by an interleaved turn. The
    /// per-activation ceiling is <see cref="MaxZeroCoverageRepairAttempts"/>
    /// captures; the counter is likewise incremented before the first await, so
    /// two interleaved turns cannot consume the same attempt.
    /// </para>
    /// <para>
    /// Across leaves, the untokened population is only those completing a
    /// checkpoint persist while still uncovered - bounded by write concurrency,
    /// not by corpus size. It does NOT reproduce #1965's burst, whose shape is
    /// the deactivation stampede at end of replay ("thousands of leaves go idle
    /// together"): this repair never runs on the deactivation path. The one
    /// mass-concurrency path it does run on is activation, and that driver
    /// passes the activation token.
    /// </para>
    /// <para>
    /// The population is also self-extinguishing, which is what keeps the cost
    /// one-off rather than per-write: the predicate requires coverage &lt; 0,
    /// the first successful capture moves coverage to 0 or above, and coverage
    /// is monotone, so a leaf leaves the eligible set permanently.
    /// </para>
    /// <para>
    /// Two residuals, stated rather than papered over. First, the cross-leaf
    /// bound is application write concurrency, which is not a Lattice-configured
    /// ceiling, so no constant in this repository names it. Second, a turn that
    /// passes the guard below and then loses the race to set the in-flight flag
    /// (the window is the <c>GetOptionsAsync</c> await inside the capture) burns
    /// an attempt without doing work, so contention alone could exhaust the
    /// budget. That is survivable precisely because exhaustion is a reported
    /// state rather than silence - see
    /// <see cref="ReportZeroCoverageRepairExhaustion"/>.
    /// </para>
    /// </summary>
    private async Task<bool> TryRepairZeroCoverageAsync(
        int partitionCount,
        CancellationToken cancellationToken = default)
    {
        if (_snapshotCaptureInFlight || !HasCheckpointedPartitionWithoutCoverage(partitionCount))
        {
            return false;
        }

        if (_zeroCoverageRepairAttempts >= MaxZeroCoverageRepairAttempts)
        {
            ReportZeroCoverageRepairExhaustion();
            return false;
        }

        _zeroCoverageRepairAttempts++;

        // Divide an oversized leaf BEFORE attempting the capture, not after a
        // failure. A leaf over the byte bound is one whose capture has to
        // materialise a payload too large to allocate contiguously under
        // ambient heap pressure, so attempting it first would burn an attempt
        // on a capture that is expected to fail, and would allocate hundreds of
        // megabytes to discover it. Splitting first makes the very first
        // capture on this path the one that succeeds.
        //
        // This is the step that makes an ALREADY-oversized deployment recover
        // on its own: the driver above runs at activation for any leaf holding
        // a checkpointed partition without coverage, so it reaches a leaf that
        // grew oversized and then went quiet, which no write-path predicate
        // ever would.
        var splitOptions = await GetOptionsAsync();
        await TrySplitForByteOverflowAsync(splitOptions.MaxLeafKeys, splitOptions.MaxLeafBytes);

        // Honour a caller deadline wherever one exists. The activation driver
        // passes the activation token, so a repair capture cannot outlive the
        // activation that started it. The post-persist driver has no ambient
        // caller token and passes none, exactly as the pre-existing cadence and
        // coverage-deficit captures on that same path do.
        await TryCaptureSnapshotForAdvisoryAsync(cancellationToken);

        if (!HasCheckpointedPartitionWithoutCoverage(partitionCount))
        {
            RecordCoverageRepairOutcome(LatticeMetrics.CoverageRepairRepaired);
        }

        return true;
    }

    /// <summary>
    /// Emits the budget-exhaustion observation at most once per activation, on
    /// both the counter and a warning carrying the leaf identity the counter
    /// deliberately does not tag.
    /// </summary>
    private void ReportZeroCoverageRepairExhaustion()
    {
        if (_zeroCoverageRepairExhaustionReported)
        {
            return;
        }

        _zeroCoverageRepairExhaustionReported = true;
        RecordCoverageRepairOutcome(LatticeMetrics.CoverageRepairExhausted);

        ResolveLogger()?.LogWarning(
            "Leaf {GrainId} on tree {TreeId} exhausted its zero-coverage snapshot repair budget ({Attempts} "
            + "attempts) with a checkpointed partition still uncovered. Its durable materialiser pin stays at "
            + "the block value, which disables cursor-based WAL trimming for the whole tree, so retained WAL "
            + "will grow until the leaf reactivates or the snapshot store recovers (issue #2692).",
            context.GrainId,
            state.State.TreeId,
            MaxZeroCoverageRepairAttempts);
    }

    private void RecordCoverageRepairOutcome(KeyValuePair<string, object?> outcome)
    {
        var treeId = state.State.TreeId;
        if (treeId is not { Length: > 0 })
        {
            return;
        }

        LatticeMetrics.LeafSnapshotCoverageRepairs.Add(
            1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
            outcome,
            LatticeTenantLabel.ForTree(treeId));
    }

    /// <summary>
    /// Records that a durable snapshot covers each partition through the
    /// offsets in <paramref name="blob"/>. A blob predating the
    /// per-partition field (legacy) is treated as covering partition 0 only,
    /// through its scalar <see cref="LeafSnapshotBlob.SnapshotOffset"/>.
    /// Coverage only ever advances (per-partition max) so an out-of-order or
    /// stale load can never lower a known covered offset.
    /// </summary>
    private void RecordDurableSnapshotCoverage(LeafSnapshotBlob blob)
    {
        var perPartition = blob.SnapshotOffsetsByPartition;
        if (perPartition is null || perPartition.Length == 0)
        {
            // Legacy blob: only partition 0 coverage is known.
            perPartition = new[] { blob.ScalarOffsetOrSentinel() };
        }

        var current = _durableSnapshotOffsetsByPartition;
        if (current is null || current.Length < perPartition.Length)
        {
            var grown = new long[perPartition.Length];
            for (var i = 0; i < grown.Length; i++)
            {
                var existing = current is not null && i < current.Length ? current[i] : -1L;
                grown[i] = Math.Max(existing, perPartition[i]);
            }
            _durableSnapshotOffsetsByPartition = grown;
            return;
        }

        for (var i = 0; i < perPartition.Length; i++)
            current[i] = Math.Max(current[i], perPartition[i]);
    }
    /// <inheritdoc />
    public Task CaptureSnapshotAsync() => CaptureSnapshotCoreAsync(CancellationToken.None);

    /// <summary>
    /// Cancellable core of the snapshot-capture seam. The grain-interface
    /// entrypoint keeps its parameterless wire signature and delegates here
    /// with <see cref="CancellationToken.None"/>; the graceful-deactivation
    /// path (issue #1965) supplies Orleans' deactivation token instead, so a
    /// leaf that overruns the deactivation deadline abandons its blob write
    /// rather than being cancelled inside the runtime's own frame.
    /// <para>
    /// <paramref name="coverageOverride"/> supplies the per-partition coverage
    /// claim instead of the current checkpoints. It is supplied only by
    /// <see cref="TryBankColdReplayProgressAsync"/>, where the honest claim is
    /// the cold re-read frontier and NOT the checkpoint - see that method.
    /// </para>
    /// </summary>
    private async Task CaptureSnapshotCoreAsync(
        CancellationToken cancellationToken,
        long[]? coverageOverride = null)
    {
        // No-op for an uninitialised leaf. TreeId is assigned during
        // SetTreeIdAsync (called by the shard root on first attach);
        // without it the snapshot grain key would be meaningless and
        // there is no cache content worth persisting anyway.
        if (state.State.TreeId is null)
        {
            ObserveSnapshotCaptureDecline(LatticeMetrics.SnapshotDeclineNoTreeId);
            return;
        }

        // The "nothing applied" sentinel (-1) means the leaf has not
        // yet absorbed any WAL entry into its projection; capturing
        // an empty cache would create a snapshot the activation path
        // is required to ignore, so the work is pure overhead. But the
        // check MUST be per-partition: gating on partition 0's scalar
        // checkpoint alone (the historical behaviour) starves every
        // leaf whose live keys hash only to non-zero partitions -
        // partition 0 stays at -1 forever while a non-zero partition
        // holds committed, block-pinned, un-trimmable WAL, so coverage
        // never advances and that partition's WAL grows unbounded
        // (reopening the #1489/#1490 growth class). Proceed when ANY
        // partition has absorbed at least one entry.
        var resolved = await GetOptionsAsync();
        var partitionCount = Math.Max(1, resolved.WalPartitions);

        // Partition 0's coverage claim is read through the per-partition
        // accessor rather than from the raw scalar, because BuildCheckpointCoverage
        // stamps offsets[0] straight from this value. The scalar is born 0 rather
        // than at the -1 sentinel (issue #2703), so reading it directly would
        // publish an offset-0 coverage claim for a partition that never applied
        // anything; the accessor resolves that ambiguity to the sentinel. Keeping
        // the claim honest is what lets ResolveDurablePinForPartition go on
        // computing min(checkpoint, covered) < 0 and retain the Zero block pin.
        var checkpoint = GetCurrentCheckpointForPartition(0);

        // The loop starts at partition 0 and reads it through the same predicate
        // as every other partition. It previously started at 1, seeding the flag
        // from a bare `checkpoint >= 0` on the raw scalar - which was ALWAYS true
        // in production, because partition 0's only negative writer is the admin
        // projection-rebuild path and the scalar is otherwise born 0 and positive
        // thereafter (issue #2703). The short-circuit therefore fired on the
        // first evaluation and the entire widening below was unreachable outside
        // that one operator-driven path, while its own comment described
        // partition 0 "staying at -1 forever" - a state the encoding cannot
        // produce. With the born-0 ambiguity resolved in
        // GetPersistedCheckpointForPartition, partition 0 now reports the
        // sentinel exactly when it has nothing applied, and the widening becomes
        // reachable for the population it was written for.
        var anyPartitionCheckpointed = false;
        for (var p = 0; p < partitionCount && !anyPartitionCheckpointed; p++)
        {
            if (IsPartitionProvenCheckpointed(p))
                anyPartitionCheckpointed = true;
        }
        if (!anyPartitionCheckpointed)
        {
            // ...but a checkpoint of -1 means "no WAL entry has been REPLAYED
            // into this projection", which is NOT the same proposition as "the
            // cache is empty". A leaf whose rows arrived as foreground writes,
            // or via a split sibling's in-memory handoff, holds live committed
            // data while every per-partition checkpoint sits at the sentinel.
            // Reading the sentinel as emptiness starved exactly those leaves of
            // capture forever: their Zero block pins never gained durable
            // coverage, so the shared-shard WAL GC early-returned idle and the
            // whole tree's retained WAL grew without bound (issue #2692). The
            // retention invariant assumes "a block pin always has a bounded
            // path to coverage", but that cadence is denominated in checkpoints
            // this leaf never takes, so the path did not merely take a long
            // time - it did not exist. Decide emptiness from the same signal
            // the durable-pin half already uses, so both halves of the machine
            // read the same quantity.
            //
            // This widens WHEN a blob is written and nothing else. A partition
            // holding rows but no checkpoint still makes no offset claim:
            // BuildCheckpointCoverage derives the coverage stamp FROM the
            // checkpoint, so it records -1, ResolveDurablePinForPartition still
            // computes min(checkpoint, covered) < 0 and retains the Zero block
            // pin, and no WAL becomes trimmable. That separation is deliberate
            // and load-bearing rather than incidental: durability of this
            // leaf's rows is earned by writing the blob, whereas authority to
            // trim is a claim about what OTHER consumers still need, and a blob
            // containing these rows says nothing about whether the materialiser
            // has consumed the corresponding WAL entries. Crucially the two
            // retention planes are coupled by a documented handoff -
            // ComputeMaterialiserOffsetFloorAsync SKIPS a -1 pin precisely
            // because "WAL retention is already enforced by the HLC block-pin
            // branch" - so lifting the block on an un-replayed partition would
            // drop BOTH protections at once and authorise trimming a prefix no
            // consumer has read. Do not "complete" this fix by advancing the
            // pin here.
            var liveData = ComputePartitionsWithLiveData(partitionCount);
            var anyPartitionHasLiveData = false;
            for (var p = 0; p < liveData.Length; p++)
            {
                if (liveData[p])
                {
                    anyPartitionHasLiveData = true;
                    break;
                }
            }
            if (!anyPartitionHasLiveData)
            {
                ObserveSnapshotCaptureDecline(LatticeMetrics.SnapshotDeclineNotEligible);
                return;
            }
        }

        // Single-flight guard. A second capture invocation that arrives
        // while a previous SaveAsync is still in flight is dropped on
        // the floor: the in-flight capture will land soon and any
        // subsequent advisory (activation re-entry or the periodic
        // recheck) will re-evaluate. This prevents an unbounded queue
        // of capture awaits when the snapshot storage provider is
        // slow.
        if (_snapshotCaptureInFlight)
        {
            ObserveSnapshotCaptureDecline(LatticeMetrics.SnapshotDeclineAlreadyInFlight);
            return;
        }
        _snapshotCaptureInFlight = true;
        // Attempt boundary. Everything above this line is a DECLINE (no tree,
        // nothing checkpointed and no live data, or a capture already in
        // flight); everything below is a genuine attempt that will either land
        // a blob or throw. Counting and timing from here - rather than from
        // method entry - is what makes both instruments readable: the gates
        // return in microseconds and are taken far more often than a capture
        // runs, so timing them would let near-zero no-ops dominate the sample
        // count and report "captures are fast" precisely when none happen.
        var captureStartedAt = Stopwatch.GetTimestamp();
        var captureSucceeded = false;
        try
        {
            // Single-threaded copy of the cache rows under the grain
            // turn. EnumerateRows yields the SortedDictionary's
            // key-ordered KeyValuePair sequence; the resulting buffer is
            // a self-contained value snapshot that survives subsequent
            // foreground mutations on this activation.
            //
            // The buffer is rented rather than allocated: it exists only to
            // hand an ordered span to the encoder, so it must not outlive the
            // capture. Draining the cache before reading Count keeps the two
            // consistent (EnumerateRows materialises any deferred rows first).
            var cacheRows = Cache.EnumerateRows();
            var rowCount = Cache.Count;
            var buffer = ArrayPool<LeafSnapshotRow>.Shared.Rent(rowCount);
            IReadOnlyList<LeafSnapshotRow> legacyRows = Array.Empty<LeafSnapshotRow>();
            byte[]? encodedRows;
            try
            {
                var written = 0;
                foreach (var kv in cacheRows)
                {
                    if (written == rowCount)
                    {
                        break;
                    }

                    buffer[written++] = new LeafSnapshotRow(kv.Key, kv.Value, Cache.GetMergeMode(kv.Key));
                }

                var rows = new ReadOnlySpan<LeafSnapshotRow>(buffer, 0, written);
                if (resolved.LeafSnapshotBinaryEncodingEnabled)
                {
                    // Compact binary frame: one allocation, raw value bytes, no
                    // per-row serializer envelope. A blob captured this way
                    // leaves the legacy Rows slot empty, which is how the lazy
                    // rewrite happens - the next natural capture of a leaf
                    // whose durable blob is still legacy simply persists the
                    // frame instead, with no migration pass anywhere.
                    encodedRows = LeafSnapshotCodec.Encode(rows);
                }
                else
                {
                    encodedRows = null;
                    var copy = new LeafSnapshotRow[written];
                    rows.CopyTo(copy);
                    legacyRows = copy;
                }
            }
            finally
            {
                // Rows hold references (key strings, value arrays); clear on
                // return so a pooled buffer cannot pin a released snapshot.
                ArrayPool<LeafSnapshotRow>.Shared.Return(buffer, clearArray: true);
            }

            // Per-partition coverage. Under the default WalPartitions = 8
            // the scalar SnapshotOffset only describes partition 0, but the
            // snapshot rows are the full entry cache and therefore cover the
            // checkpointed prefix of EVERY partition. Stamp each partition's
            // current checkpoint so the coverage-gated trim floor can
            // authorise trimming each partition's prefix independently. Slot
            // 0 mirrors the scalar SnapshotOffset for wire-compat.
            //
            // The claim is about ROWS, not about the checkpoint scalar: it
            // asserts "the rows in this blob cover [0, offset] for partition
            // p". Stamping the checkpoint is an honest way to say that on a
            // WARM capture, where the cache holds every checkpointed apply. It
            // is NOT honest mid-COLD-rebuild, where the checkpoint still sits
            // at its persisted value while the cache holds only what has been
            // re-read so far - which is why the cold-progress banking path
            // supplies the re-read frontier here instead (issue #2280).
            var perPartitionOffsets = coverageOverride ?? BuildCheckpointCoverage(partitionCount, checkpoint);
            var scalarOffset = perPartitionOffsets.Length > 0 ? perPartitionOffsets[0] : checkpoint;

            var blob = new LeafSnapshotBlob
            {
                SnapshotOffset = LeafSnapshotBlob.NormalizeScalarOffset(scalarOffset),
                Rows = legacyRows,
                EncodedRows = encodedRows,
                CapturedAtTicks = DateTime.UtcNow.Ticks,
                // Snapshot row footprint matches the leaf-state byte formula
                // by construction (the snapshot is a copy of the cache), so
                // use the cache's incrementally-maintained running total
                // rather than re-walking every row at capture time.
                SnapshotBytes = Cache.StateBytes,
                SnapshotOffsetsByPartition = perPartitionOffsets,
            };

            var snapshotGrain = grainFactory.GetGrain<ILeafSnapshotStorageGrain>(
                context.GrainId.GetGuidKey());
            await snapshotGrain.SaveAsync(blob, cancellationToken);
            _lastCapturedSnapshotBytes = blob.SnapshotBytes;
            // The blob is now durable, so the checkpointed prefix it covers
            // is recoverable independently of the WAL. Advance the coverage
            // view; the NEXT durable-pin flush will then authorise trimming
            // up to min(checkpoint, coveredOffset) per partition. Advancing
            // coverage only AFTER a confirmed SaveAsync (and the pin lagging
            // by design - the cursor report precedes this capture in
            // FlushPendingCheckpointAsync) keeps the pin conservative: it can
            // never license a trim ahead of durable coverage.
            RecordDurableSnapshotCoverage(blob);
            captureSucceeded = true;
        }
        finally
        {
            _snapshotCaptureInFlight = false;
            // Recorded in the finally so that the swallowed-exception paths are
            // counted too. That is the whole point of issue #2696: the advisory
            // handler catches every exception and only logs, so before this a
            // deployment in which every capture failed exported nothing at all
            // - not a spike, not a zero - and was indistinguishable from one
            // that had never attempted a capture.
            ObserveSnapshotCaptureAttempt(captureSucceeded, cancellationToken, captureStartedAt);
        }
    }

    /// <summary>
    /// Records one leaf-snapshot capture attempt on
    /// <see cref="LatticeMetrics.LeafSnapshotCaptures"/> and its wall-clock
    /// duration on <see cref="LatticeMetrics.LeafSnapshotCaptureDuration"/>,
    /// tagged by tree and outcome (issue #2696). Observation only: the caller's
    /// control flow, and the exception it is propagating if any, are unchanged.
    /// <para>
    /// Called from the <c>finally</c> of the capture's single-flight block, so
    /// it runs on the success path and on every throwing path alike. It must
    /// therefore not throw: a metrics write that faulted here would replace the
    /// capture's real exception with an observation defect. Both instrument
    /// writes are allocation-light tag writes against a bounded tag set and
    /// neither allocates per-leaf series - the tree is the finest label, which
    /// keeps this family at trees x outcomes rather than at leaf cardinality.
    /// </para>
    /// <para>
    /// A cancelled token is reported as <c>abandoned</c> rather than
    /// <c>failed</c>, mirroring the predicate the advisory handler already uses
    /// to swallow deactivation cancellations silently, so a fleet-wide
    /// shutdown does not read as a storage-provider outage.
    /// </para>
    /// </summary>
    private void ObserveSnapshotCaptureAttempt(
        bool succeeded,
        CancellationToken cancellationToken,
        long startedAtTimestamp)
    {
        var treeId = state.State.TreeId;
        if (treeId is not { Length: > 0 })
        {
            return;
        }

        var outcome = succeeded
            ? LatticeMetrics.SnapshotCaptureSucceeded
            : cancellationToken.IsCancellationRequested
                ? LatticeMetrics.SnapshotCaptureAbandoned
                : LatticeMetrics.SnapshotCaptureFailed;

        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        var tenantTag = LatticeTenantLabel.ForTree(treeId);

        LatticeMetrics.LeafSnapshotCaptures.Add(1, treeTag, outcome, tenantTag);
        LatticeMetrics.LeafSnapshotCaptureDuration.Record(
            Stopwatch.GetElapsedTime(startedAtTimestamp).TotalMilliseconds,
            treeTag,
            outcome,
            tenantTag);
    }

    /// <summary>
    /// Records a capture invocation that declined before the attempt boundary.
    /// <para>
    /// Declines are counted on their own instrument rather than as a fourth
    /// value of the attempt counter's outcome tag, so that the attempt counter
    /// and the duration histogram keep sharing a population exactly. A decline
    /// is never timed, so folding it into the attempt counter would make the
    /// two families silently differ.
    /// </para>
    /// <para>
    /// A <c>no_tree_id</c> decline carries no <c>tree</c> tag - there is no tree
    /// identity to report - but it still carries the derived <c>tenant</c>
    /// dimension, which <see cref="LatticeTenantLabel.ForTree(string?)"/>
    /// resolves to the platform sentinel for a null id. Emitting it with no
    /// tenant dimension at all would make it invisible to every tenant-scoped
    /// query, so an operator could not tell an unattributable measurement from
    /// a missed one - which is the same ambiguity this instrument exists to
    /// remove, reintroduced one dimension over. The uniform dimension also
    /// keeps every site on this instrument under one attribution rule, so its
    /// series never splits across two.
    /// </para>
    /// </summary>
    private void ObserveSnapshotCaptureDecline(KeyValuePair<string, object?> reason)
    {
        // Normalise an empty id to null so it resolves to the platform sentinel
        // rather than being adopted by the default tenant, matching the guard below.
        var treeId = state.State.TreeId is { Length: > 0 } id ? id : null;
        var tenantTag = LatticeTenantLabel.ForTree(treeId);

        if (treeId is null)
        {
            LatticeMetrics.LeafSnapshotCaptureDeclines.Add(1, reason, tenantTag);
            return;
        }

        LatticeMetrics.LeafSnapshotCaptureDeclines.Add(
            1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
            reason,
            tenantTag);
    }

    /// <summary>
    /// Activation-side advisory handler. Wraps
    /// <see cref="CaptureSnapshotAsync"/> in a best-effort try/catch so
    /// a transient snapshot-storage failure does not block the leaf
    /// coming online. The next periodic recheck (or the next
    /// reactivation's advisory) re-attempts the capture.
    /// <para>
    /// A cancellation raised by the caller's token is <b>not</b> a failure
    /// and is swallowed without logging: the graceful-deactivation caller
    /// (issue #1965) passes Orleans' deactivation token, and thousands of
    /// leaves going idle together would otherwise turn one log flood into
    /// another. Coverage is only ever advanced after a confirmed save, so an
    /// abandoned capture leaves the pin conservative and the WAL retained.
    /// </para>
    /// </summary>
    private async Task TryCaptureSnapshotForAdvisoryAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await CaptureSnapshotCoreAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Deliberate abandonment on a deactivation deadline; see above.
        }
        catch (Exception ex)
        {
            var logger = context.ActivationServices?
                .GetService<ILoggerFactory>()?
                .CreateLogger<BPlusLeafGrain>();
            logger?.LogWarning(
                ex,
                "Proactive snapshot capture for leaf {GrainId} failed; will retry on next periodic recheck or reactivation.",
                context.GrainId);
        }
    }

    /// <summary>
    /// Periodic snapshot-recheck hook, called by
    /// <see cref="FlushPendingCheckpointAsync"/> after every successful
    /// checkpoint persist. Increments the per-activation persist
    /// counter; when the counter reaches
    /// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/>
    /// it resets, re-classifies the leaf's WAL gap, and (on
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.FallOffLogDecision.SnapshotPending"/>) drives a
    /// capture. Returns synchronously when the option is <c>0</c>
    /// (disabled) or the threshold has not yet been reached.
    /// <para>
    /// The coverage-deficit escape (#2220) runs BEFORE that option is read,
    /// because it is activation-scoped rather than periodic and the option is
    /// documented to govern periodic capture only.
    /// </para>
    /// </summary>
    private async Task MaybeRunPeriodicSnapshotRecheckAsync()
    {
        if (state.State.TreeId is null)
        {
            return;
        }

        var resolved = await GetOptionsAsync();

        // Coverage-deficit fast path (frozen-leaf livelock escape, #2220).
        // A leaf that rehydrated a snapshot sitting BEHIND its durable
        // checkpoint (the rehydrate lowered a partition - see
        // _snapshotCoverageDeficitAtActivation) must bank a fresh snapshot
        // covering the re-advanced checkpoint DURING this activation. The
        // cadence gate below cannot do it: it fires only after `threshold`
        // persists WITHIN ONE ACTIVATION, but _checkpointPersistCountSinceRecheck
        // resets every activation, and a leaf whose replay gap exceeds
        // MaxLeafReplayEntries is torn down after only a handful of persists -
        // so it never reaches the cadence, never captures, reloads the same
        // stale snapshot next activation and rolls the same partition back
        // forever while its WAL pin stays frozen and its WAL grows unbounded.
        // Capture once per activation, off the cadence, as soon as the tail
        // replay has re-advanced a partition past the coverage the stale
        // snapshot recorded - gated by the SAME no-loss precondition the
        // graceful-deactivation capture trusts (a checkpoint advanced over
        // cache-resident applies, or a full cold rebuild), so we never stamp
        // coverage the cache does not hold. Coverage advances strictly (current
        // > the inherited snapshot offset), so even if teardown interrupts a
        // full replay each activation banks a STRICTLY higher snapshot: a
        // monotone escape that needs no single activation to finish the
        // 30k-entry replay (#2220 point 3).
        //
        // This escape deliberately sits ABOVE the cadence gate below.
        // LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints is documented
        // to govern the PERIODIC re-classification only: "Set to 0 to disable
        // the periodic re-classification entirely; only the once-per-activation
        // capture ... will fire. The activation-time capture itself is not
        // affected by this option." That is a contract, and this escape is
        // activation-scoped by construction - latched during rehydrate, gated on
        // THIS activation's no-loss precondition, one-shot per activation, and
        // explicitly off the cadence - so placing it behind the cadence gate
        // would make that documented sentence untrue. The consequence would also
        // be out of all proportion to a tuning knob: with the cadence set to 0 a
        // frozen leaf could NEVER escape, so its WAL pin would never lift and its
        // WAL would grow without bound - a disk-exhaustion failure mode reachable
        // by setting a cadence value.
        if (_snapshotCoverageDeficitAtActivation
            && !_snapshotCaptureInFlight
            && (_checkpointAdvancedThisActivation || _cacheRebuiltFromWalStartThisActivation))
        {
            var deficitPartitionCount = Math.Max(1, resolved.WalPartitions);
            var stillDeficit = false;
            for (var p = 0; p < deficitPartitionCount; p++)
            {
                if (GetCurrentCheckpointForPartition(p) > DurableSnapshotCoverageForPartition(p))
                {
                    stillDeficit = true;
                    break;
                }
            }

            if (stillDeficit)
            {
                // One-shot per activation. Clear BEFORE the capture so a
                // capture that itself fails cannot re-fire on every subsequent
                // persist this activation; the ordinary cadence path below, the
                // graceful-deactivation hook and the next reactivation remain
                // backstops, and monotone progress guarantees convergence.
                _snapshotCoverageDeficitAtActivation = false;
                await TryCaptureSnapshotForAdvisoryAsync();
                return;
            }

            // Precondition met but coverage already caught up (the ordinary
            // cadence or another capture beat us): retire the latch and fall
            // through to normal cadence handling.
            _snapshotCoverageDeficitAtActivation = false;
        }

        // Zero-coverage repair (issue #2692), deliberately ABOVE the cadence
        // gate below and above the option read, for the same reason the
        // coverage-deficit escape above is: this is a retention OUTAGE and not a
        // tuning concern, and it must not be disableable by a tuning knob.
        //
        // A partition that is checkpointed but holds no durable snapshot
        // coverage resolves its durable pin to the Zero block value, and the WAL
        // GC abandons the cursor branch for the entire tree on the first such
        // pin - so ONE leaf in this state retains every other leaf's WAL without
        // bound. The cadence path below cannot be relied on to clear it. Its
        // counter (_checkpointPersistCountSinceRecheck) resets every activation,
        // so a leaf that persists fewer than `threshold` checkpoints per
        // activation never reaches it however long it lives; and with the option
        // set to 0 the path does not exist at all, which would make an
        // unbounded-disk failure mode reachable by a tuning value. Neither is an
        // acceptable dependency for the only thing standing between a tree and
        // unbounded WAL growth.
        //
        // This widens WHEN a blob is written and nothing else - it advances no
        // pin and lifts no block. The coverage stamp still comes from
        // BuildCheckpointCoverage, so a partition with no checkpoint still
        // records -1 and still retains its Zero block pin; see
        // HasCheckpointedPartitionWithoutCoverage for why both conjuncts of the
        // predicate are load-bearing, and CaptureSnapshotCoreAsync's live-data
        // fall-through for the population this one deliberately excludes.
        //
        // Termination is structural rather than scheduled: a capture stamps
        // coverage for every checkpointed partition, coverage is monotone-max,
        // so the predicate is false from then on and this path never fires again
        // for this leaf. The attempt budget therefore bounds only repeated
        // FAILURE, and its exhaustion is reported as its own state rather than
        // being absorbed silently.
        if (await TryRepairZeroCoverageAsync(Math.Max(1, resolved.WalPartitions)))
        {
            return;
        }

        var threshold = resolved.LeafSnapshotReClassifyEveryNCheckpoints;
        if (threshold <= 0)
        {
            // Periodic recheck disabled. The activation-scoped drivers - the
            // activation-time advisory and the coverage-deficit escape above -
            // remain the only proactive-capture drivers.
            return;
        }

        _checkpointPersistCountSinceRecheck++;
        if (_checkpointPersistCountSinceRecheck < threshold)
        {
            return;
        }
        _checkpointPersistCountSinceRecheck = 0;

        if (_snapshotCaptureInFlight)
        {
            // A previous capture has not yet completed; skip this
            // recheck. The next post-threshold persist will retry.
            return;
        }

        // Per-partition "already covered" debounce. A capture is worth
        // running only when SOME partition's current checkpoint has advanced
        // beyond the offset a durable snapshot already covers for it. Gating
        // on partition 0's scalar checkpoint alone (the historical behaviour)
        // froze coverage whenever partition 0 idled while other partitions
        // took writes past the threshold: the busy partition's durable pin
        // stayed pinned at its stale covered offset (min(checkpoint, covered)
        // == covered) and its retained WAL grew unbounded. Compare each
        // partition's current checkpoint against its recorded durable coverage
        // so no partition can be starved of capture, and so a projection that
        // is already fully covered still short-circuits without a redundant
        // byte-identical blob write.
        var recheckPartitionCount = Math.Max(1, resolved.WalPartitions);
        var anyPartitionNeedsCapture = false;
        for (var p = 0; p < recheckPartitionCount; p++)
        {
            if (GetCurrentCheckpointForPartition(p) > DurableSnapshotCoverageForPartition(p))
            {
                anyPartitionNeedsCapture = true;
                break;
            }
        }
        if (!anyPartitionNeedsCapture)
        {
            return;
        }

        // Unconditional cadence capture (issue: cold-restart residual
        // prefix loss). Historically this path re-ran the fall-off-log
        // classifier and captured ONLY when it raised the SnapshotPending
        // advisory. That gate is unsafe under the coverage-gated trim floor:
        // the durable pin now BLOCKS trimming a checkpointed prefix that no
        // snapshot covers, so the WAL tail stays low and the classifier's
        // proximity heuristic (tail near checkpoint) never fires - the block
        // would then be held forever and the WAL would grow unbounded,
        // reintroducing the #1489/#1490 growth class. Capturing on the fixed
        // checkpoint cadence instead guarantees every blocked prefix is
        // covered by a durable snapshot within at most
        // LeafSnapshotReClassifyEveryNCheckpoints checkpoints, after which
        // the pin advances to min(checkpoint, coveredOffset) and the WAL GC
        // trims the now-covered prefix. This is what keeps retention bounded
        // (invariant b) while the coverage gate keeps it lossless
        // (invariant a). The single-flight guard above and the SaveAsync
        // best-effort try/catch bound the cost of a slow snapshot store.
        await TryCaptureSnapshotForAdvisoryAsync();
    }

    /// <summary>
    /// Graceful-deactivation snapshot-capture hook (issue #1537), invoked
    /// from <c>OnDeactivateAsync</c> after the final checkpoint flush and
    /// immediately before the durable materialiser-pin flush.
    /// <para>
    /// The two standing proactive-capture drivers - the activation-time
    /// advisory (<see cref="_activationSnapshotPending"/>) and the periodic
    /// recheck (<see cref="MaybeRunPeriodicSnapshotRecheckAsync"/>) - both
    /// require the leaf to stay activated long enough to either cross the
    /// activation-time WAL-tail margin or accumulate
    /// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/>
    /// checkpoint persists. A short-lived bursty activation (activate, take a
    /// few writes and checkpoints, then deactivate before the cadence
    /// threshold) fires neither, so a data-bearing leaf can checkpoint, go
    /// dormant, and leave its <see cref="HybridLogicalClock.Zero"/> block pin
    /// held forever - the shared-shard WAL is then retained without bound.
    /// This is the liveness gap on the safe side of the #1535 coverage gate:
    /// the block pin never loses data, but nothing lifts it.
    /// </para>
    /// <para>
    /// Capturing here closes the gap. Any checkpointed-but-uncovered partition
    /// gets a durable snapshot before the leaf goes dormant, so the durable
    /// pin flush that follows resolves the pin to
    /// <c>min(checkpoint, coveredOffset) == checkpoint</c>
    /// (<see cref="ResolveDurablePinForPartition"/>) and the WAL GC can trim
    /// the now-covered prefix. The capture is best-effort by construction
    /// (<see cref="TryCaptureSnapshotForAdvisoryAsync"/> swallows storage
    /// faults, and <see cref="CaptureSnapshotAsync"/> advances coverage only
    /// after a confirmed <c>SaveAsync</c>): if it fails, coverage does not
    /// advance, the pin stays a Zero block pin, and the WAL is retained rather
    /// than trimmed ahead of durable coverage - so the #1535 no-loss
    /// invariant is preserved. Crash deactivations bypass
    /// <c>OnDeactivateAsync</c> (and thus this hook) by design; the persisted
    /// checkpoint still bounds the next activation's replay cost.
    /// </para>
    /// </summary>
    private async Task TryCaptureSnapshotOnDeactivateAsync(CancellationToken cancellationToken)
    {
        if (state.State.TreeId is null)
        {
            return;
        }

        // Safety gate (the #1535 no-loss invariant). Capture only when the
        // in-memory cache faithfully holds every checkpointed prefix it would
        // stamp as covered. Two independent activations satisfy that:
        //   (a) this activation advanced a checkpoint over cache-resident applies
        //       (foreground writes or a completed tail replay) -
        //       _checkpointAdvancedThisActivation; or
        //   (b) this activation cold-rebuilt the cache from the WAL start over a
        //       pre-existing durable checkpoint (persisted offset > 0) whose
        //       prefix provably survives in the readable WAL (the #945 fall-off
        //       guard passed), so the cache holds the entire readable window, a
        //       superset of the checkpointed prefix -
        //       _cacheRebuiltFromWalStartThisActivation.
        // Signal (b) is what closes the residual #1537 gap: an already-converged
        // leaf (checkpoint already at head) cold-reactivates and rebuilds its
        // full cache but advances no checkpoint, so (a) alone stays false and the
        // Zero block pin would never lift. A brand-new leaf (no pre-existing
        // checkpoint) never satisfies (b), and a leaf that merely reactivated cold
        // WITHOUT a full rebuild (its persisted checkpoint restored from state
        // with a non-empty cache, or a rehydrate that reset uncovered partitions
        // to -1 with no forward apply) satisfies neither and does not capture, so
        // it can never write a snapshot claiming coverage of data the cache never
        // held. The one shape that could make a -1 rebuild unfaithful - a trimmed
        // WAL prefix with no covering snapshot - throws at activation (the #945
        // fall-off guard) before signal (b) is ever latched.
        if (!_checkpointAdvancedThisActivation && !_cacheRebuiltFromWalStartThisActivation)
        {
            return;
        }

        // "Already covered" debounce, mirroring the periodic recheck's
        // per-partition check: only pay for a cache copy plus blob write when
        // some partition's checkpoint has actually advanced beyond the offset
        // a durable snapshot already covers for it. A leaf that already reached
        // full coverage on the periodic cadence (the long-lived case that does
        // not hit this gap) short-circuits here with no allocation.
        var resolved = await GetOptionsAsync();
        var partitionCount = Math.Max(1, resolved.WalPartitions);
        var anyPartitionNeedsCapture = false;
        for (var p = 0; p < partitionCount; p++)
        {
            if (GetCurrentCheckpointForPartition(p) > DurableSnapshotCoverageForPartition(p))
            {
                anyPartitionNeedsCapture = true;
                break;
            }
        }
        if (!anyPartitionNeedsCapture)
        {
            return;
        }

        await TryCaptureSnapshotForAdvisoryAsync(cancellationToken);
    }

    /// <summary>
    /// True when <paramref name="error"/> is, or was caused by, an
    /// <see cref="OutOfMemoryException"/> - walking <see cref="Exception.InnerException"/>
    /// and every branch of an <see cref="AggregateException"/>.
    /// <para>
    /// The walk is necessary rather than defensive. The allocation that fails
    /// is inside the storage provider's deserialiser, several frames below the
    /// grain call this leaf issues, and it reaches the caller wrapped: Orleans
    /// surfaces a failure to read a grain's persistent state as an activation
    /// failure carrying the original as an inner exception. Testing the
    /// outermost type alone would classify every real occurrence of this fault
    /// as an ordinary storage fault - that is, it would report the exact wrong
    /// answer for the one case the classifier exists to catch, rather than
    /// reporting nothing.
    /// </para>
    /// <para>
    /// Cycle-safe by bounded depth: a hand-constructed exception graph can be
    /// cyclic, and this runs on the activation path, where a hang is a worse
    /// outcome than a missed classification.
    /// </para>
    /// </summary>
    internal static bool IsResourceExhaustion(Exception? error)
    {
        return Walk(error, 0);

        static bool Walk(Exception? candidate, int depth)
        {
            const int MaxDepth = 16;

            if (candidate is null || depth >= MaxDepth)
            {
                return false;
            }

            if (candidate is OutOfMemoryException)
            {
                return true;
            }

            if (candidate is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    if (Walk(inner, depth + 1))
                    {
                        return true;
                    }
                }

                return false;
            }

            return Walk(candidate.InnerException, depth + 1);
        }
    }

    /// <summary>
    /// Records a swallowed activation-time snapshot-load failure on
    /// <see cref="LatticeMetrics.LeafSnapshotLoadFailures"/> and logs it
    /// (issue #2364). Observation only: the caller's decline is unchanged, so
    /// availability behaviour is exactly as it was.
    /// <para>
    /// Memory exhaustion is logged at <see cref="LogLevel.Error"/> and names
    /// the real cause in the message, because the operator-visible evidence
    /// otherwise names only the storage provider. The GC's own view of the heap
    /// hard limit is included: under a container limit that ceiling is derived
    /// from the cgroup, so it is the number that turns "a leaf failed to load"
    /// into "this host is provisioned below its working set", and it is not
    /// otherwise recoverable from the logs.
    /// </para>
    /// </summary>
    private void ObserveSnapshotLoadFailure(Exception error)
    {
        var resourceExhaustion = IsResourceExhaustion(error);
        var treeId = state.State.TreeId;

        if (treeId is { Length: > 0 })
        {
            LatticeMetrics.LeafSnapshotLoadFailures.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
                resourceExhaustion
                    ? LatticeMetrics.SnapshotLoadFailureResourceExhausted
                    : LatticeMetrics.SnapshotLoadFailureFaulted,
                LatticeTenantLabel.ForTree(treeId));
        }

        var logger = context.ActivationServices?
            .GetService<ILoggerFactory>()?
            .CreateLogger<BPlusLeafGrain>();

        if (logger is null)
        {
            return;
        }

        if (resourceExhaustion)
        {
            var memoryInfo = GC.GetGCMemoryInfo();
            logger.LogError(
                error,
                "Leaf {GrainId} (tree '{TreeId}') could not load its snapshot because memory was exhausted, so "
                + "it will now activate COLD and replay its whole readable WAL window - which allocates more than "
                + "the load that just failed. This is a MEMORY fault, not a storage-provider fault: the underlying "
                + "exception is raised inside the provider's deserialise of the snapshot blob and is reported by "
                + "the provider as a failure to read grain state, which names no memory anywhere. The managed heap "
                + "is using {HeapBytes} bytes against a hard limit of {HeapHardLimitBytes} bytes (0 means "
                + "unlimited); under a container memory limit that ceiling is sized from the cgroup, so a recurring "
                + "reading here means the host is provisioned below this deployment's working set. Raise the "
                + "container memory limit rather than investigating the storage provider.",
                context.GrainId,
                treeId,
                GC.GetTotalMemory(forceFullCollection: false),
                memoryInfo.TotalAvailableMemoryBytes);
        }
        else
        {
            logger.LogWarning(
                error,
                "Leaf {GrainId} (tree '{TreeId}') could not load its snapshot; it will activate without "
                + "rehydrating, which means a cold replay of its whole readable WAL window when the entry cache is "
                + "empty. The activation itself is unaffected - the WAL can still recover the projection provided "
                + "it has not trimmed past the checkpoint.",
                context.GrainId,
                treeId);
        }
    }

    /// <summary>
    /// Activation-time rehydration seam. Consults the dedicated
    /// snapshot storage grain for a persisted blob and, when the blob
    /// is newer than the leaf's persisted
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ProjectionCheckpointOffset"/>,
    /// repopulates the in-memory entry cache from the canonical byte
    /// rows and advances the persisted checkpoint to the snapshot's
    /// offset. The projection digest is invalidated (set to <c>null</c>)
    /// so the next read or fold lazily rebuilds it via the existing
    /// <c>EnsureProjectionHashInitialized</c> path; this preserves the
    /// canonical-full-walk hash invariant the chained internal-node
    /// fold depends on.
    /// <para>
    /// No-op preconditions: tree id unset (uninitialised leaf); no
    /// snapshot present; snapshot offset not strictly greater than the
    /// persisted checkpoint (a stale snapshot whose offset the leaf
    /// has already run past). After a successful rehydrate the caller
    /// (the activation hook) drives the WAL tail-replay from the new
    /// checkpoint forward, so a snapshot that covers a prefix of the
    /// WAL plus tail-replayed suffix produces a projection identical
    /// to a from-zero replay.
    /// </para>
    /// </summary>
    internal async Task<bool> TryRehydrateFromSnapshotAsync(CancellationToken cancellationToken)
    {
        if (state.State.TreeId is null)
        {
            return false;
        }

        cancellationToken.ThrowIfCancellationRequested();

        // Resolve the leaf's own identity BEFORE the observed try block, and
        // without throwing. A leaf that is not Guid-keyed has no snapshot grain
        // to address at all, so declining here is the SAME arm as "this leaf has
        // no snapshot" - it is a precondition, not a failed load.
        //
        // Keeping this inside the try below would be a new conflation of exactly
        // the kind issue #2364 exists to remove: GetGuidKey throws
        // ArgumentException on a non-Guid key, which would be caught by the
        // observing catch and counted and logged as a snapshot LOAD failure. The
        // counter would then answer "did the snapshot store fail?" with evidence
        // about grain naming, which is a worse lie than the silence it replaced,
        // because it is a confident one.
        if (!context.GrainId.TryGetGuidKey(out var leafKey, out _))
        {
            return false;
        }

        LeafSnapshotBlob? blob;
        try
        {
            var snapshotGrain = grainFactory.GetGrain<ILeafSnapshotStorageGrain>(leafKey);
            blob = await snapshotGrain.LoadAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Deliberate abandonment on the caller's deadline, not a failure of
            // the load. Swallowed without observation for the same reason the
            // capture path swallows it: thousands of leaves standing down
            // together would turn one signal into a flood.
            return false;
        }
        catch (Exception ex)
        {
            // Snapshot load is best-effort: a transient storage failure
            // must not block the leaf coming online. The activation
            // path falls through to the existing WAL-tail replay,
            // which can still recover the projection as long as the
            // WAL has not trimmed past the checkpoint.
            //
            // The DECISION is unchanged; what changes is that it is no longer
            // silent (issue #2364). Returning false here is indistinguishable
            // at every call site from "this leaf has no snapshot": both decline
            // the rehydrate, and OnActivateAsync then sees
            // (!rehydratedFromSnapshot && Cache.Count == 0), takes the -1
            // replay-start override, and replays the WHOLE readable WAL window.
            // So a leaf whose snapshot exists and failed to load reported
            // exactly what a leaf with no snapshot reports, and the cold-replay
            // log line said "no snapshot rehydrate" - true, and read by every
            // operator as "there was no snapshot". An absence rendering as a
            // measured negative.
            //
            // That cost the deployment in issue #2364 an undiagnosed multi-hour
            // window, because the failure was memory exhaustion wearing a
            // storage fault's clothes: under a container memory limit the GC
            // heap hard limit is sized from the cgroup limit, so the process is
            // never OOM-killed (no restart, no exit code, no resource event) -
            // it throws OutOfMemoryException inside the provider's deserialise
            // of the blob, and the provider logs "Error reading grain state".
            // Nothing in that names memory. Worse, it compounds: the forced
            // cold replay allocates more than the load that just failed, so the
            // same few leaves go cold repeatedly and pressure rises.
            ObserveSnapshotLoadFailure(ex);
            return false;
        }

        if (blob is null)
        {
            return false;
        }

        // Reject a blob whose row payload cannot be read in full - a truncated
        // or corrupt binary frame, or a legacy row list with a null key -
        // BEFORE anything treats it as durable. This must happen ahead of
        // RecordDurableSnapshotCoverage: reporting coverage for a prefix the
        // blob cannot actually reproduce is precisely what would authorise the
        // coverage-gated WAL GC to trim the last durable copy of that prefix.
        // An unreadable blob is "no snapshot", so the activation falls through
        // to the ordinary WAL replay with its own fall-off guards intact.
        if (!blob.ValidateRowPayload())
        {
            return false;
        }

        // A loaded blob is durable regardless of whether it repopulates the
        // cache below. Record its coverage NOW - on both the accept and the
        // decline path - so the coverage-gated durable pin
        // (ResolveDurablePinForPartition) knows which checkpointed prefixes a
        // snapshot already protects and can authorise the WAL GC to trim
        // them. Missing this on the decline path is exactly what would keep a
        // block pin from ever lifting (unbounded WAL).
        RecordDurableSnapshotCoverage(blob);

        // Read deliberately from the raw scalar, not through
        // GetPersistedCheckpointForPartition. This is a like-for-like comparison
        // against the blob's OWN scalar offset - both sides come from the same
        // legacy partition-0 wire slot - and it decides rehydration, not a
        // retention claim, so the born-0 disambiguation of issue #2703 does not
        // apply to it. Routing it through the accessor would turn an ambiguous 0
        // into -1 and flip "decline, already absorbed" into "accept" for a blob
        // at offset 0, changing snapshot-load semantics that issues #919 and
        // #2278 pin, for no retention benefit.
        var checkpoint = state.State.ProjectionCheckpointOffset;
        if (blob.ScalarOffsetOrSentinel() <= checkpoint)
        {
            // The snapshot is at or behind the persisted partition-0
            // checkpoint. Historically this always declined ("we already
            // absorbed everything the snapshot contains via the WAL"). That
            // is only true when the WAL prefix the snapshot covers is still
            // readable. Under the coverage-gated trim floor the WAL GC trims a
            // checkpointed prefix precisely BECAUSE a snapshot covers it, so
            // in the cold-restart steady state the snapshot can be the ONLY
            // durable copy of [0, checkpoint]. Probe the WAL tail per
            // partition: if any partition's oldest readable offset has
            // advanced past 0 the covered prefix has been trimmed and this
            // snapshot MUST rehydrate it; if every tail is still 0 the WAL is
            // intact and the snapshot is redundant, so decline to avoid a
            // pointless cache replace (preserving issue #919's
            // Activation_ignores_snapshot_at_equal_offset intent for the
            // WAL-intact case). See the residual cold-restart prefix-loss
            // finding.
            //
            // The decline is gated on a NON-EMPTY cache (issue #2278). "The
            // snapshot is redundant against an intact WAL" is a statement about
            // DURABILITY, and it is true: the WAL still holds the prefix. It was
            // being applied as though it were a statement about COST, and there
            // it is exactly inverted. The only thing the decline saves is a cache
            // replace, so it is a saving only when there is a live cache to
            // preserve. On a fresh activation the cache is empty by construction,
            // and declining then does not avoid work - it forces the most
            // expensive path available: step 0.5 of OnActivateAsync sees
            // (!rehydratedFromSnapshot && Cache.Count == 0), sets the -1
            // replay-start override, and the leaf replays the WHOLE readable WAL
            // window instead of bulk-loading a blob that already covers the
            // checkpointed prefix.
            //
            // That is self-perpetuating rather than one-off. The converged
            // steady state is precisely offset == checkpoint (a capture stamps
            // the checkpoint it covers), so a leaf that reactivates, replays from
            // zero and re-captures lands back on an at-or-behind snapshot and
            // goes cold again on its NEXT activation, forever. The deployed
            // signature is the cold total sitting far above the distinct-cold-leaf
            // count - repo-context-vector-metadata measured 98 cold across 48
            // distinct leaves (2.04 per leaf), against vector-membership's 13
            // across 13 (1.00 per leaf, the benign one-time first activation) on
            // three times the traffic.
            //
            // Accepting here is not a new trust assumption and not a new code
            // path. RecordDurableSnapshotCoverage above already treats this same
            // blob as durable coverage of [0, offset] - that is what authorises
            // the coverage-gated WAL GC to TRIM that prefix. Refusing to let it
            // fill an empty cache trusts the blob with the destructive decision
            // and distrusts it for the cheap one. The accept path below is the
            // one that already runs whenever a prefix HAS been trimmed; it
            // handles an at-or-behind blob correctly by lowering each partition's
            // checkpoint to exactly what the reloaded cache holds and letting the
            // tail replay cover (offset, head]. Final state is identical to the
            // from-zero rebuild; the window read is a subset of it.
            //
            // Short-circuit order matters: with an empty cache the probe is not
            // consulted at all, which also removes a per-activation grain call
            // INTO the tree being replayed - the call the #2082 note describes as
            // most likely to fault on exactly the saturated tree where declining
            // costs the most.
            if (Cache.Count > 0 && !await AnyPartitionWalPrefixTrimmedAsync(cancellationToken))
            {
                return false;
            }
        }

        // Bulk-load the canonical byte rows. We bypass StoreEntry
        // (the per-mutation LWW funnel) because the snapshot rows
        // are themselves a point-in-time projection; running them
        // through LWW would be a no-op against an empty cache but
        // would also re-fold the digest incrementally on every row.
        // We instead invalidate the digest below and let the lazy
        // full-walk recompute it.
        //
        // Bounded hydration (issue #1839). When the blob carries a binary
        // frame and partial hydration is enabled, attach the frame as the
        // cache's lazily hydrated backing instead of decoding it: the cache
        // then reports the whole snapshot's row count, footprint and live
        // count immediately, and materialises entry ranges out of the frame
        // only as reads require them. Activation cost stops being a function
        // of blob size. The attach can decline (a legacy row list, or a frame
        // whose rows are not strictly ascending and therefore not safely
        // seekable), and declining simply falls through to the full decode
        // below - the pre-#1839 behaviour, byte for byte.
        //
        // This changes nothing about coverage. RecordDurableSnapshotCoverage
        // ran above off the blob's own offsets, and a capture materialises
        // every row before it writes, so a partially hydrated leaf can never
        // stamp coverage for rows it does not hold.
        var hydrationOptions = await GetOptionsAsync();
        var attached = false;
        if (hydrationOptions.LeafPartialHydrationEnabled
            && blob.HasBinaryRowPayload()
            && blob.EncodedRows is { Length: > 0 } frame)
        {
            attached = Cache.TryAttachSnapshot(frame, hydrationOptions.LeafHydrationResidentBytes);
        }

        if (!attached)
        {
            Cache.Clear();
            // Encoding-agnostic streaming decode. A binary blob decodes each row
            // straight into the cache with no intermediate row collection; a
            // legacy blob walks its row list exactly as before. The payload was
            // validated above, so a mid-stream parse failure is impossible here
            // and would throw rather than silently truncate the snapshot.
            foreach (var row in blob.EnumerateRows())
            {
                Cache.StoreRow(row.Key, row.Value);
                if (row.MergeMode is { } mode)
                {
                    // Recover the durable per-key merge-mode discriminator from
                    // the checkpoint so a freeze/capture after a rehydrate-from-
                    // checkpoint (without a full WAL replay) stays mode-faithful.
                    Cache.SetMergeMode(row.Key, mode);
                }
            }
        }

        // Advance the persisted checkpoint per partition to match the
        // snapshot EXACTLY - including resetting a partition to -1 when the
        // snapshot predates that partition's checkpoint. After Cache.Clear()
        // above the in-memory cache holds ONLY the snapshot's rows, so every
        // partition's checkpoint must equal what the reloaded cache actually
        // contains for it. The old `if (perPartition[p] >= 0)` skip left an
        // uncovered partition's checkpoint AHEAD of the reloaded cache: the
        // tail replay then resumed at (checkpoint_p, head] and silently
        // skipped [0, checkpoint_p], dropping every entry the snapshot did not
        // carry. Resetting to -1 is loss-free precisely because the coverage
        // gate never trims an uncovered partition: perPartition[p] == -1 on
        // the latest blob means partition p was never snapshot-covered, so
        // ResolveDurablePinForPartition held its block pin and its full WAL
        // [0, checkpoint_p] survives - the from-zero replay rebuilds it
        // intact. (Coverage is monotonic and we always load the latest blob,
        // so an ever-covered partition would carry perPartition[p] >= 0.)
        //
        // The reset must span the leaf's WHOLE partition space, not only the
        // slots the blob happens to carry (#2404). LeafSnapshotStorageGrain's
        // HasUsableSnapshot gate is sound and deliberately NOT tightened: it
        // answers "can this blob be rehydrated at all", and must not also demand
        // full coverage, because rejecting an under-covering blob would discard
        // the sole durable copy of a prefix the coverage gate has already let
        // the WAL GC trim. Usable therefore does not mean complete, and it is
        // this consumer's job to honour that. A loop bounded by the blob's array
        // left a partition the blob carries NO SLOT for holding its old, higher
        // checkpoint over the cleared cache - the same silent skip of
        // [0, checkpoint_p] the -1 sentinel reset above exists to prevent, just
        // reached by an absent slot rather than a present one. Two shapes reach
        // it: a legacy blob with a null array (multi-partition WALs predate the
        // per-partition field, whose own doc notes the scalar describes
        // partition 0 only), and a blob captured before WalPartitions was raised
        // (the leaf's own checkpoint array never shrinks, so it is strictly
        // longer). An absent slot is exactly as uncovered as a -1 one, so the
        // same loss-free argument applies unchanged:
        // DurableSnapshotCoverageForPartition reports -1 outside the recorded
        // coverage array, so the partition held a Zero block pin and its full
        // WAL survives for the from-zero replay.
        var perPartition = blob.SnapshotOffsetsByPartition;
        var blobSlots = perPartition is { Length: > 0 } ? perPartition.Length : 0;
        var resetSlots = Math.Max(
            Math.Max(blobSlots, state.State.ProjectionCheckpointOffsetsByPartition?.Length ?? 0),
            Math.Max(1, hydrationOptions.WalPartitions));
        for (var p = 0; p < resetSlots; p++)
        {
            // A slot the blob carries is authoritative. Beyond its array the
            // blob claims nothing, so partition 0 falls back to the legacy
            // scalar (which describes partition 0 alone) and every other
            // partition is uncovered, hence -1.
            var covered = p < blobSlots
                ? perPartition![p]
                : (p == 0 ? blob.ScalarOffsetOrSentinel() : -1L);
            // Frozen-leaf livelock detector (#2220). When this partition's
            // durable checkpoint sits AHEAD of the snapshot offset we are
            // about to write, the leaf is activating with a durable
            // checkpoint the snapshot does not cover - the snapshot froze
            // behind the checkpoint (an over-budget leaf that never captured
            // a fresh one). The rollback below is still REQUIRED for cache
            // coherence (the Cache.Clear above dropped the (snapshot,
            // checkpoint] rows, so the tail replay MUST resume from the
            // snapshot offset to rebuild them; keeping the higher checkpoint
            // over the cleared cache would silently skip them). But without
            // banking fresh coverage this activation, the leaf reloads the
            // same stale snapshot next activation and rolls this partition
            // back forever - a livelock whose WAL pin never lifts. Latch the
            // deficit so the first post-replay checkpoint flush captures a
            // snapshot covering the re-advanced checkpoint off the periodic
            // cadence (which a short over-budget activation never reaches)
            // and off the deactivation deadline. Only a genuine lowering of
            // a real (>= 0) prior checkpoint counts: resetting an uncovered
            // partition to -1 is the loss-free reset the coverage gate
            // already guarantees, not a deficit, and the ordinary cadence
            // handles a busy partition that merely advanced past its
            // coverage.
            if (covered >= 0 && covered < GetPersistedCheckpointForPartition(p))
            {
                _snapshotCoverageDeficitAtActivation = true;
            }
            SetPersistedCheckpointForPartition(p, covered);
        }

        // Invalidate the digest so EnsureProjectionHashInitialized's
        // lazy backfill path recomputes the canonical full-walk hash
        // over the rehydrated cache. The chained internal-node fold
        // depends on this hash matching the canonical full-walk hash
        // bit-for-bit; recomputing from scratch is the only way to
        // guarantee equivalence with a from-zero replay.
        state.State.ProjectionHash = null;

        // Drop the cached XxHash128 hasher so the next contribution
        // allocates a fresh instance. Mirrors the rebuild seam in
        // BPlusLeafGrain.ProjectionAdmin.cs - keeps the rehydrated
        // activation indistinguishable from a fresh activation.
        DisposeProjectionHasher();

        // Carry the snapshot's persisted byte total forward so the next
        // per-persist byte-footprint publish reflects the rehydrated
        // snapshot footprint without re-reading the snapshot grain.
        _lastCapturedSnapshotBytes = blob.SnapshotBytes;

        return true;
    }

    /// <summary>
    /// Best-effort probe: has any WAL partition's oldest still-readable
    /// offset advanced past <c>0</c> (i.e. has the WAL GC trimmed a prefix)?
    /// Used by <see cref="TryRehydrateFromSnapshotAsync"/> to decide whether
    /// a snapshot at or behind the persisted checkpoint is the sole durable
    /// copy of a trimmed prefix (rehydrate) or redundant against an intact
    /// WAL (decline). Mirrors the #945 fall-off guard's coordinator
    /// resolution (<c>{treeId}/{partition}</c>, <c>GetTailOffsetAsync</c>).
    /// A coordinator failure is swallowed and treated as "not trimmed" for
    /// that partition: the caller then declines the at/behind snapshot and
    /// the normal WAL replay (plus the #945 guard) still protects against
    /// loss.
    /// <para>
    /// Each partition is probed independently (issue #2082). A single
    /// faulting coordinator previously aborted the whole probe, so one slow
    /// partition reported the entire tree as untrimmed even when later
    /// partitions had advanced tails - and because this probe is itself a
    /// grain call into the tree being replayed, it is most likely to fault
    /// on exactly the saturated tree where declining costs a full replay
    /// from offset zero. That made the decline self-reinforcing. Faults are
    /// now confined to the partition that raised them.
    /// </para>
    /// <para>
    /// The decline itself is deliberately unchanged: "could not probe" and
    /// "probed everything, all genuinely zero" both return
    /// <see langword="false"/>, because a probe that could not prove a
    /// prefix was trimmed must never be read as proof that it was. The
    /// distinction is surfaced in the log rather than in the return value,
    /// so durability semantics are identical and only the diagnosis
    /// improves.
    /// </para>
    /// </summary>
    private async Task<bool> AnyPartitionWalPrefixTrimmedAsync(CancellationToken cancellationToken)
    {
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
            return false;

        int partitionCount;
        try
        {
            var resolved = await GetOptionsAsync();
            partitionCount = Math.Max(1, resolved.WalPartitions);
        }
        catch (Exception ex)
        {
            // Without the partition count there is nothing to probe, so this
            // one genuinely ends the probe rather than a single partition.
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {GrainId}: could not resolve the WAL partition count for tree '{TreeId}', so the "
                + "prefix-trimmed probe could not run and the snapshot rehydrate declines. If this repeats, "
                + "the leaf is replaying its whole WAL window on every activation.",
                context.GrainId,
                treeId);
            return false;
        }

        var unprobed = 0;
        Exception? firstFault = null;
        for (var partition = 0; partition < partitionCount; partition++)
        {
            try
            {
                var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                    $"{treeId}/{partition}");
                var tail = await coordinator.GetTailOffsetAsync(cancellationToken);
                if (tail > 0)
                    return true;
            }
            catch (OperationCanceledException)
            {
                // The activation is going away; further probes cannot help.
                return false;
            }
            catch (Exception ex)
            {
                // Confine the fault to this partition and keep probing: a
                // later partition may well have a trimmed prefix, and missing
                // it costs a full replay.
                unprobed++;
                firstFault ??= ex;
            }
        }

        if (unprobed > 0)
        {
            ResolveLogger()?.LogWarning(
                firstFault,
                "Leaf {GrainId}: {Unprobed} of {Partitions} WAL partition(s) of tree '{TreeId}' could not be "
                + "probed for a trimmed prefix, and no probed partition reported one, so the snapshot "
                + "rehydrate declines and this activation replays from the oldest readable offset. This is a "
                + "safe outcome but an expensive one, and it is NOT evidence that no prefix was trimmed - the "
                + "unprobed partitions are simply unknown. Repeats on a saturated tree are self-reinforcing, "
                + "because the probe is itself a call into the tree being replayed.",
                context.GrainId,
                unprobed,
                partitionCount,
                treeId);
        }

        return false;
    }

    /// <inheritdoc />
    public Task ForceDeactivateAsync()
    {
        // Test-only deactivation seam. Wraps the protected
        // Grain.DeactivateOnIdle() extension so integration tests can
        // drive activation-time rehydration end-to-end without relying
        // on the silo's idle-collection scheduler. The runtime
        // schedules the deactivation after the current grain turn
        // completes; the caller must briefly wait (e.g. a short delay
        // or a poll loop on a fresh activation) before observing the
        // post-rehydrate activation. We cannot block here without
        // deadlocking: OnDeactivateAsync can only run once this turn
        // ends.
        this.DeactivateOnIdle();
        return Task.CompletedTask;
    }
}