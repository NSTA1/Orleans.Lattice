using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;
/// <summary>
/// How a zero-progress page-fill ceiling fire is classified: whether the leaf
/// it named is merely slow this once, or has missed the ceiling on enough
/// consecutive attempts to warrant a bounded recovery probe.
/// </summary>
internal enum ScanPageLeafProgress
{
    /// <summary>
    /// The fire completed at least one leaf, or named no leaf at all, so it
    /// says nothing about any single leaf's readability.
    /// </summary>
    Progressing,

    /// <summary>
    /// A zero-progress fire on a leaf that has not yet reached
    /// <see cref="ShardRootGrain.StrandedLeafStallThreshold"/> consecutive
    /// such fires. Expected of a leaf replaying a long WAL window from cold.
    /// </summary>
    Slow,

    /// <summary>
    /// The same leaf has now missed the ceiling on
    /// <see cref="ShardRootGrain.StrandedLeafStallThreshold"/> consecutive
    /// attempts with nothing read on any of them. This is a stalled sequence,
    /// not proof of permanent failure; recovery may be deferred.
    /// </summary>
    Stranded,
}

internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// How many consecutive zero-progress ceiling fires on one leaf identity it
    /// takes before a bounded recovery probe is considered.
    /// <para>
    /// <b>It must be greater than one, and that is the whole design.</b> A
    /// single fire cannot distinguish the two conditions even in principle: a
    /// leaf replaying a long WAL window from cold and an orphaned read produce
    /// byte-identical attempts. A sequence warrants a recovery probe, but
    /// cannot distinguish permanent damage from shared pressure.
    /// </para>
    /// <para>
    /// Three, because two is one observation of "it happened again" and is met
    /// by a leaf whose cold replay merely spans two ceilings, while each
    /// further attempt costs one ceiling of wall clock on a shard that is
    /// already failing. It is <c>internal</c> rather than private so that the
    /// tests drive their loops from this constant instead of from a copy of its
    /// value: a fixture that hard-coded 3 would silently stop reaching the
    /// classification if the threshold were ever raised, and would pass while
    /// asserting nothing.
    /// </para>
    /// </summary>
    internal const int StrandedLeafStallThreshold = 3;

    /// <summary>
    /// The leaf named by the most recent zero-progress ceiling fire on this
    /// activation, or <see langword="null"/> when the last settled page fill
    /// made progress or named no leaf.
    /// </summary>
    private GrainId? _zeroProgressLeafId;

    /// <summary>
    /// How many consecutive zero-progress ceiling fires have now named
    /// <see cref="_zeroProgressLeafId"/>.
    /// </summary>
    private int _consecutiveZeroProgressStalls;

    private int _stallsUntilRecovery;
    private int _recoveryStallInterval = StrandedLeafStallThreshold;
    private bool _scanRecoveryApplied;
    private int _scanReplayQueued;

    private bool _scanPageZeroProgressOutcomesPrimed;

    /// <summary>
    /// Publishes both arms of
    /// <see cref="LatticeMetrics.ScanPageZeroProgressStalls"/> at zero, so an
    /// absent <c>stranded</c> arm means the build does not carry the instrument
    /// and cannot be read as "no leaf is stranded" (issue #3016).
    /// <para>
    /// The distinction matters more here than on most primed counters, because
    /// the arm this creates is the one whose <em>absence</em> would otherwise
    /// be the reassuring reading. An unprimed <c>stranded</c> series and a
    /// measured-zero <c>stranded</c> series render identically in a scrape, and
    /// the condition being measured is precisely one that had already run for
    /// three days unnoticed.
    /// </para>
    /// <para>
    /// Correct because this is a counter: adding zero is the identity, so it
    /// creates the series and changes no reading of it. Do not carry the
    /// pattern to a <see cref="System.Diagnostics.Metrics.Histogram{T}"/>,
    /// where a recorded zero is a fabricated sample.
    /// </para>
    /// </summary>
    private void PrimeScanPageZeroProgressOutcomes()
    {
        if (_scanPageZeroProgressOutcomesPrimed)
        {
            return;
        }

        _scanPageZeroProgressOutcomesPrimed = true;
        RecordScanPageZeroProgressStall(0, LatticeMetrics.OutcomeScanPageLeafSlowTag);
        RecordScanPageZeroProgressStall(0, LatticeMetrics.OutcomeScanPageLeafStrandedTag);
    }

    /// <summary>
    /// The single write seam for
    /// <see cref="LatticeMetrics.ScanPageZeroProgressStalls"/>, so that a primed
    /// arm and an armed one are the same series by construction rather than by
    /// two call sites agreeing on a tag list.
    /// </summary>
    private void RecordScanPageZeroProgressStall(long delta, KeyValuePair<string, object?> outcome) =>
        LatticeMetrics.ScanPageZeroProgressStalls.Add(
            delta,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, MetricTreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, MyShardIndex),
            outcome,
            LatticeTenantLabel.ForTree(TreeId));

    /// <summary>
    /// Classifies a ceiling fire that is about to fault, and applies the
    /// stranded-leaf recovery when the classification crosses from slow to
    /// repeatedly stalled (issues #3016 and #3521).
    /// <para>
    /// <b>The condition being detected is repeated zero progress.</b>
    /// The fire that brought us here read nothing, so <see cref="TryBankPartialScanPage{T}"/>
    /// had nothing to bank and the caller was told to retry from the
    /// continuation token it already held. That retry re-issues an
    /// argument-identical read, which
    /// <see cref="TryAttachScanPageLeafRead{TList}"/> answers by attaching to
    /// the read still in flight - correct, and the reason attempt N + 1 is no
    /// longer strictly worse than attempt N (issue 2585). What neither fix
    /// supplies is an exit: when the read in flight is one that will never
    /// return, every subsequent attempt attaches to that same read, stalls at
    /// the same zero leaves, and raises the same message. The deployed corpus
    /// in issue #3016 sat in exactly that state for three days and 307
    /// attempts, each indistinguishable from healthy retry.
    /// </para>
    /// <para>
    /// <b>Counting consecutively, and resetting on any progress, is what makes
    /// the classification honest.</b> A cumulative count would classify a busy
    /// tree that stalls occasionally on many different leaves, which is the
    /// opposite condition; the run has to be unbroken and on one leaf identity.
    /// Any settled page fill that completed a leaf clears it
    /// (<see cref="NoteScanPageProgress"/>), so a leaf whose cold replay
    /// straddles two ceilings and then answers never reaches the threshold.
    /// </para>
    /// <para>
    /// <b>A fire that names no leaf resets rather than counts.</b> Prologue and
    /// descent fires, the baseline-fold pass, and a fire that lands between two
    /// leaf reads all leave <see cref="ScanPageWalk.LeafInFlight"/> null. None
    /// of them is evidence about a leaf's readability, and counting them would
    /// let a shard whose descent is parked strand a leaf it never read.
    /// </para>
    /// </summary>
    private ScanPageLeafProgress ClassifyScanPageStall(ScanPageWalk walk)
    {
        PrimeScanPageZeroProgressOutcomes();
        _scanRecoveryApplied = false;
        _scanReplayQueued = 0;

        // Progress on this attempt, or no leaf to attribute it to: neither is
        // evidence that any single leaf cannot be read.
        if (walk.Budget.LeavesVisited != 0 || walk.LeafInFlight is not { } leafId)
        {
            NoteScanPageProgress();
            return ScanPageLeafProgress.Progressing;
        }

        if (_zeroProgressLeafId != leafId)
        {
            _zeroProgressLeafId = leafId;
            _consecutiveZeroProgressStalls = 1;
            _stallsUntilRecovery = 0;
            _recoveryStallInterval = StrandedLeafStallThreshold;
        }
        else
        {
            _consecutiveZeroProgressStalls++;
        }

        if (_consecutiveZeroProgressStalls < StrandedLeafStallThreshold)
        {
            RecordScanPageZeroProgressStall(1, LatticeMetrics.OutcomeScanPageLeafSlowTag);
            return ScanPageLeafProgress.Slow;
        }

        RecordScanPageZeroProgressStall(1, LatticeMetrics.OutcomeScanPageLeafStrandedTag);
        _scanReplayQueued = BPlusLeafGrain.QueuedReplayPermitWaiters;
        if (_stallsUntilRecovery > 0)
            _stallsUntilRecovery--;

        // A queued replay is evidence of shared pressure, not a leaf fault.
        // Keep the existing call: eviction cannot cancel it and only adds load.
        if (_scanReplayQueued == 0 && _stallsUntilRecovery == 0)
        {
            RecoverStrandedLeaf(leafId);
            _scanRecoveryApplied = true;
            _recoveryStallInterval = Math.Min(_recoveryStallInterval * 2, StrandedLeafStallThreshold * 16);
            _stallsUntilRecovery = _recoveryStallInterval;
        }
        return ScanPageLeafProgress.Stranded;
    }

    /// <summary>
    /// Clears the consecutive zero-progress run. Called whenever a page fill
    /// settles having completed at least one leaf, and whenever a fire carries
    /// no leaf identity to attribute.
    /// </summary>
    private void NoteScanPageProgress()
    {
        _zeroProgressLeafId = null;
        _consecutiveZeroProgressStalls = 0;
        _stallsUntilRecovery = 0;
        _recoveryStallInterval = StrandedLeafStallThreshold;
    }

    /// <summary>
    /// The recovery probe for a repeatedly stalled leaf: the shard root
    /// stops depending on the read it is parked on (issue #3016).
    /// <para>
    /// <b>Why evicting the coalesced read <em>is</em> the recovery, rather than
    /// bookkeeping alongside one.</b> Coalescing is what makes a retry cheap,
    /// and it does so by making the retry <em>the same read</em>. That is
    /// exactly right while the read can still complete, and it is what turns a
    /// permanently parked read into a permanently wedged tree: the entry never
    /// completes, so <see cref="TryAttachScanPageLeafRead{TList}"/> keeps
    /// handing it to every caller, and no attempt after the first ever reaches
    /// the leaf at all. Dropping the entry is the one action available to the
    /// shard root that makes the next attempt genuinely different from this
    /// one: a leaf whose old read was orphaned by a lost
    /// activation answers the fresh read immediately, and the scan converges on
    /// the very next attempt with no operator action and no
    /// <c>reset_index</c>.
    /// </para>
    /// <para>
    /// <b>Dropping the entry does not cancel the read, and must not be read as
    /// doing so.</b> The parked call keeps its place in the leaf's queue; what
    /// changes is that nothing waits on it any more, so its eventual completion
    /// - if it ever comes - is discarded rather than serving a caller. That is
    /// the same abandonment the ceiling already performs on the walk itself.
    /// </para>
    /// <para>
    /// <b>This does not claim to unwedge the leaf.</b> If the leaf's own
    /// activation is what is parked, the fresh read queues behind whatever is
    /// holding it and stalls too. Further evictions back off by doubling the
    /// number of stalled page fills between probes, capped at sixteen times the
    /// threshold. Replay queue pressure defers eviction altogether. Progress or
    /// a different leaf resets this activation-scoped backoff. Neither the run
    /// nor the durable application count proves where a fault lies.
    /// </para>
    /// </summary>
    private void RecoverStrandedLeaf(GrainId leafId)
    {
        var abandoned = EvictScanPageLeafReads(leafId);
        var applications = NoteStrandedLeafRecoveryApplied(leafId);

        logger.LogWarning(
            "Shard {Shard} of tree '{Tree}' observed leaf {Leaf} stalled after {Stalls} "
            + "consecutive scan-page ceiling fires that completed no leaf. {Abandoned} coalesced "
            + "read(s) for that leaf were abandoned so the next attempt issues a fresh read "
            + "instead of attaching to the parked one. This recovery has now been applied to this "
            + "leaf {Applications} time(s) across activations of this shard root. This count does "
            + "not prove a permanent leaf fault or exclude shared replay/storage pressure. "
            + "Repeated recovery probes back off; scans can still converge when pressure drains.",
            MyShardIndex,
            TreeId,
            leafId,
            _consecutiveZeroProgressStalls,
            abandoned,
            applications);
    }

    /// <summary>
    /// Records, <b>durably</b>, that the stranded-leaf recovery has been applied
    /// to <paramref name="leafId"/>, and returns the resulting count for that
    /// leaf across every activation of this shard root (issue #3016).
    /// <para>
    /// <b>Why this one quantity is persisted when the run that produces it is
    /// not.</b> The consecutive run is evidence about the coalesced read this
    /// activation is parked on, and the remedy it selects acts on that
    /// activation's own map, so both are correctly activation-scoped -
    /// persisting the run would strand a fresh activation's first stall on a
    /// verdict reached about a read it never held. This count is historical
    /// evidence about recovery probes, not their success. A
    /// fresh activation holds no coalesced reads, so its eviction is
    /// necessarily a no-op and its stall is byte-identical to a first-ever
    /// stall; the observation "a fresh read was already issued and the leaf
    /// still did not answer" is therefore one that no single activation can
    /// ever make about itself. Keeping it in memory is what left the deployed
    /// corpus in issue #3016 reporting 307 attempts that were indistinguishable
    /// from healthy retry.
    /// </para>
    /// <para>
    /// <b>Not cleared on the success path, deliberately.</b> Clearing would
    /// have to happen on every settled page fill - the hot read path - to be
    /// timely, and would put a storage write there to maintain a field that is
    /// read only when a leaf strands. A stale record costs nothing: it is read
    /// only by a later stranding, where "this leaf has had recovery probes
    /// before" remains true. A
    /// different leaf stranding resets it, so the count always names the leaf
    /// it is reported against.
    /// </para>
    /// <para>
    /// The write itself is deferred to <see cref="FlushStrandedLeafRecoveryAsync"/>
    /// because this runs inside the synchronous construction of the fault, which
    /// must carry the new count in its typed slot before the persist can be
    /// awaited.
    /// </para>
    /// </summary>
    private int NoteStrandedLeafRecoveryApplied(GrainId leafId)
    {
        var id = leafId.ToString();
        if (state.State.StrandedScanLeafId == id)
        {
            state.State.StrandedScanRecoveries++;
        }
        else
        {
            state.State.StrandedScanLeafId = id;
            state.State.StrandedScanRecoveries = 1;
        }

        _strandedLeafRecoveryNeedsPersist = true;
        return state.State.StrandedScanRecoveries;
    }

    /// <summary>
    /// Set by <see cref="NoteStrandedLeafRecoveryApplied"/> when the in-memory
    /// state has been advanced and the matching storage write is still owed.
    /// </summary>
    private bool _strandedLeafRecoveryNeedsPersist;

    /// <summary>
    /// Persists a pending stranded-leaf recovery record, immediately before the
    /// fault that carries it is thrown. A no-op unless a recovery was applied on
    /// this page fill, so the ordinary stall path and the entire success path
    /// pay nothing.
    /// <para>
    /// <b>Best-effort by design.</b> This runs on a shard that is already
    /// failing its page fills, and the value being written is diagnostic. A
    /// storage write that throws here must not replace the
    /// <see cref="ScanPageStalledException"/> - which names the actual wedge and
    /// is what the caller retries on - with a storage fault that names the
    /// symptom's bookkeeping. The in-memory advance survives the failed write
    /// for this activation's remaining lifetime, so the count is still correct
    /// for every fault raised until the activation ends, and a later stranding
    /// re-attempts the write.
    /// </para>
    /// </summary>
    private async Task FlushStrandedLeafRecoveryAsync()
    {
        if (!_strandedLeafRecoveryNeedsPersist)
        {
            return;
        }

        _strandedLeafRecoveryNeedsPersist = false;
        try
        {
            await WriteShardStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Shard {Shard} of tree '{Tree}' could not persist the stranded-leaf recovery record "
                + "for leaf {Leaf}. The count is still correct for the remaining lifetime of this "
                + "activation and will be re-attempted on the next stranding, but if this "
                + "activation ends first the record is lost and a later activation will report the "
                + "next stranding of that leaf as its first.",
                MyShardIndex,
                TreeId,
                state.State.StrandedScanLeafId);
        }
    }
}
