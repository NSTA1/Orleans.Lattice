using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The live checkpoint advance (issue #5572): a leaf advances its projection
/// checkpoint over the appends it issued itself, without reading the WAL.
/// <para>
/// <b>Why.</b> A live write appends its record and then applies it, and before
/// this change nothing on that path moved the checkpoint: only a replay did. A
/// leaf whose rows all arrived live therefore published the block pin from
/// birth, and its first checkpoint came from a replay of every partition from
/// <c>-1</c>, scheduled by its coverage-lag timer or a WAL GC touch against a
/// bounded permit share. On a freshly written estate every leaf of every tree
/// needed that replay at once, the permits saturated, and every tree's WAL floor
/// stayed blocked (issue #5570).
/// </para>
/// <para>
/// <b>The tracker.</b> <see cref="OverrideHoldCommitLogWriter"/> counts every
/// append before it is issued (<see cref="NoteLiveAppendIssued"/>). Each append
/// site, once its apply - including the relocation of any stranded rows - has
/// completed, releases that count and records the offsets its append returned
/// (<see cref="MarkLiveAppendApplied(WalRecord, long)"/>), as the lowest and
/// highest own offset per partition. A site that forgets to release, or an
/// apply that throws, leaves the count above zero, which only turns the advance
/// off for the activation; a release with no matching issue latches it off. So
/// a missed site costs the advance, never correctness. The write path gains an
/// integer increment, a partition hash and two array writes per append: no
/// allocation and no grain call.
/// </para>
/// <para>
/// <b>When an advance is safe.</b> Advancing partition <c>p</c> to the highest
/// own offset <c>H</c> claims the leaf has applied every entry it owns at or
/// below <c>H</c>. Every own append of this activation is applied once the count
/// is zero. An entry the leaf owns from anywhere else must already be behind the
/// checkpoint, and two conditions establish that. Either this activation bound
/// the leaf's tree id through a create path, which names a leaf that does not
/// exist yet (issue #4654), so nothing it must apply predates it - a leaf
/// re-created from a purge's clear is excluded, since its purged entries may
/// still be readable. Or the checkpoint is already within one of the lowest own
/// offset: offsets are allocated in order, so an entry the leaf owns from before
/// this activation lies below that offset, and the replay has read it. A saga
/// terminal is appended to the partition its shard index names, not the
/// prepare's, so the advance holds on every partition while any prepare is
/// pending. It also holds while another replay or a warm rescue is in flight,
/// so it never moves a checkpoint under a replay that is still advancing it.
/// </para>
/// <para>
/// The advance only records a pending checkpoint through the projection seam.
/// Its caller persists it at once, outside the coalescing window - the
/// coverage-lag tick through its bank step, the starvation drive through its own
/// flush - because the pin is bounded by the persisted checkpoint; capturing the
/// covering snapshot and publishing the pin follow that flush, unchanged.
/// <c>WalDurability.tla</c>'s <c>LiveAdvance</c> is the model of this rule.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>Own appends issued in this activation whose apply has not been recorded.</summary>
    private int _liveAppendsUnapplied;

    /// <summary>Set when the tracker can no longer vouch for every own append; per activation.</summary>
    private bool _liveAdvancePoisoned;

    /// <summary>Whether this activation bound the leaf's tree id through a create path.</summary>
    private bool _liveAdvanceBorn;

    /// <summary>The partition count the tracker's arrays were sized for.</summary>
    private int _liveAppendPartitionCount;

    /// <summary>Per partition, the lowest own offset this activation applied; <see cref="long.MaxValue"/> when none.</summary>
    private long[]? _liveOwnLow;

    /// <summary>Per partition, the highest own offset this activation applied; <c>-1</c> when none.</summary>
    private long[]? _liveOwnHigh;

    /// <summary>
    /// Counts an append this leaf is about to issue. Called by
    /// <see cref="OverrideHoldCommitLogWriter"/> before every append, so no append
    /// path can escape the count.
    /// </summary>
    internal void NoteLiveAppendIssued() => _liveAppendsUnapplied++;

    /// <summary>
    /// Records that the append of <paramref name="entry"/>, which returned
    /// <paramref name="offset"/>, has been applied.
    /// </summary>
    private void MarkLiveAppendApplied(WalRecord entry, long offset)
    {
        if (TryReleaseLiveAppend())
        {
            RecordLiveAppendOffset(entry, offset);
        }
    }

    /// <summary>
    /// Records that a batched append of the first <paramref name="count"/>
    /// records of <paramref name="entries"/>, which returned
    /// <paramref name="offsets"/>, has been applied.
    /// </summary>
    private void MarkLiveAppendApplied(WalRecord[] entries, int count, IReadOnlyList<long>? offsets)
    {
        NoteLiveAppendOffsets(entries, count, offsets);
        ReleaseLiveAppendApplied();
    }

    /// <summary>
    /// Records the offsets a batched append of the first <paramref name="count"/>
    /// records of <paramref name="entries"/> returned, without releasing its
    /// count: for a site whose record buffer is returned before its apply, which
    /// then calls <see cref="ReleaseLiveAppendApplied"/>. Recording early is safe,
    /// because no advance runs until every counted append is released.
    /// </summary>
    private void NoteLiveAppendOffsets(WalRecord[] entries, int count, IReadOnlyList<long>? offsets)
    {
        if (_liveAdvancePoisoned)
        {
            return;
        }

        if (offsets is null || offsets.Count != count)
        {
            _liveAdvancePoisoned = true;
            return;
        }

        for (var i = 0; i < count && !_liveAdvancePoisoned; i++)
        {
            RecordLiveAppendOffset(entries[i], offsets[i]);
        }
    }

    /// <summary>
    /// Releases one counted append whose offsets <see cref="NoteLiveAppendOffsets"/>
    /// recorded, once its apply has completed.
    /// </summary>
    private void ReleaseLiveAppendApplied() => TryReleaseLiveAppend();

    /// <summary>
    /// Records that a batched append of <paramref name="entries"/>, which returned
    /// <paramref name="offsets"/>, has been applied.
    /// </summary>
    private void MarkLiveAppendApplied(IReadOnlyList<WalRecord> entries, IReadOnlyList<long>? offsets)
    {
        if (!TryReleaseLiveAppend())
        {
            return;
        }

        if (offsets is null || offsets.Count != entries.Count)
        {
            _liveAdvancePoisoned = true;
            return;
        }

        for (var i = 0; i < entries.Count && !_liveAdvancePoisoned; i++)
        {
            RecordLiveAppendOffset(entries[i], offsets[i]);
        }
    }

    /// <summary>
    /// Releases one counted append. A release with nothing counted means a site
    /// released twice or an append escaped the count, so the tracker is latched
    /// off. Returns whether the offsets should be recorded.
    /// </summary>
    private bool TryReleaseLiveAppend()
    {
        if (_liveAppendsUnapplied <= 0)
        {
            _liveAdvancePoisoned = true;
            return false;
        }

        _liveAppendsUnapplied--;
        return !_liveAdvancePoisoned;
    }

    private void RecordLiveAppendOffset(WalRecord entry, long offset)
    {
        // The options were resolved before any append; an unresolved or changed
        // partition count means the offsets cannot be attributed, so stop.
        var partitionCount = _options is { } options ? Math.Max(1, options.WalPartitions) : 0;
        if (partitionCount == 0
            || offset < 0
            || (_liveAppendPartitionCount != 0 && _liveAppendPartitionCount != partitionCount))
        {
            _liveAdvancePoisoned = true;
            return;
        }

        if (_liveOwnHigh is null)
        {
            _liveAppendPartitionCount = partitionCount;
            _liveOwnLow = new long[partitionCount];
            _liveOwnHigh = new long[partitionCount];
            Array.Fill(_liveOwnLow, long.MaxValue);
            Array.Fill(_liveOwnHigh, -1L);
        }

        var partition = OverrideHoldPartitionOf(entry, partitionCount);
        if (offset < _liveOwnLow![partition])
        {
            _liveOwnLow[partition] = offset;
        }

        if (offset > _liveOwnHigh[partition])
        {
            _liveOwnHigh[partition] = offset;
        }
    }

    /// <summary>
    /// Whether nothing currently stops a live advance: every own append is
    /// applied and none failed, no saga prepare is pending, no replay is pending,
    /// and no other replay or warm rescue is in flight.
    /// </summary>
    /// <param name="withinDrive">
    /// The caller is the starvation drive that has itself claimed
    /// <see cref="_starvationDriveInFlight"/> and has not started its replay.
    /// </param>
    private bool IsLiveAdvanceQuiescent(bool withinDrive) =>
        !_liveAdvancePoisoned
        // PERTURBATION P1 (red-proof, reverted): the unapplied-count guard is gone.
        && _liveOwnHigh is not null
        && state.State.TreeId is { Length: > 0 }
        // The barrier a data operation would wait on is clear: the replay has
        // completed, or none was ever armed - in a silo the activation hook
        // always arms one, so the second case is a directly constructed grain.
        // AwaitReplayBarrierAsync applies the same rule; a retired barrier is not
        // treated as clear here.
        && (_replayBarrierSatisfied || !_replayBarrierArmed)
        && !_replayBarrierRetired
        && (withinDrive || !_starvationDriveInFlight)
        && !_warmRescueInFlight;
        // PERTURBATION P2 (red-proof, reverted): the pending-prepare guard is gone.

    /// <summary>
    /// Advances each partition's checkpoint to the highest offset this activation's
    /// own applied appends returned, where that is safe (see the type remarks).
    /// Returns whether any partition advanced.
    /// </summary>
    /// <param name="withinDrive">
    /// Whether the caller is the starvation drive, before its replay.
    /// </param>
    internal async Task<bool> TryAdvanceLiveCheckpointAsync(bool withinDrive = false)
    {
        var advanced = false;
        for (var p = 0; ; p++)
        {
            // Re-checked per partition: the stamp below may await a persist, and a
            // write or a prepare can interleave with it.
            if (!IsLiveAdvanceQuiescent(withinDrive) || p >= _liveOwnHigh!.Length)
            {
                return advanced;
            }

            var target = _liveOwnHigh[p];
            if (target < 0 || IsPartitionStaleLatched(p))
            {
                continue;
            }

            var current = GetCurrentCheckpointForPartition(p);
            // PERTURBATION P3 (red-proof, reverted): the arming check is gone.
            if (target <= current)
            {
                continue;
            }

            // PERTURBATION P6 (red-proof, reverted): the stamp lands one short.
            await StampCheckpointHintAsync(p, target - 1);
            advanced = true;
        }
    }

    /// <summary>Test seam: the tracker's count of own appends not yet applied.</summary>
    internal int LiveAppendsUnappliedForTest => _liveAppendsUnapplied;

    /// <summary>Test seam: whether the tracker is latched off for this activation.</summary>
    internal bool LiveAdvancePoisonedForTest => _liveAdvancePoisoned;

    /// <summary>Test seam: whether this activation bound the tree id through a create path.</summary>
    internal bool LiveAdvanceBornForTest => _liveAdvanceBorn;
}
