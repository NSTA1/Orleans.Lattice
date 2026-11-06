using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The live checkpoint advance (issue #5572; epic #5605, member 4): a leaf advances
/// its projection checkpoint over the appends it issued itself, without reading the
/// WAL, whenever the append fences prove it holds every entry it owns below them.
/// <para>
/// <b>Why.</b> A live write appends its record and then applies it, and before this
/// change nothing on that path moved the checkpoint: only a replay did. A leaf whose
/// rows all arrived live therefore published the block pin from birth, and its first
/// checkpoint came from a replay of every partition from <c>-1</c>, scheduled by its
/// coverage-lag timer or a WAL GC touch against a bounded permit share. On a freshly
/// written estate every leaf of every tree needed that replay at once, the permits
/// saturated, and every tree's WAL floor stayed blocked (issue #5570).
/// </para>
/// <para>
/// <b>The tracker.</b> <see cref="OverrideHoldCommitLogWriter"/> counts every append
/// before it is issued (<see cref="NoteLiveAppendIssued"/>). Each append site, once its
/// apply - including the relocation of any stranded rows - has completed, releases that
/// count and records the offsets its append returned
/// (<see cref="MarkLiveAppendApplied(WalRecord, long)"/>), as the highest own offset per
/// partition. A site that forgets to release, or an apply that throws, leaves the count
/// above zero, which only turns the advance off for the activation; a release with no
/// matching issue latches it off. So a missed site costs the advance, never
/// correctness. The one failure that is released is a definite refusal that allocated
/// nothing at any partition - routine since the hand-off fences and the clock floor -
/// which the writer reports through the fence (<see cref="DrainRefusedUnallocatedAppends"/>,
/// #5708). The write path gains an integer increment, a partition hash and an
/// array write per append: no allocation and no grain call.
/// </para>
/// <para>
/// <b>When an advance is safe.</b> Advancing partition <c>p</c> to the highest own
/// offset <c>H</c> claims the leaf has applied every entry it owns at or below
/// <c>H</c>. Replay ownership is by key range, not by author, so the claim rests on the
/// fences of <c>spec/wal/design/LiveAdvanceDesign.md</c> (modelled by
/// <c>WalLiveAdvance.tla</c>). The decision is <see cref="LeafLiveAdvanceCore"/>, the
/// model's <c>Armed</c> predicate: the activation's epoch claim at <c>p</c> is raised
/// (a duplicate activation is then rejected at <c>p</c>) and it has read to the claim
/// head; every hand-off overlapping the leaf is closed and read past, and its marker
/// state is initialised; every unstamped offset reported to it has been read; and
/// every own append is applied. The markers and the shard root's view are read fresh,
/// after the own appends the advance would pass. The advance also holds while a saga
/// prepare is pending (a terminal is appended to the partition its shard index names,
/// not the prepare's), while another replay or a warm rescue is in flight, and whenever
/// another live leaf may own the same keys or the storage provider is not known to
/// enforce ETags (design note section 4, "The leaf takes the replay-based advance").
/// Whenever the leaf cannot arm, it advances by replay, as before.
/// </para>
/// <para>
/// The advance only records a pending checkpoint through the projection seam. Its
/// caller persists it at once, outside the coalescing window - the coverage-lag tick
/// through its bank step, the starvation drive through its own flush - because the pin
/// is bounded by the persisted checkpoint; capturing the covering snapshot and
/// publishing the pin follow that flush, unchanged.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>Own appends issued in this activation whose apply has not been recorded.</summary>
    private int _liveAppendsUnapplied;

    /// <summary>Set when the tracker can no longer vouch for every own append; per activation.</summary>
    private bool _liveAdvancePoisoned;

    /// <summary>The partition count the tracker's array was sized for.</summary>
    private int _liveAppendPartitionCount;

    /// <summary>Per partition, the highest own offset this activation applied; <c>-1</c> when none.</summary>
    private long[]? _liveOwnHigh;

    /// <summary>The reused copy of <see cref="_liveOwnHigh"/> an advance decides on.</summary>
    private long[]? _liveOwnHighSnapshot;

    /// <summary>Whether an advance is deciding on <see cref="_liveOwnHighSnapshot"/>.</summary>
    private bool _liveAdvanceInFlight;

    private IShardRootGrain? _liveAdvanceShardRoot;
    private string? _liveAdvanceShardRootTree;
    private int _liveAdvanceShardRootIndex;

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
        // The options were resolved before any append, so the count is known here.
        // They are cached for the activation, so it cannot change under the
        // tracker; an unresolved count means the offsets cannot be attributed.
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
            _liveOwnHigh = new long[partitionCount];
            Array.Fill(_liveOwnHigh, -1L);
        }

        var partition = OverrideHoldPartitionOf(entry, partitionCount);
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
        DrainRefusedUnallocatedAppends()
        && _liveOwnHigh is not null
        && state.State.TreeId is { Length: > 0 }
        && LeafLiveAdvanceCore.IsQuiescent(
            poisoned: _liveAdvancePoisoned,
            unappliedAppends: _liveAppendsUnapplied,
            // The barrier a data operation would wait on is clear: the replay has
            // completed, or none was ever armed - in a silo the activation hook
            // always arms one, so the second case is a directly constructed grain.
            // AwaitReplayBarrierAsync applies the same rule; a retired barrier is
            // not treated as clear here.
            replayBarrierClear: (_replayBarrierSatisfied || !_replayBarrierArmed) && !_replayBarrierRetired,
            replayInFlight: (!withinDrive && _starvationDriveInFlight) || _warmRescueInFlight,
            sagaPending: (_pendingTx is not null && _pendingTx.Count != 0)
                || (_pendingTxDeltas is not null && _pendingTxDeltas.Count != 0));

    /// <summary>
    /// Whether no other live leaf can own this leaf's keys and the storage provider is
    /// known to enforce ETags (design note section 4). A slot-less legacy leaf owns
    /// every shard's entries in replay; a rangeless leaf with siblings is a legacy bulk
    /// leaf with no ownership range; and the claim's conditional writes prove nothing on
    /// a provider the fencing probe did not find fenced, including one whose probe is
    /// disabled.
    /// </summary>
    private bool IsLiveAdvanceOverlapExcluded() =>
        state.State.ShardIndex is not null
        && (HasDeclaredSpan || (state.State.NextSibling is null && state.State.PrevSibling is null))
        && context.ActivationServices?.GetService<GrainStorageFencingCheck>()?.LastResult?.Verdict
            == GrainStorageFencingVerdict.Fenced;

    /// <summary>
    /// Advances each partition's checkpoint to the highest offset this activation's own
    /// applied appends returned, where the leaf is armed there (see the type remarks).
    /// Returns whether any partition advanced.
    /// </summary>
    /// <param name="withinDrive">
    /// Whether the caller is the starvation drive, before its replay.
    /// </param>
    internal async Task<bool> TryAdvanceLiveCheckpointAsync(bool withinDrive = false)
    {
        // One advance at a time: the tick and the starvation drive run on interleaving
        // turns, and an advance spans awaits while it decides on the reused snapshot.
        if (_liveAdvanceInFlight
            || !IsLiveAdvanceQuiescent(withinDrive)
            || _appendFence is not { } fence
            || !IsLiveAdvanceOverlapExcluded()
            || SnapshotLiveAdvanceCandidates() is not { } ownHigh)
        {
            return false;
        }

        // #5833, revision 2: the arm's demand is a pin holding the WAL floor - the drive
        // runs for one, and a leaf with live data and no proven checkpoint on any
        // partition holds its floor by construction. Never a write or a routine tick.
        var armDemand = (withinDrive || IsStarvedOfDurableCheckpoint(ownHigh.Length)) && IsPreArmGateOpen();

        // #5837: a pre-arm leaf may also advance claimlessly, on what its appends were reported.
        if (!armDemand && !AnyPartitionClaimed(fence, ownHigh.Length) && !IsPreArmGateOpen())
        {
            return false;
        }

        _liveAdvanceInFlight = true;
        try
        {
            // Read after the snapshot above, so every own append the advance can pass
            // predates the read (design note section 2, "Shard-level marker").
            if (await ReadLiveAdvanceHandOffAsync() is not { } handOff)
            {
                return false;
            }

            var low = state.State.LowKeyInclusive;
            var high = state.State.HighKeyExclusive;
            if (armDemand && handOff.Clear && !fence.HasPendingCloses)
            {
                // Never while a hand-off is open on it: a bulk-born leaf arms only after
                // its switch's close recorded the allocator head (B's
                // SwitchCloseCoversPriorAllocations).
                await ArmForLiveAdvanceAsync(fence, ownHigh, handOff.LeafMarker, low, high);
            }

            var advanced = false;
            for (var p = 0; p < ownHigh.Length; p++)
            {
                // Re-checked per partition: the stamp below may await a persist, and a
                // write or a prepare can interleave with it.
                if (!IsLiveAdvanceQuiescent(withinDrive))
                {
                    return advanced;
                }

                var claimRaised = fence.TryGetArmingView(p, out var claimHead, out var unstampedSeen);
                var closeHead = Math.Max(
                    HandOffMarkerCore.RequiredHead(handOff.LeafMarker, low, high, p),
                    HandOffMarkerCore.HeadAt(handOff.ShardCloseHeads, p));
                var hasReport = false;
                var reportedBound = -1L;
                if (!claimRaised)
                {
                    hasReport = fence.TryGetPreArmView(p, out reportedBound);
                }

                var target = !claimRaised
                    // #5837, the claimless advance: every foreign, stale or unrouted record
                    // below an own append was reported with it, so reading past that bound
                    // and every close head reads past every other author.
                    ? LeafLiveAdvanceCore.TryFreeAdvance(
                        hasReport: hasReport,
                        reportedBound: reportedBound,
                        handOffClear: handOff.Clear,
                        closeHead: closeHead,
                        freeTarget: LeafLiveAdvanceCore.FreeTarget(ownHigh, p),
                        current: GetCurrentCheckpointForPartition(p),
                        staleLatched: IsPartitionStaleLatched(p))
                    : LeafLiveAdvanceCore.TargetFor(
                    claimRaised: claimRaised,
                    claimHead: claimHead,
                    handOffClear: handOff.Clear,
                    closeHead: closeHead,
                    unstampedSeen: unstampedSeen,
                    ownHigh: ownHigh[p],
                    current: GetCurrentCheckpointForPartition(p),
                    staleLatched: IsPartitionStaleLatched(p));
                if (target < 0)
                {
                    if (!claimRaised)
                    {
                        // #5851, ruling 2 widened (W1-W5): held only by its reported bound,
                        // or by concrete close heads alone, the leaf owes a catch-up read the
                        // coverage-lag tick dispatches, never this path. Judged on this read.
                        NoteReportedBoundCatchUp(p, ownHigh.Length, LeafLiveAdvanceCore.CatchUpTarget(
                            hasReport: hasReport,
                            reportedBound: reportedBound,
                            handOffClear: handOff.Clear,
                            closeHead: closeHead,
                            closesConcrete: AreCloseHeadsConcrete(handOff, low, high),
                            boundPending: HandOffMarkerCore.HasPendingDonorBound(handOff.LeafMarker, low, high),
                            freeTarget: LeafLiveAdvanceCore.FreeTarget(ownHigh, p),
                            current: GetCurrentCheckpointForPartition(p),
                            staleLatched: IsPartitionStaleLatched(p)));
                    }

                    continue;
                }

                await StampCheckpointHintAsync(p, target);
                advanced = true;
            }

            return advanced;
        }
        finally
        {
            _liveAdvanceInFlight = false;
        }
    }
    /// <summary>
    /// Whether every close head <paramref name="handOff"/> bears on the leaf with is concrete
    /// (#5851, W3): no close on its marker, and none folded into its shard's heads, is
    /// deferred at any partition. A deferred head is resolved by the leaf's arm, never by a
    /// catch-up read.
    /// </summary>
    private static bool AreCloseHeadsConcrete(LiveAdvanceHandOff handOff, string? low, string? high) =>
        !HandOffMarkerCore.HasDeferredClose(handOff.LeafMarker, low, high)
        && !HandOffMarkerCore.AnyDeferred(handOff.ShardCloseHeads);

    /// <summary>
    /// Per partition, the highest catch-up target a claimless advance or a split hint was
    /// held at (#5851, ruling 2 widened by W1-W5) - a reported bound, or a concrete close
    /// head minus one - and the highest one a catch-up was already dispatched for; activation
    /// memory, allocated on the first such hold. Keyed by the target value (W5): a target
    /// already dispatched is read once, a raised one re-arms it.
    /// </summary>
    private long[]? _catchUpPending;

    private long[]? _catchUpDispatched;

    /// <summary>
    /// Records that partition <paramref name="partition"/> is held only by what a catch-up
    /// read to <paramref name="bound"/> passes (#5851, ruling 2 widened by W1-W5), a value
    /// <see cref="LeafLiveAdvanceCore.CatchUpTarget"/> or <see cref="LeafLiveAdvanceCore.SplitCatchUpTarget"/>
    /// computed; -1 records nothing. Never reads or writes storage, and never reads the WAL:
    /// the read is the tick's (W5), and the checkpoint it leaves is the read's own (W1).
    /// </summary>
    internal void NoteReportedBoundCatchUp(int partition, int partitions, long bound)
    {
        if (bound < 0 || partition < 0 || partition >= partitions)
        {
            return;
        }

        if (_catchUpPending is null || _catchUpPending.Length != partitions)
        {
            _catchUpPending = new long[partitions];
            _catchUpDispatched = new long[partitions];
            Array.Fill(_catchUpPending, -1L);
            Array.Fill(_catchUpDispatched, -1L);
        }

        _catchUpPending[partition] = Math.Max(_catchUpPending[partition], bound);
    }

    /// <summary>Test seam: the catch-up target recorded at <paramref name="partition"/>; -1 when none.</summary>
    internal long PendingCatchUpTargetForTest(int partition) =>
        _catchUpPending is { } pending && partition >= 0 && partition < pending.Length ? pending[partition] : -1L;

    /// <summary>
    /// Whether a catch-up read is owed now (#5851, ruling 2): some partition's checkpoint
    /// is still below a recorded bound no catch-up was dispatched for. Marks those bounds
    /// dispatched, so each (partition, bound value) is read once; a new raise re-arms it.
    /// </summary>
    internal bool TakeReportedBoundCatchUp()
    {
        if (_catchUpPending is not { } pending || _catchUpDispatched is not { } dispatched)
        {
            return false;
        }

        var owed = false;
        for (var p = 0; p < pending.Length; p++)
        {
            if (pending[p] > dispatched[p] && GetCurrentCheckpointForPartition(p) < pending[p])
            {
                dispatched[p] = pending[p];
                owed = true;
            }
        }

        return owed;
    }

    /// <summary>
    /// Re-arms every dispatched catch-up bound the leaf has not yet read past (#5851,
    /// a5e2e2ef's N1): called when a coverage-lag drive did not run its replay - deferred,
    /// refused, latched, already driving, timed out or faulted - so the next tick
    /// dispatches it again. A drive that ran keeps its bounds dispatched: once per bound.
    /// </summary>
    private void ReArmReportedBoundCatchUp()
    {
        if (_catchUpDispatched is not { } dispatched)
        {
            return;
        }

        for (var p = 0; p < dispatched.Length; p++)
        {
            if (dispatched[p] >= 0 && GetCurrentCheckpointForPartition(p) < dispatched[p])
            {
                dispatched[p] = -1L;
            }
        }
    }

    /// <summary>
    /// Arms this activation where the drive could advance it (#5833, revision 2): every
    /// partition with an own append above its checkpoint that is not armed, and every armed
    /// one where a hand-off closed since by a deferred fence. Each arm resolves the highest
    /// deferred generation pending on the leaf's marker there (C2); the resolution is applied
    /// to <paramref name="marker"/>, which the advance reads next, and persisted best-effort,
    /// so a later activation need not re-claim it. A failed arm leaves its partition
    /// pre-arm, on the replay-based advance.
    /// </summary>
    private async Task<HashSet<int>?> ArmForLiveAdvanceAsync(
        LeafAppendEpochFence fence, long[] ownHigh, LeafHandOffState marker, string? low, string? high, bool[]? carryOnly = null)
    {
        var treeId = state.State.TreeId!;
        List<(int Partition, WalRoutingInitiator Initiator, long Generation)>? requests = null;
        for (var p = 0; p < ownHigh.Length; p++)
        {
            var deferred = HandOffMarkerCore.TryGetPendingDeferred(marker, low, high, p, out var shard, out var generation);

            // a5e2e2ef's G4 (A1, A5): a witnessed donor close still carried here is resolved
            // by this arm's re-claim of its shard's generation at or above the witness - the
            // re-claim the arm already makes for a deferred close, raised to cover both.
            if (HandOffMarkerCore.TryGetCarriedWitness(marker, low, high, p, out var carryShard, out var carryGeneration)
                && (!deferred || carryShard == shard))
            {
                deferred = true;
                shard = carryShard;
                generation = Math.Max(generation, carryGeneration);
            }

            if (carryOnly is not null)
            {
                // The backstop (A4) arms only the partitions it was asked to resolve.
                if (p >= carryOnly.Length || !carryOnly[p] || !deferred)
                {
                    continue;
                }
            }
            else
            {
                var armed = fence.TryGetArmingView(p, out _, out _);
                if ((armed && !deferred) || (!armed && ownHigh[p] <= GetCurrentCheckpointForPartition(p)))
                {
                    continue;
                }
            }

            (requests ??= []).Add(deferred
                ? (p, WalRoutingInitiator.ShardRoot(shard), generation)
                : (p, WalRoutingInitiator.None, 0L));
        }

        if (requests is null)
        {
            return null;
        }

        (long Head, long Generation)?[] heads;
        try
        {
            heads = await fence.ArmAsync(
                partition => grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{partition}"),
                requests,
                CancellationToken.None);
        }
        catch (LeafRetiredException superseded) when (superseded.ActivationSuperseded)
        {
            return null;
        }

        List<(int Partition, int Shard, long Generation, long Head)>? resolved = null;
        for (var i = 0; i < requests.Count; i++)
        {
            if (heads[i] is null)
            {
                // Review N1: counted by the fence; the partition stays pre-arm.
                ResolveLogger()?.LogDebug(
                    "Leaf {LeafId}: could not arm at WAL partition {Partition}; it stays pre-arm and advances there by replay until a later arm lands (#5833).",
                    context.GrainId,
                    requests[i].Partition);
                continue;
            }

            if (requests[i].Initiator.IsSet && heads[i] is { Head: >= 0 } reclaim)
            {
                // Review N2: resolved with the generation the re-claim actually claimed,
                // which also covers a close deferred above the one requested.
                (resolved ??= []).Add((requests[i].Partition, requests[i].Initiator.Index, Math.Max(requests[i].Generation, reclaim.Generation), reclaim.Head));
            }
        }

        if (resolved is null)
        {
            return null;
        }

        var partitions = ownHigh.Length;
        foreach (var r in resolved)
        {
            HandOffMarkerCore.ResolveDeferred(marker, r.Partition, r.Shard, r.Generation, r.Head);
        }

        if (HandOffMarkerStorage() is not { } storage)
        {
            return null;
        }

        try
        {
            // A2: the re-claim head the arm's own re-claim returned, and the carry's resolved
            // bit, land in this one marker write; a failure leaves the partition carried (A3).
            await HandOffMarkerRows.UpdateLeafAsync(storage, context.GrainId.GetGuidKey(), m =>
            {
                var changed = false;
                foreach (var r in resolved)
                {
                    changed |= HandOffMarkerCore.ResolveDeferred(m, r.Partition, r.Shard, r.Generation, r.Head);
                    changed |= HandOffMarkerCore.ResolveCarried(m, partitions, r.Partition, r.Shard, r.Generation, r.Head);
                }

                return changed;
            });
        }
        catch (Exception ex)
        {
            ResolveLogger()?.LogDebug(
                ex,
                "Leaf {LeafId}: could not record its resolved deferred hand-off heads; a later arm re-claims them (#5833).",
                context.GrainId);
            return null;
        }

        var landed = new HashSet<int>();
        foreach (var r in resolved)
        {
            landed.Add(r.Partition);
        }

        return landed;
    }

    /// <summary>
    /// Whether any partition holds a claim the advance could arm on. When none does - the
    /// gate is closed, as it is throughout a rolling upgrade from a build without the
    /// fence, or nothing has claimed yet - the tick issues no marker read and no shard-root
    /// call, so a 10.2.0 shard root is never asked for its live-advance view.
    /// </summary>
    private static bool AnyPartitionClaimed(LeafAppendEpochFence fence, int partitions)
    {
        for (var p = 0; p < partitions; p++)
        {
            if (fence.TryGetArmingView(p, out _, out _))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// A copy of the highest own offset per partition, or <see langword="null"/> when no
    /// partition has an own append above its checkpoint, so an idle tick reads nothing.
    /// The copy reuses one array per activation: the tick runs on the grain turn, and only
    /// one advance reads it at a time.
    /// </summary>
    private long[]? SnapshotLiveAdvanceCandidates()
    {
        var own = _liveOwnHigh!;
        for (var p = 0; p < own.Length; p++)
        {
            if (own[p] > GetCurrentCheckpointForPartition(p))
            {
                var copy = _liveOwnHighSnapshot ??= new long[own.Length];
                own.CopyTo(copy, 0);
                return copy;
            }
        }

        return null;
    }

    /// <summary>
    /// Reads the hand-off state that bounds the advance, fresh: this leaf's marker and
    /// its shard root's live-advance view. Returns <see langword="null"/> - hold - when
    /// either cannot be read, when the leaf is suspected to be an orphan (descent does
    /// not reach it, or orphan repair is in progress), or when the shard root's build
    /// does not know the read.
    /// </summary>
    /// <remarks>
    /// An existing leaf adopts its shard's initialised flag once that is set and the
    /// leaf has no split in flight (design note section 2, rule 2); a leaf born under
    /// the gate initialised its marker at its birth.
    /// </remarks>
    private async Task<LiveAdvanceHandOff?> ReadLiveAdvanceHandOffAsync()
    {
        if (HandOffMarkerStorage() is not { } storage
            || state.State.TreeId is not { Length: > 0 } treeId
            || state.State.ShardIndex is not { } shardIndex)
        {
            return null;
        }

        try
        {
            var leafKey = context.GrainId.GetGuidKey();
            var leafMarker = (await HandOffMarkerRows.ReadLeafAsync(storage, leafKey)).State;
            var low = state.State.LowKeyInclusive;
            var high = state.State.HighKeyExclusive;
            var view = await LiveAdvanceShardRoot(treeId, shardIndex)
                .GetLiveAdvanceViewAsync(context.GrainId, low, high);
            if (!view.Reachable || view.OrphanRepairInProgress)
            {
                // #5709: a leaf descent no longer reaches, with no hand-off open on it,
                // stops stamping for the rest of the activation.
                // #5837, the latch exemption: a sibling its donor closed before its own
                // link is witnessed is deferred under the pre-arm gate - a proven (P1)
                // activation included, which must not claim before its link (C's
                // UnlinkedNeverClaims) - and proves again after a backoff; a witnessed link
                // means its keys moved since, and orphan repair always latches
                // (a5e2e2ef's B2 ruling on #5866).
                var exempt = !view.OrphanRepairInProgress
                    && IsPreArmGateOpen()
                    && HoldsPendingDonorBound(leafMarker);
                if (!view.Reachable && leafMarker.Open.Count == 0 && _appendFence is { } fence)
                {
                    if (exempt)
                    {
                        // Its stamps carry the donor closes still unresolved per partition (G4).
                        SetCarriedCloses(fence, leafMarker, low, high, boundPending: true);
                        fence.OnReachabilityDeferred();
                    }
                    else
                    {
                        StopStampingUnreachable(fence);
                    }
                }

                return null;
            }

            if (!leafMarker.Initialised
                && view.Initialised
                && !state.State.SplitInFlight
                && state.State.UnlinkedSplitSiblingId is null)
            {
                leafMarker = await HandOffMarkerRows.UpdateLeafAsync(storage, leafKey, static m =>
                {
                    if (m.Initialised)
                    {
                        return false;
                    }

                    m.Initialised = true;
                    return true;
                });
            }

            var clear = leafMarker.Initialised
                && leafMarker.Open.Count == 0
                && !view.HeldOpen;
            if (_appendFence is { } closesFence)
            {
                // #5837 and a5e2e2ef's G4 (A3, A6): the donor closes still carried, per
                // partition until the sibling's arm resolves them there, ride every later
                // stamp; a bound pending - the link unwitnessed - is tracked apart, and alone
                // gates promotion, arming and claiming.
                var boundPending = HoldsPendingDonorBound(leafMarker);
                SetCarriedCloses(closesFence, leafMarker, low, high, boundPending);

                // a5e2e2ef's G2: reachable and witnessed - no bound pending - proves a
                // deferred activation again; reachable alone (linked, unwitnessed) does not.
                if (!boundPending)
                {
                    closesFence.PromoteDeferredReachable();
                }
            }

            return new LiveAdvanceHandOff(clear, leafMarker, view.CloseHeads);
        }
        catch (Exception ex)
        {
            ResolveLogger()?.LogDebug(
                ex,
                "Leaf {LeafId}: could not read its hand-off state; its checkpoint advances by replay this tick (epic #5605).",
                context.GrainId);
            return null;
        }
    }

    /// <summary>
    /// The leaf's shard root, resolved once per activation and slot: the advance tick
    /// would otherwise format its grain key on every attempt.
    /// </summary>
    private IShardRootGrain LiveAdvanceShardRoot(string treeId, int shardIndex)
    {
        if (_liveAdvanceShardRoot is not { } cached
            || _liveAdvanceShardRootIndex != shardIndex
            || !string.Equals(_liveAdvanceShardRootTree, treeId, StringComparison.Ordinal))
        {
            cached = grainFactory.GetGrain<IShardRootGrain>($"{treeId}/{shardIndex}");
            _liveAdvanceShardRoot = cached;
            _liveAdvanceShardRootTree = treeId;
            _liveAdvanceShardRootIndex = shardIndex;
        }

        return cached;
    }

    /// <summary>Reused by each advance tick's marker read to collect the pending donor closes (#5837).</summary>
    private List<WalPendingClose>? _pendingDonorClosesScratch;

    /// <summary>The hand-off conditions one advance tick read.</summary>
    /// <param name="Clear">The marker state is initialised and nothing overlapping the leaf is open.</param>
    /// <param name="LeafMarker">The leaf's own marker, whose closes bound the replay per partition.</param>
    /// <param name="ShardCloseHeads">The shard marker's close heads bearing on the leaf; <see langword="null"/> when none.</param>
    private readonly record struct LiveAdvanceHandOff(bool Clear, LeafHandOffState LeafMarker, long[]? ShardCloseHeads);

    /// <summary>
    /// Releases the counted appends the writer reported refused without allocating
    /// anything (#5708): the self-fence of a write admitted before a hand-off close, a
    /// stale routing generation, or a single-record clock-floor refusal. Each is routine
    /// and retried, and it left no own record to apply, so keeping its count would turn
    /// the advance off for the activation. Runs on the grain turn; the fence counts the
    /// refusals off it. A refusal is reported only after its append was counted and is
    /// never released by its site, so more refusals than counted appends means the
    /// tracker can no longer vouch for them, and it latches off. Always returns
    /// <see langword="true"/>, so it can lead a condition.
    /// </summary>
    private bool DrainRefusedUnallocatedAppends()
    {
        if (_appendFence is { } fence && fence.TakeRefusedUnallocated() is var refused and > 0)
        {
            if (refused > _liveAppendsUnapplied)
            {
                _liveAdvancePoisoned = true;
            }
            else
            {
                _liveAppendsUnapplied -= refused;
            }
        }

        return true;
    }

    /// <summary>Test seam: the tracker's count of own appends not yet applied.</summary>
    internal int LiveAppendsUnappliedForTest => DrainRefusedUnallocatedAppends() ? _liveAppendsUnapplied : 0;

    /// <summary>Test seam: whether the tracker is latched off for this activation.</summary>
    internal bool LiveAdvancePoisonedForTest => _liveAdvancePoisoned;

    /// <summary>
    /// Records on <paramref name="fence"/> the donor closes still carried on
    /// <paramref name="marker"/>, each with the partitions its arm resolved, and whether a
    /// donor bound is pending (a5e2e2ef's G4, A3 and A6). Reuses one scratch list per
    /// activation; runs on a view read, never on a write.
    /// </summary>
    private void SetCarriedCloses(LeafAppendEpochFence fence, LeafHandOffState marker, string? low, string? high, bool boundPending)
    {
        // Review N2: no fallback partition count; each close also counts its own heads.
        var partitions = _options?.WalPartitions ?? 0;
        foreach (var close in marker.Closes)
        {
            partitions = Math.Max(partitions, close.Heads.Length);
        }

        var carried = _carriedClosesScratch ??= new List<(WalPendingClose Close, bool[] ResolvedAt)>(WalAppendStamp.MaxPendingCloses);
        carried.Clear();
        HandOffMarkerCore.CollectCarriedDonorCloses(marker, low, high, partitions, carried);
        fence.SetCarriedCloses(carried, boundPending);
        _carriedClosesRead = true;

        // A4: note the witnessed partitions still carried, for the tick's backstop.
        _carryDemand = null;
        _carryAwaitsWitness = carried.Count > 0 && boundPending;
        if (carried.Count == 0 || boundPending)
        {
            return;
        }

        for (var p = 0; p < partitions; p++)
        {
            if (HandOffMarkerCore.TryGetCarriedWitness(marker, low, high, p, out _, out var witness))
            {
                (_carryDemand ??= new long[partitions])[p] = witness;
                Interlocked.Increment(ref s_carriedUnresolvedObserved);
            }
        }
    }

    /// <summary>
    /// Whether the last view read found a carried close whose bound is still pending: the
    /// tick re-reads the view until it sees the witness (A4's first tick after it). A close
    /// carried with no armable witness - a shard mismatch (A1) - leaves this false, so the
    /// tick does not loop on it.
    /// </summary>
    private bool _carryAwaitsWitness;

    /// <summary>
    /// Whether this activation's fence has been given the carried closes from a view read
    /// (#5851, LA4); until then a split's cut reader would carry none of them.
    /// </summary>
    private bool _carriedClosesRead;

    private List<(WalPendingClose Close, bool[] ResolvedAt)>? _carriedClosesScratch;

    /// <summary>Per partition, the link witness of a donor close still carried there, as the last view read it (A4); 0 or null for none.</summary>
    private long[]? _carryDemand;

    /// <summary>Per partition, the witness the backstop last armed for (A4): once per (partition, witness) per activation.</summary>
    private long[]? _carryBackstopped;

    private static long s_carryBackstopArms;

    private static long s_carriedUnresolvedObserved;

    /// <summary>How many partitions the carry backstop has armed, process-wide (A4); for probes and tests.</summary>
    internal static long CarryBackstopArmsForProbe => Interlocked.Read(ref s_carryBackstopArms);

    /// <summary>How many carried, witnessed, unresolved partitions view reads have seen, process-wide (A4); for probes and tests.</summary>
    internal static long CarriedUnresolvedObservedForProbe => Interlocked.Read(ref s_carriedUnresolvedObserved);

    /// <summary>
    /// The coverage-lag tick's backstop for a witnessed donor close still carried at a
    /// partition (a5e2e2ef's G4, A4, as amended for #5851 by LA1-LA2): it arms there,
    /// re-claiming the close's shard's generation at or above the witness, at most once per
    /// (partition, witness) per activation, but only while the carriage is binding there -
    /// the leaf holds an own applied append above its checkpoint that this tick's advance
    /// could not pass. A carriage that binds nothing is left as it is: the arm is delayed,
    /// never the carriage ended, so every stamp keeps the close (LA1). Binding is checked
    /// first in memory, so a non-binding sibling reads nothing, and again on the backstop's
    /// own fresh view read, which alone triggers the re-claim (LA2). An attempt that does
    /// not land re-arms the partition for the next tick. Off the write path.
    /// </summary>
    internal async Task<bool> TryBackstopCarriedClosesAsync()
    {
        if (_liveAdvanceInFlight
            || _appendFence is not { } fence
            || !IsPreArmGateOpen()
            || state.State.TreeId is not { Length: > 0 })
        {
            return false;
        }

        if (_carryDemand is null && _carryAwaitsWitness && fence.HasCarriedClosesForBackstop)
        {
            // The fence still carries a close whose bound the last view read found pending:
            // read it again, which notes any partition witnessed since (A4's first tick).
            _liveAdvanceInFlight = true;
            try
            {
                await ReadLiveAdvanceHandOffAsync();
            }
            finally
            {
                _liveAdvanceInFlight = false;
            }
        }

        if (_carryDemand is not { } demand)
        {
            return false;
        }

        var backstopped = _carryBackstopped ??= new long[demand.Length];
        if (!AnyCarryOwed(demand, backstopped))
        {
            // #5851's A4 amendment (LA1): no carried partition is binding, so nothing is
            // read; the carriage is untouched and every stamp keeps the close.
            return false;
        }

        _liveAdvanceInFlight = true;
        HashSet<int>? landed = null;
        bool[]? due = null;
        try
        {
            if (await ReadLiveAdvanceHandOffAsync() is { Clear: true } handOff
                && !fence.HasPendingCloses
                && _carryDemand is { } fresh)
            {
                // LA2: binding is judged again on this read, which triggers the re-claim; a
                // partition no longer binding is skipped and stays owed, never marked done.
                for (var p = 0; p < fresh.Length && p < backstopped.Length; p++)
                {
                    if (IsCarryOwed(fresh, backstopped, p))
                    {
                        (due ??= new bool[fresh.Length])[p] = true;
                        backstopped[p] = fresh[p];
                    }
                }

                if (due is not null)
                {
                    landed = await ArmForLiveAdvanceAsync(
                        fence, new long[fresh.Length], handOff.LeafMarker, state.State.LowKeyInclusive, state.State.HighKeyExclusive, due);
                }
            }
        }
        finally
        {
            _liveAdvanceInFlight = false;
            for (var p = 0; due is not null && p < due.Length; p++)
            {
                if (due[p])
                {
                    if (landed is not null && landed.Contains(p))
                    {
                        Interlocked.Increment(ref s_carryBackstopArms);
                    }
                    else
                    {
                        // N1's rule: an attempt that did not land is owed again.
                        backstopped[p] = 0;
                    }
                }
            }
        }

        return landed is { Count: > 0 };
    }

    private bool AnyCarryOwed(long[] demand, long[] backstopped)
    {
        for (var p = 0; p < demand.Length && p < backstopped.Length; p++)
        {
            if (IsCarryOwed(demand, backstopped, p))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Whether partition <paramref name="p"/> owes the backstop's re-claim: a witnessed
    /// carriage not yet armed for this witness, binding now (#5851, LA1).
    /// </summary>
    private bool IsCarryOwed(long[] demand, long[] backstopped, int p) =>
        demand[p] > 0 && backstopped[p] < demand[p] && IsCarryBinding(p);

    /// <summary>
    /// Whether a carried close is binding at <paramref name="p"/> (#5851, LA1): the leaf has
    /// an own applied append above its checkpoint there, which the tick's advance ran
    /// before and did not pass. Read from the current state, never a remembered verdict (LA2).
    /// </summary>
    private bool IsCarryBinding(int p) =>
        _liveOwnHigh is { } own && p < own.Length && own[p] > GetCurrentCheckpointForPartition(p);
}
