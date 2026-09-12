using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf-node split logic: two-phase crash-safe split with deterministic sibling identity.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Per-activation gated entry point for <see cref="SplitAsync"/>.
    /// Tries to acquire <c>_splitGate</c> <em>without blocking</em>; the
    /// single turn that wins the gate owns the split and runs it, while
    /// every concurrent overflowing turn returns immediately rather than
    /// convoying on the gate. Inside the gate the overflow predicate is
    /// re-checked (a just-completed in-flight split may have already
    /// pushed <c>Cache.Count</c> back under the threshold), and the
    /// split runs only if still required. Returns <see langword="null"/>
    /// when the caller did not own the split (either the gate was held
    /// by an in-flight split, or the re-check found nothing to do),
    /// mirroring the no-split branch of the foreground commit paths.
    /// <para>
    /// Why non-blocking: the mutation surface
    /// (<see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetAsync(string, byte[])"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetManyAsync"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.DeleteAsync"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.MergeManyAsync"/>) is marked
    /// <c>[AlwaysInterleave]</c>, so under a write burst many interleaved
    /// turns observe overflow on the same hot leaf at once. Each turn's
    /// data is already durable (WAL append + projection apply both run
    /// <em>before</em> this predicate is evaluated), and an in-flight
    /// split's cross-grain migration runs a long chain of Azure-Table
    /// round-trips while holding the gate. A blocking
    /// <c>WaitAsync()</c> here parks every concurrent producer slot on
    /// the gate for the full migration duration only for each to
    /// discover, via the re-check, that the in-flight split already
    /// absorbed its overflow - a dead convoy that stalls ingest under
    /// table saturation. Skipping when the gate is contended keeps those
    /// slots flowing; the leaf is transiently over-full but correct (the
    /// owning split, or the next write that wins the gate, migrates the
    /// excess), and reads/writes against an over-full leaf are
    /// unaffected.
    /// </para>
    /// </summary>
    private async Task<SplitResult?> SplitIfNeededUnderGateAsync(int maxLeafKeys, long maxLeafBytes = 0)
    {
        // Non-blocking acquire: the loser of the race does NOT wait for
        // the in-flight split's cross-grain migration to drain. Its
        // write is already durable and the owning split (or a later
        // write) will carry the leaf back under threshold, so returning
        // null here is the same observable outcome the blocking re-check
        // would have produced - minus the convoy wait.
        if (!_splitGate.Wait(0))
            return null;
        try
        {
            // Re-check inside the gate. A concurrent turn may have
            // already split this leaf and removed our overflow's
            // entries to the sibling, leaving Cache.Count back under
            // the threshold; in that case we have nothing to do.
            if (!IsLeafOverCapacity(maxLeafKeys, maxLeafBytes))
                return null;
            return await SplitAsync();
        }
        finally
        {
            _splitGate.Release();
        }
    }

    /// <summary>
    /// The leaf overflow predicate: whether the leaf exceeds either the
    /// structural key-count bound or the <see cref="LatticeOptions.MaxLeafBytes"/>
    /// byte bound. Both call sites on the write path and the snapshot-capture
    /// self-repair path evaluate it through here, so the two can never drift
    /// into disagreeing about what "over capacity" means.
    /// <para>
    /// The byte arm carries an extra condition the count arm does not need:
    /// <c>Cache.Count &gt; 1</c>. A split pivots on the median key, so a leaf
    /// holding one entry has no median: <c>SplitAsync</c> would choose that
    /// single key as the split key, migrate every entry to the sibling, and
    /// leave an empty donor. The predicate would then hold on the sibling,
    /// which would split again, forever, allocating a fresh leaf grain each
    /// time and never making progress. Excluding the single-entry case makes
    /// the predicate strictly progress-bounded: every leaf it fires on has at
    /// least two entries, so a split always leaves both sides non-empty and
    /// strictly smaller than the original.
    /// </para>
    /// <para>
    /// A value larger than the bound on its own is therefore irreducible by
    /// splitting and is reported rather than repaired; see
    /// <see cref="LatticeMetrics.LeafByteOverflowIrreducible"/>.
    /// </para>
    /// </summary>
    private bool IsLeafOverCapacity(int maxLeafKeys, long maxLeafBytes)
        => Cache.Count > maxLeafKeys
            || (maxLeafBytes > 0 && Cache.Count > 1 && Cache.StateBytes > maxLeafBytes);

    /// <summary>
    /// Per-pass ceiling on consecutive byte-overflow splits. Each split halves
    /// the donor, so the passes needed are logarithmic in the overshoot: eight
    /// admits a leaf 256 times the bound, which is far beyond anything the
    /// key-count bound can let accumulate. It exists to keep the loop provably
    /// terminating rather than because the limit is expected to bind.
    /// </summary>
    private const int MaxByteOverflowSplitsPerPass = 8;

    /// <summary>
    /// Divides a leaf that is over the <see cref="LatticeOptions.MaxLeafBytes"/>
    /// bound back under it, splitting repeatedly because one split only halves
    /// the donor and a leaf may be several multiples of the bound.
    /// <para>
    /// <b>This is the self-repair half of the byte bound, and it is what makes
    /// an already-oversized deployment recover without operator action.</b> The
    /// write-path predicate alone cannot do that: it is only evaluated when a
    /// leaf is written, so a leaf that grew oversized and then went quiet would
    /// stay oversized, stay uncapturable, and keep its tree's WAL trim floor
    /// pinned at zero forever. This entry point is reached from
    /// <c>CaptureSnapshotCoreAsync</c>, the single seam every snapshot-capture
    /// driver passes through, so a leaf is divided before its payload is
    /// materialised whichever driver brought it to capture - including on a
    /// tree that has stopped taking writes entirely.
    /// <para>
    /// It was previously reached only from the zero-coverage repair driver,
    /// behind that driver's <c>HasCheckpointedPartitionWithoutCoverage</c>
    /// predicate. A tree with no proven-checkpointed partition never satisfied
    /// it, so no leaf on such a tree was ever divided and every capture route
    /// threw <see cref="OutOfMemoryException"/> instead (issue #2733). Do not
    /// re-add a call behind a driver-specific predicate: the guard is only
    /// sound where every driver passes.
    /// </para>
    /// <para>
    /// Returns whether any split occurred. A leaf that cannot be divided (one
    /// entry larger than the bound) is reported on
    /// <see cref="LatticeMetrics.LeafByteOverflows"/> as <c>irreducible</c> and
    /// left intact; see <see cref="IsLeafOverCapacity"/> for why splitting it
    /// anyway would not terminate.
    /// </para>
    /// </summary>
    private async Task<bool> TrySplitForByteOverflowAsync(int maxLeafKeys, long maxLeafBytes)
    {
        // Issue #2756. Zero-prime BOTH outcomes before any early return, so a
        // leaf that reaches this seam and needs no division still mints the
        // series. A Counter exports nothing at all until its first Add, so
        // without this an absent leaf_byte_overflow_total spans three states
        // that a reader cannot tell apart: the hoist of this call to the
        // capture seam (issue #2733) did not land, or it landed and no leaf is
        // oversized, or no leaf has activated yet. That ambiguity is not
        // hypothetical - the absence of this very series was read as evidence
        // the capture-seam hoist had failed, when it equally indicated success.
        //
        // Priming HERE rather than at activation is what makes the series a
        // reachability proof: this method is reached only through
        // CaptureSnapshotCoreAsync, so a minted zero says the capture seam ran
        // and evaluated the bound, which is the property in question. An absent
        // series then means the seam was never reached, which is a positive
        // statement rather than silence.
        //
        // Primed through RecordLeafByteOverflow rather than by calling Add
        // directly, so the primed series carries a tag set identical to a real
        // emission BY CONSTRUCTION. A prime on a divergent tag shape would mint
        // a second series that never converges with the one actually counting,
        // leaving a permanently-zero line beside a live counter - worse than
        // absence, because it reads as a measured zero.
        RecordLeafByteOverflow(LatticeMetrics.LeafByteOverflowSplit, 0);
        RecordLeafByteOverflow(LatticeMetrics.LeafByteOverflowIrreducible, 0);

        if (maxLeafBytes <= 0 || Cache.StateBytes <= maxLeafBytes)
        {
            return false;
        }

        var splits = 0;
        while (splits < MaxByteOverflowSplitsPerPass
               && IsLeafOverCapacity(maxLeafKeys, maxLeafBytes))
        {
            // A contended gate returns null, as does a re-check that finds the
            // leaf already back under bound. Either way there is no progress to
            // make on this turn, so stop rather than spin.
            if (await SplitIfNeededUnderGateAsync(maxLeafKeys, maxLeafBytes) is null)
            {
                break;
            }

            splits++;
        }

        if (splits > 0)
        {
            RecordLeafByteOverflow(LatticeMetrics.LeafByteOverflowSplit);
        }

        // Report separately from the split outcome rather than as an else: a
        // leaf can both split usefully and still end up irreducible, when the
        // divisions strand a single oversized entry on one side.
        if (Cache.Count <= 1 && Cache.StateBytes > maxLeafBytes)
        {
            RecordLeafByteOverflow(LatticeMetrics.LeafByteOverflowIrreducible);

            // Say it ONCE per activation, not once per capture.
            //
            // This seam is now reached by every capture route, and the hottest
            // of them is the cadence recheck, which fires after every durable
            // checkpoint flush. On the deployment that motivated issue #2733
            // that produced 86 identical capture failures in 19 minutes across
            // 32 leaves - 2,761 OutOfMemoryException lines - each saying the
            // capture "will retry on next periodic recheck or reactivation".
            // An irreducible leaf makes that retry loop unbounded and
            // structurally hopeless: no number of further attempts can divide a
            // single entry larger than the bound, so repeating the message per
            // attempt buries the one fact an operator needs.
            //
            // The latch is per-activation, which needs no explicit reset: an
            // Orleans activation is a fresh grain instance, so the field starts
            // false each time the leaf comes online. That is the behaviour we
            // want - a condition that survives a restart is re-announced once,
            // rather than being silenced for the life of the process.
            if (!_irreducibleByteOverflowAnnounced)
            {
                _irreducibleByteOverflowAnnounced = true;
                ResolveLogger()?.LogWarning(
                    "Leaf {GrainId} on tree {TreeId} holds a single entry of {StateBytes} bytes, "
                    + "over the {MaxLeafBytes}-byte MaxLeafBytes bound, and cannot be divided - a "
                    + "split needs at least two entries to pivot on. Snapshot capture for this "
                    + "leaf will keep failing if the payload exceeds what can be serialised "
                    + "contiguously, which holds its tree's WAL trim floor at zero. This is "
                    + "reported once per activation; the leaf_byte_overflow_total counter carries "
                    + "the per-attempt series under outcome=irreducible.",
                    context.GrainId,
                    state.State.TreeId ?? string.Empty,
                    Cache.StateBytes,
                    maxLeafBytes);
            }
        }

        return splits > 0;
    }

    /// <summary>
    /// Latches the once-per-activation irreducible-leaf warning above. An
    /// Orleans activation is a fresh grain instance, so this starts false every
    /// time the leaf comes online and needs no explicit reset.
    /// </summary>
    private bool _irreducibleByteOverflowAnnounced;

    /// <summary>
    /// Records one byte-overflow outcome, or mints its series without moving it
    /// when <paramref name="delta"/> is zero. Both the real emission and the
    /// zero-prime go through here so they cannot drift apart in tag shape.
    /// </summary>
    private void RecordLeafByteOverflow(KeyValuePair<string, object?> outcome, long delta = 1)
    {
        var treeId = state.State.TreeId ?? string.Empty;
        LatticeMetrics.LeafByteOverflows.Add(
            delta,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
            outcome,
            LatticeTenantLabel.ForTree(treeId));
    }

    /// <summary>
    /// Per-activation gated entry point for the recovery-path
    /// <see cref="CompleteSplitAsync"/> calls in
    /// <see cref="SetCoreAsync"/> and
    /// <see cref="MergeManyAsync"/>.
    /// Acquires <c>_splitGate</c>, re-checks
    /// <see cref="Primitives.SplitState.SplitInProgress"/> inside the
    /// gate (a concurrent turn may have already completed the split),
    /// and runs <see cref="CompleteSplitAsync"/> + <see cref="PersistAsync"/>
    /// only if the in-progress state is still observed. Returns
    /// <see langword="null"/> when a concurrent turn already finished
    /// the recovery; the caller still has stable
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.SplitKey"/> /
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.SplitSiblingId"/> fields to
    /// route its own write across the donor / sibling boundary, so
    /// the post-gate routing in the caller is correct either way.
    /// <para>
    /// This recovery acquire stays <em>blocking</em> (unlike the
    /// non-blocking acquire in <see cref="SplitIfNeededUnderGateAsync"/>)
    /// because it guards the migration-serialisation invariant: while a
    /// split is mid-flight, <see cref="CompleteSplitAsync"/> snapshots
    /// the donor's <c>&gt;= splitKey</c> entries into the sibling and
    /// then removes them from the donor. A contended turn that skipped
    /// the gate here would route its write to a sibling that is not yet
    /// initialised (tree id / key range / entries unset) or race the
    /// snapshot-then-remove window and lose the write. The thundering
    /// herd that motivated the non-blocking split-predicate acquire is
    /// the simultaneous-overflow case on a not-yet-splitting leaf, which
    /// arrives through <see cref="SplitIfNeededUnderGateAsync"/>, not
    /// here; mid-migration arrivals through this path are comparatively
    /// few and must serialise for correctness.
    /// </para>
    /// </summary>
    private async Task<SplitResult?> CompleteRecoverySplitUnderGateAsync()
    {
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            if (state.State.SplitState != Primitives.SplitState.SplitInProgress)
                return null;
            var recovered = await CompleteSplitAsync();
            await PersistAsync();
            return recovered;
        }
        finally
        {
            _splitGate.Release();
        }
    }

    private async Task<SplitResult> SplitAsync()
    {
        // Only the median key is needed to pivot the split. Asking the cache's
        // ordered key view for it looks free - it reads as a projection over an
        // in-memory dictionary - but Keys calls HydrateAll() first, so placing
        // the cut used to require materialising every row in the leaf. That is
        // self-defeating on exactly the leaves this exists to divide: the
        // larger the leaf, the more certain the hydration fails, and division
        // is the only thing that would have made it smaller (issue #2771).
        //
        // Take the pivot from the frame's ordinal index instead, which decodes
        // one key and no payload. The fallback is the old path, used when
        // nothing is lazily hydrated (the leaf is already resident, so the
        // ordered view costs nothing extra) or when a strictly interior pivot
        // cannot be established from the frame alone.
        if (!Cache.TryGetBisectingKeyWithoutHydrating(out var splitKey))
        {
            var keys = Cache.Keys;
            int mid = keys.Count() / 2;
            splitKey = keys.ElementAt(mid);
        }

        // Snapshot the WAL head per partition before the split's
        // intent is persisted. Under multi-partition replay every
        // partition has its own offset space, so the sibling's per-
        // partition replay-from-zero must be bounded by the matching
        // partition's head; a single scalar would conflate them.
        var treeId = state.State.TreeId;
        long[]? walHeadsAtSplit = null;
        if (!string.IsNullOrEmpty(treeId))
        {
            walHeadsAtSplit = await CaptureWalHeadsByPartitionAsync(treeId);
        }

        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitInProgress);
        state.State.SplitKey = splitKey;
        state.State.SplitSiblingId = grainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid()).GetGrainId();
        state.State.OldNextSibling = state.State.NextSibling;
        state.State.NextSibling = state.State.SplitSiblingId;
        await PersistAsync();

        LatticeMetrics.LeafSplits.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty),
            LatticeTenantLabel.ForTree(state.State.TreeId ?? string.Empty));
        return await CompleteSplitAsync(walHeadsAtSplit);
    }

    /// <summary>
    /// Captures the current WAL head offset across every partition for
    /// the leaf's tree. Returns a non-null array whose length is the
    /// configured <see cref="LatticeOptions.WalPartitions"/>; entry
    /// <c>i</c> is the head of partition <c>i</c> at capture time.
    /// </summary>
    private async Task<long[]> CaptureWalHeadsByPartitionAsync(string treeId)
    {
        var options = await GetOptionsAsync();
        var partitionCount = Math.Max(1, options.WalPartitions);
        if (partitionCount == 1)
        {
            // Common single-partition shape: skip the WhenAll plumbing.
            var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                $"{treeId}/0");
            return [await coordinator.GetHeadOffsetAsync(CancellationToken.None)];
        }

        // Each partition's head lives on an independent coordinator grain,
        // so the reads have no ordering dependency - fan them out in
        // parallel instead of awaiting each one serially. On the split
        // fast-path this turns an O(WalPartitions) round-trip chain into a
        // single round-trip's worth of wall-clock latency.
        var tasks = new Task<long>[partitionCount];
        for (var p = 0; p < partitionCount; p++)
        {
            var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                $"{treeId}/{p}");
            tasks[p] = coordinator.GetHeadOffsetAsync(CancellationToken.None);
        }
        return await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Completes (or resumes) a split whose intent has already been persisted.
    /// Safe to call multiple times - MergeEntriesAsync is idempotent (LWW merge).
    /// On the recovery path (caller does not hold captured WAL heads) the
    /// optional <paramref name="walHeadsAtSplit"/> is omitted; the recovery
    /// branch reads the current WAL heads fresh, which is still safe - a
    /// later head only causes the sibling to skip more replay, never less.
    /// </summary>
    private async Task<SplitResult> CompleteSplitAsync(long[]? walHeadsAtSplit = null)
    {
        var splitKey = state.State.SplitKey!;
        var siblingId = state.State.SplitSiblingId!.Value;
        var donorPreSplitHigh = state.State.HighKeyExclusive;
        var newLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(siblingId);

        // Fresh per-partition WAL-head reads on the recovery path.
        long[]? resolvedHeads = walHeadsAtSplit;
        if (resolvedHeads is null)
        {
            var treeId = state.State.TreeId;
            if (!string.IsNullOrEmpty(treeId))
            {
                resolvedHeads = await CaptureWalHeadsByPartitionAsync(treeId);
            }
        }

        var oldNextId = state.State.OldNextSibling;

        // The old-next leaf's back-pointer fixup targets a different grain
        // than the sibling-seeding chain below, so it has no ordering
        // dependency on it - kick it off now and await it alongside the
        // sibling work to overlap the two cross-grain round-trips.
        Task oldNextFixup = Task.CompletedTask;
        if (oldNextId is not null)
        {
            var oldNext = grainFactory.GetGrain<IBPlusLeafGrain>(oldNextId.Value);
            oldNextFixup = oldNext.SetPrevSiblingAsync(siblingId);
        }

        // Seed every birth-time metadata slot on the sibling in one
        // round-trip: tree id, shard index, ownership key range, and the
        // next/prev sibling pointers. This replaces five separate gated
        // setter RPCs (each its own gate acquire + WriteStateAsync) with a
        // single gate acquire and a single persist on the sibling.
        //
        // The sibling inherits this leaf's binding verbatim, so a donor that
        // is itself unbound mints an unbound sibling - which is how a single
        // unseeded node propagates across a key range (issue #1744). Surface
        // it rather than passing it on silently; the shard root re-binds both
        // on the next typed CRDT write routed to them, so this is a warning
        // and not a fault.
        if (string.IsNullOrEmpty(state.State.TreeId))
        {
            ResolveLogger()?.LogWarning(
                "Leaf {LeafId} is splitting with no tree id bound, so its new sibling {SiblingId} inherits an "
                + "unbound tree id and will reject typed CRDT writes until the owning shard root re-binds it.",
                context.GrainId,
                siblingId);
        }

        await newLeaf.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = state.State.TreeId!,
            ShardIndex = state.State.ShardIndex,
            LowKeyInclusive = splitKey,
            HighKeyExclusive = donorPreSplitHigh,
            NextSibling = oldNextId,
            PrevSibling = context.GrainId,
        });

        // Join the back-pointer fixup before mutating the donor's own
        // state so a thrown fixup surfaces here (and not on a later
        // unobserved-task path). Awaited ahead of the transfer rather than
        // after it, as it was while the transfer was a single pass: the
        // transfer now removes rows batch by batch, so donor mutation begins
        // at the first batch rather than after the last. It still overlaps
        // the InitializeSiblingAsync round-trip above.
        await oldNextFixup;

        // Migrate the >= splitKey rows in bounded batches rather than in one
        // pass. The single pass read them through Cache.EnumerateRows(), which
        // - like Keys - calls HydrateAll() first, so it materialised the whole
        // leaf, built a dictionary holding the entire right half, and handed
        // that to MergeEntriesAsync to be deep-copied: three whole-leaf-scale
        // costs alive at once, on a leaf already known to be oversized.
        //
        // Batching bounds the peak to one batch regardless of how large the
        // leaf is, which is the property that makes this a fix rather than a
        // mitigation: a leaf twice the size divides at the same peak, not at
        // twice the peak. The batch width is derived at runtime from the
        // frame's own measured mean row footprint against an option that
        // already exists and already defaults sanely, so no new constant is
        // introduced and nothing is tuned to a particular host's memory.
        //
        // Crash-safety is unchanged by batching. The split intent - SplitKey,
        // SplitSiblingId and NextSibling - is already durable before any row
        // moves (persisted by SplitAsync, or carried by the recovery path), so
        // a process that dies mid-transfer reactivates with SplitInProgress and
        // re-runs this method against the same durable SplitKey. The donor's
        // own remaining rows are the resume cursor: rows already migrated and
        // removed are simply not seen again, and rows migrated but not yet
        // removed are re-sent into an idempotent LWW merge. That is the same
        // contract the single-pass transfer relied on - it too had a durable
        // window, between the sibling's merge landing and the donor's removals
        // being persisted, in which a key existed on both leaves - so batching
        // changes how many such windows occur, not what state they leave
        // behind.
        var transferOptions = await GetOptionsAsync();
        var batchBoundaries = Cache.GetTransferBatchBoundariesWithoutHydrating(
            splitKey, transferOptions.LeafHydrationResidentBytes);

        var batchStart = splitKey;
        for (var boundary = 0; boundary <= batchBoundaries.Count; boundary++)
        {
            var batchEndExclusive = boundary < batchBoundaries.Count
                ? batchBoundaries[boundary]
                : null;

            // Materialised into a dictionary before any mutation: EnumerateRange
            // hands back a live view over the backing dictionary, and RemoveEntry
            // below structurally modifies it.
            var batch = new Dictionary<string, LwwValue<byte[]>>();
            foreach (var (key, lww) in Cache.EnumerateRange(batchStart, batchEndExclusive))
            {
                batch[key] = lww;
            }

            if (batch.Count > 0)
            {
                // Arm the sibling's read gate BEFORE the migrated entries land
                // on it. While a cross-shard reshard saga is mid-flight a leaf
                // can hold an IsMigrated=true value for a key whose atomic
                // isolation is provided EITHER by a destination-side shadow
                // marker (_shadowedSagas, installed by the shard shadow-forward)
                // OR by a locally prepared saga bucket (_pendingTx, when the
                // saga prepared directly on this leaf). Both are per-key state on
                // the donor; a split moves only the committed Entries row to the
                // sibling. Without carrying that isolation the sibling would
                // surface the migrated pre-saga value ungated, and once the saga
                // commits a concurrent reader could observe it while sibling keys
                // already show the post-saga value - the torn read the reshard
                // chaos fixture catches. Re-arm the sibling with a shadow marker
                // for every such saga so the read gate rejects a
                // Committed-without-backstop read until the saga's committed-values
                // backstop terminal lands on the sibling (it routes there as the
                // key's current owner and clears the marker). Must precede
                // MergeEntriesAsync so the gate is armed before the migrated value
                // becomes visible on the sibling.
                await TransferShadowMarkersToSiblingAsync(newLeaf, batch.Keys);
                await newLeaf.MergeEntriesAsync(batch);

                // Drop the batch from the donor before reading the next one, so
                // the migrated payload is released rather than accumulating
                // across batches. The rows are resident from the enumeration
                // just above, so this costs no further hydration.
                foreach (var key in batch.Keys)
                {
                    RemoveEntry(key);
                }
            }

            if (batchEndExclusive is null)
            {
                break;
            }

            batchStart = batchEndExclusive;
        }

        // Per-partition projection-checkpoint hints on the sibling, applied
        // in a single round-trip. Each partition's hint is scoped to that
        // partition inside the callee so the sibling's clamp targets the
        // right offset space - replacing the per-partition RPC fan-out.
        if (resolvedHeads is not null)
        {
            await newLeaf.SetCheckpointOffsetHintsAsync(resolvedHeads);
        }

        state.State.HighKeyExclusive = splitKey;
        state.State.OldNextSibling = null;
        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitComplete);

        // Advance the donor's per-partition projection checkpoints to
        // the WAL heads captured at split time. Each partition's
        // SetCheckpointOffsetAsync call is scoped to that partition so
        // the per-partition clamp is applied correctly.
        //
        // A split is not instantaneous: while it is in flight the donor
        // keeps applying WAL entries on these partitions, advancing the
        // projection checkpoint past the head captured at split start.
        // The advance here is only meant to push the donor forward to
        // the split frontier, so when the donor's current checkpoint for
        // a partition already meets or exceeds the captured head, skip
        // the advance entirely. Calling SetCheckpointOffsetAsync with a
        // stale head would otherwise ask it to move the checkpoint
        // backward and trip the monotonic-non-decreasing guard. See
        // issue 905.
        if (resolvedHeads is not null)
        {
            var projection = (ILeafProjection)this;
            for (var p = 0; p < resolvedHeads.Length; p++)
            {
                var donorHead = resolvedHeads[p];
                if (donorHead > 0 && GetCurrentCheckpointForPartition(p) < donorHead)
                {
                    using (LatticeApplyOffsetContext.BeginScope(p, donorHead))
                    {
                        await projection.SetCheckpointOffsetAsync(donorHead, CancellationToken.None);
                    }
                }
            }
        }

        await PublishDigestUpwardInlineAsync();

        return new SplitResult
        {
            PromotedKey = splitKey,
            NewSiblingId = siblingId,
            ChildIsLeaf = true,
        };
    }

    /// <summary>
    /// Re-arms the sibling's reshard read gate for the keys moving to it in
    /// a split, by installing a destination-side shadow marker
    /// (<see cref="MarkSagaShadowAsync"/>) on the sibling for every saga
    /// that is still isolating one of those keys on this donor - both the
    /// keys already carrying an explicit <see cref="_shadowedSagas"/> marker
    /// and the keys with a locally prepared, not-yet-terminal bucket in
    /// <see cref="_pendingTx"/>. Armed on the sibling before the migrated
    /// rows are merged there, this preserves the reshard atomic-visibility
    /// gate across a leaf split: an
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.IsMigrated"/>=<c>true</c>
    /// value that moves to the sibling stays gated for a
    /// Committed-without-backstop read until the saga's committed-values
    /// backstop terminal lands on the sibling (which routes there as the
    /// key's current owner and clears the marker via
    /// <see cref="ApplyTxTerminalAsync"/>).
    /// <para>
    /// Modelling the moved-key isolation as a shadow marker on the sibling -
    /// rather than copying the prepared <see cref="_pendingTx"/> bucket and
    /// its per-partition WAL offsets - keeps the sibling's projection-
    /// checkpoint offset space untouched (the offsets are meaningful only in
    /// the donor's replay stream) while still gating the read: an InFlight or
    /// Aborted saga passes through (serving the migrated pre-saga value is the
    /// strict-isolation-correct answer), and a Committed saga gates until its
    /// backstop lands, exactly as the shard shadow-forward path does.
    /// </para>
    /// <para>
    /// The donor keeps its own <see cref="_pendingTx"/> bucket and
    /// <see cref="_shadowedSagas"/> markers for the moved keys: they are inert
    /// once the split shrinks its key range (the donor no longer owns or
    /// serves those keys) and are cleared per-saga by
    /// <see cref="ApplyTxTerminalAsync"/> on the saga's terminal, so their
    /// lifetime stays bounded by saga progress. Removing them here instead
    /// would open a window - between this transfer and the donor dropping the
    /// moved rows from its own cache - in which the donor still serves the
    /// migrated value but no longer gates it.
    /// </para>
    /// </summary>
    private async Task TransferShadowMarkersToSiblingAsync(
        IBPlusLeafGrain sibling,
        IReadOnlyCollection<string> movedKeys)
    {
        var haveMarkers = _shadowedSagas is { Count: > 0 };
        var havePending = _pendingTx is { Count: > 0 };
        if (!haveMarkers && !havePending)
            return;

        Dictionary<Guid, List<string>>? bySaga = null;

        // Existing destination-side markers for the moved keys.
        if (haveMarkers)
        {
            foreach (var key in movedKeys)
            {
                if (_shadowedSagas!.TryGetValue(key, out var sagas))
                    foreach (var txid in sagas)
                        AddSagaKeyMarker(ref bySaga, txid, key);
            }
        }

        // Locally prepared, not-yet-terminal sagas whose bucket still holds a
        // moved key - the isolation that a same-shard prepare relies on, which
        // has no explicit marker of its own.
        if (havePending)
        {
            var moved = movedKeys as HashSet<string>
                ?? new HashSet<string>(movedKeys, StringComparer.Ordinal);
            foreach (var (txid, bucket) in _pendingTx!)
            {
                foreach (var key in bucket.Keys)
                {
                    if (moved.Contains(key))
                        AddSagaKeyMarker(ref bySaga, txid, key);
                }
            }
        }

        if (bySaga is null)
            return;

        foreach (var (txid, keys) in bySaga)
            await sibling.MarkSagaShadowAsync(txid, keys);
    }

    private static void AddSagaKeyMarker(
        ref Dictionary<Guid, List<string>>? bySaga,
        Guid txid,
        string key)
    {
        bySaga ??= new Dictionary<Guid, List<string>>();
        if (!bySaga.TryGetValue(txid, out var list))
        {
            list = new List<string>();
            bySaga[txid] = list;
        }
        if (!list.Contains(key))
            list.Add(key);
    }
}
