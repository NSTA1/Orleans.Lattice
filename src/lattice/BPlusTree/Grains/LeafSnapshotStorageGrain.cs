using System.Globalization;
using System.Text;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILeafSnapshotStorageGrain"/> implementation.
/// Holds a single persisted <see cref="LeafSnapshotBlob"/> per leaf
/// via the lattice storage provider configured by
/// <see cref="LatticeOptions.StorageProviderName"/>.
/// <para>
/// The implementation is intentionally minimal: one read, one write,
/// one clear. No projection-side logic lives here; capture and
/// rehydrate logic is owned by <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/> and the
/// maintenance grain that schedules captures.
/// </para>
/// </summary>
internal sealed class LeafSnapshotStorageGrain(
    IGrainContext context,
    [PersistentState("leaf-snapshot", LatticeOptions.StorageProviderName)]
    IPersistentState<LeafSnapshotBlob> state,
    IGrainFactory? grainFactory = null,
    IOptionsMonitor<LatticeOptions>? options = null) : ILeafSnapshotStorageGrain, IGrainBase
{
    /// <summary>
    /// Per-row allowance covering the frame's index entry and the row's own
    /// length and discriminator fields. Deliberately generous: over-reserving
    /// costs one extra segment, while under-reserving lets a run encode above
    /// the window, which is the failure the window exists to prevent.
    /// </summary>
    private const long PerRowOverheadBytes = LeafSnapshotSegmentPlan.PerRowOverheadBytes;

    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>
    /// <see langword="true"/> when this activation can address segment grains.
    /// <para>
    /// False only when the grain was constructed directly with the two-argument
    /// shape, which is a unit-test affordance rather than a deployment: under
    /// the silo both dependencies are registered and always injected. A grain
    /// built without a factory persists inline regardless of size, which is
    /// byte-for-byte the behaviour that existed before segmentation, so the
    /// fixtures that construct it that way are unaffected. It is stated here
    /// rather than left implicit because "segmentation silently never happens"
    /// is exactly the condition that would let a test asserting segmentation
    /// pass for the wrong reason - the segmentation fixtures therefore supply a
    /// factory and assert a non-zero segment count rather than only asserting a
    /// successful round-trip.
    /// </para>
    /// </summary>
    private bool CanSegment => grainFactory is not null;

    /// <summary>
    /// Generation a staged capture is writing frames into, or <c>-1</c> when no
    /// capture is staging. Activation-scoped on purpose: a capture whose
    /// activation is lost mid-flight leaves only frames no manifest references,
    /// which are inert, and the next capture starts afresh. The alternative -
    /// persisting the staging cursor - would buy nothing, since a torn capture
    /// has to be redone either way.
    /// </summary>
    private int stagingGeneration = -1;

    private int stagedSegmentCount;

    private long stagedFrameBytes;

    /// <summary>
    /// Segment window in bytes, clamped to
    /// <see cref="LatticeOptions.MinimumLeafSnapshotSegmentBytes"/>.
    /// <para>
    /// Read from the unnamed (global) options rather than a per-tree named
    /// instance, because a snapshot storage grain is addressed by the owning
    /// leaf's Guid and carries no tree name to resolve one with. That is a real
    /// limitation and is stated rather than hidden: a per-tree override of
    /// <see cref="LatticeOptions.LeafSnapshotSegmentBytes"/> does not reach
    /// this grain, so the window must be set globally to take effect. It is an
    /// acceptable one because the value bounds a process-wide allocation
    /// property (the largest contiguous array a hydration will demand), which
    /// is a property of the host rather than of a tree.
    /// </para>
    /// </summary>
    private long SegmentWindowBytes => LeafSnapshotSegmentPlan.Window(
        options?.CurrentValue.LeafSnapshotSegmentBytes ?? LatticeOptions.DefaultLeafSnapshotSegmentBytes);

    /// <summary>
    /// Grain key of segment <paramref name="index"/> in generation
    /// <paramref name="generation"/> for this leaf: <c>{leafKey}/{index}</c> at
    /// generation zero, and <c>{leafKey}/g{generation}/{index}</c> above it.
    /// <para>
    /// Derived from the raw grain key rather than from a parsed Guid so the
    /// mapping holds for any key representation the runtime hands back, and so
    /// a key that does not parse as a Guid degrades to a distinct-but-valid
    /// segment address instead of silently collapsing every leaf onto
    /// <see cref="Guid.Empty"/>.
    /// </para>
    /// <para>
    /// Generation zero deliberately keeps the pre-generation address form, so a
    /// snapshot segmented by an earlier build stays addressable and no
    /// migration pass is needed. The <c>g</c> marker keeps the two forms
    /// unambiguous: a leaf key ending in a digit cannot make
    /// <c>{leafKey}/{index}</c> collide with a generation-qualified address.
    /// </para>
    /// </summary>
    private string SegmentKey(int generation, int index)
        => generation == 0
            ? string.Create(CultureInfo.InvariantCulture, $"{context.GrainId.Key}/{index}")
            : string.Create(CultureInfo.InvariantCulture, $"{context.GrainId.Key}/g{generation}/{index}");

    private ILeafSnapshotSegmentGrain Segment(int generation, int index)
        => grainFactory!.GetGrain<ILeafSnapshotSegmentGrain>(SegmentKey(generation, index));

    /// <summary>
    /// True when <paramref name="blob"/> is a snapshot a leaf can actually
    /// rehydrate from: it carries a durably-captured prefix <b>and</b> its row
    /// payload reads back in full. A truncated or corrupt row payload - in
    /// either encoding - must present as "no snapshot", never as a snapshot
    /// with fewer rows. A blob reporting coverage it cannot reproduce would let
    /// the coverage-gated WAL GC trim the last durable copy of that prefix, so
    /// this is a fail-closed gate rather than a nicety.
    /// </summary>
    private static bool HasUsableSnapshot(LeafSnapshotBlob blob)
        => HasCapturedPrefix(blob) && blob.ValidateRowPayload();

    /// <summary>
    /// True when <paramref name="blob"/> carries a durably-captured prefix that
    /// a leaf can rehydrate from. The scalar <see cref="LeafSnapshotBlob.SnapshotOffset"/>
    /// only describes partition 0; under the default <c>WalPartitions = 8</c> a
    /// leaf whose live keys hash entirely to a non-zero partition captures a
    /// blob whose scalar offset is the <c>-1</c> "partition 0 idle" sentinel yet
    /// whose <see cref="LeafSnapshotBlob.SnapshotOffsetsByPartition"/> covers the
    /// busy partition. Keying the load/clear guards on the scalar alone would
    /// discard that blob on cold restart - and because the coverage-gated WAL GC
    /// has already trimmed the busy partition's covered prefix, discarding the
    /// sole durable copy silently loses it. Treat a blob as captured when the
    /// scalar is non-negative OR any per-partition slot is. Legacy blobs
    /// (persisted before the per-partition slot existed) decode
    /// <see cref="LeafSnapshotBlob.SnapshotOffsetsByPartition"/> as <c>null</c>
    /// and so fall back to the scalar-only check, exactly as before.
    /// </summary>
    private static bool HasCapturedPrefix(LeafSnapshotBlob blob)
    {
        if (blob.ScalarOffsetOrSentinel() >= 0)
        {
            return true;
        }

        var perPartition = blob.SnapshotOffsetsByPartition;
        if (perPartition is not null)
        {
            foreach (var offset in perPartition)
            {
                if (offset >= 0)
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <inheritdoc />
    public async Task SaveAsync(LeafSnapshotBlob blob, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(blob);
        cancellationToken.ThrowIfCancellationRequested();

        // Coverage-monotonicity invariant. The durable blob is what authorises
        // the coverage-gated WAL GC trim floor (a leaf reports its durable pin
        // as min(checkpoint, per-partition covered offset) from
        // BPlusLeafGrain.ResolveDurablePinForPartition), and once the GC has
        // trimmed a partition's [0, N] prefix the ONLY durable recovery of that
        // prefix is a snapshot that still covers >= N. A blind last-writer-wins
        // overwrite lets a later capture - one whose recomputed per-partition
        // coverage REGRESSED below an earlier blob (a partition checkpoint
        // lowered by a rehydrate reset or a projection rebuild, then recomputed
        // from current state on the next capture) - shrink the durable coverage
        // below the offset the earlier blob already authorised the GC to trim to.
        // The in-memory monotonic-max pin (RecordDurableSnapshotCoverage) cannot
        // regress, so the trim outlives the durable coverage that justified it,
        // and the next cold restart rehydrates from the under-covering blob,
        // advances the partition checkpoint only to the lower offset, and the
        // tail replay finds the WAL trimmed past checkpoint + 1
        // (LeafProjectionStaleException - "fall off the log"). Merge the incoming
        // blob with the stored one so per-partition coverage is monotonic
        // non-decreasing and the retained rows back the higher coverage; the
        // rehydrate path already relies on exactly this ("Coverage is monotonic
        // and we always load the latest blob" in TryRehydrateFromSnapshotAsync).
        //
        // Refuse an incoming blob whose own row payload does not read back:
        // overwriting a good durable snapshot with an unreadable one would
        // strand the coverage it already authorised the GC to trim to. An
        // unreadable incoming blob can only be a caller bug, and dropping the
        // write leaves the last known-good snapshot in place for the next
        // capture to supersede.
        if (!blob.ValidateRowPayload())
        {
            return;
        }

        var previousSegmentCount = state.State.SegmentCount;
        var merged = MergeMonotone(state.State, blob);
        if (!ReferenceEquals(merged, state.State))
        {
            await PersistAsync(merged, previousSegmentCount, cancellationToken).ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Persists <paramref name="merged"/>, splitting its row payload across
    /// segment grains when the encoded frame exceeds
    /// <see cref="SegmentWindowBytes"/>.
    /// <para>
    /// Write order is load-bearing and is the whole safety argument for
    /// segmentation. Segments are written first and the manifest last, so the
    /// manifest is the commit point: a capture that dies part-way leaves the
    /// previous manifest in place, still referencing the previous snapshot's
    /// segments, which have not been touched. A torn capture therefore
    /// degrades to "the older snapshot is still authoritative" rather than to a
    /// manifest pointing at segments that never landed - which would report
    /// coverage the snapshot cannot reproduce and let the coverage-gated WAL GC
    /// trim the sole durable copy of that prefix.
    /// </para>
    /// <para>
    /// Surplus segments from a longer previous snapshot are retired only AFTER
    /// the new manifest commits, because until then the old manifest still
    /// references them.
    /// </para>
    /// </summary>
    private async Task PersistAsync(LeafSnapshotBlob merged, int previousSegmentCount, CancellationToken cancellationToken)
    {
        var window = SegmentWindowBytes;
        var inlineFrame = merged.EncodedRows;
        var previousGeneration = state.State.SegmentGeneration;

        if (!CanSegment || inlineFrame is not { Length: > 0 } || inlineFrame.LongLength <= window)
        {
            merged.SegmentCount = 0;
            merged.SegmentFrameBytes = 0;
            merged.SegmentGeneration = previousGeneration;
            state.State = merged;
            await state.WriteStateAsync().ConfigureAwait(true);
            await RetireSegmentsAsync(previousGeneration, 0, previousSegmentCount, cancellationToken).ConfigureAwait(true);
            return;
        }

        // Stage into the NEXT generation, so nothing the live manifest
        // references is touched before the new manifest commits. Writing into
        // the live generation would leave a torn capture's manifest pointing at
        // a mixture of rewritten and untouched segments.
        var generation = previousGeneration + 1;
        var runs = PlanSegments(merged, window);
        long totalFrameBytes = 0;
        for (var i = 0; i < runs.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var frame = LeafSnapshotCodec.Encode(runs[i]);
            totalFrameBytes += frame.LongLength;
            await Segment(generation, i).SaveAsync(frame, runs[i].Length, cancellationToken).ConfigureAwait(true);
        }

        // The manifest keeps the coverage and carries no rows: the segments are
        // the only copy of the row payload now.
        merged.Rows = Array.Empty<LeafSnapshotRow>();
        merged.EncodedRows = null;
        merged.SegmentCount = runs.Count;
        merged.SegmentFrameBytes = totalFrameBytes;
        merged.SegmentGeneration = generation;
        state.State = merged;
        await state.WriteStateAsync().ConfigureAwait(true);

        // Retire the whole previous generation only now the new manifest has
        // committed: until this write landed, the old manifest was still the
        // authoritative one and still referenced every one of them.
        await RetireSegmentsAsync(previousGeneration, 0, previousSegmentCount, cancellationToken).ConfigureAwait(true);
    }

    /// <summary>
    /// Splits <paramref name="merged"/>'s rows into contiguous row-aligned runs
    /// whose encoded frames are each intended to fit inside
    /// <paramref name="window"/>.
    /// <para>
    /// Runs are built greedily from each row's own payload cost rather than by
    /// dividing the row count evenly, because rows vary in size by orders of
    /// magnitude and an even split would size every segment by the average
    /// while the peak is what matters. Ascending key order is preserved: each
    /// run is a contiguous slice of an already-ordered sequence, which is what
    /// the frame's index table requires for a key-range seek to mean anything.
    /// </para>
    /// <para>
    /// Honest limit: a single row whose own payload exceeds the window still
    /// encodes into a frame above it. Splitting one row across segments would
    /// mean a partial row is not independently decodable, which is the property
    /// the whole design rests on, so the row wins and the window yields.
    /// </para>
    /// </summary>
    private static List<LeafSnapshotRow[]> PlanSegments(LeafSnapshotBlob merged, long window)
    {
        // Leave headroom for the frame header and index table, which scale with
        // the run rather than with any one row.
        var budget = LeafSnapshotSegmentPlan.Budget(window);
        var runs = new List<LeafSnapshotRow[]>();
        var current = new List<LeafSnapshotRow>();
        long currentCost = 0;

        foreach (var row in merged.EnumerateRows())
        {
            var rowCost = LeafSnapshotSegmentPlan.RowCost(row);

            if (LeafSnapshotSegmentPlan.MustCloseRun(current.Count, currentCost, rowCost, budget))
            {
                runs.Add(current.ToArray());
                current.Clear();
                currentCost = 0;
            }

            current.Add(row);
            currentCost += rowCost;
        }

        if (current.Count > 0)
        {
            runs.Add(current.ToArray());
        }

        return runs;
    }

    /// <summary>
    /// Clears segments <paramref name="keepCount"/> through
    /// <paramref name="previousCount"/> - 1 of generation
    /// <paramref name="generation"/>, which the newly committed manifest no
    /// longer references. Best-effort: an orphaned segment wastes a row but
    /// is unreachable, so a failure here must not fail the capture that has
    /// already durably committed.
    /// <para>
    /// A segmented capture retires the whole previous generation
    /// (<paramref name="keepCount"/> zero), because the new manifest addresses
    /// a different generation entirely and keeps none of the old segments.
    /// </para>
    /// </summary>
    private async Task RetireSegmentsAsync(int generation, int keepCount, int previousCount, CancellationToken cancellationToken)
    {
        for (var i = keepCount; i < previousCount; i++)
        {
            if (!CanSegment)
            {
                break;
            }

            try
            {
                await Segment(generation, i).ClearAsync(cancellationToken).ConfigureAwait(true);
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    /// <inheritdoc />
    public async Task<byte[]?> LoadSegmentFrameAsync(int index, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!CanSegment || index < 0 || index >= state.State.SegmentCount)
        {
            return null;
        }

        return await Segment(state.State.SegmentGeneration, index)
            .LoadFrameAsync(cancellationToken)
            .ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task<int> StageSnapshotSegmentAsync(byte[] frame, int rowCount, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(frame);
        cancellationToken.ThrowIfCancellationRequested();

        if (frame.Length == 0)
        {
            throw new ArgumentException("A staged segment frame must be non-empty.", nameof(frame));
        }

        if (rowCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(rowCount), rowCount, "A staged segment frame must encode at least one row.");
        }

        if (!CanSegment)
        {
            throw new InvalidOperationException(
                "Segment staging requires a grain factory. A host without one must capture through SaveAsync instead.");
        }

        if (stagingGeneration < 0)
        {
            // One generation above the live manifest's, so every address this
            // capture writes is one the live manifest does not reference.
            stagingGeneration = state.State.SegmentGeneration + 1;
            stagedSegmentCount = 0;
            stagedFrameBytes = 0;
        }

        var index = stagedSegmentCount;
        await Segment(stagingGeneration, index).SaveAsync(frame, rowCount, cancellationToken).ConfigureAwait(true);
        stagedSegmentCount = index + 1;
        stagedFrameBytes += frame.LongLength;
        return index;
    }

    /// <inheritdoc />
    public async Task<bool> CommitStagedSnapshotAsync(LeafSnapshotBlob manifest, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        cancellationToken.ThrowIfCancellationRequested();

        if (stagingGeneration < 0 || stagedSegmentCount == 0)
        {
            throw new InvalidOperationException(
                "No staged segments to commit. Stage at least one frame with StageSnapshotSegmentAsync first.");
        }

        if (manifest.Rows.Count > 0 || manifest.EncodedRows is { Length: > 0 })
        {
            throw new ArgumentException(
                "A staged manifest carries coverage only; its rows live in the staged segments.",
                nameof(manifest));
        }

        var generation = stagingGeneration;
        var segmentCount = stagedSegmentCount;
        var frameBytes = stagedFrameBytes;
        stagingGeneration = -1;
        stagedSegmentCount = 0;
        stagedFrameBytes = 0;

        // The staged snapshot's rows live in segments, so the row-merging slow
        // path is unavailable to it for exactly the reason MergeMonotone
        // declines a segmented stored blob: there are no inline rows to merge,
        // and loading every segment to merge them would reinstate the
        // whole-snapshot residency segmentation exists to remove. Accept when
        // coverage does not regress, decline otherwise.
        var previousSegmentCount = state.State.SegmentCount;
        var previousGeneration = state.State.SegmentGeneration;
        if (HasUsableSnapshot(state.State) && RegressesCoverage(state.State, manifest))
        {
            await RetireSegmentsAsync(generation, 0, segmentCount, cancellationToken).ConfigureAwait(true);
            return false;
        }

        manifest.Rows = Array.Empty<LeafSnapshotRow>();
        manifest.EncodedRows = null;
        manifest.SegmentCount = segmentCount;
        manifest.SegmentFrameBytes = frameBytes;
        manifest.SegmentGeneration = generation;

        // Manifest last: until this write lands the previous snapshot is still
        // the authoritative one, and the frames backing it are untouched.
        state.State = manifest;
        await state.WriteStateAsync().ConfigureAwait(true);

        await RetireSegmentsAsync(previousGeneration, 0, previousSegmentCount, cancellationToken).ConfigureAwait(true);
        return true;
    }

    /// <summary>
    /// Whether <paramref name="incoming"/> covers any partition less far than
    /// <paramref name="existing"/> does. Shared by the inline merge and the
    /// staged commit so both admit exactly the same captures.
    /// </summary>
    private static bool RegressesCoverage(LeafSnapshotBlob existing, LeafSnapshotBlob incoming)
    {
        var slots = Math.Max(
            Math.Max(EffectiveLength(existing), EffectiveLength(incoming)),
            1);

        for (var p = 0; p < slots; p++)
        {
            if (EffectiveOffset(existing, p) > EffectiveOffset(incoming, p))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Returns a blob whose per-partition coverage is the element-wise maximum of
    /// <paramref name="existing"/> and <paramref name="incoming"/>, backed by the
    /// last-writer-wins union of both row sets so the retained higher coverage is
    /// always row-backed. The common case (the incoming capture advances every
    /// partition, or there is no prior durable prefix) returns
    /// <paramref name="incoming"/> verbatim so a normal save stays a plain
    /// overwrite; the row-merging slow path runs only when a partition would
    /// otherwise regress.
    /// </summary>
    private static LeafSnapshotBlob MergeMonotone(LeafSnapshotBlob existing, LeafSnapshotBlob incoming)
    {
        // No durable prefix yet (first capture, or post-clear), or a stored
        // blob whose row payload no longer reads back: the incoming blob
        // is authoritative verbatim. Preserves the exact first-save contract the
        // capture round-trip tests and the byte-size lazy back-fill rely on, and
        // makes a corrupt stored blob heal on the next capture instead of
        // poisoning the merge.
        if (!HasUsableSnapshot(existing))
        {
            return incoming;
        }

        var slots = Math.Max(
            Math.Max(EffectiveLength(existing), EffectiveLength(incoming)),
            1);

        // Fast path: the incoming capture covers every partition at least as far
        // as the stored blob, so its projection is a superset and a plain
        // overwrite cannot regress coverage. This is the steady-state case
        // (captures normally advance), so it stays allocation-free beyond the
        // pre-existing overwrite.
        if (!RegressesCoverage(existing, incoming))
        {
            return incoming;
        }

        // A segmented stored blob carries NO inline rows - the segments are the
        // only copy - so the row-merging slow path below would enumerate an
        // empty row set and produce a blob retaining the higher coverage with
        // none of the rows backing it. That is precisely the shape that lets
        // the coverage-gated WAL GC trim the last durable copy of a prefix, so
        // decline instead and keep the stored snapshot verbatim: the incoming
        // capture is dropped, and the next one supersedes it.
        //
        // Loading every segment to merge them would reintroduce the
        // whole-snapshot residency that segmentation exists to remove, so the
        // conservative answer is also the one that keeps the bound.
        //
        // Only `existing` can be segmented here: segmentation happens in
        // PersistAsync, after this merge, so `incoming` is always inline.
        if (existing.IsSegmented())
        {
            return existing;
        }

        // Slow path: the incoming capture would lower coverage for at least one
        // partition. Take the element-wise Math.Max of the per-partition coverage
        // and LWW-merge the two row sets (a CRDT join that cannot lose data), so
        // the partition whose coverage is retained from the stored blob keeps the
        // rows that back it while any partition the incoming blob advanced keeps
        // the fresher rows.
        var mergedOffsets = new long[slots];
        for (var p = 0; p < slots; p++)
        {
            mergedOffsets[p] = Math.Max(EffectiveOffset(existing, p), EffectiveOffset(incoming, p));
        }

        // Ordinal-sorted so the merged row set carries the same ascending key
        // order a capture produces, which is what the binary frame's index
        // table is required to be in for a key-range seek to be meaningful.
        var mergedRows = new SortedDictionary<string, LeafSnapshotRow>(StringComparer.Ordinal);
        foreach (var row in existing.EnumerateRows())
        {
            mergedRows[row.Key] = row;
        }
        foreach (var row in incoming.EnumerateRows())
        {
            if (mergedRows.TryGetValue(row.Key, out var prior))
            {
                // LWW.Merge returns one of its two arguments verbatim, so the
                // winning row is the one whose value the merge kept - preserving
                // that row's per-key MergeMode discriminator alongside its value.
                var winner = LwwValue<byte[]>.Merge(prior.Value, row.Value);
                mergedRows[row.Key] = EqualityComparer<LwwValue<byte[]>>.Default.Equals(winner, row.Value)
                    ? row
                    : prior;
            }
            else
            {
                mergedRows[row.Key] = row;
            }
        }

        var rows = new LeafSnapshotRow[mergedRows.Count];
        var index = 0;
        foreach (var row in mergedRows.Values)
        {
            rows[index++] = row;
        }

        // Preserve the incoming capture's encoding so a merge never silently
        // downgrades a frame-encoded blob back to the legacy row graph (nor
        // upgrades one while the write-side switch is off - the switch is what
        // decided the incoming shape).
        var encodeBinary = incoming.HasBinaryRowPayload();

        return new LeafSnapshotBlob
        {
            SnapshotOffset = LeafSnapshotBlob.NormalizeScalarOffset(mergedOffsets[0]),
            Rows = encodeBinary ? Array.Empty<LeafSnapshotRow>() : rows,
            EncodedRows = encodeBinary ? LeafSnapshotCodec.Encode(rows) : null,
            CapturedAtTicks = Math.Max(existing.CapturedAtTicks, incoming.CapturedAtTicks),
            // The row set changed, so the incoming blob's precomputed footprint no
            // longer describes it. Leave the slot at 0 so GetSnapshotByteSizeAsync
            // lazily recomputes and caches the correct total from the merged rows.
            SnapshotBytes = 0L,
            SnapshotOffsetsByPartition = mergedOffsets,
        };
    }

    /// <summary>
    /// Effective per-partition coverage array length for <paramref name="blob"/>:
    /// the explicit per-partition array length, or <c>1</c> for a legacy blob that
    /// carries only the scalar partition-0 offset.
    /// </summary>
    private static int EffectiveLength(LeafSnapshotBlob blob)
        => blob.SnapshotOffsetsByPartition is { Length: > 0 } perPartition ? perPartition.Length : 1;

    /// <summary>
    /// Effective covered offset of partition <paramref name="partition"/> for
    /// <paramref name="blob"/>, folding the legacy scalar-only shape (a
    /// <see langword="null"/> per-partition array covers only partition 0 at the
    /// scalar <see cref="LeafSnapshotBlob.SnapshotOffset"/>) into the same view as
    /// an explicit per-partition array.
    /// </summary>
    private static long EffectiveOffset(LeafSnapshotBlob blob, int partition)
    {
        var perPartition = blob.SnapshotOffsetsByPartition;
        if (perPartition is not null && partition < perPartition.Length)
        {
            return perPartition[partition];
        }
        return partition == 0 ? blob.ScalarOffsetOrSentinel() : -1L;
    }

    /// <inheritdoc />
    public Task<LeafSnapshotBlob?> LoadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // A null (or legacy -1) SnapshotOffset is the "nothing captured" reading,
        // but the scalar only describes partition 0.
        // A blob captured for a leaf whose live data is in a non-zero partition
        // (partition 0 idle) carries a -1 scalar yet a >= 0 per-partition slot;
        // it is loadable and MUST NOT be discarded (see HasCapturedPrefix). A
        // blob whose row payload does not read back is reported as absent, so
        // the caller falls through to WAL replay rather than treating an
        // unreadable snapshot as coverage.
        // Returns state.State BY REFERENCE, not a projection of it. That is an
        // invariant later changes depend on and it is invisible at the call
        // site, so: the deep copy Orleans performs on this response is what
        // isolates the caller from the grain's live persisted state. The blob's
        // payload members are marked [Immutable] so the bulk is shared rather
        // than reallocated (issue #2481 - the second contiguous copy is what
        // exhausts the heap on a large leaf), but the SHELL is still copied,
        // deliberately, and must stay that way. Do not mark the blob type
        // [Immutable] or wrap this return in Immutable<T>: either would alias a
        // caller to state this grain still writes to, since
        // GetSnapshotByteSizeAsync back-fills SnapshotBytes on it below.
        if (!HasUsableSnapshot(state.State))
        {
            return Task.FromResult<LeafSnapshotBlob?>(null);
        }

        return Task.FromResult<LeafSnapshotBlob?>(state.State);
    }

    /// <inheritdoc />
    public Task<long> GetSnapshotByteSizeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!HasUsableSnapshot(state.State))
        {
            return Task.FromResult(0L);
        }

        // O(1) field read for blobs persisted with the precomputed byte
        // total. Legacy blobs (persisted before the SnapshotBytes slot
        // existed) decode the slot as 0; recompute once from the rows and
        // cache the answer on the in-memory state (no WriteStateAsync) so
        // a subsequent reactivation reading the legacy blob picks the
        // same value back up on first read and the next foreground
        // capture-overwrite stamps the slot durably.
        if (state.State.SnapshotBytes > 0 || state.State.GetRowCount() == 0)
        {
            // A segmented manifest holds no inline rows, so GetRowCount() is 0
            // and SnapshotBytes was never stamped. Report the manifest's
            // recorded segment total rather than reading any segment: reading
            // them to measure them would reintroduce exactly the whole-payload
            // residency segmentation exists to remove, and this is a reporting
            // call, not a correctness one.
            if (state.State.SnapshotBytes == 0 && state.State.IsSegmented())
            {
                return Task.FromResult(state.State.SegmentFrameBytes);
            }

            return Task.FromResult(state.State.SnapshotBytes);
        }

        // A binary frame carries every length it needs inline, so the total is
        // summed by walking the frame without materialising a single key string
        // or value array.
        if (state.State.EncodedRows is { Length: > 0 } frame
            && LeafSnapshotCodec.TryComputeStateBytes(frame, out var framedBytes))
        {
            state.State.SnapshotBytes = framedBytes;
            return Task.FromResult(framedBytes);
        }

        long bytes = 0;
        foreach (var row in state.State.EnumerateRows())
        {
            bytes += System.Text.Encoding.UTF8.GetByteCount(row.Key)
                + (row.Value.IsTombstone ? 0 : (row.Value.Value?.Length ?? 0));
        }
        state.State.SnapshotBytes = bytes;
        return Task.FromResult(bytes);
    }

    /// <inheritdoc />
    public async Task ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!HasCapturedPrefix(state.State))
        {
            // Nothing to clear; ClearStateAsync still touches the
            // provider, so short-circuit to keep idempotent calls
            // I/O-free.
            return;
        }

        var segmentCount = state.State.SegmentCount;
        var segmentGeneration = state.State.SegmentGeneration;
        await state.ClearStateAsync().ConfigureAwait(true);

        // Retire segments AFTER the manifest clear, never before. An orphaned
        // segment is harmless (unreachable, wastes a row); a live manifest
        // pointing at emptied segments is not - it reports coverage it cannot
        // reproduce.
        await RetireSegmentsAsync(segmentGeneration, 0, segmentCount, cancellationToken).ConfigureAwait(true);

        // After ClearStateAsync the in-memory state is reset by the
        // provider; defensively re-seed the sentinel so LoadAsync's
        // null contract holds without relying on the provider's
        // post-clear state shape.
        state.State = new LeafSnapshotBlob();
    }
}
