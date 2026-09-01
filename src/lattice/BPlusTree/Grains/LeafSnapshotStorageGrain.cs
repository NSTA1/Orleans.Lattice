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
    IPersistentState<LeafSnapshotBlob> state) : ILeafSnapshotStorageGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

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

        state.State = MergeMonotone(state.State, blob);
        await state.WriteStateAsync().ConfigureAwait(true);
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
        var regresses = false;
        for (var p = 0; p < slots; p++)
        {
            if (EffectiveOffset(existing, p) > EffectiveOffset(incoming, p))
            {
                regresses = true;
                break;
            }
        }
        if (!regresses)
        {
            return incoming;
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

        await state.ClearStateAsync().ConfigureAwait(true);

        // After ClearStateAsync the in-memory state is reset by the
        // provider; defensively re-seed the sentinel so LoadAsync's
        // null contract holds without relying on the provider's
        // post-clear state shape.
        state.State = new LeafSnapshotBlob();
    }
}
