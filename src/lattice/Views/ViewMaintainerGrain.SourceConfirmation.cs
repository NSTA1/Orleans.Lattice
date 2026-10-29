using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Views;

/// <summary>
/// Source confirmation of a completed atomic batch before it is flushed. The
/// staged-key supersession record only sees an ordinary write read <i>after</i> the
/// prepare was staged, and a range delete only through its recorded matched keys.
/// Neither ordering is guaranteed: replication appends entries in arrival order,
/// not HLC order, so a newer ordinary write can be logged (and applied to the view)
/// before an older prepare for the same key; and an unconstrained range delete
/// records only its bounds, not which keys it tombstoned. In both cases the source
/// keeps the newer value by last-writer-wins when the batch commits, but the view
/// would install the older prepare over it.
/// <para>
/// Before flushing, the maintainer therefore reads the batch's last-writer-wins
/// prepared keys back from the source. A key that still holds its prepared revision
/// flushes normally. A strictly newer stored revision proves supersession, but the
/// superseding write may not yet have reached the view: the batch is deferred,
/// whole, until the drain has consumed every WAL offset below the per-partition
/// heads sampled after that read (so every write the read observed has been
/// applied), and only then is the key dropped from the flush. A batch is never
/// flushed with a key missing whose replacement the view has not yet applied, so
/// no reader observes a partial batch or a value the source never held.
/// An absent, older, or same-version nonmatching row provides no such proof and
/// keeps the batch staged, even after its terminal WAL horizon is consumed.
/// </para>
/// </summary>
internal sealed partial class ViewMaintainerGrain
{
    /// <summary>
    /// Confirms every completed batch in <paramref name="completed"/> against the
    /// source and removes the ones that must wait, so neither the survivor guard nor
    /// the flush acts on them this pass. A deferred batch stays staged (holding the
    /// checkpoint back) and, still being complete, is re-seeded on the next pass.
    /// </summary>
    private async Task ConfirmCompletedBatchesAgainstSourceAsync(
        List<Guid> completed,
        string walTreeId,
        int partitions,
        IReadOnlyDictionary<int, long> consumed,
        CancellationToken cancellationToken)
    {
        for (var i = completed.Count - 1; i >= 0; i--)
        {
            if (_staging.TryGetValue(completed[i], out var tx)
                && !await ConfirmStagedBatchAgainstSourceAsync(walTreeId, partitions, tx, consumed, cancellationToken))
            {
                completed.RemoveAt(i);
            }
        }
    }

    /// <summary>
    /// <see langword="true"/> when source confirmation established that
    /// <paramref name="prepared"/>'s key holds a strictly newer source revision.
    /// </summary>
    private static bool IsSourceSuperseded(StagedTransaction tx, in LatticeMutation prepared)
        => tx.SourceSupersededKeys?.Contains(prepared.Key) == true;

    /// <summary>
    /// Merges the pass's starting read checkpoints with the offsets it advanced to:
    /// the highest WAL offset, per partition, the maintainer has consumed.
    /// </summary>
    private static Dictionary<int, long> ConsumedThrough(
        IReadOnlyDictionary<int, long> readFrom,
        IReadOnlyDictionary<int, long> advanced)
    {
        var consumed = new Dictionary<int, long>(readFrom);
        foreach (var (partition, offset) in advanced)
        {
            if (offset > consumed.GetValueOrDefault(partition, -1))
            {
                consumed[partition] = offset;
            }
        }

        return consumed;
    }

    /// <summary>
    /// <see langword="true"/> when <paramref name="tx"/> may flush now, with any key
    /// the source provably superseded at a newer revision recorded in
    /// <see cref="StagedTransaction.SourceSupersededKeys"/>; <see langword="false"/>
    /// to defer the whole batch to a later pass. Only last-writer-wins prepares are
    /// checked: a CRDT delta or merge is folded rather than replaced. A key another
    /// staged batch also prepares is checked too:
    /// <see cref="RecordFlushedPreparesOverStagedKeys"/> orders overlapping batches
    /// against each other, but neither batch records an ordinary write logged before
    /// both prepares, and with independently ordered HLCs that write can be newer
    /// than both.
    /// </summary>
    private async Task<bool> ConfirmStagedBatchAgainstSourceAsync(
        string walTreeId,
        int partitions,
        StagedTransaction tx,
        IReadOnlyDictionary<int, long> consumed,
        CancellationToken cancellationToken)
    {
        List<string>? keys = null;
        Dictionary<string, (HybridLogicalClock Timestamp, bool IsDeletion, byte[]? Value)>? expected = null;
        foreach (var prepared in tx.PreparesByIndex.Values)
        {
            if (prepared.Delta is not null
                || prepared.IsMerge
                || IsSupersededByOrdinary(prepared)
                || tx.SourceSupersededKeys?.Contains(prepared.Key) == true)
            {
                continue;
            }

            expected ??= new(StringComparer.Ordinal);
            keys ??= [];
            if (expected.TryAdd(prepared.Key, (prepared.Timestamp, IsDeletion(prepared), prepared.Value)))
            {
                keys.Add(prepared.Key);
            }
        }

        if (keys is null)
        {
            ClearPendingSourceCheck(tx);
            return true;
        }

        var source = await ReadSourceAsync(walTreeId, keys,
            static (root, page) => root.GetVisibleRawEntriesAsync(page), cancellationToken);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        HashSet<string>? mismatched = null;
        var unconfirmed = false;
        foreach (var key in keys)
        {
            var held = source.GetValueOrDefault(key);
            var wanted = expected![key];
            var matches = held is { } entry
                && entry.Timestamp.CompareTo(wanted.Timestamp) == 0
                && (wanted.IsDeletion
                    ? entry.IsTombstone
                    : !entry.IsTombstone && !entry.ToLwwValue().IsExpired(nowTicks)
                        && entry.Value.AsSpan().SequenceEqual(wanted.Value));
            if (!matches)
            {
                if (held is not { } observed || observed.Timestamp.CompareTo(wanted.Timestamp) <= 0)
                {
                    unconfirmed = true;
                    continue;
                }

                (mismatched ??= new HashSet<string>(StringComparer.Ordinal)).Add(key);
            }
        }

        if (unconfirmed)
        {
            // A consumed WAL horizon cannot prove a missing or older row replaced
            // the prepare: source application may still be behind the decision.
            ClearPendingSourceCheck(tx);
            return false;
        }

        if (mismatched is null)
        {
            ClearPendingSourceCheck(tx);
            return true;
        }

        // Every key still mismatched was already mismatched when the pending heads
        // were sampled, and the drain has consumed every offset below them, so each
        // superseding write that read observed is already in the view.
        if (tx.PendingSourceKeys is { } pendingKeys
            && tx.PendingSourceHeads is { } heads
            && mismatched.IsSubsetOf(pendingKeys))
        {
            if (Covers(consumed, heads))
            {
                tx.SourceSupersededKeys ??= new HashSet<string>(StringComparer.Ordinal);
                tx.SourceSupersededKeys.UnionWith(mismatched);
                ClearPendingSourceCheck(tx);
                return true;
            }

            // Keep the earlier heads: re-sampling under sustained writes would push
            // them forward every pass and never let the drain catch up.
            tx.PendingSourceKeys = mismatched;
            return false;
        }

        tx.PendingSourceKeys = mismatched;
        tx.PendingSourceHeads = await SampleHeadOffsetsAsync(walTreeId, partitions, cancellationToken);
        return false;
    }

    private static bool IsDeletion(in LatticeMutation prepared)
        => prepared.IsTombstone || prepared.Kind is MutationKind.Delete or MutationKind.Tombstone;

    private static void ClearPendingSourceCheck(StagedTransaction tx)
    {
        tx.PendingSourceKeys = null;
        tx.PendingSourceHeads = null;
    }

    private static bool Covers(IReadOnlyDictionary<int, long> consumed, long[] heads)
    {
        for (var partition = 0; partition < heads.Length; partition++)
        {
            if (consumed.GetValueOrDefault(partition, -1) < heads[partition] - 1)
            {
                return false;
            }
        }

        return true;
    }

    private async Task<long[]> SampleHeadOffsetsAsync(string walTreeId, int partitions, CancellationToken cancellationToken)
    {
        var count = partitions > 0 ? partitions : 1;
        if (count == 1)
        {
            return [await commitLogReader.GetHeadOffsetAsync(walTreeId, 0, cancellationToken)];
        }

        // Sample only after the source comparison, and retain partition order:
        // the complete vector is the replay fence, not a simultaneous WAL cut.
        return await BoundedFanOut.RunAsync(
            count,
            BoundedFanOut.DefaultWidth,
            partition => commitLogReader.GetHeadOffsetAsync(walTreeId, partition, cancellationToken),
            cancellationToken);
    }
}