using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Views;

/// <summary>
/// Visibility gate for completed atomic batches whose omitted key relies on a write
/// the view has not yet applied. A batch omits a prepared key at flush time when a
/// newer write superseded it (a recorded ordinary write over the staged key, or a
/// source-confirmed mismatch). The view then shows the batch's other keys beside
/// whatever it currently holds for the omitted key, which is only a consistent cut
/// once the superseding write itself has been applied. Two cases break that:
/// <list type="bullet">
/// <item>the superseding ordinary write was itself dropped as superseded at the
/// source (issue #4885), so the newer source write that replaces it has not been
/// applied yet - the batch must wait until the drain consumes the WAL offsets
/// below the heads sampled when the drop was decided;</item>
/// <item>the replacement is a prepare of another staged batch that is not flushing
/// this pass (deferred or still incomplete) - the omitting batch must wait for it,
/// and batches flushing in the same pass flush newest-first so the replacement is
/// installed before the batch that omits it.</item>
/// </list>
/// </summary>
internal sealed partial class ViewMaintainerGrain
{
    // Source keys of staged prepares that had an ordinary write dropped as
    // superseded at the source, with the highest dropped HLC and the per-partition
    // WAL heads sampled after the version read that decided the drop.
    private readonly Dictionary<string, (HybridLogicalClock Hlc, long[] Heads)> _droppedSuperseders = new(StringComparer.Ordinal);

    /// <summary>
    /// Records, for every dropped write whose source key is staged, the dropped HLC
    /// and the WAL heads the replacement write is guaranteed to lie below.
    /// </summary>
    private async Task RecordDroppedSupersedersAsync(
        Dictionary<string, HybridLogicalClock>? droppedOverStaged,
        string walTreeId,
        int partitions,
        CancellationToken cancellationToken)
    {
        if (droppedOverStaged is null || droppedOverStaged.Count == 0)
        {
            return;
        }

        var heads = await SampleHeadOffsetsAsync(walTreeId, partitions, cancellationToken);
        foreach (var (key, hlc) in droppedOverStaged)
        {
            if (_droppedSuperseders.TryGetValue(key, out var existing))
            {
                var merged = (long[])existing.Heads.Clone();
                if (merged.Length < heads.Length)
                {
                    Array.Resize(ref merged, heads.Length);
                }

                for (var p = 0; p < heads.Length; p++)
                {
                    merged[p] = Math.Max(merged[p], heads[p]);
                }

                _droppedSuperseders[key] = (existing.Hlc.CompareTo(hlc) >= 0 ? existing.Hlc : hlc, merged);
            }
            else
            {
                _droppedSuperseders[key] = (hlc, heads);
            }
        }
    }

    /// <summary>
    /// Removes from <paramref name="completed"/> every batch that must not become
    /// visible yet (see the type remarks) and orders the rest newest-first by their
    /// highest prepare HLC. A removed batch stays staged and is re-seeded next pass.
    /// </summary>
    private void GateBatchesOnUnappliedReplacements(List<Guid> completed, IReadOnlyDictionary<int, long> consumed)
    {
        if (_droppedSuperseders.Count > 0)
        {
            List<string>? retired = null;
            foreach (var (key, entry) in _droppedSuperseders)
            {
                if (Covers(consumed, entry.Heads) || !_stagedSourceKeyRefCount.ContainsKey(key))
                {
                    (retired ??= []).Add(key);
                }
            }

            if (retired is not null)
            {
                foreach (var key in retired)
                {
                    _droppedSuperseders.Remove(key);
                }
            }
        }

        if (completed.Count == 0)
        {
            return;
        }

        for (var i = completed.Count - 1; i >= 0; i--)
        {
            if (_staging.TryGetValue(completed[i], out var tx) && WaitsOnDroppedSuperseder(tx))
            {
                completed.RemoveAt(i);
            }
        }

        // Deferral to a batch outside the flush set is transitive: a batch deferred
        // here can itself hold the replacement another completed batch omits.
        var flushing = new HashSet<Guid>(completed);
        bool changed;
        do
        {
            changed = false;
            for (var i = completed.Count - 1; i >= 0; i--)
            {
                if (_staging.TryGetValue(completed[i], out var tx) && WaitsOnNonFlushingBatch(tx, flushing))
                {
                    flushing.Remove(completed[i]);
                    completed.RemoveAt(i);
                    changed = true;
                }
            }
        }
        while (changed);

        if (completed.Count > 1)
        {
            completed.Sort((a, b) =>
            {
                var byHlc = MaxPrepareHlc(b).CompareTo(MaxPrepareHlc(a));
                return byHlc != 0 ? byHlc : a.CompareTo(b);
            });
        }
    }

    private bool WaitsOnDroppedSuperseder(StagedTransaction tx)
    {
        if (_droppedSuperseders.Count == 0)
        {
            return false;
        }

        foreach (var prepared in tx.PreparesByIndex.Values)
        {
            if (prepared.Delta is null
                && !prepared.IsMerge
                && _droppedSuperseders.TryGetValue(prepared.Key, out var entry)
                && entry.Hlc.CompareTo(prepared.Timestamp) > 0)
            {
                return true;
            }
        }

        return false;
    }

    private bool WaitsOnNonFlushingBatch(StagedTransaction tx, HashSet<Guid> flushing)
    {
        foreach (var prepared in tx.PreparesByIndex.Values)
        {
            if (prepared.Delta is not null
                || prepared.IsMerge
                || !(IsSupersededByOrdinary(prepared) || IsSourceSuperseded(tx, prepared)))
            {
                continue;
            }

            _ordinaryHlcOverStagedKey.TryGetValue(prepared.Key, out var ordinary);
            foreach (var (otherId, other) in _staging)
            {
                if (ReferenceEquals(other, tx) || flushing.Contains(otherId))
                {
                    continue;
                }

                foreach (var replacement in other.PreparesByIndex.Values)
                {
                    if (string.Equals(replacement.Key, prepared.Key, StringComparison.Ordinal)
                        && replacement.Timestamp.CompareTo(prepared.Timestamp) > 0
                        && replacement.Timestamp.CompareTo(ordinary) > 0)
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private HybridLogicalClock MaxPrepareHlc(Guid txId)
    {
        var max = HybridLogicalClock.Zero;
        if (_staging.TryGetValue(txId, out var tx))
        {
            foreach (var prepared in tx.PreparesByIndex.Values)
            {
                if (prepared.Timestamp.CompareTo(max) > 0)
                {
                    max = prepared.Timestamp;
                }
            }
        }

        return max;
    }
}