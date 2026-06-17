using System.IO.Hashing;
using System.Text;
using static Orleans.Lattice.Views.AggregationRowCodec;

namespace Orleans.Lattice.Views;

/// <summary>
/// Folds <see cref="AggregationContribution"/>s into an aggregation view's
/// per-group accumulators and re-materialises each affected group's reduced
/// value under its bare group key, so view readers are oblivious to the internal
/// accumulator / inverse / membership rows (see <see cref="AggregationRowCodec"/>).
/// <para>
/// <b>Retraction.</b> Every contribution is applied as a read-before-write: the
/// source key's membership row records the group and value it last contributed,
/// so a <c>Set</c> retracts the prior contribution (even when it re-grouped) and
/// a delete retracts it outright, all without an unbounded multiset. <c>count</c>
/// and <c>sum</c> hold only a per-group running count + sum; <c>min</c>,
/// <c>max</c>, and <c>set-union</c> inherently need the full multiset and so keep
/// an inverse row of per-source-key contributions, optionally bounded
/// (<see cref="_maxGroupEntries"/>) to a top-K (min / max) or distinct sample
/// (set-union) for unbounded-cardinality groups.
/// </para>
/// <para>
/// <b>Crash idempotency.</b> WAL delivery is at-least-once and the maintainer
/// checkpoints once per drain batch, so a silo crash mid-drain replays the whole
/// batch. For <c>count</c> / <c>sum</c> the membership row and the affected
/// accumulator slot(s) are therefore flipped to their final byte-state together
/// in one all-or-nothing <see cref="IAggregationViewStore.SetManyAtomicAsync"/>,
/// keyed by a deterministic operation id derived from the contribution identity:
/// a replay either dedups the saga outright or recomputes a net-zero delta from
/// the already-advanced membership pointer, so the numeric accumulator can never
/// double-count. <c>min</c> / <c>max</c> / <c>set-union</c> mutate their inverse
/// map by <c>map[sourceKey]=entry</c> / <c>map.Remove(sourceKey)</c>, which is
/// already idempotent on replay, so they keep the simpler separate-write path.
/// </para> Each group is sharded into <see cref="_fanout"/> accumulators
/// hashed on the source key; a group's materialised value merges the shards. A
/// fanout of 1 is a single accumulator (identical result).
/// </para>
/// </summary>
internal sealed class AggregationApplier(
    IAggregationViewStore store,
    AggregationKind kind,
    int fanout,
    int maxGroupEntries,
    string operationEpoch)
{
    private readonly int _fanout = fanout < 1 ? 1 : fanout;
    private readonly int _maxGroupEntries = maxGroupEntries;
    private readonly string _operationEpoch = operationEpoch;

    private bool IsNumeric => kind is AggregationKind.Count or AggregationKind.Sum;

    /// <summary>Applies a single contribution and re-materialises every group it touches.</summary>
    public async Task ApplyAsync(AggregationContribution contribution, CancellationToken cancellationToken = default)
    {
        switch (contribution.Kind)
        {
            case AggregationContributionKind.Contribute:
                await ContributeAsync(contribution, cancellationToken);
                return;
            case AggregationContributionKind.Retract:
                await RetractAsync(contribution, cancellationToken);
                return;
            default:
                // RangeReconcile is resolved to a rebuild by the maintainer before
                // it ever reaches the applier.
                return;
        }
    }

    private Task ContributeAsync(AggregationContribution contribution, CancellationToken cancellationToken) =>
        IsNumeric
            ? ContributeNumericAsync(contribution, cancellationToken)
            : ContributeInverseAsync(contribution, cancellationToken);

    private Task RetractAsync(AggregationContribution contribution, CancellationToken cancellationToken) =>
        IsNumeric
            ? RetractNumericAsync(contribution, cancellationToken)
            : RetractInverseAsync(contribution.SourceKey, cancellationToken);

    // --- count / sum: crash-idempotent atomic membership + accumulator flip ---
    //
    // The membership row and the affected accumulator slot(s) are computed to
    // their FINAL byte-state in memory, then flipped together in one
    // all-or-nothing SetManyAtomicAsync keyed by a deterministic operationId
    // derived from the contribution identity (rebuild generation + source key +
    // source HLC). Because
    // the flip moves membership and accumulators as a unit, a mid-drain crash
    // plus a full-batch WAL replay is self-correcting:
    //   * if the flip committed, membership shows the NEW contribution, so a
    //     replay computes retract(new) + add(new) = a net-zero accumulator delta
    //     (and the deterministic operationId dedups the saga outright); and
    //   * if it did not commit, membership shows OLD and the first-time
    //     computation is reproduced exactly.
    // The non-atomic legacy path (increment, then write membership separately)
    // could replay an increment whose membership write was lost mid-crash, which
    // double-counted count/sum groups. min/max/set-union are unaffected (their
    // inverse-map mutation map[k]=entry / map.Remove(k) is already replay
    // idempotent), so they keep the simpler inverse path below.
    private async Task ContributeNumericAsync(AggregationContribution contribution, CancellationToken cancellationToken)
    {
        var sourceKey = contribution.SourceKey;
        var membershipKey = MembershipKey(sourceKey);
        var prior = await ReadMembershipAsync(membershipKey, cancellationToken);
        var newGroup = contribution.GroupKey;

        // Accumulate every touched slot's final row in memory. The old-group slot
        // and the new-group slot can be the same key (a same-group overwrite), so
        // a dictionary both de-duplicates the atomic batch and folds the retract
        // and add onto one row.
        var slots = new Dictionary<string, AccumulatorRow>(StringComparer.Ordinal);

        string? oldGroup = null;
        if (prior is { } old)
        {
            oldGroup = old.GroupKey;
            var oldKey = AccumulatorKey(old.GroupKey, Slot(sourceKey, _fanout));
            var current = await ReadAccumulatorAsync(oldKey, cancellationToken) ?? new AccumulatorRow(0, 0);
            slots[oldKey] = new AccumulatorRow(current.Count - 1, current.Sum - old.Numeric);
        }

        var newKey = AccumulatorKey(newGroup, Slot(sourceKey, _fanout));
        var baseRow = slots.TryGetValue(newKey, out var pending)
            ? pending
            : await ReadAccumulatorAsync(newKey, cancellationToken) ?? new AccumulatorRow(0, 0);
        slots[newKey] = new AccumulatorRow(baseRow.Count + 1, baseRow.Sum + contribution.Numeric);

        var entries = BuildSlotEntries(slots);
        entries.Add(new KeyValuePair<string, byte[]>(
            membershipKey,
            EncodeMembership(new MembershipRow(newGroup, contribution.Numeric, contribution.Member))));

        await store.SetManyAtomicAsync(entries, OperationId(sourceKey, contribution.Timestamp), cancellationToken);

        if (oldGroup is not null && !string.Equals(oldGroup, newGroup, StringComparison.Ordinal))
        {
            await MaterialiseAccumulatorAsync(oldGroup, cancellationToken);
        }

        await MaterialiseAccumulatorAsync(newGroup, cancellationToken);

        // Opportunistic, idempotent cleanup of slots the flip emptied. The atomic
        // batch could only flip an emptied slot to the empty sentinel; deleting it
        // now keeps storage bounded without needing an atomic delete. Cleanup is
        // driven by the store's actual value (not the computed row), so it is a
        // safe no-op when the flip was deduped by a replay's saga re-attach.
        foreach (var key in slots.Keys)
        {
            await CleanupIfEmptyAsync(key, cancellationToken);
        }
    }

    private async Task RetractNumericAsync(AggregationContribution contribution, CancellationToken cancellationToken)
    {
        var sourceKey = contribution.SourceKey;
        var membershipKey = MembershipKey(sourceKey);
        var prior = await ReadMembershipAsync(membershipKey, cancellationToken);
        if (prior is not { } old)
        {
            // Nothing recorded for this source key: idempotent no-op.
            return;
        }

        var oldKey = AccumulatorKey(old.GroupKey, Slot(sourceKey, _fanout));
        var current = await ReadAccumulatorAsync(oldKey, cancellationToken) ?? new AccumulatorRow(0, 0);
        var next = new AccumulatorRow(current.Count - 1, current.Sum - old.Numeric);

        // Flip the decremented slot and the retracted membership row together. The
        // membership row vanishes via the empty sentinel (the atomic batch cannot
        // delete); both are cleaned up after materialising.
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new(oldKey, next.Count <= 0 ? EmptyRow() : EncodeAccumulator(next)),
            new(membershipKey, EmptyRow()),
        };

        await store.SetManyAtomicAsync(entries, OperationId(sourceKey, contribution.Timestamp), cancellationToken);

        await MaterialiseAccumulatorAsync(old.GroupKey, cancellationToken);

        // Opportunistic, idempotent cleanup of the sentinels the flip wrote (a
        // store-state-driven no-op when the flip was deduped by a replay).
        await CleanupIfEmptyAsync(oldKey, cancellationToken);
        await CleanupIfEmptyAsync(membershipKey, cancellationToken);
    }

    private List<KeyValuePair<string, byte[]>> BuildSlotEntries(Dictionary<string, AccumulatorRow> slots)
    {
        var entries = new List<KeyValuePair<string, byte[]>>(slots.Count + 1);
        foreach (var (key, row) in slots)
        {
            entries.Add(new KeyValuePair<string, byte[]>(key, row.Count <= 0 ? EmptyRow() : EncodeAccumulator(row)));
        }

        return entries;
    }

    private async Task CleanupIfEmptyAsync(string key, CancellationToken cancellationToken)
    {
        var bytes = await store.GetAsync(key, cancellationToken);
        if (bytes is not null && IsEmpty(bytes))
        {
            await store.DeleteAsync(key, cancellationToken);
        }
    }

    // A deterministic idempotency key for a contribution's atomic flip: identical
    // across every replay of the same source mutation within a rebuild generation
    // (so the saga dedups) yet distinct per source mutation, and freshened by the
    // rebuild epoch so a post-rebuild flip never re-attaches to the completed saga
    // of a row the rebuild deleted. Hashed so it is short and cannot contain the
    // '/' the saga reserves as its grain-key separator. The view tree's grain id
    // namespaces the saga ({treeId}/{operationId}), so per-contribution
    // uniqueness within one view is sufficient.
    private string OperationId(string sourceKey, HybridLogicalClock timestamp)
    {
        var payload = $"{_operationEpoch}\u0000{sourceKey}\u0000{timestamp.WallClockTicks}\u0000{timestamp.Counter}";
        var hash = XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(payload));
        return "agg-" + hash.ToString("x16");
    }

    // --- min / max / set-union: inverse-row path (already replay idempotent) ---
    private async Task ContributeInverseAsync(AggregationContribution contribution, CancellationToken cancellationToken)
    {
        var sourceKey = contribution.SourceKey;
        var membershipKey = MembershipKey(sourceKey);
        var prior = await ReadMembershipAsync(membershipKey, cancellationToken);

        // Retract the prior contribution first (handles a value change and a
        // re-group to a different group key). The inverse mutation is
        // map.Remove(sourceKey) / map[sourceKey]=entry, which is idempotent on
        // replay, so this path needs no atomic flip.
        if (prior is { } old)
        {
            await MutateInverseAsync(old.GroupKey, sourceKey, add: null, cancellationToken);
        }

        await MutateInverseAsync(
            contribution.GroupKey,
            sourceKey,
            add: new MemberEntry(contribution.Numeric, contribution.Member),
            cancellationToken);

        await store.SetAsync(
            membershipKey,
            EncodeMembership(new MembershipRow(contribution.GroupKey, contribution.Numeric, contribution.Member)),
            cancellationToken);

        if (prior is { } o && !string.Equals(o.GroupKey, contribution.GroupKey, StringComparison.Ordinal))
        {
            await MaterialiseInverseAsync(o.GroupKey, cancellationToken);
        }

        await MaterialiseInverseAsync(contribution.GroupKey, cancellationToken);
    }

    private async Task RetractInverseAsync(string sourceKey, CancellationToken cancellationToken)
    {
        var membershipKey = MembershipKey(sourceKey);
        var prior = await ReadMembershipAsync(membershipKey, cancellationToken);
        if (prior is not { } old)
        {
            // Nothing recorded for this source key: idempotent no-op.
            return;
        }

        await MutateInverseAsync(old.GroupKey, sourceKey, add: null, cancellationToken);
        await store.DeleteAsync(membershipKey, cancellationToken);
        await MaterialiseInverseAsync(old.GroupKey, cancellationToken);
    }

    private async Task MutateInverseAsync(string groupKey, string sourceKey, MemberEntry? add, CancellationToken cancellationToken)
    {
        var slot = Slot(sourceKey, _fanout);
        var key = InverseKey(groupKey, slot);
        var bytes = await store.GetAsync(key, cancellationToken);
        var map = bytes is null ? new Dictionary<string, MemberEntry>(StringComparer.Ordinal) : DecodeInverse(bytes);

        if (add is { } entry)
        {
            map[sourceKey] = entry;
            ApproximateBound(map);
        }
        else
        {
            map.Remove(sourceKey);
        }

        if (map.Count == 0)
        {
            await store.DeleteAsync(key, cancellationToken);
        }
        else
        {
            await store.SetAsync(key, EncodeInverse(map), cancellationToken);
        }
    }

    // Opt-in approximate mode: cap an inverse shard at _maxGroupEntries by
    // evicting the least useful entry. For min keep the smallest numerics, for
    // max the largest, for set-union an arbitrary deterministic distinct sample.
    // NOTE: this is a bounded top-K / sample, NOT a HyperLogLog estimator; true
    // HLL cardinality for set-union is a documented stub left for a later phase.
    private void ApproximateBound(Dictionary<string, MemberEntry> map)
    {
        if (_maxGroupEntries <= 0 || map.Count <= _maxGroupEntries)
        {
            return;
        }

        while (map.Count > _maxGroupEntries)
        {
            string evict = kind switch
            {
                AggregationKind.Min => WorstKey(map, keepSmallest: true),
                AggregationKind.Max => WorstKey(map, keepSmallest: false),
                _ => LargestSourceKey(map),
            };
            map.Remove(evict);
        }
    }

    private static string WorstKey(Dictionary<string, MemberEntry> map, bool keepSmallest)
    {
        // Keeping the smallest numerics (min) means evicting the largest, and vice
        // versa, so the surviving extremum stays exact until K deletes.
        string worst = string.Empty;
        var worstValue = keepSmallest ? double.NegativeInfinity : double.PositiveInfinity;
        var first = true;
        foreach (var (sourceKey, entry) in map)
        {
            var isWorse = first
                || (keepSmallest ? entry.Numeric > worstValue : entry.Numeric < worstValue)
                || (entry.Numeric == worstValue && string.CompareOrdinal(sourceKey, worst) > 0);
            if (isWorse)
            {
                worst = sourceKey;
                worstValue = entry.Numeric;
                first = false;
            }
        }

        return worst;
    }

    private static string LargestSourceKey(Dictionary<string, MemberEntry> map)
    {
        string largest = string.Empty;
        var first = true;
        foreach (var sourceKey in map.Keys)
        {
            if (first || string.CompareOrdinal(sourceKey, largest) > 0)
            {
                largest = sourceKey;
                first = false;
            }
        }

        return largest;
    }

    private async Task MaterialiseAccumulatorAsync(string groupKey, CancellationToken cancellationToken)
    {
        long totalCount = 0;
        double totalSum = 0;
        for (var slot = 0; slot < _fanout; slot++)
        {
            var row = await ReadAccumulatorAsync(AccumulatorKey(groupKey, slot), cancellationToken);
            if (row is { } r)
            {
                totalCount += r.Count;
                totalSum += r.Sum;
            }
        }

        if (totalCount <= 0)
        {
            await store.DeleteAsync(groupKey, cancellationToken);
            return;
        }

        var value = kind == AggregationKind.Count
            ? LatticeAggregationValue.EncodeInt64(totalCount)
            : LatticeAggregationValue.EncodeDouble(totalSum);
        await store.SetAsync(groupKey, value, cancellationToken);
    }

    private async Task MaterialiseInverseAsync(string groupKey, CancellationToken cancellationToken)
    {
        var hasAny = false;
        var extreme = kind == AggregationKind.Min ? double.PositiveInfinity : double.NegativeInfinity;
        var members = kind == AggregationKind.SetUnion ? new HashSet<string>(StringComparer.Ordinal) : null;

        for (var slot = 0; slot < _fanout; slot++)
        {
            var bytes = await store.GetAsync(InverseKey(groupKey, slot), cancellationToken);
            if (bytes is null)
            {
                continue;
            }

            foreach (var entry in DecodeInverse(bytes).Values)
            {
                hasAny = true;
                if (kind == AggregationKind.Min)
                {
                    extreme = Math.Min(extreme, entry.Numeric);
                }
                else if (kind == AggregationKind.Max)
                {
                    extreme = Math.Max(extreme, entry.Numeric);
                }
                else if (entry.Member is not null)
                {
                    members!.Add(entry.Member);
                }
            }
        }

        if (!hasAny)
        {
            await store.DeleteAsync(groupKey, cancellationToken);
            return;
        }

        var value = kind == AggregationKind.SetUnion
            ? LatticeAggregationValue.EncodeInt64(members!.Count)
            : LatticeAggregationValue.EncodeDouble(extreme);
        await store.SetAsync(groupKey, value, cancellationToken);
    }

    private async Task<MembershipRow?> ReadMembershipAsync(string key, CancellationToken cancellationToken)
    {
        var bytes = await store.GetAsync(key, cancellationToken);
        return bytes is null || IsEmpty(bytes) ? null : DecodeMembership(bytes);
    }

    private async Task<AccumulatorRow?> ReadAccumulatorAsync(string key, CancellationToken cancellationToken)
    {
        var bytes = await store.GetAsync(key, cancellationToken);
        return bytes is null || IsEmpty(bytes) ? null : DecodeAccumulator(bytes);
    }
}
