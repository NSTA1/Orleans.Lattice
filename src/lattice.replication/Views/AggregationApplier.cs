using static Orleans.Lattice.Replication.Views.AggregationRowCodec;

namespace Orleans.Lattice.Replication.Views;

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
/// <b>Fanout.</b> Each group is sharded into <see cref="_fanout"/> accumulators
/// hashed on the source key; a group's materialised value merges the shards. A
/// fanout of 1 is a single accumulator (identical result).
/// </para>
/// </summary>
internal sealed class AggregationApplier(
    IAggregationViewStore store,
    AggregationKind kind,
    int fanout,
    int maxGroupEntries)
{
    private readonly int _fanout = fanout < 1 ? 1 : fanout;
    private readonly int _maxGroupEntries = maxGroupEntries;

    /// <summary>Applies a single contribution and re-materialises every group it touches.</summary>
    public async Task ApplyAsync(AggregationContribution contribution, CancellationToken cancellationToken = default)
    {
        switch (contribution.Kind)
        {
            case AggregationContributionKind.Contribute:
                await ContributeAsync(contribution, cancellationToken);
                return;
            case AggregationContributionKind.Retract:
                await RetractAsync(contribution.SourceKey, cancellationToken);
                return;
            default:
                // RangeReconcile is resolved to a rebuild by the maintainer before
                // it ever reaches the applier.
                return;
        }
    }

    private async Task ContributeAsync(AggregationContribution contribution, CancellationToken cancellationToken)
    {
        var sourceKey = contribution.SourceKey;
        var membershipKey = MembershipKey(sourceKey);
        var prior = await ReadMembershipAsync(membershipKey, cancellationToken);

        // Retract the prior contribution first (handles a value change and a
        // re-group to a different group key).
        if (prior is { } old)
        {
            await RemoveContributionAsync(old.GroupKey, sourceKey, old, cancellationToken);
        }

        await AddContributionAsync(contribution.GroupKey, sourceKey, contribution, cancellationToken);

        // The membership row is the read-before-write pointer; write it after the
        // accumulator move so a re-applied contribution recomputes a zero delta.
        await store.SetAsync(
            membershipKey,
            EncodeMembership(new MembershipRow(contribution.GroupKey, contribution.Numeric, contribution.Member)),
            cancellationToken);

        if (prior is { } o && !string.Equals(o.GroupKey, contribution.GroupKey, StringComparison.Ordinal))
        {
            await MaterialiseAsync(o.GroupKey, cancellationToken);
        }

        await MaterialiseAsync(contribution.GroupKey, cancellationToken);
    }

    private async Task RetractAsync(string sourceKey, CancellationToken cancellationToken)
    {
        var membershipKey = MembershipKey(sourceKey);
        var prior = await ReadMembershipAsync(membershipKey, cancellationToken);
        if (prior is not { } old)
        {
            // Nothing recorded for this source key: idempotent no-op.
            return;
        }

        await RemoveContributionAsync(old.GroupKey, sourceKey, old, cancellationToken);
        await store.DeleteAsync(membershipKey, cancellationToken);
        await MaterialiseAsync(old.GroupKey, cancellationToken);
    }

    private async Task AddContributionAsync(string groupKey, string sourceKey, AggregationContribution contribution, CancellationToken cancellationToken)
    {
        if (kind is AggregationKind.Count or AggregationKind.Sum)
        {
            var slot = Slot(sourceKey, _fanout);
            var key = AccumulatorKey(groupKey, slot);
            var row = await ReadAccumulatorAsync(key, cancellationToken) ?? new AccumulatorRow(0, 0);
            row = new AccumulatorRow(row.Count + 1, row.Sum + contribution.Numeric);
            await store.SetAsync(key, EncodeAccumulator(row), cancellationToken);
            return;
        }

        await MutateInverseAsync(groupKey, sourceKey, add: new MemberEntry(contribution.Numeric, contribution.Member), cancellationToken);
    }

    private async Task RemoveContributionAsync(string groupKey, string sourceKey, MembershipRow old, CancellationToken cancellationToken)
    {
        if (kind is AggregationKind.Count or AggregationKind.Sum)
        {
            var slot = Slot(sourceKey, _fanout);
            var key = AccumulatorKey(groupKey, slot);
            var row = await ReadAccumulatorAsync(key, cancellationToken);
            if (row is not { } existing)
            {
                return;
            }

            var next = new AccumulatorRow(existing.Count - 1, existing.Sum - old.Numeric);
            if (next.Count <= 0)
            {
                await store.DeleteAsync(key, cancellationToken);
            }
            else
            {
                await store.SetAsync(key, EncodeAccumulator(next), cancellationToken);
            }

            return;
        }

        await MutateInverseAsync(groupKey, sourceKey, add: null, cancellationToken);
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

    private async Task MaterialiseAsync(string groupKey, CancellationToken cancellationToken)
    {
        switch (kind)
        {
            case AggregationKind.Count:
            case AggregationKind.Sum:
                await MaterialiseAccumulatorAsync(groupKey, cancellationToken);
                return;
            default:
                await MaterialiseInverseAsync(groupKey, cancellationToken);
                return;
        }
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
        return bytes is null ? null : DecodeMembership(bytes);
    }

    private async Task<AccumulatorRow?> ReadAccumulatorAsync(string key, CancellationToken cancellationToken)
    {
        var bytes = await store.GetAsync(key, cancellationToken);
        return bytes is null ? null : DecodeAccumulator(bytes);
    }
}
