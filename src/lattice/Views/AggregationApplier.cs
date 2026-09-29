using System.Buffers;
using System.Globalization;
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
/// </para>
/// <para>Each group is sharded into <see cref="_fanout"/> accumulators
/// hashed on the source key; a group's materialised value merges the shards. A
/// fanout of 1 is a single accumulator (identical result).
/// </para>
/// </summary>
internal sealed class AggregationApplier(
    IAggregationViewStore store,
    AggregationKind kind,
    int fanout,
    int maxGroupEntries,
    string operationEpoch,
    ILatticeFoldProjection? fold = null,
    string? viewName = null,
    KeyValuePair<string, object?>? tenantTag = null)
{
    private static readonly System.Diagnostics.Metrics.Counter<long> Rejected = LatticeMetrics.ViewAggregationRejected;

    /// <summary>The literal every aggregation saga operation id starts with.</summary>
    private const string OperationIdPrefix = "agg-";

    /// <summary>Widest invariant "G" rendering of an <see cref="long"/> (<c>long.MinValue</c>).</summary>
    private const int MaxInt64Digits = 20;

    /// <summary>Widest invariant "G" rendering of an <see cref="int"/> (<c>int.MinValue</c>).</summary>
    private const int MaxInt32Digits = 11;

    /// <summary>The three NUL bytes separating the operation id's four fields.</summary>
    private const int SeparatorCount = 3;

    private readonly int _fanout = fanout < 1 ? 1 : fanout;
    private readonly int _maxGroupEntries = maxGroupEntries;
    private readonly string _operationEpoch = operationEpoch;
    private readonly ILatticeFoldProjection? _fold = fold;
    private readonly string? _viewName = viewName;

    // The owning tenant of the view this applier maintains, supplied by the
    // maintainer that already derived it once per activation. Falls back to the
    // platform sentinel when an applier is built outside a maintainer.
    private readonly KeyValuePair<string, object?> _tenantTag = tenantTag ?? LatticeTenantLabel.Platform;

    private bool IsNumeric => kind is AggregationKind.Count or AggregationKind.Sum;

    private bool IsFold => kind == AggregationKind.Fold;

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

    private Task ContributeAsync(AggregationContribution contribution, CancellationToken cancellationToken)
    {
        // A group value is materialised under its bare group key, so a key in the
        // reserved region (empty, or NUL-prefixed) would sort below every readable
        // key and could collide with an internal accumulator / inverse / membership
        // row. Rather than corrupt the view silently, drop the contribution and
        // meter it: the rejection is deterministic on the key, so every cluster
        // drops the same members and the view stays convergent. A regrouped source
        // key whose new group is reserved simply keeps its prior (valid) group.
        if (IsReservedGroupKey(contribution.GroupKey))
        {
            Rejected.Add(1, new KeyValuePair<string, object?>(LatticeMetrics.TagView, _viewName), _tenantTag);
            return Task.CompletedTask;
        }

        return IsNumeric
            ? ContributeNumericAsync(contribution, cancellationToken)
            : IsFold
                ? ContributeFoldAsync(contribution, cancellationToken)
                : ContributeInverseAsync(contribution, cancellationToken);
    }

    private Task RetractAsync(AggregationContribution contribution, CancellationToken cancellationToken) =>
        IsNumeric
            ? RetractNumericAsync(contribution, cancellationToken)
            : IsFold
                ? RetractFoldAsync(contribution.SourceKey, cancellationToken)
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

        // Accumulate every touched slot's final row in memory. A contribution
        // touches at most two accumulator keys - the retracted old-group slot and
        // the added new-group slot - and a same-group overwrite makes them the
        // same key, which must fold onto one row and reach the atomic batch once.
        //
        // That is two locals and one string comparison, so it was a dictionary's
        // whole job for a map that can never hold a third entry. Every numeric
        // contribution paid for a Dictionary (its bucket and entry arrays) plus a
        // second List to re-extract the very keys it had just put in, on the
        // hottest path an aggregation view has. Both are gone; `sameSlot` below
        // carries the de-duplication the dictionary used to provide.
        //
        // The source key's accumulator shard is a pure function of the key and
        // the fanout, and neither changes across this method - but a re-group
        // touches two accumulator keys, and deriving the slot for each of them
        // separately transcoded the key to UTF-8 and hashed it twice per
        // contribution. Derive it once.
        var slot = Slot(sourceKey, _fanout);

        string? oldGroup = null;
        string? oldKey = null;
        var oldRow = new AccumulatorRow(0, 0);
        if (prior is { } old)
        {
            oldGroup = old.GroupKey;
            oldKey = AccumulatorKey(old.GroupKey, slot);
            var current = await ReadAccumulatorAsync(oldKey, cancellationToken) ?? new AccumulatorRow(0, 0);
            oldRow = new AccumulatorRow(current.Count - 1, current.Sum - old.Numeric);
        }

        var newKey = AccumulatorKey(newGroup, slot);

        // A same-group overwrite retracts and adds on one key. Fold onto the
        // retracted row rather than re-reading the store, exactly as the
        // dictionary's TryGetValue hit used to.
        var sameSlot = oldKey is not null && string.Equals(oldKey, newKey, StringComparison.Ordinal);
        var baseRow = sameSlot
            ? oldRow
            : await ReadAccumulatorAsync(newKey, cancellationToken) ?? new AccumulatorRow(0, 0);
        var newRow = new AccumulatorRow(baseRow.Count + 1, baseRow.Sum + contribution.Numeric);

        // Insertion order matters: the dictionary this replaces enumerated the
        // old-group slot first, so the atomic batch keeps that order.
        var entries = new List<KeyValuePair<string, byte[]>>(sameSlot || oldKey is null ? 2 : 3);
        if (oldKey is not null && !sameSlot)
        {
            entries.Add(new KeyValuePair<string, byte[]>(oldKey, SlotValue(oldRow)));
        }

        entries.Add(new KeyValuePair<string, byte[]>(newKey, SlotValue(newRow)));
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
        // safe no-op when the flip was deduped by a replay's saga re-attach. The
        // probes go out as one batched read: the old-group and new-group slots'
        // emptiness decisions are independent of each other, so there is nothing
        // to gain from reading them one at a time. The keys are already in hand,
        // so the list is built straight from them rather than re-extracted from a
        // dictionary that existed only to hold them.
        var cleanupKeys = oldKey is null || sameSlot ? [newKey] : new List<string> { oldKey, newKey };

        await CleanupIfEmptyAsync(cleanupKeys, cancellationToken);
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
        await CleanupIfEmptyAsync([oldKey, membershipKey], cancellationToken);
    }

    /// <summary>
    /// Encodes one accumulator slot's final row, collapsing a slot the flip
    /// emptied to the empty sentinel (the atomic batch cannot delete).
    /// </summary>
    /// <param name="row">The slot's computed final row.</param>
    private static byte[] SlotValue(AccumulatorRow row)
        => row.Count <= 0 ? EmptyRow() : EncodeAccumulator(row);

    /// <summary>
    /// Deletes each of <paramref name="keys"/> that currently holds the empty
    /// sentinel, probing every candidate in one store read instead of one read
    /// per key.
    /// </summary>
    /// <remarks>
    /// Every numeric flip ends by probing the keys it may have emptied, and those
    /// probes are mutually independent - the decision for one key never depends on
    /// another's value - so issuing them serially spends one round trip per key
    /// for no ordering benefit. A single <c>GetManyAsync</c> collapses the probe
    /// phase to one round trip; the deletes that follow remain per-key because the
    /// store exposes no batched delete, but only genuinely emptied keys reach
    /// them, and an emptied slot is the uncommon case. Cleanup stays driven by the
    /// store's actual value rather than the computed row, so it remains a safe
    /// no-op when a replay's saga re-attach deduped the flip. Keys absent from the
    /// result were never written, which is already the state cleanup is trying to
    /// reach, so omitting them matches the per-key form exactly.
    /// </remarks>
    private async Task CleanupIfEmptyAsync(List<string> keys, CancellationToken cancellationToken)
    {
        if (keys.Count == 0)
        {
            return;
        }

        var rows = await store.GetManyAsync(keys, cancellationToken);
        List<string>? empties = null;
        foreach (var (key, bytes) in rows)
        {
            if (bytes is not null && IsEmpty(bytes))
            {
                (empties ??= []).Add(key);
            }
        }

        if (empties is null)
        {
            return;
        }

        foreach (var key in empties)
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
        // Compose the hash input DIRECTLY into a stack (or pooled, for long keys)
        // UTF-8 buffer. The payload was previously interpolated into a string
        // purely so it could be transcoded into that same buffer on the next
        // line and then thrown away - a whole heap string, sized by the source
        // key, materialised per numeric contribution and retraction on the
        // per-write view hot path, that no caller ever saw.
        //
        // The bytes are unchanged, so the returned id is unchanged: '\u0000'
        // encodes to the single byte 0x00, and the "G" format of a non-negative
        // integer is the same digit sequence in every culture, which is what the
        // interpolation emitted. The integers are formatted invariantly here so
        // that stays true of a negative value too.
        var maxByteCount = Encoding.UTF8.GetMaxByteCount(_operationEpoch.Length)
            + Encoding.UTF8.GetMaxByteCount(sourceKey.Length)
            + MaxInt64Digits + MaxInt32Digits + SeparatorCount;

        byte[]? rented = null;
        Span<byte> buffer = maxByteCount <= 256
            ? stackalloc byte[maxByteCount]
            : (rented = ArrayPool<byte>.Shared.Rent(maxByteCount));
        try
        {
            var written = 0;
            written += Encoding.UTF8.GetBytes(_operationEpoch, buffer);
            buffer[written++] = 0x00;
            written += Encoding.UTF8.GetBytes(sourceKey, buffer[written..]);
            buffer[written++] = 0x00;
            timestamp.WallClockTicks.TryFormat(
                buffer[written..], out var ticksWritten, default, CultureInfo.InvariantCulture);
            written += ticksWritten;
            buffer[written++] = 0x00;
            timestamp.Counter.TryFormat(
                buffer[written..], out var counterWritten, default, CultureInfo.InvariantCulture);
            written += counterWritten;

            var hash = XxHash64.HashToUInt64(buffer[..written]);

            // One allocation for the id rather than three: the interpolated
            // payload, the "x16" hash string and the concatenation that joined it
            // to the prefix were each a separate heap string.
            return string.Create(
                OperationIdPrefix.Length + 16,
                hash,
                static (destination, value) =>
                {
                    OperationIdPrefix.CopyTo(destination);
                    value.TryFormat(destination[OperationIdPrefix.Length..], out _, "x16", CultureInfo.InvariantCulture);
                });
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
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
        var absent = bytes is null || IsEmpty(bytes);

        // A contribution changes exactly ONE entry of this shard, so splice the
        // encoded row rather than decoding every entry into a dictionary and
        // re-encoding every entry back out - both of which cost the shard's whole
        // size for a constant-sized change. See SpliceInverse for why the result
        // is byte-identical to the decode / mutate / re-encode it replaces, and
        // why it validates the row just as strictly.
        //
        // The approximate mode is the one caller that genuinely needs the map: it
        // evicts by comparing entries against each other, which is not a splice.
        // It keeps the original path unchanged, so the gate is a single field
        // test on a method that is already making a store round trip.
        if (_maxGroupEntries <= 0)
        {
            var spliced = SpliceInverse(absent ? EmptyEntryRow : bytes, sourceKey, add);
            if (spliced is null)
            {
                await store.DeleteAsync(key, cancellationToken);
            }
            else
            {
                await store.SetAsync(key, spliced, cancellationToken);
            }

            return;
        }

        var map = absent
            ? new Dictionary<string, MemberEntry>(StringComparer.Ordinal)
            : DecodeInverse(bytes!);

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
        // Iterate the dictionary directly rather than through its `.Keys`
        // collection, mirroring the sibling WorstKey above. The map is a fresh
        // per-mutation decode (see MutateInverseAsync), so each call otherwise
        // allocates a throwaway KeyCollection wrapper on first access; walking
        // the entries needs only the struct enumerator.
        string largest = string.Empty;
        var first = true;
        foreach (var (sourceKey, _) in map)
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

        // Gather every accumulator shard for the group in one batched read rather
        // than one store call per slot, matching MaterialiseInverseAsync and
        // MaterialiseFoldAsync below. Summation is order-independent, so the
        // arbitrary iteration order of the returned rows is immaterial, and the
        // pass costs one round trip at any fanout instead of `fanout` of them.
        var slotKeys = new List<string>(_fanout);
        for (var slot = 0; slot < _fanout; slot++)
        {
            slotKeys.Add(AccumulatorKey(groupKey, slot));
        }

        var shards = await store.GetManyAsync(slotKeys, cancellationToken);

        // GetManyAsync omits absent keys but still returns a slot holding the
        // empty sentinel, which ReadAccumulatorAsync treats as "no row"; keep the
        // IsEmpty guard so the batched pass decodes exactly what the per-slot loop
        // did.
        foreach (var (_, bytes) in shards)
        {
            if (bytes is null || IsEmpty(bytes))
            {
                continue;
            }

            var r = DecodeAccumulator(bytes);
            totalCount += r.Count;
            totalSum += r.Sum;
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

        // Gather every inverse shard for the group in one batched read rather than
        // one store call per slot; min/max/set-union are order-independent, so the
        // arbitrary iteration order of the returned rows is immaterial.
        var slotKeys = new List<string>(_fanout);
        for (var slot = 0; slot < _fanout; slot++)
        {
            slotKeys.Add(InverseKey(groupKey, slot));
        }

        var shards = await store.GetManyAsync(slotKeys, cancellationToken);

        // Walk the freshly-materialised shard map by its struct enumerator rather
        // than through `.Values`: it is a fresh GetMany result, so every `.Values`
        // access otherwise allocates a throwaway ValueCollection.
        //
        // Each shard's entries are then walked STRAIGHT OUT OF THE ROW. This pass
        // reduces a shard to one extremum or a member union and never looks a
        // source key up, so the keyed Dictionary the decoder used to hand it - and
        // the freshly-decoded string it held for every source key in the shard -
        // were built only to be dropped. The cursor materialises neither, and the
        // min/max walk additionally steps over the member string it does not read.
        foreach (var (_, bytes) in shards)
        {
            // GetManyAsync can return a slot holding the empty sentinel (and a
            // buffered delete reads as absent), exactly as the accumulator pass
            // above guards for. Skip those rather than handing them to the decoder.
            if (bytes is null || IsEmpty(bytes))
            {
                continue;
            }

            var scan = new InverseRowScan(bytes);
            if (kind == AggregationKind.Min)
            {
                while (scan.MoveNextNumeric())
                {
                    hasAny = true;
                    extreme = Math.Min(extreme, scan.Numeric);
                }
            }
            else if (kind == AggregationKind.Max)
            {
                while (scan.MoveNextNumeric())
                {
                    hasAny = true;
                    extreme = Math.Max(extreme, scan.Numeric);
                }
            }
            else
            {
                while (scan.MoveNext())
                {
                    hasAny = true;
                    if (scan.Member is not null)
                    {
                        members!.Add(scan.Member);
                    }
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

    // --- fold: per-source-key value rows re-folded on every change ---
    //
    // A custom fold is not invertible, so the applier cannot un-apply a single
    // member. Instead it keeps each source key's contributed value (+ its source
    // HLC) in a fold-inverse shard (mirroring the min/max/set-union inverse path),
    // and RE-FOLDS the whole group - over its surviving members, in ascending
    // (HLC, sourceKey) order - whenever a member is added, retracted, or
    // re-grouped. The fold-inverse mutation (map[k]=entry / map.Remove(k)) is
    // idempotent on replay, so like the inverse path it needs no atomic flip.
    private async Task ContributeFoldAsync(AggregationContribution contribution, CancellationToken cancellationToken)
    {
        var sourceKey = contribution.SourceKey;
        var membershipKey = MembershipKey(sourceKey);
        var prior = await ReadMembershipAsync(membershipKey, cancellationToken);

        if (prior is { } old)
        {
            await MutateFoldAsync(old.GroupKey, sourceKey, add: null, cancellationToken);
        }

        await MutateFoldAsync(
            contribution.GroupKey,
            sourceKey,
            add: new FoldMember(contribution.Value ?? [], contribution.Timestamp),
            cancellationToken);

        await store.SetAsync(
            membershipKey,
            EncodeMembership(new MembershipRow(contribution.GroupKey, 0, null)),
            cancellationToken);

        if (prior is { } o && !string.Equals(o.GroupKey, contribution.GroupKey, StringComparison.Ordinal))
        {
            await MaterialiseFoldAsync(o.GroupKey, cancellationToken);
        }

        await MaterialiseFoldAsync(contribution.GroupKey, cancellationToken);
    }

    private async Task RetractFoldAsync(string sourceKey, CancellationToken cancellationToken)
    {
        var membershipKey = MembershipKey(sourceKey);
        var prior = await ReadMembershipAsync(membershipKey, cancellationToken);
        if (prior is not { } old)
        {
            // Nothing recorded for this source key: idempotent no-op.
            return;
        }

        await MutateFoldAsync(old.GroupKey, sourceKey, add: null, cancellationToken);
        await store.DeleteAsync(membershipKey, cancellationToken);
        await MaterialiseFoldAsync(old.GroupKey, cancellationToken);
    }

    private async Task MutateFoldAsync(string groupKey, string sourceKey, FoldMember? add, CancellationToken cancellationToken)
    {
        var key = FoldInverseKey(groupKey, Slot(sourceKey, _fanout));
        var bytes = await store.GetAsync(key, cancellationToken);
        var absent = bytes is null || IsEmpty(bytes);

        // See MutateInverseAsync: one entry changes, so the row is spliced rather
        // than round-tripped through a dictionary. This path has no approximate
        // mode, so there is no fallback to gate - and it saves strictly more,
        // because a fold entry's decode also copies its whole value payload onto
        // the heap only for the re-encode to copy it straight back out.
        var spliced = SpliceFoldInverse(absent ? EmptyEntryRow : bytes, sourceKey, add);
        if (spliced is null)
        {
            await store.DeleteAsync(key, cancellationToken);
        }
        else
        {
            await store.SetAsync(key, spliced, cancellationToken);
        }
    }

    private async Task MaterialiseFoldAsync(string groupKey, CancellationToken cancellationToken)
    {
        // Gather every surviving member across the group's shards, then re-fold in
        // ascending (HLC, sourceKey) order so the materialised value is a pure,
        // convergent function of the group's member set regardless of delivery
        // order. The source-key tie-break makes equal-HLC members deterministic.
        var members = new List<(string SourceKey, FoldMember Member)>();

        // Gather every fold-inverse shard for the group in one batched read
        // rather than one store call per slot; the members are re-sorted by
        // (HLC, sourceKey) below, so the arbitrary order of the returned rows
        // does not affect the folded result.
        var slotKeys = new List<string>(_fanout);
        for (var slot = 0; slot < _fanout; slot++)
        {
            slotKeys.Add(FoldInverseKey(groupKey, slot));
        }

        var shards = await store.GetManyAsync(slotKeys, cancellationToken);

        // Walk the freshly-materialised shard map directly rather than through
        // `.Values`: `shards` is a fresh GetMany result, so a `.Values` access
        // otherwise allocates a throwaway ValueCollection wrapper per call.
        //
        // Each shard's members are appended straight from the row. The decoder
        // used to build a keyed Dictionary per shard and this loop immediately
        // walked it back out into the flat list below, so every member paid a
        // hash and a bucket insert into a map nothing ever probed. The cursor's
        // declared entry count also lets the list grow once to the exact incoming
        // size rather than doubling into it.
        foreach (var (_, bytes) in shards)
        {
            // See MaterialiseInverseAsync: an empty-sentinel or absent slot is not
            // a decodable row.
            if (bytes is null || IsEmpty(bytes))
            {
                continue;
            }

            var scan = new FoldInverseRowScan(bytes);
            members.EnsureCapacity(members.Count + scan.Remaining);
            while (scan.MoveNext())
            {
                members.Add((scan.SourceKey, scan.Member));
            }
        }

        if (members.Count == 0)
        {
            await store.DeleteAsync(groupKey, cancellationToken);
            return;
        }

        members.Sort(static (a, b) =>
        {
            var cmp = a.Member.Timestamp.CompareTo(b.Member.Timestamp);
            return cmp != 0 ? cmp : string.CompareOrdinal(a.SourceKey, b.SourceKey);
        });

        var accumulator = _fold!.Initial();
        foreach (var (sourceKey, member) in members)
        {
            accumulator = _fold.Apply(accumulator, sourceKey, member.Value, member.Timestamp);
        }

        await store.SetAsync(groupKey, accumulator, cancellationToken);
    }

    private async Task<MembershipHead?> ReadMembershipAsync(string key, CancellationToken cancellationToken)
    {
        // Every caller of this method uses the row for exactly two things: the
        // group shard to retract from and the numeric to subtract. None reads the
        // member, so the member string is decoded here only to be dropped - once
        // per contribute and once per retract, on every set-union view. See
        // DecodeMembershipHead: the member's length prefix is still read and
        // still bounded, so the row is validated exactly as strictly.
        var bytes = await store.GetAsync(key, cancellationToken);
        return bytes is null || IsEmpty(bytes) ? null : DecodeMembershipHead(bytes);
    }

    private async Task<AccumulatorRow?> ReadAccumulatorAsync(string key, CancellationToken cancellationToken)
    {
        var bytes = await store.GetAsync(key, cancellationToken);
        return bytes is null || IsEmpty(bytes) ? null : DecodeAccumulator(bytes);
    }
}
