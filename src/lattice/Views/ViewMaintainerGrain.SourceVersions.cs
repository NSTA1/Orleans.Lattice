using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Views;

/// <summary>
/// Last-writer suppression of ordinary writes the source has already superseded
/// (issue #4885). The commit log is appended in arrival order, not HLC order:
/// replication appends a remote write when it arrives, so an older write for a key
/// can be logged after a newer one the source already holds. The source resolves
/// the pair by last-writer-wins and keeps the newer value, but the view would apply
/// whichever it reads last and install the older one.
/// <para>
/// Before a drained batch is applied, the maintainer therefore reads the stored
/// version (the committed row's HLC, tombstones included) of every key an
/// eligible ordinary mutation touched, in one batched read per source shard, and
/// drops every projected write whose source key now holds a strictly newer
/// version. Strictly newer is exactly the source's own last-writer-wins rule, and
/// a version the source holds was appended to the log before it was applied, so
/// the write that holds it is never dropped (its own version is not newer than
/// itself) and reaches the view in this pass or a later one. The newest write per
/// key is therefore always applied, whatever the read races with.
/// </para>
/// <para>
/// Only last-writer-wins point writes and matched-key range deletes are eligible
/// (a replication-apply merge envelope included: the receiver resolves it by the
/// same last-writer-wins rule): a CRDT delta is folded rather than replaced, and an
/// accumulative (history) view records every revision by design, so it keeps
/// them all. An unconstrained key-preserving range delete is lowered to exact
/// per-key deletes by <see cref="ApplyRangeDeleteAgainstSourceAsync"/>.
/// </para>
/// </summary>
internal sealed partial class ViewMaintainerGrain
{
    /// <summary>
    /// Keys per shard-root version read, so a range delete matching many keys is
    /// confirmed in bounded calls rather than one unbounded batch.
    /// </summary>
    internal const int SourceVersionReadPageSize = 512;

    private const int SourceVersionRoutingAttempts = 4;

    /// <summary>
    /// The projected writes, <c>[Start, End)</c> of the drain buffer, of one ordinary
    /// mutation eligible for last-writer suppression. <see cref="Key"/> is the
    /// mutation's source key, or <see langword="null"/> for a matched-key range
    /// delete, whose writes are decided one by one by their own source key.
    /// </summary>
    private readonly record struct OrdinaryWriteSpan(int Start, int End, string? Key, HybridLogicalClock Timestamp);

    /// <summary>
    /// <see langword="true"/> when <paramref name="mutation"/>'s projected writes may
    /// be dropped once the source holds a newer version of the key they derive from.
    /// </summary>
    private static bool IsLastWriterSuppressible(in LatticeMutation mutation)
        => mutation.Mode == LatticeMergeMode.LwwRegister
            && mutation.Delta is null
            && (mutation.Kind is MutationKind.Set or MutationKind.Delete
                || (mutation.Kind == MutationKind.DeleteRange && mutation.MatchedKeys is { Count: > 0 }));

    /// <summary>
    /// Records the span of <paramref name="mutation"/>'s projected writes, appended to
    /// the drain buffer from index <paramref name="start"/> to <paramref name="end"/>,
    /// when it is eligible for last-writer suppression.
    /// </summary>
    private static void RecordOrdinaryWriteSpan(List<OrdinaryWriteSpan> spans, in LatticeMutation mutation, int start, int end)
    {
        if (end > start && IsLastWriterSuppressible(mutation))
        {
            spans.Add(new OrdinaryWriteSpan(
                start, end, mutation.Kind == MutationKind.DeleteRange ? null : mutation.Key, mutation.Timestamp));
        }
    }

    /// <summary>
    /// Removes from <paramref name="writes"/> every write of a recorded span whose
    /// source key the source now holds at a strictly newer version than the
    /// mutation that produced it. Order of the remaining writes is preserved.
    /// </summary>
    private async Task DropWritesSupersededAtSourceAsync<T>(
        List<T> writes,
        List<OrdinaryWriteSpan> spans,
        Func<T, string?> sourceKeyOf,
        string walTreeId,
        int partitions,
        CancellationToken cancellationToken)
    {
        if (spans.Count == 0)
        {
            return;
        }

        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var span in spans)
        {
            if (span.Key is { } key)
            {
                keys.Add(key);
                continue;
            }

            for (var i = span.Start; i < span.End; i++)
            {
                if (sourceKeyOf(writes[i]) is { } sourceKey)
                {
                    keys.Add(sourceKey);
                }
            }
        }

        if (keys.Count == 0)
        {
            return;
        }

        var versions = await ReadSourceVersionsAsync(walTreeId, [.. keys], cancellationToken);
        foreach (var hold in state.State.CrossTreeRepairHolds.Values)
        {
            if (!hold.RepairApplied || !string.Equals(hold.WalTreeId, walTreeId, StringComparison.Ordinal))
            {
                continue;
            }

            foreach (var (key, version) in hold.RepairSourceVersions)
            {
                if (keys.Contains(key) && version > versions.GetValueOrDefault(key))
                {
                    versions[key] = version;
                }
            }
        }

        bool[]? drop = null;
        Dictionary<string, HybridLogicalClock>? droppedOverStaged = null;
        foreach (var span in spans)
        {
            for (var i = span.Start; i < span.End; i++)
            {
                var key = span.Key ?? sourceKeyOf(writes[i]);
                if (key is not null
                    && versions.TryGetValue(key, out var version)
                    && version > span.Timestamp)
                {
                    (drop ??= new bool[writes.Count])[i] = true;
                    if (_stagedSourceKeyRefCount.ContainsKey(key))
                    {
                        droppedOverStaged ??= new(StringComparer.Ordinal);
                        if (!droppedOverStaged.TryGetValue(key, out var seen) || span.Timestamp.CompareTo(seen) > 0)
                        {
                            droppedOverStaged[key] = span.Timestamp;
                        }
                    }
                }
            }
        }

        if (drop is null)
        {
            return;
        }

        // A dropped write may be the very write a staged batch omits its key for;
        // record it so that batch waits until the source's replacement is applied.
        await RecordDroppedSupersedersAsync(droppedOverStaged, walTreeId, partitions, cancellationToken);

        var kept = 0;
        for (var i = 0; i < writes.Count; i++)
        {
            if (!drop[i])
            {
                writes[kept++] = writes[i];
            }
        }

        writes.RemoveRange(kept, writes.Count - kept);
    }

    /// <summary>
    /// The stored last-writer-wins version of every key in <paramref name="keys"/> on
    /// the source tree <paramref name="walTreeId"/> (tombstones included;
    /// <see cref="HybridLogicalClock.Zero"/> for a key holding no row). See
    /// <see cref="ReadSourceAsync{T}"/> for the batching and failure semantics.
    /// </summary>
    private Task<Dictionary<string, HybridLogicalClock>> ReadSourceVersionsAsync(
        string walTreeId,
        List<string> keys,
        CancellationToken cancellationToken)
        => ReadSourceAsync(walTreeId, keys, static (root, page) => root.GetRawVersionsAsync(page), cancellationToken);

    /// <summary>
    /// Reads <paramref name="keys"/> from the source tree <paramref name="walTreeId"/>
    /// with <paramref name="read"/>, a positional batched shard-root read, issuing one
    /// bounded call per owning shard root per page. Stale routing (a split, reshard or
    /// alias swap mid-read) refreshes the map and re-reads; any other failure
    /// propagates and aborts the pass before anything is applied.
    /// </summary>
    private async Task<Dictionary<string, T>> ReadSourceAsync<T>(
        string walTreeId,
        List<string> keys,
        Func<IShardRootGrain, List<string>, Task<List<T>>> read,
        CancellationToken cancellationToken)
    {
        var held = state.State.CrossTreeRepairHolds.Values.FirstOrDefault(
            hold => string.Equals(hold.WalTreeId, walTreeId, StringComparison.Ordinal));
        if (held is not null)
        {
            return await ReadJointSourceAsync(held, keys, read, cancellationToken);
        }

        var lattice = grainFactory.GetGrain<ILattice>(walTreeId);
        var routing = await lattice.GetRoutingAsync(cancellationToken);
        for (var attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await ReadSourceOnceAsync(routing, keys, read, cancellationToken);
            }
            catch (Exception ex) when ((ex is StaleShardRoutingException or StaleTreeRoutingException)
                && attempt < SourceVersionRoutingAttempts)
            {
                routing = await lattice.GetRoutingAsync(forceRefresh: true, cancellationToken);
            }
        }
    }

    /// <summary>
    /// One routing attempt of <see cref="ReadSourceAsync{T}"/>. The per-shard pages
    /// are dispatched through <see cref="BoundedFanOut"/>, so at most
    /// <see cref="BoundedFanOut.DefaultWidth"/> reads are in flight however many keys
    /// the drain touched, and a page not yet dispatched is never sent once
    /// <paramref name="cancellationToken"/> is cancelled.
    /// </summary>
    private async Task<Dictionary<string, T>> ReadSourceOnceAsync<T>(
        RoutingInfo routing,
        List<string> keys,
        Func<IShardRootGrain, List<string>, Task<List<T>>> read,
        CancellationToken cancellationToken)
    {
        var byShard = new Dictionary<int, List<string>>();
        foreach (var key in keys)
        {
            var shard = routing.Map.Resolve(key);
            if (!byShard.TryGetValue(shard, out var bucket))
            {
                byShard[shard] = bucket = [];
            }

            bucket.Add(key);
        }

        var pages = new List<(int Shard, List<string> Keys)>();
        foreach (var (shard, bucket) in byShard)
        {
            for (var offset = 0; offset < bucket.Count; offset += SourceVersionReadPageSize)
            {
                pages.Add((shard, bucket.GetRange(offset, Math.Min(SourceVersionReadPageSize, bucket.Count - offset))));
            }
        }

        var results = await BoundedFanOut.RunAsync(
            pages.Count,
            BoundedFanOut.DefaultWidth,
            slot => read(
                grainFactory.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{pages[slot].Shard}"),
                pages[slot].Keys),
            cancellationToken);

        var values = new Dictionary<string, T>(keys.Count, StringComparer.Ordinal);
        for (var slot = 0; slot < pages.Count; slot++)
        {
            var (shard, page) = pages[slot];
            var result = results[slot];
            if (result is null || result.Count != page.Count)
            {
                // The reply is positional: a short or long answer cannot be
                // attributed to keys, and guessing could drop a write the source
                // still needs.
                throw new InvalidOperationException(
                    $"Shard root '{routing.PhysicalTreeId}/{shard}' returned {result?.Count ?? 0} result(s) for {page.Count} key(s).");
            }

            for (var i = 0; i < page.Count; i++)
            {
                values[page[i]] = result[i];
            }
        }

        return values;
    }

    /// <summary>
    /// Applies a key-preserving unconstrained range delete exactly: the view's keys
    /// in <c>[Key, EndKey)</c> are listed a page at a time and every listed key is
    /// deleted except those whose source holds a version strictly newer than the
    /// range delete. Such a key's newer write survived the range delete at the
    /// source by last-writer-wins, and is either already in the view or still to be
    /// applied from the log, so removing its view row would lose it (issue #4885).
    /// A key the source holds <b>live</b> is spared whatever its version: the range
    /// delete tombstoned every row live in its span at its own HLC, which
    /// last-writer-wins keeps over any write at or below it, so a live row at or below
    /// the range is an older write that reached the source after the range ran there
    /// (a replicated or clock-skewed arrival). The log orders it after the range, but
    /// this pass applies writes in HLC order, so deleting the key would drop the row
    /// for good on a single pass and roll it back on a replay (issue #WWWW).
    /// A key an atomic batch completing this pass writes at a newer version than the
    /// range is spared too, whatever the source read reports: on a replay its flip
    /// re-attaches and writes nothing, so deleting it would remove part of the
    /// batch's slice non-atomically (issue #4869).
    /// <para>
    /// Only keys this pass listed and confirmed are deleted, each by an exact point
    /// delete. The view tree has writers other than this drain (the cross-tree
    /// coordinator commits joint batches into it, and drift can land out of band),
    /// so a range delete spanning the gap between two listed keys could remove a
    /// row this pass never saw or confirmed against the source. A row that appears
    /// behind the cursor after its page was listed is therefore left alone: it was
    /// written after this pass decided that page.
    /// </para>
    /// <para>
    /// Each page is listed through a fresh resumable <c>ScanKeysAsync</c> that is
    /// closed before the page's source reads and deletes start, so no key stream is
    /// held open across that work (an idle server-side enumerator can expire, and a
    /// stateless-worker hop can misroute) and the deletes cannot disturb the
    /// listing. The next page resumes strictly after the last listed key, which the
    /// deletes never reach past, so no key of the range is skipped.
    /// </para>
    /// </summary>
    private async Task ApplyRangeDeleteAgainstSourceAsync(
        ILattice viewTree,
        ViewWrite rangeDelete,
        string walTreeId,
        Dictionary<string, HybridLogicalClock>? batchHlcByViewKey,
        CancellationToken cancellationToken)
    {
        var page = new List<string>(SourceVersionReadPageSize);
        var cursor = rangeDelete.Key;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            page.Clear();
            await foreach (var key in viewTree.ScanKeysAsync(cursor, rangeDelete.EndKey, cancellationToken: cancellationToken))
            {
                page.Add(key);
                if (page.Count == SourceVersionReadPageSize)
                {
                    break;
                }
            }

            if (page.Count == 0)
            {
                return;
            }

            await DeleteListedPageAsync(viewTree, page, rangeDelete.Timestamp, walTreeId, batchHlcByViewKey, cancellationToken);
            if (page.Count < SourceVersionReadPageSize)
            {
                return;
            }

            // The ordinal successor of the last listed key: nothing sorts between.
            cursor = page[^1] + '\0';
        }
    }

    private async Task DeleteListedPageAsync(
        ILattice viewTree,
        List<string> page,
        HybridLogicalClock rangeTimestamp,
        string walTreeId,
        Dictionary<string, HybridLogicalClock>? batchHlcByViewKey,
        CancellationToken cancellationToken)
    {
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var entries = await ReadSourceAsync(
            walTreeId, page, static (root, keys) => root.GetVisibleRawEntriesAsync(keys), cancellationToken);
        var deletable = new List<string>(page.Count);
        foreach (var key in page)
        {
            var raw = entries[key];
            if (raw is { IsTombstone: false } entry
                && !entry.ToLwwValue().IsExpired(nowTicks))
            {
                // A row the source holds live was never tombstoned by this range,
                // whatever its version: an unconstrained range delete tombstones
                // every key live in its span at its own HLC, which last-writer-wins
                // keeps over any write at or below it. A live version at or below
                // the range therefore reached the source after the range was
                // applied there (an older write arriving late), and removing its
                // view row on a replay would roll the view back to before that
                // write until the log re-applies it.
                continue;
            }

            if (batchHlcByViewKey is not null
                && batchHlcByViewKey.TryGetValue(key, out var batchHlc)
                && batchHlc > rangeTimestamp)
            {
                continue;
            }

            if ((raw?.Timestamp ?? HybridLogicalClock.Zero) <= rangeTimestamp)
            {
                deletable.Add(key);
            }
        }

        // Independent, idempotent point deletes of distinct keys: completion order
        // within a wave is immaterial.
        await BoundedFanOut.ForEachAsync(
            deletable,
            BoundedFanOut.DefaultWidth,
            key => viewTree.DeleteAsync(key, cancellationToken));
    }
}
