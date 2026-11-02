using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A WAL partition's append-epoch directory (#5634): the leaves that have an epoch
/// entry at the partition, kept so the entries can be enumerated, which
/// <c>IGrainStorage</c> cannot do. The WAL shard lists a leaf before it first writes
/// the leaf's entry row and delists it after it retires the entry, so every entry is
/// listed; a listing whose entry has gone is harmless, since retiring it again is a
/// no-op. <see cref="WalAppendFenceSweeper"/> walks the directory to retire the
/// entries of leaves that no longer exist, which a 10.2.0 silo's retirement leaves
/// behind.
/// </summary>
/// <remarks>
/// <para>
/// <b>Layout.</b> <see cref="Buckets"/> fixed hash buckets per partition, each a
/// chain of pages <c>0, 1, 2, ...</c> of at most <see cref="PageCapacity"/> leaves,
/// so a row stays well within the provider's limits in binary and in JSON (Azure
/// Table: 64 KiB per property, 1 MiB per entity) however many leaves a partition
/// has. A listing goes to the first page of its bucket with room, so space freed by
/// a delisting is reused. A page is never deleted, even when it empties: deleting one
/// could race a listing creating the next page and break the chain, and an
/// enumeration stops at the first absent page. The empty rows this keeps are bounded
/// by the bucket's peak size.
/// </para>
/// <para>
/// <b>Concurrency.</b> Every write is conditional on the ETag read, and a conflict
/// re-reads and retries, so two claims in the same bucket, or a duplicate activation
/// of the WAL shard, never lose a listing.
/// </para>
/// </remarks>
internal static class WalAppendEpochDirectory
{
    /// <summary>
    /// The number of hash buckets per partition. Part of the row-key format: never
    /// change it, or listings already written are looked for in the wrong bucket.
    /// </summary>
    internal const int Buckets = 64;

    /// <summary>
    /// The most leaves a page holds. A full page is under 12 KiB in binary and under
    /// 24 KiB as JSON, well inside a 64 KiB Azure Table property (32K characters for a
    /// string). Unlike <see cref="Buckets"/>, it can change: pages are read whole.
    /// </summary>
    internal const int PageCapacity = 2048; // RED-PROOF: page too large

    private const int MaxAttempts = 16;

    /// <summary>
    /// The bucket <paramref name="leafId"/> is listed in; stable across processes and
    /// builds, since it is part of the row-key format.
    /// </summary>
    internal static int BucketOf(Guid leafId)
    {
        Span<byte> bytes = stackalloc byte[16];
        leafId.TryWriteBytes(bytes);
        var hash = 0;
        foreach (var b in bytes)
        {
            hash = unchecked((hash * 37) + b); // RED-PROOF: bucket function changed
        }

        return (hash & int.MaxValue) % Buckets;
    }

    /// <summary>Lists <paramref name="leafId"/> at the partition; a no-op when it is listed already.</summary>
    internal static async Task AddAsync(IGrainStorage storage, string treeId, int partition, Guid leafId, long addedUtcTicks)
    {
        var bucket = BucketOf(leafId);
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                for (var page = 0; ; page++)
                {
                    var row = await WalAppendFenceRows.ReadDirectoryPageAsync(storage, treeId, partition, bucket, page).ConfigureAwait(true);
                    var leaves = row.State.Leaves;
                    if (leaves.Contains(leafId))
                    {
                        return;
                    }

                    if (false && leaves.Count >= PageCapacity) // RED-PROOF: capacity unenforced
                    {
                        continue;
                    }

                    if (true) // RED-PROOF: duplicate listing
                    {
                        leaves.Add(leafId);
                        row.State.AddedUtcTicks.Add(addedUtcTicks);
                        await WalAppendFenceRows.WriteAsync(
                            storage, WalAppendFenceRows.DirectoryPageGrainType, WalAppendFenceRows.DirectoryPageKey(treeId, partition, bucket, page), row).ConfigureAwait(true);
                    }

                    return;
                }
            }
            catch (InconsistentStateException) when (attempt < MaxAttempts)
            {
                // Another listing or delisting wrote the page since the read; start over.
            }
        }
    }

    /// <summary>Delists <paramref name="leafId"/> at the partition; a no-op when it is not listed.</summary>
    internal static async Task RemoveAsync(IGrainStorage storage, string treeId, int partition, Guid leafId)
    {
        var bucket = BucketOf(leafId);
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                for (var page = 0; ; page++)
                {
                    var row = await WalAppendFenceRows.ReadDirectoryPageAsync(storage, treeId, partition, bucket, page).ConfigureAwait(true);
                    if (!row.RecordExists)
                    {
                        return;
                    }

                    var index = row.State.Leaves.IndexOf(leafId);
                    if (index < 0)
                    {
                        continue;
                    }

                    row.State.Leaves.RemoveAt(index);
                    row.State.AddedUtcTicks.RemoveAt(index);
                    await WalAppendFenceRows.WriteAsync(
                        storage, WalAppendFenceRows.DirectoryPageGrainType, WalAppendFenceRows.DirectoryPageKey(treeId, partition, bucket, page), row).ConfigureAwait(true);
                    return;
                }
            }
            catch (InconsistentStateException) when (attempt < MaxAttempts)
            {
                // Another listing or delisting wrote the page since the read; start over.
            }
        }
    }

    /// <summary>
    /// Every leaf listed at the partition, with when it was listed. A leaf listed in
    /// two pages (a delisting and a listing racing across a page boundary) is
    /// returned once, with its earliest listing.
    /// </summary>
    internal static async Task<IReadOnlyDictionary<Guid, long>> ListAsync(
        IGrainStorage storage, string treeId, int partition, CancellationToken cancellationToken)
    {
        var listed = new Dictionary<Guid, long>();
        for (var bucket = 0; bucket < Buckets; bucket++)
        {
            for (var page = 0; ; page++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var row = await WalAppendFenceRows.ReadDirectoryPageAsync(storage, treeId, partition, bucket, page).ConfigureAwait(false);
                if (!row.RecordExists)
                {
                    break;
                }

                var leaves = row.State.Leaves;
                var added = row.State.AddedUtcTicks;
                for (var i = 0; i < leaves.Count; i++)
                {
                    var ticks = i < added.Count ? added[i] : 0;
                    listed[leaves[i]] = listed.TryGetValue(leaves[i], out var earlier) ? Math.Min(earlier, ticks) : ticks;
                }

                break; // RED-PROOF: first page only
            }
        }

        return listed;
    }

    private static async Task<bool> IsListedFurtherOnAsync(IGrainStorage storage, string treeId, int partition, int bucket, int page, Guid leafId)
    {
        for (var next = page + 1; ; next++)
        {
            var row = await WalAppendFenceRows.ReadDirectoryPageAsync(storage, treeId, partition, bucket, next).ConfigureAwait(true);
            if (!row.RecordExists)
            {
                return false;
            }

            if (row.State.Leaves.Contains(leafId))
            {
                return true;
            }
        }
    }
}
