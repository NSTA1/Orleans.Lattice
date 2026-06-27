using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Test helper that reads the records that actually ship from the
/// per-tree leaf write-ahead log. After the producer-side vector-clock
/// cache was removed, the shipped causal frontier is sourced entirely
/// from the leaf WAL (tailed by the background shipper), so integration
/// tests assert field fidelity against these records rather than against
/// a captured observer <see cref="WalRecord"/>. Walks every replog
/// partition backing a tree and returns the records in shard order.
/// </summary>
internal static class LeafWalReader
{
    // Matches LatticeReplicationOptions.DefaultReplogPartitions /
    // LatticeOptions.DefaultWalPartitions; the two-site fixtures leave
    // both at the package default, and writes are routed across
    // [0, partitions) by key hash, so reading this many shards captures
    // every record authored for a tree.
    private const int DefaultPartitions = 8;

    private const int PageSize = 256;

    /// <summary>
    /// Reads every record currently retained in the leaf WAL for
    /// <paramref name="treeId"/>, across all replog partitions, in
    /// ascending shard then sequence order.
    /// </summary>
    public static async Task<IReadOnlyList<WalRecord>> ReadAllAsync(
        IGrainFactory grainFactory,
        string treeId,
        int partitions = DefaultPartitions,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var records = new List<WalRecord>();
        for (var partition = 0; partition < partitions; partition++)
        {
            var grain = grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{partition}");
            long cursor = 0;
            while (true)
            {
                var page = await grain.ReadAsync(cursor, PageSize, cancellationToken);
                if (page.Entries.Count == 0)
                {
                    break;
                }

                foreach (var sequenced in page.Entries)
                {
                    records.Add(sequenced.Entry);
                }

                cursor = page.NextSequence;
            }
        }

        return records;
    }

    /// <summary>
    /// Polls the leaf WAL until at least <paramref name="minCount"/>
    /// records matching <paramref name="predicate"/> are present (or the
    /// retry budget is exhausted), then returns the matching records. The
    /// foreground commit-log writer batches its flush, so a bounded poll
    /// keeps field-fidelity assertions deterministic without depending on
    /// flush timing.
    /// </summary>
    public static async Task<IReadOnlyList<WalRecord>> WaitForRecordsAsync(
        IGrainFactory grainFactory,
        string treeId,
        Func<WalRecord, bool> predicate,
        int minCount = 1,
        int partitions = DefaultPartitions,
        int attempts = 100,
        int delayMs = 50,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        IReadOnlyList<WalRecord> matches = Array.Empty<WalRecord>();
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            var all = await ReadAllAsync(grainFactory, treeId, partitions, cancellationToken);
            matches = all.Where(predicate).ToList();
            if (matches.Count >= minCount)
            {
                return matches;
            }

            await Task.Delay(delayMs, cancellationToken);
        }

        return matches;
    }
}
