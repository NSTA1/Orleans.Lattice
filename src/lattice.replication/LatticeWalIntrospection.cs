using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ILatticeWalIntrospection"/> implementation.
/// Walks each <see cref="IWalShardGrain"/> activation backing the
/// named tree, fetches the head entry of every shard, and returns
/// the minimum timestamp across the heads.
/// </summary>
internal sealed class LatticeWalIntrospection(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor)
    : ILatticeWalIntrospection
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor =
        optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));

    /// <inheritdoc />
    public async Task<HybridLogicalClock?> GetOldestAvailableHlcAsync(
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        var partitions = _optionsMonitor.Get(treeName).ReplogPartitions;
        if (partitions <= 0)
        {
            return null;
        }

        // Fan out one ReadAsync(0, 1) per partition in parallel — for hosts
        // configured with a high partition count this turns N sequential
        // grain round-trips into a single concurrent batch.
        var pageTasks = new Task<WalShardPage>[partitions];
        for (var partition = 0; partition < partitions; partition++)
        {
            var grain = _grainFactory.GetGrain<IWalShardGrain>($"{treeName}/{partition}");
            // ReadAsync(0, 1) returns the head entry of the shard
            // post-trim: GC trims a contiguous prefix and the next read
            // from sequence 0 yields the oldest entry that survived.
            pageTasks[partition] = grain.ReadAsync(0, 1, cancellationToken);
        }

        var pages = await Task.WhenAll(pageTasks).ConfigureAwait(false);

        HybridLogicalClock? oldest = null;
        foreach (var page in pages)
        {
            if (page.Entries.Count == 0)
            {
                continue;
            }

            var head = page.Entries[0].Entry.Timestamp;
            if (oldest is null || head.CompareTo(oldest.Value) < 0)
            {
                oldest = head;
            }
        }

        return oldest;
    }
}