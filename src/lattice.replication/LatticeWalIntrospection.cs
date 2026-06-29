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

    // Bounded head window scanned per shard when grouping the oldest
    // retained entry by origin. Trim removes a contiguous prefix, so
    // the oldest retained entry of every origin that could have
    // fallen off the log clusters near the head; with a handful of
    // replicating clusters a small window captures every distinct
    // origin while keeping the probe at one bounded RPC per shard and
    // a correspondingly small page allocation.
    private const int OriginScanBudget = 64;

    private static readonly IReadOnlyDictionary<string, HybridLogicalClock> EmptyOriginMap =
        new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);

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

        // Fan out one ReadAsync(0, 1) per partition in parallel - for hosts
        // configured with a high partition count this turns N sequential
        // grain round-trips into a single concurrent batch.
        var pageTasks = new Task<WalShardPage>[partitions];
        for (var partition = 0; partition < partitions; partition++)
        {
            var grain = _grainFactory.GetGrain<IWalShardGrain>($"{treeName}/{partition}");
            // ReadAsync(0, 1) returns the head entry of the shard
            // post-trim: GC trims a contiguous prefix and the next read
            // from sequence 0 yields the oldest entry that survived.
            pageTasks[partition] = grain.ReadAsync(0, 1, cancellationToken).AsTask();
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

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetOldestAvailableHlcByOriginAsync(
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        var options = _optionsMonitor.Get(treeName);
        var partitions = options.ReplogPartitions;
        if (partitions <= 0)
        {
            return EmptyOriginMap;
        }

        var pageTasks = new Task<WalShardPage>[partitions];
        for (var partition = 0; partition < partitions; partition++)
        {
            var grain = _grainFactory.GetGrain<IWalShardGrain>($"{treeName}/{partition}");
            // Read a bounded head window so the oldest retained entry
            // of each distinct origin near the trim frontier is seen,
            // not just the single global-oldest head entry.
            pageTasks[partition] = grain.ReadAsync(0, OriginScanBudget, cancellationToken).AsTask();
        }

        var pages = await Task.WhenAll(pageTasks).ConfigureAwait(false);

        var localClusterId = options.ClusterId;
        var oldestByOrigin = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        foreach (var page in pages)
        {
            foreach (var sequenced in page.Entries)
            {
                var origin = sequenced.Entry.OriginClusterId;
                if (string.IsNullOrEmpty(origin))
                {
                    // Entries authored before origin stamping are
                    // attributed to the local cluster so they group
                    // as self-origin data (which the receiver-side
                    // probe skips - you never fall off your own log).
                    origin = localClusterId;
                }

                var ts = sequenced.Entry.Timestamp;
                if (!oldestByOrigin.TryGetValue(origin, out var existing)
                    || ts.CompareTo(existing) < 0)
                {
                    oldestByOrigin[origin] = ts;
                }
            }
        }

        return oldestByOrigin;
    }
}