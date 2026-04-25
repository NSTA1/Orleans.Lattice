using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IChangeFeed"/> implementation. Walks every WAL
/// partition for the requested tree, filters entries by HLC cursor and
/// origin, and yields the merged stream in HLC ascending order.
/// <para>
/// The implementation is pull-only: each call takes a snapshot of the
/// WAL at invocation time and completes when that snapshot is
/// exhausted. Consumers re-subscribe with an updated cursor to pick up
/// later commits. This matches the cursor-driven, pure-pull contract
/// in the replication design and avoids leaking transport-shaped acks
/// into the public surface.
/// </para>
/// <para>
/// Per-partition reads use a fixed page size (<see cref="PageSize"/>);
/// the merge is performed by collecting filtered entries into a single
/// list and sorting by <see cref="HybridLogicalClock"/>. This is
/// O(N log N) in the number of entries that pass the cursor filter and
/// is acceptable for the bootstrap-and-test use cases this seam
/// enables; the outbound shipper introduced in later phases will swap
/// to a streaming k-way merge if the change-feed consumer count grows.
/// </para>
/// <para>
/// The DeleteRange caveat documented on
/// <see cref="ReplogEntry.Timestamp"/> still applies: range-delete
/// entries carry <see cref="HybridLogicalClock.Zero"/>, so a
/// non-<c>Zero</c> cursor filters them out. This is a pre-existing
/// property of the ReplogEntry shape, not a property of the change
/// feed itself, and is fixed at the ReplogEntry layer in a later phase.
/// </para>
/// </summary>
internal sealed class ChangeFeed(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options) : IChangeFeed
{
    private const int PageSize = 256;

    /// <inheritdoc />
    public async IAsyncEnumerable<ReplogEntry> Subscribe(
        string treeName,
        HybridLogicalClock cursor,
        bool includeLocalOrigin = true,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeName);

        var resolved = options.Get(treeName);
        var partitions = resolved.ReplogPartitions;
        var localClusterId = resolved.ClusterId;

        var collected = new List<ReplogEntry>();
        for (var partition = 0; partition < partitions; partition++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var grain = grainFactory.GetGrain<IReplogShardGrain>($"{treeName}/{partition}");
            var nextSequence = 0L;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var page = await grain.ReadAsync(nextSequence, PageSize, cancellationToken).ConfigureAwait(false);
                var pageEntries = page.Entries;
                if (pageEntries.Count == 0)
                {
                    break;
                }

                for (var i = 0; i < pageEntries.Count; i++)
                {
                    var entry = pageEntries[i].Entry;
                    if (entry.Timestamp <= cursor)
                    {
                        continue;
                    }

                    if (!includeLocalOrigin
                        && entry.OriginClusterId is { } origin
                        && string.Equals(origin, localClusterId, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    collected.Add(entry);
                }

                nextSequence = page.NextSequence;
                if (pageEntries.Count < PageSize)
                {
                    break;
                }
            }
        }

        collected.Sort(static (a, b) => a.Timestamp.CompareTo(b.Timestamp));

        for (var i = 0; i < collected.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return collected[i];
        }
    }
}
