using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The production <see cref="IWalReReplaySource"/>: reads retained
/// write-ahead-log entries from the local shard's WAL partition grains. It
/// addresses them as <c>{treeName}/{partition}</c> using exactly the tree name it
/// is constructed with, and does not resolve a registry alias. The outbound
/// shipper instead addresses the WAL by the tree's resolved physical id, which is
/// what the WAL is keyed by after an alias swap (shadow-cutover restore, resize,
/// schema remediation, or operator alias change), so for an aliased tree constructed with its logical name this source
/// reads the partitions of that name rather than the physical tree's. It reads
/// oldest-first per partition up to a bounded budget shared across partitions; a
/// partition whose oldest retained entry sits at a sequence greater than zero is
/// reported as trimmed so the engine can detect a garbage-collected-past-divergence
/// gap, including a partition an earlier one left no budget to read, whose oldest
/// retained entry is probed without being collected.
/// Strictly read-only.
/// </summary>
internal sealed class WalGrainReReplaySource(
    IGrainFactory grainFactory,
    string treeName,
    int partitionCount,
    int pageSize) : IWalReReplaySource
{
    /// <inheritdoc />
    public async ValueTask<WalReReplayReadResult> ReadAsync(CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(treeName);

        var partitions = Math.Max(1, partitionCount);
        var page = Math.Max(1, pageSize);
        var budget = partitions * page;

        var collected = new List<WalRecord>();
        var wasTrimmed = false;
        var haveOldest = false;
        HybridLogicalClock oldest = default;

        for (var p = 0; p < partitions; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var grain = grainFactory.GetGrain<IWalShardGrain>($"{treeName}/{p}");
            var cursor = 0L;
            var first = true;

            // The budget is shared, so an earlier partition can exhaust it before
            // this one is read. Its trim point is still probed (one entry, not
            // collected): an unexamined partition would read as untrimmed and let
            // the repair proceed past a gap its WAL can no longer fill.
            while (first || collected.Count < budget)
            {
                var probeOnly = collected.Count >= budget;
                var result = await grain.ReadAsync(cursor, probeOnly ? 1 : page, cancellationToken).ConfigureAwait(false);
                if (result.Entries.Count == 0)
                {
                    break;
                }

                if (first && result.Entries[0].Sequence > 0)
                {
                    // The provider returned the oldest *retained* offset rather
                    // than offset 0, so this partition's tail was trimmed.
                    wasTrimmed = true;
                    var firstHlc = result.Entries[0].Entry.Timestamp;
                    if (!haveOldest || firstHlc < oldest)
                    {
                        oldest = firstHlc;
                        haveOldest = true;
                    }
                }
                first = false;

                if (probeOnly)
                {
                    break;
                }

                foreach (var sequenced in result.Entries)
                {
                    collected.Add(sequenced.Entry);
                }

                if (result.Entries.Count < page)
                {
                    break; // Reached the end of this partition's retained log.
                }
                cursor = result.NextSequence;
            }
        }

        return new WalReReplayReadResult
        {
            Entries = collected,
            WasTrimmed = wasTrimmed,
            OldestRetainedHlc = haveOldest ? oldest : HybridLogicalClock.Zero,
        };
    }
}
