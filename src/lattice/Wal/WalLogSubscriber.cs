using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Wal;

/// <summary>
/// Default <see cref="IWalSubscriber"/>. Implements the generic per-shard WAL
/// tailing loop on top of the internal <see cref="ICommitLogReader"/> read path
/// and the <see cref="IWalCursorRegistry"/> pin. Stateless and safe for
/// concurrent use across consumers - all per-drain state lives in the supplied
/// <see cref="WalSubscriptionContext"/> and the locals of a single
/// <see cref="DrainAsync"/> call.
/// </summary>
internal sealed class WalLogSubscriber(
    ICommitLogReader commitLogReader,
    IWalCursorRegistry cursorRegistry) : IWalSubscriber
{
    /// <inheritdoc />
    public async Task<WalDrainResult> DrainAsync(
        WalSubscriptionContext context,
        IWalSubscriptionHandler handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(handler);

        var sourceTreeId = context.SourceTreeId;
        var partitions = context.Partitions;
        var checkpoints = context.Checkpoints;

        // Fall-off-log guard: if any partition's oldest still-readable offset has
        // advanced past our next-to-read position, the entries we need were
        // trimmed and the consumer must rebuild from current source state rather
        // than tail-replay. Surface nothing and advance nothing on this pass.
        for (var partition = 0; partition < partitions; partition++)
        {
            var checkpoint = checkpoints.GetValueOrDefault(partition, -1);

            // Idle fast-path: a consumer already caught up to the head can never
            // have fallen off the log. The oldest still-readable offset is always
            // at or below the head, so once checkpoint + 1 >= head the trailing
            // edge sits at or behind our cursor and the fall-off probe is provably
            // unnecessary. GetHeadOffsetAsync is served from the WAL grain's
            // in-memory next-sequence cursor, whereas GetTailOffsetAsync is a
            // storage round-trip (a manifest range scan against Azure Table
            // Storage); skipping it here removes the per-partition read that
            // otherwise fires on every idle drain tick - the dominant idle load,
            // since each maintainer polls every partition on a fixed cadence.
            var head = await commitLogReader
                .GetHeadOffsetAsync(sourceTreeId, partition, cancellationToken)
                .ConfigureAwait(false);
            if (checkpoint + 1 >= head)
            {
                continue;
            }

            var tail = await commitLogReader
                .GetTailOffsetAsync(sourceTreeId, partition, cancellationToken)
                .ConfigureAwait(false);
            if (tail > checkpoint + 1)
            {
                return new WalDrainResult { FellOffLog = true };
            }
        }

        var batchSize = context.BatchSize > 0
            ? context.BatchSize
            : WalSubscriptionContext.DefaultBatchSize;
        var shardFilter = context.ShardIndexFilter;
        var skipMaintenance = context.MaintenancePolicy == WalMaintenancePolicy.Skip;

        var advancedOffsets = new Dictionary<int, long>(partitions);
        var highest = context.HighestApplied;
        long entriesRead = 0;
        long entriesSurfaced = 0;

        for (var partition = 0; partition < partitions; partition++)
        {
            var checkpoint = checkpoints.GetValueOrDefault(partition, -1);
            var lastOffset = checkpoint;
            var readThisPartition = 0;

            await foreach (var (offset, mutation) in commitLogReader
                .ReadAsync(sourceTreeId, partition, checkpoint, cancellationToken)
                .ConfigureAwait(false))
            {
                lastOffset = offset;
                readThisPartition++;
                entriesRead++;

                // Advance the consumed-HLC high-water mark for every entry read
                // past, applicable or not, so a skipped maintenance / sibling-shard
                // entry at the source head does not leave the cursor stuck below a
                // non-surfaced head.
                if (mutation.Timestamp > highest)
                {
                    highest = mutation.Timestamp;
                }

                // ShardIndex partition filtering: a consumer pinned to one logical
                // shard skips sibling chain-shard entries that share the physical
                // WAL partition. The cursor still advances past them above.
                if (shardFilter is { } wanted && mutation.ShardIndex != wanted)
                {
                    if (readThisPartition >= batchSize)
                    {
                        break;
                    }

                    continue;
                }

                // Maintenance filtering: structural rewrites are replays of state
                // already authored by user writes; under Skip they never reach the
                // handler but the cursor still advances.
                if (skipMaintenance && mutation.Category == MutationCategory.Maintenance)
                {
                    if (readThisPartition >= batchSize)
                    {
                        break;
                    }

                    continue;
                }

                var entry = new WalSubscriptionEntry(partition, offset, mutation);
                handler.OnEntry(in entry);
                entriesSurfaced++;

                if (readThisPartition >= batchSize)
                {
                    break;
                }
            }

            // Only record an advance when the partition actually moved, so a
            // partition that read nothing is absent from the merged checkpoint.
            if (lastOffset != checkpoint)
            {
                advancedOffsets[partition] = lastOffset;
            }
        }

        if (context.PinWal)
        {
            var blockedAtHlc = handler.BlockedAtHlc;
            if (highest > HybridLogicalClock.Zero || blockedAtHlc is not null)
            {
                await cursorRegistry
                    .ReportCursorAsync(sourceTreeId, context.ConsumerId, highest, blockedAtHlc, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        return new WalDrainResult
        {
            FellOffLog = false,
            EntriesRead = entriesRead,
            EntriesSurfaced = entriesSurfaced,
            HighestTimestamp = highest,
            AdvancedOffsets = advancedOffsets,
        };
    }
}
