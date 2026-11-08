using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
/// <para>
/// The one exception is the empty-backlog streak (issue #6036): a partition
/// whose readable head says entries are waiting past a consumer's checkpoint,
/// but whose read returns nothing, is metered on every drain and, once that has
/// persisted for <see cref="EmptyBacklogEscalationAge"/>, logged at Warning and
/// sent a trim-watermark refresh. The streak is the only cross-drain state the
/// subscriber keeps, and it is empty while every read returns what the head
/// promises.
/// </para>
/// </summary>
internal sealed class WalLogSubscriber(
    ICommitLogReader commitLogReader,
    IWalCursorRegistry cursorRegistry,
    ILogger<WalLogSubscriber>? logger = null) : IWalSubscriber
{
    /// <summary>
    /// How long a partition must keep reading empty against a waiting head before
    /// the subscriber escalates. Twice the Azure Table provider's default phase-2
    /// commit timeout (12 s), plus margin, so the legitimate lag between a batch
    /// becoming readable at the head and its manifest row landing never escalates.
    /// </summary>
    internal static readonly TimeSpan EmptyBacklogEscalationAge = TimeSpan.FromSeconds(30);

    /// <summary>Consecutive empty-backlog reads required, as well as the age, before escalating.</summary>
    internal const int EmptyBacklogEscalationReads = 3;

    private readonly ILogger _logger = logger ?? NullLogger<WalLogSubscriber>.Instance;

    private readonly ConcurrentDictionary<(string Tree, string Consumer, int Partition), EmptyBacklogStreak> _emptyBacklogStreaks = new();

    /// <summary>The clock the empty-backlog streak is aged on; tests replace it.</summary>
    internal TimeProvider Clock { get; init; } = TimeProvider.System;

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
        // The readable head of each partition with a backlog, 0 for none: a
        // backlog means head > checkpoint + 1 >= 0, so a backlogged head is never 0.
        var backlogHeads = new long[partitions];

        // Fall-off-log guard: if any partition's oldest still-readable offset has
        // advanced past our next-to-read position, the entries we need were
        // trimmed and the consumer must rebuild from current source state rather
        // than tail-replay. Surface nothing and advance nothing on this pass.
        for (var partition = 0; partition < partitions; partition++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var checkpoint = checkpoints.GetValueOrDefault(partition, -1);

            // Idle fast-path: a consumer already caught up to the head can never
            // have fallen off the log. The oldest still-readable offset is always
            // at or below the head, so once checkpoint + 1 >= head the trailing
            // edge sits at or behind our cursor and the fall-off probe is provably
            // unnecessary. GetHeadOffsetAsync is served from the WAL grain's
            // durable readable prefix (not its allocator), whereas GetTailOffsetAsync is a
            // storage round-trip (a manifest range scan against Azure Table
            // Storage); skipping it here removes the per-partition read that
            // otherwise fires on every idle drain tick - the dominant idle load,
            // since each maintainer polls every partition on a fixed cadence.
            var head = await commitLogReader
                .GetHeadOffsetAsync(sourceTreeId, partition, cancellationToken)
                .ConfigureAwait(false);
            if (checkpoint + 1 >= head)
            {
                ClearEmptyBacklogStreak(context, partition);
                continue;
            }

            backlogHeads[partition] = head;
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
            cancellationToken.ThrowIfCancellationRequested();
            // No cursor moves for an idle partition. Writes arriving after its
            // head probe remain eligible on the next drain, including late flushes.
            if (backlogHeads[partition] == 0)
            {
                continue;
            }

            var checkpoint = checkpoints.GetValueOrDefault(partition, -1);
            var lastOffset = checkpoint;
            var readThisPartition = 0;

            await foreach (var (offset, mutation) in commitLogReader
                .ReadAsync(sourceTreeId, partition, checkpoint, cancellationToken)
                .ConfigureAwait(false))
            {
                // Read-side fall-off check (issues #4584, #4579). The guard above
                // probes the tail before the read, so a trim landing between the
                // probe and the read, or between two pages of it, would hand back
                // the first retained entry past a trimmed range and this consumer
                // would advance across entries it never saw. Offsets are dense in
                // what the log hands out, so a jump is either that trim or a slot
                // whose flush failed and was never acknowledged (a resync after a
                // failed slot can leave one). Only the tail, probed after the
                // jump was observed, tells them apart: a tail past the next offset
                // needed is a trim, and the consumer has fallen off the log.
                if (offset > lastOffset + 1)
                {
                    var tailAfterRead = await commitLogReader
                        .GetTailOffsetAsync(sourceTreeId, partition, cancellationToken)
                        .ConfigureAwait(false);
                    if (tailAfterRead > lastOffset + 1)
                    {
                        return new WalDrainResult { FellOffLog = true };
                    }
                }

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

            if (readThisPartition == 0)
            {
                await NoteEmptyBacklogReadAsync(context, partition, checkpoint, backlogHeads[partition], cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                ClearEmptyBacklogStreak(context, partition);
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
                if (highest > HybridLogicalClock.Zero)
                {
                    for (var partition = 0; partition < partitions; partition++)
                    {
                        var checkpoint = advancedOffsets.TryGetValue(partition, out var advanced)
                            ? advanced : checkpoints.GetValueOrDefault(partition, -1);
                        await cursorRegistry.ReportCheckpointAsync(
                            sourceTreeId, context.ConsumerId, highest, partition, checkpoint, cancellationToken)
                            .ConfigureAwait(false);
                    }
                }
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

    /// <summary>
    /// Meters a read that returned nothing although the partition's readable head
    /// was past the checkpoint, and escalates once that has persisted for both
    /// <see cref="EmptyBacklogEscalationReads"/> reads and
    /// <see cref="EmptyBacklogEscalationAge"/>: it logs at Warning and has the
    /// partition re-derive its trim watermark from storage, which un-hides entries
    /// a cached watermark above the stored trim point was clamping away. Not a
    /// fall-off: an empty read is legitimate while a phase-2 commit lands, and a
    /// rebuild on every one would storm.
    /// </summary>
    private async Task NoteEmptyBacklogReadAsync(
        WalSubscriptionContext context,
        int partition,
        long checkpoint,
        long head,
        CancellationToken cancellationToken)
    {
        LatticeMetrics.WalSubscriberEmptyBacklogReads.Add(
            1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, context.SourceTreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, partition),
            LatticeTenantLabel.ForTree(context.SourceTreeId));

        var now = Clock.GetUtcNow();
        var key = (context.SourceTreeId, context.ConsumerId, partition);
        var streak = _emptyBacklogStreaks.AddOrUpdate(
            key,
            static (_, at) => new EmptyBacklogStreak(at, 1),
            static (_, current, _) => current with { Reads = current.Reads + 1 },
            now);
        if (streak.Reads < EmptyBacklogEscalationReads || now - streak.Since < EmptyBacklogEscalationAge)
        {
            return;
        }

        // Restart the window, so a partition that stays unreadable escalates again
        // one age later rather than on every drain.
        _emptyBacklogStreaks[key] = new EmptyBacklogStreak(now, 0);
        _logger.LogWarning(
            "WAL partition {Partition} of '{Tree}' has read empty for consumer '{Consumer}' for {Age} across {Reads} drains "
            + "although its readable head ({Head}) is past the checkpoint ({Checkpoint}); refreshing its trim watermark.",
            partition, context.SourceTreeId, context.ConsumerId, now - streak.Since, streak.Reads, head, checkpoint);
        try
        {
            await commitLogReader.RefreshTrimWatermarkAsync(context.SourceTreeId, partition, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A silo that predates the refresh cannot answer it; the next escalation retries.
            _logger.LogWarning(ex, "Refreshing the trim watermark of WAL partition {Partition} of '{Tree}' failed.",
                partition, context.SourceTreeId);
        }
    }

    private void ClearEmptyBacklogStreak(WalSubscriptionContext context, int partition)
    {
        if (!_emptyBacklogStreaks.IsEmpty)
        {
            _emptyBacklogStreaks.TryRemove((context.SourceTreeId, context.ConsumerId, partition), out _);
        }
    }

    private readonly record struct EmptyBacklogStreak(DateTimeOffset Since, int Reads);
}
