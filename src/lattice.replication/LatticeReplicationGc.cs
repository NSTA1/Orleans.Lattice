using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ILatticeReplicationGc"/> implementation. Walks
/// every WAL partition for the named tree from the head, finds the
/// largest contiguous prefix whose entries satisfy the GC predicate,
/// and asks the configured <see cref="IWalStorageProvider"/> to trim
/// through that offset.
/// <para>
/// The predicate combines two conditions, joined as a logical OR:
/// <list type="bullet">
///   <item>
///     <b>Cursor</b> - <c>entry.Timestamp &lt;= minCursor</c> when the
///     <see cref="ILatticeReplicationCursorRegistry"/> reports a
///     non-<see langword="null"/> minimum across registered consumers.
///   </item>
///   <item>
///     <b>TTL ceiling</b> - when
///     <see cref="LatticeReplicationOptions.WalRetention"/> is set,
///     the predicate also accepts entries whose
///     <see cref="HybridLogicalClock.WallClockTicks"/> is older than
///     <c>now - WalRetention</c>. A lagging consumer that pins the log
///     past the ceiling is intentionally allowed to "fall off the log"
///     so disk usage stays bounded; that consumer will detect the gap
///     on its next read and re-bootstrap (later phase).
///   </item>
/// </list>
/// </para>
/// <para>
/// The scan stops at the first non-eligible entry: WAL offsets are
/// dense and append-only, but HLC <see cref="HybridLogicalClock.WallClockTicks"/>
/// is mostly-monotonic-with-skew, so a strictly conservative
/// "stop at first miss" walk preserves correctness while a more
/// aggressive scan would risk trimming an entry younger than a still-
/// pinned later entry. The conservative shape is sufficient: as the
/// minimum cursor advances, subsequent passes pick up the entries
/// skipped this round.
/// </para>
/// </summary>
public sealed class LatticeReplicationGc(
    IServiceProvider services,
    ILatticeReplicationCursorRegistry cursors,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    TimeProvider? timeProvider = null) : ILatticeReplicationGc
{
    /// <summary>Page size for reading the head of each shard during the scan.</summary>
    private const int ScanPageSize = 256;

    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <inheritdoc />
    public async Task<ReplicationGcReport> RunOnceAsync(
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = optionsMonitor.Get(treeName);
        var partitions = resolved.ReplogPartitions;
        var provider = resolved.WalStorageProvider?.Invoke(treeName)
            ?? services.GetRequiredService<IWalStorageProvider>();

        var minCursor = await cursors.GetMinCursorAsync(treeName, cancellationToken).ConfigureAwait(false);
        HybridLogicalClock? ttlCeiling = null;
        if (resolved.WalRetention is { } retention)
        {
            var nowTicks = _time.GetUtcNow().UtcTicks;
            var ceilingTicks = nowTicks - retention.Ticks;
            // The TTL ceiling is "every entry whose wall-clock time is
            // older than this is trim-eligible". We model it as an HLC
            // whose Counter is int.MaxValue so a strict <= comparison
            // against an entry HLC means "the entry's WallClockTicks
            // is < ceilingTicks, OR it equals ceilingTicks and the
            // entry's Counter is anything <= int.MaxValue". This avoids
            // a separate WallClockTicks accessor while preserving the
            // intended semantics.
            ttlCeiling = new HybridLogicalClock { WallClockTicks = ceilingTicks, Counter = int.MaxValue };
        }

        // Range-delete entries carry HybridLogicalClock.Zero by design;
        // a min cursor that is itself Zero (or unset) must not flush
        // them out the moment they land. The cursor branch is therefore
        // gated on minCursor > Zero, which is enforced by the registry's
        // ReportCursorAsync precondition.
        var hasCursorPredicate = minCursor is { } mc && mc > HybridLogicalClock.Zero;
        var hasTtlPredicate = ttlCeiling is not null;

        if (!hasCursorPredicate && !hasTtlPredicate)
        {
            // Nothing to do: no consumer has reported a cursor and no
            // TTL is configured. Return early so the run is observably
            // a no-op (counter is zero, ShipDuration is unaffected).
            return new ReplicationGcReport(treeName, minCursor, ttlCeiling, partitions, 0);
        }

        long totalTrimmed = 0;
        for (var partition = 0; partition < partitions; partition++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            totalTrimmed += await TrimShardAsync(provider, treeName, partition, minCursor, ttlCeiling, cancellationToken).ConfigureAwait(false);
        }

        if (totalTrimmed > 0)
        {
            LatticeReplicationMetrics.WalEntriesTrimmed.Add(
                totalTrimmed,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName));
        }

        return new ReplicationGcReport(treeName, minCursor, ttlCeiling, partitions, totalTrimmed);
    }

    private static async Task<long> TrimShardAsync(
        IWalStorageProvider provider,
        string treeId,
        int shardIndex,
        HybridLogicalClock? minCursor,
        HybridLogicalClock? ttlCeiling,
        CancellationToken cancellationToken)
    {
        long lastEligibleOffset = -1;
        long fromOffsetExclusive = -1;
        long eligibleCount = 0;
        var stop = false;

        while (!stop)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var pageEntries = 0;
            var lastSeenOffset = fromOffsetExclusive;
            await foreach (var walEntry in provider
                .ReadAsync(treeId, shardIndex, fromOffsetExclusive, ScanPageSize, cancellationToken)
                .ConfigureAwait(false))
            {
                pageEntries++;
                lastSeenOffset = walEntry.Offset;

                if (IsEligible(walEntry.Entry.Timestamp, minCursor, ttlCeiling))
                {
                    lastEligibleOffset = walEntry.Offset;
                    eligibleCount++;
                }
                else
                {
                    // First non-eligible entry stops the scan: offsets
                    // are dense and the conservative shape forbids
                    // jumping over a pinned entry to trim a later one.
                    stop = true;
                    break;
                }
            }

            if (pageEntries == 0)
            {
                // Provider exhausted; nothing more to scan.
                break;
            }

            if (pageEntries < ScanPageSize)
            {
                // Partial final page; the entire log up to this point
                // was eligible (no `stop = true` hit) and there are no
                // further entries to consider.
                break;
            }

            // Full eligible page; advance the cursor and keep walking.
            fromOffsetExclusive = lastSeenOffset;
        }

        if (lastEligibleOffset < 0)
        {
            return 0;
        }

        await provider.TrimAsync(treeId, shardIndex, lastEligibleOffset, cancellationToken).ConfigureAwait(false);
        return eligibleCount;
    }

    private static bool IsEligible(
        HybridLogicalClock entryTimestamp,
        HybridLogicalClock? minCursor,
        HybridLogicalClock? ttlCeiling)
    {
        if (minCursor is { } mc && mc > HybridLogicalClock.Zero && entryTimestamp <= mc)
        {
            return true;
        }
        if (ttlCeiling is { } ceiling && entryTimestamp <= ceiling)
        {
            return true;
        }
        return false;
    }
}

