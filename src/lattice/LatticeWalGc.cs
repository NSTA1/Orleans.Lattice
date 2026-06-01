using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Default <see cref="ILatticeWalGc"/> implementation. Walks
/// every WAL partition for the named tree from the head, finds the
/// largest contiguous prefix whose entries satisfy the GC predicate,
/// and asks the configured <see cref="IWalStorageProvider"/> to trim
/// through that offset.
/// <para>
/// The predicate combines four conditions:
/// <list type="bullet">
///   <item>
///     <b>Cursor</b> - <c>entry.Timestamp &lt;= minCursor</c> when the
///     <see cref="IWalCursorRegistry"/> reports a
///     non-<see langword="null"/> minimum across registered consumers.
///   </item>
///   <item>
///     <b>TTL ceiling</b> - when
///     <see cref="LatticeOptions.WalRetention"/> is set,
///     the predicate also accepts entries whose
///     <see cref="HybridLogicalClock.WallClockTicks"/> is older than
///     <c>now - WalRetention</c>. A lagging consumer that pins the log
///     past the ceiling is intentionally allowed to "fall off the log"
///     so disk usage stays bounded; that consumer will detect the gap
///     on its next read and re-bootstrap via the fall-off-log detector.
///   </item>
///   <item>
///     <b>Causal-stable frontier</b> - when at least one consumer has
///     reported a per-origin <see cref="VersionVector"/> through the
///     causal+ overload of
///     <see cref="IWalCursorRegistry.ReportCursorAsync(string, string, HybridLogicalClock, VersionVector, CancellationToken)"/>,
///     the GC AND-s
///     <c>causalStable.DominatesOrEquals(entry.VectorClock)</c> into
///     the predicate. The cursor / TTL branches above remain for
///     safety: an entry must satisfy the HLC-shaped clauses AND the
///     causal-stable clause before it is trimmed. When no consumer has
///     reported a vector the GC degrades cleanly to the legacy
///     HLC-only predicate.
///   </item>
///   <item>
///     <b>Blocked-floor</b> - when at least one consumer has
///     reported a non-<see langword="null"/> <c>BlockedAtHlc</c> pin
///     through the blocked-floor overloads of
///     <see cref="IWalCursorRegistry.ReportCursorAsync(string, string, HybridLogicalClock, HybridLogicalClock?, CancellationToken)"/>,
///     the GC AND-s a strict-less <c>entry.Timestamp &lt; blockedFloor</c>
///     clause where <c>blockedFloor = min(BlockedAtHlc across reporting
///     consumers)</c>. The strict-less semantics protect the buffered
///     entry itself from being trimmed: a partial atomic batch with
///     lowest staged HLC <c>t</c> reports <c>blockedFloor=t</c> and
///     blocks the trim of every WAL row at or after <c>t</c> until
///     the buffer drains or the batch is evicted (later phase). When no
///     consumer reports a pin the GC degrades cleanly to the cursor /
///     TTL / causal-stable branches.
///   </item>
/// </list>
/// </para>
/// <para>
/// Legacy / range-delete entries with a <see langword="null"/>
/// <see cref="LatticeMutation.VectorClock"/> are treated as the empty VC,
/// which is dominated by every non-<see langword="null"/> causal-stable
/// frontier and therefore passes the causal-stable clause without
/// blocking the existing HLC-shaped trim path.
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
public sealed class LatticeWalGc(
    IServiceProvider services,
    IWalCursorRegistry cursors,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    TimeProvider? timeProvider = null) : ILatticeWalGc
{
    /// <summary>Page size for reading the head of each shard during the scan.</summary>
    private const int ScanPageSize = 256;

    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    // Resolved lazily so a host that never registered the storage-usage
    // sink (or replaced the WAL GC in isolation in a unit test) still works;
    // the over-threshold gauge is simply not driven by the GC in that case.
    private LatticeStorageUsageMetrics? _storageMetricsCache;
    private bool _storageMetricsResolved;

    private LatticeStorageUsageMetrics? _storageMetrics
    {
        get
        {
            if (!_storageMetricsResolved)
            {
                _storageMetricsCache = services.GetService<LatticeStorageUsageMetrics>();
                _storageMetricsResolved = true;
            }
            return _storageMetricsCache;
        }
    }

    /// <inheritdoc />
    public async Task<LatticeWalGcReport> RunOnceAsync(
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = optionsMonitor.Get(treeName);
        var partitions = resolved.WalPartitions;
        var provider = resolved.WalStorageProvider?.Invoke(treeName)
            ?? services.GetRequiredService<IWalStorageProvider>();

        var minCursor = await cursors.GetMinCursorAsync(treeName, cancellationToken).ConfigureAwait(false);
        var causalStable = await cursors.GetCausalStableAsync(treeName, cancellationToken).ConfigureAwait(false);
        var blockedFloor = await cursors.GetBlockedFloorAsync(treeName, cancellationToken).ConfigureAwait(false);
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

        // Sample retained bytes once up front so a byte-pressure trigger is
        // decided against the pre-trim footprint. Returns null when the
        // policy is disabled or the provider does not support byte accounting.
        var (ceiling, retainedBefore) = await SampleRetainedBytesAsync(
            provider, resolved, treeName, partitions, cancellationToken).ConfigureAwait(false);
        var triggered = retainedBefore is { } before && ceiling is { } cap && before > cap;
        if (triggered)
        {
            LatticeMetrics.StoragePolicyTrimTriggered.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName),
                LatticeMetrics.ReasonBytePressure);
        }

        if (!hasCursorPredicate && !hasTtlPredicate)
        {
            // Nothing to do: no consumer has reported a cursor and no
            // TTL is configured. Return early so the run is observably
            // a no-op (counter is zero, ShipDuration is unaffected).
            // Neither the causal-stable frontier nor the
            // blocked-floor alone permits trimming - they only block
            // entries that the HLC-shaped clauses would otherwise
            // allow. So a present-but-unused frontier or floor is
            // still reported in the diagnostic for transparency. The
            // byte-pressure policy is still evaluated: a tree over its
            // ceiling with no consumer cursor is the canonical
            // "lagging consumer pins the WAL" advisory case where the
            // breach is published but no bytes are reclaimed.
            var over0 = FinishBytePressure(treeName, ceiling, retainedBefore, retainedBefore);
            return new LatticeWalGcReport(
                treeName, minCursor, ttlCeiling, causalStable, blockedFloor, partitions, 0,
                ceiling, retainedBefore, retainedBefore, triggered, over0);
        }

        long totalTrimmed = 0;
        for (var partition = 0; partition < partitions; partition++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            totalTrimmed += await TrimShardAsync(provider, treeName, partition, minCursor, ttlCeiling, causalStable, blockedFloor, cancellationToken).ConfigureAwait(false);
        }

        if (totalTrimmed > 0)
        {
            LatticeMetrics.WalEntriesTrimmed.Add(
                totalTrimmed,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName));
        }

        var (_, retainedAfter) = await SampleRetainedBytesAsync(
            provider, resolved, treeName, partitions, cancellationToken).ConfigureAwait(false);
        var overThreshold = FinishBytePressure(treeName, ceiling, retainedBefore, retainedAfter);

        return new LatticeWalGcReport(
            treeName, minCursor, ttlCeiling, causalStable, blockedFloor, partitions, totalTrimmed,
            ceiling, retainedBefore, retainedAfter, triggered, overThreshold);
    }

    /// <summary>
    /// Emits the byte-pressure reclaim and over-threshold signals after a
    /// trim pass and returns whether the post-trim footprint still breaches
    /// the ceiling. When a byte-pressure trigger reclaimed bytes
    /// (<paramref name="retainedBefore"/> &gt; <paramref name="retainedAfter"/>),
    /// increments <see cref="LatticeMetrics.StoragePolicyBytesReclaimed"/> by
    /// the freed byte count. Also pushes the over-threshold flag to the
    /// observable storage gauge so the 0/1 series tracks the WAL GC's own
    /// sampling between aggregator scrapes. Returns <see langword="false"/>
    /// when the policy is disabled or byte accounting is unsupported.
    /// </summary>
    private bool FinishBytePressure(string treeName, long? ceiling, long? retainedBefore, long? retainedAfter)
    {
        if (ceiling is not { } cap || retainedAfter is not { } after)
        {
            return false;
        }

        if (retainedBefore is { } before && before > after)
        {
            LatticeMetrics.StoragePolicyBytesReclaimed.Add(
                before - after,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName));
        }

        var over = after > cap;
        _storageMetrics?.PublishOverThreshold(treeName, over);
        return over;
    }

    /// <summary>
    /// Samples the advisory WAL byte-pressure inputs: the configured ceiling
    /// (<see cref="LatticeOptions.WalMaxRetainedBytes"/>) and the retained-byte
    /// total summed across every partition. Returns <c>(null, null)</c> when
    /// the policy is disabled and <c>(ceiling, null)</c> when the provider does
    /// not support byte accounting (every partition returned the <c>-1</c>
    /// sentinel). The policy never trims past the safe frontier; the sampled
    /// total only feeds the advisory report and metrics.
    /// </summary>
    private static async Task<(long? Ceiling, long? Retained)> SampleRetainedBytesAsync(
        IWalStorageProvider provider,
        LatticeOptions resolved,
        string treeName,
        int partitions,
        CancellationToken cancellationToken)
    {
        if (resolved.WalMaxRetainedBytes is not { } ceiling || ceiling <= 0)
        {
            // Policy disabled - zero hot-path cost.
            return (null, null);
        }

        long retained = 0;
        var anySupported = false;
        for (var partition = 0; partition < partitions; partition++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var bytes = await provider.GetRetainedByteSizeAsync(treeName, partition, cancellationToken).ConfigureAwait(false);
            if (bytes < 0)
            {
                // -1 sentinel: this partition's provider does not support
                // byte accounting. Skip it; if every partition is
                // unsupported the policy reports "no data".
                continue;
            }
            anySupported = true;
            retained += bytes;
        }

        return anySupported ? (ceiling, retained) : (ceiling, null);
    }

    private static async Task<long> TrimShardAsync(
        IWalStorageProvider provider,
        string treeId,
        int shardIndex,
        HybridLogicalClock? minCursor,
        HybridLogicalClock? ttlCeiling,
        VersionVector? causalStable,
        HybridLogicalClock? blockedFloor,
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

                if (IsEligible(walEntry.Mutation, minCursor, ttlCeiling, causalStable, blockedFloor))
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
        LatticeMutation entry,
        HybridLogicalClock? minCursor,
        HybridLogicalClock? ttlCeiling,
        VersionVector? causalStable,
        HybridLogicalClock? blockedFloor)
    {
        // HLC-shaped clause: cursor OR TTL must accept the entry
        // (existing legacy HLC-only behaviour).
        var hlcAccepted = false;
        if (minCursor is { } mc && mc > HybridLogicalClock.Zero && entry.Timestamp <= mc)
        {
            hlcAccepted = true;
        }
        else if (ttlCeiling is { } ceiling && entry.Timestamp <= ceiling)
        {
            hlcAccepted = true;
        }

        if (!hlcAccepted)
        {
            return false;
        }

        // Causal-stable clause: when at least one consumer has reported
        // a per-origin frontier, the entry's VectorClock must be
        // dominated by it. A null entry.VectorClock means the entry
        // pre-dates causal+ stamping (legacy peer or hand-constructed
        // test entry) or carries the empty frontier by design (range
        // delete) - both are dominated by every non-null frontier.
        // When causalStable itself is null, no consumer has reported a
        // vector and the GC degrades cleanly to the HLC-only predicate.
        if (causalStable is not null)
        {
            var entryVc = entry.VectorClock;
            if (entryVc is not null && !causalStable.DominatesOrEquals(entryVc))
            {
                return false;
            }
        }

        // Blocked-floor clause: when at least one consumer
        // reports a non-null buffer pin, every WAL entry whose HLC is
        // at or after the floor is held back so the receiver can
        // recover from buffer state. Strict-less semantics protect the
        // buffered entry itself: an entry whose Timestamp equals the
        // floor is the buffer's lowest staged entry and must remain on
        // the WAL until the batch completes or evicts. Range-delete
        // entries carry HybridLogicalClock.Zero and are therefore
        // never blocked by a positive floor (Zero < any positive HLC).
        if (blockedFloor is { } floor && entry.Timestamp >= floor)
        {
            return false;
        }

        return true;
    }
}

