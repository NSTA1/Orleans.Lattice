using System.Runtime.InteropServices;
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
    /// <summary>
    /// How many blocking materialiser-pin consumer ids a single pass carries out
    /// alongside <see cref="LatticeWalGcReport.BlockingConsumerId"/>.
    /// </summary>
    /// <remarks>
    /// A blast-radius bound, not a tuning knob: the blocked-leaf population is
    /// unbounded, so the report must be bounded, and the scheduler acts on a
    /// bounded number of blockers per pass regardless. It is deliberately no
    /// smaller than the scheduler's per-pass touch budget, so the bound that
    /// decides how much healing a pass can do is the scheduler's and not an
    /// accident of how many ids the GC happened to carry.
    /// </remarks>
    internal const int MaxReportedBlockingConsumers = 8;

    /// <summary>Page size for reading the head of each shard during the scan.</summary>
    private const int ScanPageSize = 256;

    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    // Per-tree byte-pressure latch for the advisory policy's hysteresis band.
    // A tree is "armed" (in pressure) once its retained WAL crosses the full
    // ceiling (high-water), and stays armed - re-triggering a trim on each
    // pass - until a trim drives retained below WalBytePressureReclaimTarget x
    // ceiling (low-water). While disarmed, growth between the low- and high-
    // water marks does not re-trigger, so a tree hovering near the ceiling is
    // not trimmed on every pass. The singleton lifetime of this GC carries the
    // latch across passes for the life of the silo.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, bool> _bytePressureArmed = new(StringComparer.Ordinal);

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

    // Resolved lazily so a host (or a unit test) that constructs the GC with a
    // bare IServiceProvider still works: when the resolver is absent the GC
    // falls back to the legacy single-provider-per-tree resolution. When
    // present, the GC resolves a provider per partition from the durable WAL
    // placement pin so a moved partition is trimmed on its own backend.
    private BPlusTree.LatticeOptionsResolver? _optionsResolverCache;
    private bool _optionsResolverResolved;

    private BPlusTree.LatticeOptionsResolver? OptionsResolver
    {
        get
        {
            if (!_optionsResolverResolved)
            {
                _optionsResolverCache = services.GetService<BPlusTree.LatticeOptionsResolver>();
                _optionsResolverResolved = true;
            }
            return _optionsResolverCache;
        }
    }

    // Resolved lazily so a unit test that constructs the GC with a bare
    // IServiceProvider (no grain runtime) still works: when the grain factory
    // is absent the durable-materialiser-pin floor is simply not consulted and
    // the GC trims by the in-memory registry exactly as before. When present,
    // the GC floors its trim point under the slowest leaf's durable checkpoint
    // for any leaf MISSING from the process-local registry, so a full silo /
    // cluster restart that wiped the registry cannot trim past a dormant
    // leaf's durable frontier.
    private IGrainFactory? _grainFactoryCache;
    private bool _grainFactoryResolved;

    private IGrainFactory? GrainFactory
    {
        get
        {
            if (!_grainFactoryResolved)
            {
                _grainFactoryCache = services.GetService<IGrainFactory>();
                _grainFactoryResolved = true;
            }
            return _grainFactoryCache;
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

        // Prime the trim-stop arms before any early return below, so a tree whose
        // pass returns without reaching the trim loop still publishes four
        // measured zeros rather than nothing at all (issue #3149).
        PrimeTrimStopSeries(treeName);

        // Resolve a provider per partition from the durable WAL placement pin so
        // a partition that was moved to a named storage backend is sampled and
        // trimmed on that backend rather than on the baseline provider. When the
        // resolver is unavailable (a bare-IServiceProvider construction in a
        // unit test) fall back to the legacy single-provider-per-tree shape.
        var pin = OptionsResolver is { } pinResolver
            ? await pinResolver.GetWalPlacementSnapshotAsync(treeName).ConfigureAwait(false)
            : BPlusTree.State.WalPlacementPin.Create();

        IWalStorageProvider? ResolvePartitionProvider(int partition)
        {
            if (OptionsResolver is { } r)
            {
                try
                {
                    return r.ResolveWalProvider(treeName, pin, partition).Provider;
                }
                catch (LatticeWalProviderMissingException)
                {
                    // This silo cannot resolve the partition's pinned provider
                    // key; skip it (another silo that registered the key trims
                    // it) rather than failing the whole tree's GC pass.
                    return null;
                }
            }
            return resolved.WalStorageProvider?.Invoke(treeName)
                ?? services.GetRequiredService<IWalStorageProvider>();
        }

        var minCursor = await cursors.GetMinCursorAsync(treeName, cancellationToken).ConfigureAwait(false);
        // Offset-space retention floor. The HLC floor below cannot protect a
        // low-HLC / high-offset WAL entry (a tombstone-compaction reap re-emits
        // an old timestamp at a new offset, so the WAL is not HLC-monotonic in
        // offset): such an entry is HLC-eligible under any positive cursor yet
        // sits above a lagging leaf's projection checkpoint offset. Flooring
        // the trim at the lowest such offset keeps every entry the lagging leaf
        // has not yet read readable, so the leaf never falls off its own log.
        //
        // Those offsets are SCANNED-through, not applied-through (issue #2270):
        // replay advances a leaf's checkpoint over entries it skips as another
        // leaf's, so a checkpoint here can sit above entries this leaf never
        // applied. Taking the MINIMUM is exactly what makes that safe. Skipping
        // only ever inflates the checkpoint of a leaf that does NOT own the
        // entry; the single leaf that does own it cannot skip it, so it holds
        // the minimum below that offset until it genuinely applies, and the
        // entry is retained. See BPlusLeafGrain.RebuildProjectionFromWalAsync,
        // which names this floor as the reason scanned-through advance is
        // load-bearing rather than an oversight.
        //
        // Read BEFORE the durable HLC floor because the set of consumers this
        // floor speaks for is an input to it (issue #3172): the HLC floor has to
        // publish a second minimum taken over the consumers this one does NOT
        // cover, which is what makes the offset axis safe to grant entitlement
        // with rather than only to subtract it with.
        var offsetCoverage = await ComputeMaterialiserOffsetFloorAsync(treeName).ConfigureAwait(false);
        var offsetFloor = offsetCoverage.Floor;
        // Floor the trim point under the durable leaf-materialiser pins for
        // any leaf MISSING from the in-memory registry. This survives a full
        // silo/cluster restart that wiped the registry: a forward consumer
        // (e.g. the replication shipper) re-reports its durably-advanced
        // cursor eagerly, but dormant leaves re-register only lazily, so
        // without this floor the GC would trim past a leaf's durable
        // checkpoint and lose its committed-but-not-yet-checkpointed WAL tail.
        var floorResult = await ApplyDurableMaterialiserFloorAsync(
            treeName, minCursor, partitions, offsetCoverage.CoveredConsumerIds, cancellationToken).ConfigureAwait(false);
        var cursorBlocked = floorResult.Blocked;
        var blockingConsumerId = floorResult.BlockingConsumerId;
        var blockingConsumerIds = floorResult.BlockingConsumerIds;
        // The report keeps the pre-#2849 shape: a tree with ANY blocked
        // partition reports a null cursor and BlockedByUnusablePin, so the
        // scheduler's blocked-leaf remedy and its cadence floor are driven by
        // exactly the condition they were driven by before. Only what the pass
        // is allowed to TRIM is decomposed, and only for partitions no unusable
        // pin covers.
        minCursor = cursorBlocked ? null : floorResult.Floor;
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

        // The cursor floor a given WAL partition trims against. It is the
        // tree-wide floor for every partition no unusable pin covers, and null
        // for the rest (issue #2849).
        //
        // Before this, ONE unusable pin disabled the cursor branch for the WHOLE
        // tree, so a single quiet leaf that had never reached a durable
        // projection checkpoint stranded every other leaf's WAL indefinitely -
        // and, because the in-memory cursor registry is per-activation, a restart
        // re-established the condition from cold rather than clearing it.
        //
        // Note what is decomposed and what is NOT. The floor stays a tree-wide
        // MINIMUM over every usable pin: a minimum over leaves is not in general
        // safely recomputed over a subset of them, so this change never narrows
        // the population the floor is minimised over. What IS attributed per
        // partition is the block, and only the block. That attribution is sound
        // by construction rather than by argument: a WAL entry is routed to
        // exactly one partition by WalPartitionHash over its key, a leaf reports
        // one materialiser pin per WAL partition under that same hash, and the
        // GC already trims each partition separately. So an unusable pin for
        // partition p says nothing about the entries in partition q, and
        // retaining q's head on its account retains WAL no consumer needs.
        //
        // Unattributable pins fail closed - see ApplyDurableMaterialiserFloorAsync.
        HybridLogicalClock? PartitionCursor(int partition) =>
            floorResult.IsPartitionBlocked(partition) ? null : floorResult.Floor;

        // The offset-space entitlement a given WAL partition trims against
        // (issue #3172), or null where the offset axis may not grant anything.
        //
        // Three independent conditions must all hold, and each is a fail-closed
        // gate rather than a refinement:
        //
        //   1. A durable offset floor was actually computed this pass. A null
        //      floor - an unreachable pin store, a host that reports no offsets,
        //      an all-"-1" pin set - admits NOTHING, leaving the predicate
        //      byte-identical to its pre-#3172 behaviour.
        //   2. The uncovered-consumer cursor was computed over both retention
        //      populations. Where ApplyDurableMaterialiserFloorAsync took an
        //      early exit it has not established who the floor fails to speak
        //      for, and an unknown uncovered population must never be read as an
        //      empty one.
        //   3. The partition is not blocked by an unusable pin. A blocked
        //      partition has a leaf that never reached a durable checkpoint;
        //      that leaf reports offset -1, which is excluded from the floor, so
        //      the floor demonstrably does not speak for it. The cursor branch is
        //      already disabled there and the offset branch must be too.
        //
        // The Floor carried here is the same tree-wide value the offset-floor
        // STOP below compares against, and the comparisons are exact
        // complements (stop on `> floor`, admit on `<= floor`), so admission can
        // never reach an entry the stop would not already have walked past. That
        // is what keeps the cross-partition conservatism of the single global
        // minimum intact: it is not re-derived, merely read in the other
        // direction.
        WalGcOffsetAdmission? PartitionOffsetAdmission(int partition)
            => offsetFloor is { } floor
                && floorResult.UncoveredCursorComputed
                && !floorResult.IsPartitionBlocked(partition)
                    ? new WalGcOffsetAdmission(floor, floorResult.UncoveredCursor)
                    : null;

        var anyPartitionHasCursorPredicate = false;
        for (var partition = 0; partition < partitions; partition++)
        {
            if (PartitionCursor(partition) is { } pc && pc > HybridLogicalClock.Zero)
            {
                anyPartitionHasCursorPredicate = true;
                break;
            }
        }

        // Why the cursor branch is in the state it is. A null minCursor is
        // ambiguous between "nobody is consuming this tree" (benign, and the
        // scheduler should back off) and "an unusable durable pin short-circuited
        // the floor" (a defect state in which the tree cannot reclaim at all and
        // its WAL grows without bound). Collapsing the two is issue #2702; the
        // scheduler reads this to schedule them differently. Purely diagnostic -
        // the trim predicate below is unchanged.
        var cursorFloorState = cursorBlocked
            ? WalGcCursorFloorState.BlockedByUnusablePin
            : hasCursorPredicate
                ? WalGcCursorFloorState.Available
                : WalGcCursorFloorState.NoCursorReported;

        // Sample retained bytes once up front so a byte-pressure trigger is
        // decided against the pre-trim footprint. Returns null when the
        // policy is disabled or the provider does not support byte accounting.
        var (ceiling, retainedBefore) = await SampleRetainedBytesAsync(
            ResolvePartitionProvider, resolved, treeName, partitions, cancellationToken).ConfigureAwait(false);
        var triggered = EvaluateBytePressureTrigger(treeName, resolved, ceiling, retainedBefore);
        if (triggered)
        {
            LatticeMetrics.StoragePolicyTrimTriggered.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName),
                LatticeMetrics.ReasonBytePressure,
                LatticeTenantLabel.ForTree(treeName));
        }

        if (!anyPartitionHasCursorPredicate && !hasTtlPredicate)
        {
            // The second site at which compaction evaluation is unreachable,
            // and the one that governs a tree whose durable pins are unusable
            // (issue #3207). This early return is taken before the partition
            // loop below, so TrimShardAsync is never entered for any partition
            // and the evaluation added there is never reached either. Field
            // measurement on a tree in this state shows the trim-stop series
            // absent entirely rather than zero, which is exactly the signature
            // of a pass that returned above the loop.
            //
            // The guard is not the blocked state - that is explicitly
            // diagnostic and leaves the trim predicate unchanged - it is the
            // absence of any usable trim predicate at all. A tree reaches it
            // whenever no partition holds a cursor floor and no TTL is
            // configured, which is precisely the condition an unusable durable
            // pin produces, and which can persist indefinitely.
            //
            // Reclaiming space already classified as dead does not require a
            // trim predicate: the bytes stopped being live when they were
            // trimmed, on some earlier pass, under whatever predicate then
            // applied. Conditioning their reclamation on the tree's present
            // ability to trim *more* is what strands them.
            for (var partition = 0; partition < partitions; partition++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (ResolvePartitionProvider(partition) is { } idleProvider)
                {
                    await idleProvider
                        .EvaluateCompactionAsync(treeName, partition, cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            // Nothing to do: no partition has a usable cursor floor and no
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
            var over0 = FinishBytePressure(treeName, resolved, ceiling, retainedBefore, retainedBefore);
            return new LatticeWalGcReport(
                treeName, minCursor, ttlCeiling, causalStable, blockedFloor, partitions, 0,
                ceiling, retainedBefore, retainedBefore, triggered, over0, cursorFloorState, blockingConsumerId,
                blockingConsumerIds);
        }

        long totalTrimmed = 0;
        var retainedBacklog = false;
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName);
        var tenantTag = LatticeTenantLabel.ForTree(treeName);
        for (var partition = 0; partition < partitions; partition++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var partitionProvider = ResolvePartitionProvider(partition);
            if (partitionProvider is null)
            {
                // Partition pinned to a provider key this silo cannot resolve;
                // skip trimming it here.
                continue;
            }
            var shardScan = await TrimShardAsync(partitionProvider, treeName, partition, PartitionCursor(partition), ttlCeiling, causalStable, blockedFloor, offsetFloor, PartitionOffsetAdmission(partition), cancellationToken).ConfigureAwait(false);
            totalTrimmed += shardScan.EligibleCount;
            retainedBacklog |= IsRetentionStop(shardScan.StopReason);
            RecordTrimStop(treeName, shardScan.StopReason);
            RecordEntriesTrimmed(treeTag, tenantTag, partition, shardScan.EligibleCount);
        }

        var (_, retainedAfter) = await SampleRetainedBytesAsync(
            ResolvePartitionProvider, resolved, treeName, partitions, cancellationToken).ConfigureAwait(false);
        var overThreshold = FinishBytePressure(treeName, resolved, ceiling, retainedBefore, retainedAfter);

        return new LatticeWalGcReport(
            treeName, minCursor, ttlCeiling, causalStable, blockedFloor, partitions, totalTrimmed,
            ceiling, retainedBefore, retainedAfter, triggered, overThreshold, cursorFloorState, blockingConsumerId,
            blockingConsumerIds, retainedBacklog);
    }

    /// <summary>
    /// Whether a shard scan's stop reason means the scan met an entry it had to
    /// <i>retain</i>, and therefore that WAL outlived the pass (issue #3213).
    /// </summary>
    /// <remarks>
    /// <para>
    /// The trim scan walks ascending offsets and stops at the first entry it may
    /// not trim, so every reason other than the two terminal ones is by
    /// construction a stop <i>at</i> a retained entry: the shard holds at least
    /// that entry and every entry above it. <see cref="WalGcTrimStopReason.Exhausted"/>
    /// means the scan consumed the whole log without meeting such an entry and
    /// <see cref="WalGcTrimStopReason.Empty"/> means there was no log to consume,
    /// so those two - and only those two - leave nothing behind.
    /// </para>
    /// <para>
    /// This is the backlog evidence that survives a default deployment.
    /// <see cref="LatticeWalGcReport.RetainedBytesAfter"/> is the obvious
    /// quantity to reach for and it is <see langword="null"/> whenever
    /// <see cref="LatticeOptions.WalMaxRetainedBytes"/> is unset, because
    /// <c>SampleRetainedBytesAsync</c> returns early for zero hot-path cost when
    /// the policy is disabled - which is exactly the deployment shape in which a
    /// stranded tree had no corrective signal at all. The stop reason is decided
    /// by the scan itself on every pass, so it costs nothing extra and is never
    /// gated behind an opt-in.
    /// </para>
    /// <para>
    /// Written as an explicit two-member exclusion rather than a list of the four
    /// retention reasons so that a stop reason added later is treated as
    /// retention - the conservative reading - instead of silently joining the
    /// "nothing left behind" set and re-opening the defect.
    /// </para>
    /// </remarks>
    private static bool IsRetentionStop(WalGcTrimStopReason reason)
        => reason is not (WalGcTrimStopReason.Exhausted or WalGcTrimStopReason.Empty);

    /// <summary>
    /// Lowers <paramref name="registryMin"/> to account for durable
    /// leaf-materialiser pins (<see cref="IWalMaterialiserPinGrain"/>) whose
    /// owning leaf is <b>absent</b> from the in-memory cursor registry - the
    /// post-restart window where a dormant leaf has not yet re-activated and
    /// re-reported its pin. For a present consumer the in-memory value is
    /// fresher and already folded into <paramref name="registryMin"/>, so its
    /// durable pin is skipped and steady-state trimming is byte-for-byte
    /// unchanged.
    /// <para>
    /// A missing pin at a real frontier lowers the effective floor (more WAL
    /// retained, always safe). A missing pin at
    /// <see cref="HybridLogicalClock.Zero"/> - a leaf whose durable pin carries
    /// no usable offset, most often because it is fully checkpointed but holds
    /// no durable snapshot, and otherwise because it has no usable checkpoint -
    /// <b>blocks the cursor branch for the WAL partition that pin belongs to</b>,
    /// so that partition's WAL head is retained for that leaf (the TTL ceiling
    /// still bounds growth). When the grain factory is unavailable (a
    /// bare-IServiceProvider unit-test construction) or no durable pins exist,
    /// the registry minimum is returned unchanged with nothing blocked.
    /// </para>
    /// <para>
    /// <b>The block is per partition; the floor is not (issue #2849).</b> An
    /// unusable pin used to disable the cursor branch for the entire tree, so a
    /// single quiet, data-bearing, never-checkpointed leaf stranded every other
    /// leaf's WAL indefinitely - and because the in-memory registry is
    /// per-activation, a restart rebuilt the condition from cold instead of
    /// clearing it. Attributing the block to one partition is sound by
    /// construction: a WAL entry routes to exactly one partition under
    /// <c>WalPartitionHash</c> over its key, a leaf publishes one pin per WAL
    /// partition under that same hash, and the GC already trims each partition
    /// separately, so an unusable pin for partition <c>p</c> constrains nothing
    /// in partition <c>q</c>.
    /// </para>
    /// <para>
    /// The <c>Floor</c> itself is deliberately <b>not</b> decomposed. It stays a
    /// minimum over every usable pin on the tree, because a minimum over leaves
    /// is not in general safely recomputed over a subset of them; narrowing that
    /// population would change what the pass may trim, where this change only
    /// changes which partitions may trim at all. An unblocked partition
    /// therefore trims against exactly the floor the whole tree would have used
    /// had nothing been blocked - never a higher one.
    /// </para>
    /// <para>
    /// <b>Unattributable pins fail closed.</b> A pin whose consumer id carries
    /// no partition suffix (the single-partition shape, a legacy pin, or a
    /// consumer id this build cannot parse) is applied to <i>every</i> partition,
    /// which reproduces the pre-#2849 whole-tree block exactly. Guessing a
    /// partition for an id that does not state one would trim WAL a leaf still
    /// needs, so an unrecognised id is treated as covering everything.
    /// </para>
    /// <para>
    /// The <c>Blocked</c> flag exists because a <see langword="null"/> floor is
    /// otherwise ambiguous: it is also what an unconsumed tree yields. Only a
    /// blocking pin is a defect state, and only the caller that can tell them
    /// apart can schedule them differently (issue #2702). The flag is
    /// diagnostic; it does not participate in the trim predicate.
    /// </para>
    /// <para>
    /// <c>BlockingConsumerId</c> names a consumer whose pin blocked a partition,
    /// and is <see langword="null"/> on every other path (issue #2464). It is
    /// what turns "this tree cannot reclaim" into an actionable statement,
    /// because the id embeds the owning leaf's grain id. Like <c>Blocked</c> it
    /// is diagnostic only and never widens what a pass is allowed to trim.
    /// </para>
    /// <para>
    /// <c>UncoveredCursor</c> is the second minimum this method publishes for
    /// issue #3172: the lowest cursor held by any consumer the durable
    /// <em>offset</em> floor does NOT speak for. The offset floor is a minimum
    /// over leaf materialisers that reported a real offset; a view maintainer,
    /// a log subscriber, the backup capture service and the replication shipper
    /// all report cursors and never offsets, so the offset axis is only safe to
    /// grant trim entitlement with when those consumers are separately checked.
    /// It is folded over exactly the two populations the HLC floor itself is -
    /// the in-memory registry and the registry-absent durable pins - minus the
    /// covered ids, so it cannot miss a retention holder the HLC floor can see.
    /// <c>UncoveredCursorComputed</c> distinguishes "nothing left uncovered"
    /// from "never established", because only the first may admit anything.
    /// </para>
    /// </summary>
    private async Task<DurableMaterialiserFloor> ApplyDurableMaterialiserFloorAsync(
        string treeName,
        HybridLogicalClock? registryMin,
        int partitions,
        IReadOnlySet<string>? coveredConsumerIds,
        CancellationToken cancellationToken)
    {
        var factory = GrainFactory;
        if (factory is null)
        {
            return DurableMaterialiserFloor.Unblocked(registryMin);
        }

        IReadOnlyDictionary<string, HybridLogicalClock> pins;
        try
        {
            pins = await ReadDurablePinsAsync(factory, treeName).ConfigureAwait(false);
        }
        catch
        {
            // The durable pin store is unavailable on this pass; fall back to
            // the in-memory floor rather than failing the whole GC run. The
            // next pass retries; a missed floor never trims unsafely because
            // the present in-memory consumers still constrain the trim point.
            return DurableMaterialiserFloor.Unblocked(registryMin);
        }

        if (pins.Count == 0)
        {
            return DurableMaterialiserFloor.Unblocked(registryMin);
        }

        var snapshot = await cursors.SnapshotAsync(treeName, cancellationToken).ConfigureAwait(false);
        var present = new HashSet<string>(snapshot.Count, StringComparer.Ordinal);
        // The lowest cursor held by a registry consumer the offset floor does
        // not speak for (issue #3172). Zero-cursor consumers are skipped for
        // exactly the reason GetMinCursorAsync skips them: a Zero cursor is a
        // block-pin-only registration, which the block-pin clause guards
        // independently and which would otherwise pin this minimum at Zero and
        // refuse every entry.
        HybridLogicalClock? uncovered = null;
        for (var i = 0; i < snapshot.Count; i++)
        {
            var entry = snapshot[i];
            present.Add(entry.ConsumerId);
            if (coveredConsumerIds is not null && coveredConsumerIds.Contains(entry.ConsumerId))
            {
                continue;
            }

            if (entry.Cursor <= HybridLogicalClock.Zero)
            {
                continue;
            }

            if (uncovered is not { } lowest || entry.Cursor < lowest)
            {
                uncovered = entry.Cursor;
            }
        }

        var floor = registryMin;
        bool[]? blockedPartitions = null;
        var blockedCount = 0;
        string? blockingConsumerId = null;
        List<string>? blockingConsumerIds = null;

        foreach (var (consumerId, pin) in pins)
        {
            // A consumer present in the in-memory registry has a fresher
            // (>=) cursor already folded into registryMin; its durable pin
            // (possibly staler) must not raise the floor.
            if (present.Contains(consumerId))
            {
                continue;
            }

            if (pin <= HybridLogicalClock.Zero)
            {
                // The durable OFFSET floor already speaks for this consumer, so
                // its Zero frontier is not evidence of an unprotected prefix
                // (issue #3094). A consumer lands in the coverage set only by
                // reporting a real checkpoint offset (>= 0);
                // ComputeMaterialiserOffsetFloorAsync folds exactly those into
                // the offset floor and deliberately excludes "-1" reporters.
                // TrimShardAsync then refuses to trim any entry ABOVE that
                // offset floor EVEN WHEN IT IS HLC-ELIGIBLE, so everything this
                // pin exists to retain is retained on the offset axis without
                // the frontier branch having to act.
                //
                // Why this is a removal of redundancy and not a relaxation: the
                // block-pin branch does not gate one partition, it disables the
                // cursor trim for the entire tree. A newborn leaf's keys hash
                // across every WAL partition, and splits admit newborns
                // continuously, so under the old "-1" seed a growing tree always
                // held at least one Zero-frontier pin and therefore never
                // scanned a single shard. The pins that genuinely have no offset
                // cover - a never-checkpointed leaf that reported "-1", the case
                // this branch was written for - are absent from the coverage set
                // and still block below, unchanged.
                //
                // THIS CLAUSE IS DELIBERATELY INSIDE THE ZERO BRANCH, and must
                // stay here. Hoisting it above the branch would also skip the
                // `floor` fold at the bottom of the loop, silently dropping a
                // covered consumer that publishes a REAL non-Zero frontier out
                // of the HLC cursor floor - a no-loss regression, because that
                // floor is what keeps a reactivated leaf's live tail readable.
                // The offset-axis safety argument above does NOT transfer to
                // that value: `floor` is the HLC cursor floor, a separate field
                // of DurableMaterialiserFloor from the offset floor
                // TrimShardAsync vetoes on. Inside this branch the exemption is
                // inert with respect to the fold by construction - the branch
                // condition makes `pin` a no-usable-frontier sentinel, and every
                // pre-existing path out of it already `continue`s without
                // folding ("a blocking pin contributes no usable frontier").
                // So this clause can only change whether the consumer is
                // RECORDED AS BLOCKING; it cannot change what is folded.
                //
                // It is the narrow sibling of the entitlement #3172 established
                // at the foot of this loop, which exempts covered consumers from
                // the `uncovered` fold and deliberately leaves `floor` alone.
                if (coveredConsumerIds is not null && coveredConsumerIds.Contains(consumerId))
                {
                    continue;
                }

                // Pin carries no usable offset: block the cursor branch for the
                // partition this pin belongs to, so nothing in that partition is
                // trimmed by cursor. Both a never-checkpointed leaf and a
                // fully-checkpointed leaf with no durable snapshot land here;
                // the caller reports this as blocked without asserting which.
                //
                // The consumer id IS carried out (issue #2464). Reporting that
                // a tree is blocked without naming the consumer leaves an
                // operator to guess which of potentially thousands of leaves is
                // holding the tree, and leaves a fix unable to demonstrate it
                // cleared every blocking leaf rather than some. The id encodes
                // the owning leaf's grain id, so naming it is the whole
                // difference between observing the condition and acting on it.
                //
                // It is deliberately returned rather than tagged onto a metric:
                // the leaf population is unbounded, so the id is an unbounded
                // metric dimension and belongs on the log line instead.
                //
                // This names ONE blocker in <c>BlockingConsumerId</c>, and the
                // first one wins so a tree's reported blocker is stable while it
                // drains. A later pass naming a different consumer is expected
                // and is progress rather than a regression.
                //
                // A BOUNDED SET of further blockers is carried alongside it
                // (issue #2768). Naming only the first made the scheduler's
                // blocked-leaf remedy structurally incapable of converging on a
                // tree with many blocked leaves: its attempt budget, minimum
                // block age and retry cooldown are all reasoned about and
                // documented PER BLOCKING CONSUMER, but a report that can only
                // ever name one consumer collapses them into a PER TREE rate
                // limit of roughly one leaf per cooldown. On a tree with
                // thousands of blocked leaves that never converges, and the
                // measured consequence is a sweep that attempted 2 touches
                // across 46 blocked passes and healed none. Reporting a bounded
                // set costs nothing here - the pin dictionary is already fully
                // in hand - and restores the per-consumer limits to the scope
                // they were written for.
                blockedPartitions ??= new bool[partitions];
                blockingConsumerId ??= consumerId;
                blockingConsumerIds ??= new List<string>(MaxReportedBlockingConsumers);
                if (blockingConsumerIds.Count < MaxReportedBlockingConsumers)
                {
                    blockingConsumerIds.Add(consumerId);
                }

                if (TryResolvePinPartition(consumerId, partitions) is { } blockedPartition)
                {
                    if (!blockedPartitions[blockedPartition])
                    {
                        blockedPartitions[blockedPartition] = true;
                        blockedCount++;
                    }
                }
                else
                {
                    // Unattributable: fail closed onto every partition, which is
                    // the pre-#2849 whole-tree block.
                    for (var p = 0; p < partitions; p++)
                    {
                        if (!blockedPartitions[p])
                        {
                            blockedPartitions[p] = true;
                            blockedCount++;
                        }
                    }
                }

                // Every partition is blocked AND the reported-blocker set is
                // full, so no further pin can change the outcome: the floor
                // that remains is unusable everywhere and no further id would
                // be carried. This preserves the cheap short-circuit for the
                // case that used to take it unconditionally; the residual scan
                // when the set is not yet full walks a dictionary already held
                // in memory and issues no I/O.
                if (blockedCount >= partitions
                    && blockingConsumerIds.Count >= MaxReportedBlockingConsumers)
                {
                    // The uncovered-cursor fold is abandoned unfinished here, so
                    // it is reported as NOT computed (issue #3172). Every
                    // partition is blocked, so no offset admission is granted on
                    // this path regardless; saying "not computed" keeps that
                    // true by construction rather than by coincidence.
                    return new DurableMaterialiserFloor(
                        null, blockedPartitions, true, blockingConsumerId, blockingConsumerIds, null, false);
                }

                // A blocking pin contributes no usable frontier, so it is not
                // folded into the floor. The enumeration continues rather than
                // short-circuiting, because the partitions this pin does not
                // cover still need the tree-wide minimum computed over the rest.
                continue;
            }

            floor = floor is { } current
                ? (pin < current ? pin : current)
                : pin;

            // A registry-absent durable pin is a retention holder the HLC floor
            // sees, so it must also constrain the uncovered minimum unless the
            // offset floor already speaks for it (issue #3172).
            if (coveredConsumerIds is null || !coveredConsumerIds.Contains(consumerId))
            {
                uncovered = uncovered is { } lowestUncovered
                    ? (pin < lowestUncovered ? pin : lowestUncovered)
                    : pin;
            }
        }

        return new DurableMaterialiserFloor(
            floor,
            blockedPartitions,
            blockedPartitions is not null,
            blockingConsumerId,
            blockingConsumerIds,
            uncovered,
            true);
    }

    /// <summary>
    /// Resolves the WAL partition a leaf-materialiser pin belongs to from its
    /// consumer id, or <see langword="null"/> when the id does not state one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A leaf builds its consumer id as
    /// <c>{prefix}{treeId}_{grainId}</c> on a single-partition tree and
    /// <c>{prefix}{treeId}_{grainId}_{partition}</c> when
    /// <see cref="LatticeOptions.WalPartitions"/> is greater than one, so the
    /// suffix is the only place the partition is recorded and it is absent by
    /// design on the legacy shape.
    /// </para>
    /// <para>
    /// Fail-closed, and deliberately stricter than a bare "parse the trailing
    /// number": the suffix is read only when the tree is actually partitioned,
    /// and only when it names a partition in range. A grain id that legitimately
    /// ends in <c>_&lt;digits&gt;</c> on a single-partition tree is therefore not
    /// truncated, and an out-of-range or unparsable suffix yields
    /// <see langword="null"/> rather than a guess. The caller applies a
    /// <see langword="null"/> to every partition, so the cost of any ambiguity is
    /// retained WAL, never a trim past a leaf that still needs the entries.
    /// </para>
    /// <para>
    /// A residual ambiguity is unavoidable and is resolved the same way: on a
    /// partitioned tree a grain id ending in <c>_3</c> is indistinguishable from
    /// a partition-3 suffix. Mis-reading it as partition 3 would under-block, so
    /// the population of ids that can reach here is worth stating - they are
    /// produced by <c>BPlusLeafGrain</c> from its own <c>GrainId</c>, which ends
    /// in the leaf's key string, and the suffix is appended by the same code
    /// path that this one mirrors. The pre-existing
    /// <c>LatticeWalGcScheduler.TryResolveLeafGrainId</c> makes the identical
    /// trade for the identical reason.
    /// </para>
    /// </remarks>
    private static int? TryResolvePinPartition(string consumerId, int partitions)
    {
        if (partitions <= 1)
        {
            return null;
        }

        var separator = consumerId.LastIndexOf('_');
        if (separator <= 0 || separator == consumerId.Length - 1)
        {
            return null;
        }

        return int.TryParse(
                consumerId.AsSpan(separator + 1),
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out var partition)
            && partition >= 0
            && partition < partitions
            ? partition
            : null;
    }

    /// <summary>
    /// The outcome of folding the durable leaf-materialiser pins into a pass's
    /// cursor floor: the tree-wide floor, which WAL partitions (if any) an
    /// unusable pin has blocked, and the consumer id of a blocking pin.
    /// </summary>
    /// <param name="Floor">
    /// The trim floor for every partition that is not blocked - a minimum over
    /// the registry cursor and every <i>usable</i> durable pin on the tree. It is
    /// deliberately tree-wide rather than per partition: narrowing the population
    /// a minimum is taken over is not safe in general, and this change is about
    /// which partitions may trim, not about how far they may trim.
    /// </param>
    /// <param name="BlockedPartitions">
    /// One flag per WAL partition, or <see langword="null"/> when nothing is
    /// blocked (the common case, which allocates no array).
    /// </param>
    /// <param name="Blocked">
    /// Whether any partition is blocked. The report's
    /// <see cref="WalGcCursorFloorState"/> is derived from this, so a tree with
    /// one blocked partition still reports
    /// <see cref="WalGcCursorFloorState.BlockedByUnusablePin"/> and still drives
    /// the scheduler's blocked-leaf remedy at its cadence floor.
    /// </param>
    /// <param name="BlockingConsumerId">
    /// The consumer id of the first blocking pin encountered, or
    /// <see langword="null"/> when nothing is blocked.
    /// </param>
    /// <param name="BlockingConsumerIds">
    /// Up to <see cref="MaxReportedBlockingConsumers"/> blocking consumer ids in
    /// encounter order, or <see langword="null"/> when nothing is blocked. The
    /// first element is always <see cref="BlockingConsumerId"/>. Bounded rather
    /// than complete: the blocked-leaf population is unbounded, and the consumer
    /// of this list acts on a bounded number of them per pass anyway.
    /// </param>
    /// <param name="UncoveredCursor">
    /// The lowest cursor held by a retention holder the durable <em>offset</em>
    /// floor does not speak for, or <see langword="null"/> when every holder is
    /// covered by it (issue #3172). This is what the offset axis is checked
    /// against before it may grant trim entitlement, and it is <em>only</em>
    /// meaningful when <see cref="UncoveredCursorComputed"/> is
    /// <see langword="true"/>.
    /// </param>
    /// <param name="UncoveredCursorComputed">
    /// Whether <see cref="UncoveredCursor"/> was folded over both retention
    /// populations on this pass. A <see langword="false"/> value means the
    /// uncovered population is UNKNOWN, not empty, and the offset axis must
    /// grant nothing. Every early exit takes that value, so the tri-state is
    /// what stops "we did not look" being read as "there is nobody there".
    /// </param>
    private readonly record struct DurableMaterialiserFloor(
        HybridLogicalClock? Floor,
        bool[]? BlockedPartitions,
        bool Blocked,
        string? BlockingConsumerId,
        IReadOnlyList<string>? BlockingConsumerIds = null,
        HybridLogicalClock? UncoveredCursor = null,
        bool UncoveredCursorComputed = false)
    {
        /// <summary>
        /// A floor with nothing blocked: every partition trims against
        /// <paramref name="floor"/>. The uncovered-consumer cursor is reported
        /// as not computed, because every caller of this factory took an early
        /// exit before establishing one.
        /// </summary>
        public static DurableMaterialiserFloor Unblocked(HybridLogicalClock? floor)
            => new(floor, null, false, null, null, null, false);

        /// <summary>
        /// Whether an unusable durable pin has disabled the cursor branch for
        /// <paramref name="partition"/>.
        /// </summary>
        public bool IsPartitionBlocked(int partition)
            => BlockedPartitions is { } blocked
                && (uint)partition < (uint)blocked.Length
                && blocked[partition];
    }

    /// <summary>
    /// Reads and unions the durable leaf-materialiser pins for
    /// <paramref name="treeName"/> across every shard activation plus the legacy
    /// unsuffixed key. Sharding spreads the pin-store write fan-in across
    /// <see cref="LatticeOptions.WalMaterialiserPinShards"/> grains; the GC must
    /// reconstruct the full floor by reading all of them. The dual-read of the
    /// legacy key keeps pins written before the upgrade counted. Shards are read
    /// concurrently; per consumer id the pin at the key the current build would
    /// write to wins outright, and only when that key holds nothing do the
    /// remaining (stranded) pins fold to the lowest.
    /// </summary>
    private async Task<IReadOnlyDictionary<string, HybridLogicalClock>> ReadDurablePinsAsync(
        IGrainFactory factory,
        string treeName)
    {
        var shardCount = WalMaterialiserPinRouting.ResolveShardCount(optionsMonitor);
        var keys = WalMaterialiserPinRouting.EnumerateReadKeys(treeName, shardCount);
        if (keys.Count == 1)
        {
            return await factory.GetGrain<IWalMaterialiserPinGrain>(keys[0]).GetPinsAsync().ConfigureAwait(false);
        }

        var reads = new Task<IReadOnlyDictionary<string, HybridLogicalClock>>[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            reads[i] = factory.GetGrain<IWalMaterialiserPinGrain>(keys[i]).GetPinsAsync();
        }

        var results = await Task.WhenAll(reads).ConfigureAwait(false);
        // Presize to the widest shard's consumer count. Consumers overlap
        // heavily across pin shards - the routing shards a tree's pins, it does
        // not partition its consumers - so the union lands between that maximum
        // and the sum, and seeding at the maximum removes most of the
        // grow-and-rehash chain the prior grown-from-empty map paid.
        var union = new Dictionary<string, HybridLogicalClock>(
            WidestResultCount(results), StringComparer.Ordinal);
        HashSet<string>? authoritative = null;
        for (var i = 0; i < results.Length; i++)
        {
            foreach (var (consumerId, pin) in results[i])
            {
                var isAuthoritative = i < shardCount
                    && WalMaterialiserPinRouting.AuthoritativeKeyIndex(consumerId, shardCount) == i;

                // Single-probe fold: the prior shape probed `union` twice per
                // consumer (a TryGetValue then an indexer set on the same key)
                // in both branches. Nothing mutates `union` while the ref is
                // live - the authoritative set is a separate collection.
                ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(union, consumerId, out var existed);
                if (isAuthoritative)
                {
                    // Only one key in the enumeration is authoritative for a
                    // given consumer, and it is the only key the current build
                    // writes to, so its pin supersedes every stranded duplicate
                    // outright rather than folding against it.
                    slot = pin;
                    (authoritative ??= new HashSet<string>(StringComparer.Ordinal)).Add(consumerId);
                    continue;
                }

                if (!existed)
                {
                    slot = pin;
                    continue;
                }

                if (authoritative?.Contains(consumerId) != true && pin < slot)
                {
                    slot = pin;
                }
            }
        }

        return union;
    }

    /// <summary>
    /// Returns the largest entry count across <paramref name="results"/>, a
    /// lower bound on the size of their union and therefore a safe capacity
    /// hint for it. Null shard results are skipped.
    /// </summary>
    private static int WidestResultCount<TValue>(IReadOnlyDictionary<string, TValue>?[] results)
    {
        var widest = 0;
        for (var i = 0; i < results.Length; i++)
        {
            var count = results[i]?.Count ?? 0;
            if (count > widest) widest = count;
        }
        return widest;
    }

    /// <summary>
    /// Computes the offset-space retention floor for <paramref name="treeName"/>:
    /// the lowest leaf-materialiser checkpoint offset across every pin shard. The
    /// WAL GC must never trim an entry at or above this offset, because a leaf
    /// whose checkpoint sits there has not yet consumed the entries above it -
    /// including a low-HLC / high-offset tombstone-compaction reap that the HLC
    /// floor alone would wrongly consider trim-eligible.
    /// <para>
    /// Note the reported checkpoints are SCANNED-through, not applied-through
    /// (issue #2270): a leaf advances its checkpoint over entries it skips as
    /// another leaf's work. That is safe HERE, and only because this is a
    /// MINIMUM. Skipping inflates the checkpoint of leaves that do not own the
    /// entry, while the one leaf that does own it cannot skip it and so holds the
    /// minimum below that offset until it truly applies. Do not re-derive this
    /// floor from any per-leaf quantity that is not minimised over the owning
    /// population, and do not "tighten" the leaf-side advance to applied-only:
    /// a leaf owning nothing in a partition would then never advance and would
    /// pin this floor permanently.
    /// </para>
    /// Returns <see langword="null"/> when the durable pin store is unavailable,
    /// carries no offsets (state predating this field, or a host that never
    /// reports offsets), so the GC degrades cleanly to the pre-existing HLC-only
    /// behaviour. A single global minimum is applied to every WAL partition:
    /// exact for the common single-partition layout and conservatively safe
    /// (over-retains) across partitions, whose offsets are otherwise incomparable.
    /// <para>
    /// The <b>set of consumers the floor speaks for</b> is returned alongside it
    /// (issue #3172), because the floor is only half of an offset-space
    /// entitlement. It is a minimum over the leaf materialisers that reported a
    /// real offset - and over nothing else. Every other WAL consumer (a view
    /// maintainer, a log subscriber, the backup capture service, the replication
    /// shipper) reports an HLC cursor and never an offset, so a rule that read
    /// the floor alone as "every consumer has applied through here" would trim
    /// straight past them. The caller pairs this set with a second cursor
    /// minimum taken over its complement.
    /// </para>
    /// </summary>
    private async Task<MaterialiserOffsetCoverage> ComputeMaterialiserOffsetFloorAsync(string treeName)
    {
        var factory = GrainFactory;
        if (factory is null)
        {
            return MaterialiserOffsetCoverage.None;
        }

        try
        {
            var offsets = await ReadDurablePinOffsetsAsync(factory, treeName).ConfigureAwait(false);
            if (offsets is null)
            {
                return MaterialiserOffsetCoverage.None;
            }

            long? floor = null;
            HashSet<string>? covered = null;
            foreach (var (consumerId, offset) in offsets)
            {
                // Skip the "-1" sentinel: a consumer reports -1 when it has no
                // WAL-replay dependency at all. Three ways to get there, and
                // only two of them carry a block pin: a genuinely empty
                // partition (no durable checkpoint AND no live cache row, so
                // there is no committed prefix to lose - reported with the
                // leaf's REAL frontier, deliberately without a block pin); a
                // never-checkpointed or uncovered data-bearing partition (whose
                // WAL retention IS enforced by the Zero-HLC block-pin branch,
                // which disables the cursor trim entirely); or a split sibling
                // that received its data via an in-memory handoff rather than
                // WAL replay. Letting a -1 collapse the floor would wedge the
                // trim for the whole tree - the empty-partition case reports -1
                // indefinitely and legitimately - so only real checkpoints
                // (offset >= 0) constrain the offset floor.
                if (offset < 0)
                {
                    continue;
                }

                // This consumer HAS told us it durably applied through `offset`,
                // so the floor genuinely speaks for it (issue #3172). A "-1"
                // reporter is skipped above and is therefore deliberately NOT
                // covered: it is protected by its cursor or its block pin, not
                // by this floor.
                covered ??= new HashSet<string>(offsets.Count, StringComparer.Ordinal);
                covered.Add(consumerId);

                if (floor is not { } current || offset < current)
                {
                    floor = offset;
                }
            }

            return new MaterialiserOffsetCoverage(floor, covered);
        }
        catch
        {
            // Durable pin store unavailable on this pass (including an older
            // pin grain without GetPinOffsetsAsync during a rolling upgrade):
            // fall back to no offset floor. The HLC floor still constrains the
            // trim, and the next pass retries once the store is reachable.
            //
            // This fallback was previously completely silent (issue #2314): a
            // persistently unreachable pin store removes the offset floor on
            // EVERY pass indefinitely, with no signal, indistinguishable from a
            // tree that legitimately has no offset floor to apply. The counter
            // makes "no floor because unreachable" (this catch) separable from
            // "no floor because none needed" (factory null / empty offsets,
            // which return null WITHOUT reaching here). It changes no trim
            // behaviour - it only makes the swallowed failure observable.
            //
            // Note the deeper population caveat this counter deliberately does
            // NOT try to fix (also #2314): the floor below is a minimum over the
            // leaves that REPORTED an offset, not over the leaves that OWE
            // entries. A leaf absent from the pin set does not constrain the
            // floor at all, and absence is NOT the same state as a reported -1:
            // a reported -1 comes from a participating leaf that has told us it
            // owes nothing, and is covered either by a paired Zero HLC block pin
            // (the data-bearing, not-durably-recoverable case) or by there being
            // no committed prefix to lose at all (the genuinely-empty case,
            // which ResolveDurablePinForPartition reports with the leaf's REAL
            // frontier and so with no block pin - it does not need one). An
            // absent leaf - one whose birth block-pin seed was swallowed, or
            // that predates the durable pin store being wired - has told us
            // nothing and carries neither cover. Making absence constrain the
            // floor conservatively (e.g. treating absence as offset 0) would pin
            // the WAL forever for any permanently-departed leaf, so it is NOT
            // done here; distinguishing absent from reported -1 needs an
            // independent owner census this seam does not have.
            LatticeMetrics.WalGcOffsetFloorUnavailable.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName),
                LatticeTenantLabel.ForTree(treeName));
            return MaterialiserOffsetCoverage.None;
        }
    }

    /// <summary>
    /// The durable materialiser offset floor for a pass, paired with the set of
    /// consumer ids that floor was minimised over.
    /// </summary>
    /// <param name="Floor">
    /// The lowest real (non-"-1") reported checkpoint offset, or
    /// <see langword="null"/> when no offset floor could be established on this
    /// pass. A <see langword="null"/> floor grants no offset-space entitlement at
    /// all, which is what makes an unreachable pin store byte-identical to
    /// pre-#3172 behaviour rather than merely close to it.
    /// </param>
    /// <param name="CoveredConsumerIds">
    /// The consumers whose reported offset was folded into <paramref name="Floor"/>,
    /// or <see langword="null"/> when none was. These, and only these, are the
    /// consumers the floor is evidence about; every other WAL consumer must still
    /// be protected by its HLC cursor.
    /// </param>
    private readonly record struct MaterialiserOffsetCoverage(
        long? Floor,
        IReadOnlySet<string>? CoveredConsumerIds)
    {
        /// <summary>
        /// No offset floor on this pass, and therefore no consumer covered by
        /// one. The fail-closed value.
        /// </summary>
        public static MaterialiserOffsetCoverage None => new(null, null);
    }

    /// <summary>
    /// Reads and unions the durable leaf-materialiser checkpoint offsets for
    /// <paramref name="treeName"/> across every shard activation plus the legacy
    /// unsuffixed key, mirroring <see cref="ReadDurablePinsAsync"/>. Per consumer
    /// id the offset at the key the current build would write to wins outright,
    /// and only when that key holds nothing do the remaining (stranded) offsets
    /// fold to the lowest. A grain that returns <see langword="null"/> (an older
    /// activation predating the offset contract, surfaced by a substitute in
    /// tests) contributes nothing rather than faulting the read.
    /// </summary>
    private async Task<IReadOnlyDictionary<string, long>> ReadDurablePinOffsetsAsync(
        IGrainFactory factory,
        string treeName)
    {
        var shardCount = WalMaterialiserPinRouting.ResolveShardCount(optionsMonitor);
        var keys = WalMaterialiserPinRouting.EnumerateReadKeys(treeName, shardCount);
        if (keys.Count == 1)
        {
            return await factory.GetGrain<IWalMaterialiserPinGrain>(keys[0]).GetPinOffsetsAsync().ConfigureAwait(false)
                ?? EmptyOffsets;
        }

        var reads = new Task<IReadOnlyDictionary<string, long>>[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            reads[i] = factory.GetGrain<IWalMaterialiserPinGrain>(keys[i]).GetPinOffsetsAsync();
        }

        var results = await Task.WhenAll(reads).ConfigureAwait(false);
        // Presized on the same reasoning as ReadDurablePinsAsync above.
        var union = new Dictionary<string, long>(
            WidestResultCount(results), StringComparer.Ordinal);
        HashSet<string>? authoritative = null;
        for (var i = 0; i < results.Length; i++)
        {
            if (results[i] is null)
            {
                continue;
            }

            foreach (var (consumerId, offset) in results[i])
            {
                var isAuthoritative = i < shardCount
                    && WalMaterialiserPinRouting.AuthoritativeKeyIndex(consumerId, shardCount) == i;

                // Single-probe route-authority fold, as in ReadDurablePinsAsync
                // above. This plane carries the same defect and must be fixed
                // with it: a floor repaired on one plane and left stranded on
                // the other still pins the WAL.
                ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(union, consumerId, out var existed);
                if (isAuthoritative)
                {
                    slot = offset;
                    (authoritative ??= new HashSet<string>(StringComparer.Ordinal)).Add(consumerId);
                    continue;
                }

                if (!existed)
                {
                    slot = offset;
                    continue;
                }

                if (authoritative?.Contains(consumerId) != true && offset < slot)
                {
                    slot = offset;
                }
            }
        }

        return union;
    }

    private static readonly IReadOnlyDictionary<string, long> EmptyOffsets =
        new Dictionary<string, long>(StringComparer.Ordinal);

    /// <summary>
    /// Finalises the byte-pressure accounting for a completed WAL GC
    /// trim pass and returns whether the post-trim footprint still breaches
    /// the ceiling. When a byte-pressure trigger reclaimed bytes
    /// (<paramref name="retainedBefore"/> &gt; <paramref name="retainedAfter"/>),
    /// increments <see cref="LatticeMetrics.StoragePolicyBytesReclaimed"/> by
    /// the freed byte count. Updates the per-tree hysteresis latch against the
    /// post-trim footprint: a trim that drove retained below the low-water mark
    /// (<see cref="LatticeOptions.WalBytePressureReclaimTarget"/> of the ceiling)
    /// disarms the policy so it does not re-trigger until retained crosses the
    /// ceiling again. Also pushes the over-threshold flag to the observable
    /// storage gauge so the 0/1 series tracks the WAL GC's own sampling between
    /// aggregator scrapes. Returns <see langword="false"/> when the policy is
    /// disabled or byte accounting is unsupported.
    /// </summary>
    private bool FinishBytePressure(string treeName, LatticeOptions resolved, long? ceiling, long? retainedBefore, long? retainedAfter)
    {
        if (ceiling is not { } cap || retainedAfter is not { } after)
        {
            return false;
        }

        if (retainedBefore is { } before && before > after)
        {
            LatticeMetrics.StoragePolicyBytesReclaimed.Add(
                before - after,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName),
                LatticeTenantLabel.ForTree(treeName));
        }

        // Resolve the hysteresis latch against the post-trim footprint so the
        // next pass sees consistent armed state.
        if (after <= LowWater(cap, resolved.WalBytePressureReclaimTarget))
        {
            _bytePressureArmed[treeName] = false;
        }
        else if (after > cap)
        {
            _bytePressureArmed[treeName] = true;
        }

        var over = after > cap;
        _storageMetrics?.PublishOverThreshold(treeName, over);
        return over;
    }

    /// <summary>
    /// Decides whether this pass triggers a byte-pressure trim, applying the
    /// hysteresis band defined by
    /// <see cref="LatticeOptions.WalBytePressureReclaimTarget"/>. A tree arms
    /// (enters pressure) only when its retained WAL crosses the full ceiling
    /// (high-water) and stays armed - re-triggering on every pass - until a
    /// trim drives retained at or below the low-water mark
    /// (<c>reclaimTarget x ceiling</c>). While disarmed, growth between the
    /// low- and high-water marks does not re-trigger, so a tree hovering just
    /// under the ceiling is not trimmed on every pass. Returns
    /// <see langword="false"/> when the policy is disabled or byte accounting
    /// is unsupported.
    /// </summary>
    private bool EvaluateBytePressureTrigger(string treeName, LatticeOptions resolved, long? ceiling, long? retained)
    {
        if (ceiling is not { } cap || retained is not { } bytes)
        {
            // Policy disabled or byte accounting unsupported: clear any latch
            // so a re-enable starts from a clean disarmed state.
            _bytePressureArmed.TryRemove(treeName, out _);
            return false;
        }

        if (bytes > cap)
        {
            // Crossed the high-water mark: arm and trigger.
            _bytePressureArmed[treeName] = true;
            return true;
        }

        if (bytes <= LowWater(cap, resolved.WalBytePressureReclaimTarget))
        {
            // At or below the low-water mark: disarm. No trigger.
            _bytePressureArmed[treeName] = false;
            return false;
        }

        // In the hysteresis band (lowWater < bytes <= ceiling): keep
        // re-triggering only while already armed, otherwise stay quiet.
        return _bytePressureArmed.TryGetValue(treeName, out var armed) && armed;
    }

    /// <summary>
    /// Computes the low-water byte mark a byte-pressure trim aims to bring
    /// retained WAL at or below, from the ceiling and the configured reclaim
    /// target. The target is clamped to the open-closed interval <c>(0, 1]</c>;
    /// out-of-range or non-finite values fall back to the default.
    /// </summary>
    private static long LowWater(long ceiling, double reclaimTarget)
    {
        var target = reclaimTarget;
        if (double.IsNaN(target) || target <= 0)
        {
            target = LatticeOptions.DefaultWalBytePressureReclaimTarget;
        }
        else if (target > 1)
        {
            target = 1;
        }

        return (long)(ceiling * target);
    }

    /// <summary>
    /// Samples the advisory WAL byte-pressure inputs: the configured ceiling
    /// (<see cref="LatticeOptions.WalMaxRetainedBytes"/>) and the occupancy
    /// total summed across every partition. Returns <c>(null, null)</c> when
    /// the policy is disabled and <c>(ceiling, null)</c> when the provider does
    /// not support byte accounting (every partition returned the <c>-1</c>
    /// sentinel). The policy never trims past the safe frontier; the sampled
    /// total only feeds the advisory report and metrics.
    /// <para>
    /// Each partition is sampled with
    /// <see cref="IWalStorageProvider.GetPhysicalByteSizeAsync"/> in
    /// preference to <see cref="IWalStorageProvider.GetRetainedByteSizeAsync"/>,
    /// falling back per-partition when a provider does not support physical
    /// accounting. A ceiling expressed in bytes exists to bound disk, and the
    /// retained figure cannot do that: it omits dead (trimmed but not yet
    /// compacted) bytes, which for a log-structured backend can equal the live
    /// payload, so sampling it lets a WAL legitimately occupy approaching twice
    /// the configured ceiling without ever reporting a breach (issue #3107).
    /// The fallback is exact rather than a degradation for the backends that
    /// take it: a provider whose trim deletes rows outright carries no dead
    /// bytes, so its retained total already is its occupancy.
    /// </para>
    /// </summary>
    private static async Task<(long? Ceiling, long? Retained)> SampleRetainedBytesAsync(
        Func<int, IWalStorageProvider?> resolveProvider,
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
            var provider = resolveProvider(partition);
            if (provider is null)
            {
                // Partition pinned to a provider key this silo cannot resolve;
                // omit it from the sample (its bytes are accounted by the silo
                // that owns the key).
                continue;
            }

            var bytes = await provider
                .GetPhysicalByteSizeAsync(treeName, partition, cancellationToken)
                .ConfigureAwait(false);
            if (bytes < 0)
            {
                // -1 sentinel: no physical accounting. Fall back to the
                // logical retained total, which is this backend's occupancy
                // when its trim deletes rather than marks dead.
                bytes = await provider
                    .GetRetainedByteSizeAsync(treeName, partition, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (bytes < 0)
            {
                // -1 again: this partition's provider supports neither form
                // of byte accounting. Skip it; if every partition is
                // unsupported the policy reports "no data".
                continue;
            }

            anySupported = true;
            retained += bytes;
        }

        return anySupported ? (ceiling, retained) : (ceiling, null);
    }

    private static async Task<(long EligibleCount, WalGcTrimStopReason StopReason)> TrimShardAsync(
        IWalStorageProvider provider,
        string treeId,
        int shardIndex,
        HybridLogicalClock? minCursor,
        HybridLogicalClock? ttlCeiling,
        VersionVector? causalStable,
        HybridLogicalClock? blockedFloor,
        long? offsetFloor,
        WalGcOffsetAdmission? offsetAdmission,
        CancellationToken cancellationToken)
    {
        long lastEligibleOffset = -1;
        long fromOffsetExclusive = -1;
        long eligibleCount = 0;
        long entriesSeen = 0;
        var stopReason = WalGcTrimStopReason.Exhausted;
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
                entriesSeen++;
                lastSeenOffset = walEntry.Offset;

                // Offset-space retention floor: never trim an entry ABOVE the
                // lowest durably-applied leaf checkpoint offset, even when it is
                // HLC-eligible. This is what keeps a low-HLC / high-offset reap
                // entry readable until the slowest leaf has applied it. The
                // checkpoint offset itself is last-applied and trimmable (the
                // HLC floor already trims through it for a healthy WAL), so the
                // floor only diverges from HLC eligibility for entries strictly
                // above it. Offsets are scanned ascending, so the first entry
                // above the floor stops the pass just like a non-eligible entry.
                if (offsetFloor is { } floor && walEntry.Offset > floor)
                {
                    // Recorded separately from the eligibility stop below
                    // (issue #3149). The two look identical from outside - both
                    // are "the scan stopped here" - but they indict different
                    // subsystems, and this one is the arm on which a tree can be
                    // stranded indefinitely while every floor-state series it
                    // publishes reads healthy.
                    stopReason = WalGcTrimStopReason.OffsetFloor;
                    stop = true;
                    break;
                }

                var eligibility = ClassifyEligibility(
                    walEntry.Mutation, walEntry.Offset, minCursor, ttlCeiling, causalStable, blockedFloor, offsetAdmission);
                if (eligibility == WalGcTrimEligibility.Eligible)
                {
                    lastEligibleOffset = walEntry.Offset;
                    eligibleCount++;
                }
                else
                {
                    // First non-eligible entry stops the scan: offsets
                    // are dense and the conservative shape forbids
                    // jumping over a pinned entry to trim a later one.
                    //
                    // Which clause refused it is carried through to the arm
                    // (issue #3155). The three clauses are independent and indict
                    // a consumer cursor, a replication origin and a buffering
                    // receiver respectively, so reporting them as one stop leaves
                    // a stranded tree observable but its holder unnameable - the
                    // same collapse this instrument was added to remove one level
                    // up.
                    stopReason = ClassifyIneligibility(eligibility);
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

        // A shard that offered no entries at all is reported as empty rather
        // than exhausted. Both trimmed nothing, but only one of them has a
        // backlog to account for, and collapsing them would put every idle
        // shard in the fleet on the same arm as a shard that just reclaimed its
        // whole log.
        if (!stop && entriesSeen == 0)
        {
            stopReason = WalGcTrimStopReason.Empty;
        }

        if (lastEligibleOffset < 0)
        {
            // Nothing was released, so the TrimAsync below - and with it the
            // unconditional compaction evaluation the file provider performs
            // at the end of it - is skipped. That is the only site at which a
            // shard's already-dead bytes are ever measured against any
            // threshold, so a shard whose scan keeps stopping is not merely
            // trimming slowly: it is never evaluated for reclamation at all,
            // at any dead ratio, for as long as the stop persists. No value of
            // any compaction option can reach that state, because none of them
            // is ever read (issue #3207).
            //
            // Deliberately NOT conditioned on why the scan stopped. Keying it
            // to OffsetFloor would rebuild the same unreachable-site defect
            // one level along: the floor advances by a single entry, the arm
            // becomes Exhausted, and reclamation silently stops again. The
            // quantity that matters is "this shard holds dead bytes", which is
            // the provider's to judge and is independent of every stop reason.
            //
            // This is a reachability repair and not a threshold change. The
            // provider evaluates exactly the policy it already applies after a
            // trim, so a shard below its thresholds still declines - it now
            // declines visibly, having been asked, rather than never being
            // asked at all.
            await provider.EvaluateCompactionAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(false);
            return (0, stopReason);
        }

        await provider.TrimAsync(treeId, shardIndex, lastEligibleOffset, cancellationToken).ConfigureAwait(false);
        return (eligibleCount, stopReason);
    }

    /// <summary>
    /// Maps a <see cref="WalGcTrimStopReason"/> onto its pre-allocated
    /// <see cref="LatticeMetrics.TagReason"/> tag. Held exhaustively armed by the
    /// instrumented-enum gate, so a reason added later cannot be reported under
    /// another reason's arm or under none.
    /// </summary>
    private static KeyValuePair<string, object?> ClassifyTrimStop(WalGcTrimStopReason reason)
        => reason switch
        {
            WalGcTrimStopReason.Exhausted => LatticeMetrics.ReasonTrimExhausted,
            WalGcTrimStopReason.Empty => LatticeMetrics.ReasonTrimEmpty,
            WalGcTrimStopReason.OffsetFloor => LatticeMetrics.ReasonTrimOffsetFloor,
            WalGcTrimStopReason.CursorFloor => LatticeMetrics.ReasonTrimCursorFloor,
            WalGcTrimStopReason.CausalFrontier => LatticeMetrics.ReasonTrimCausalFrontier,
            WalGcTrimStopReason.BlockPin => LatticeMetrics.ReasonTrimBlockPin,
            _ => throw new ArgumentOutOfRangeException(
                nameof(reason), reason, "Unarmed WAL GC trim stop reason."),
        };

    /// <summary>
    /// Records the entries one shard scan trimmed, tagged with that shard's
    /// index.
    /// <para>
    /// Trimming is decided and performed per shard, so a tree-scoped total -
    /// which is what this instrument carried before issue #3206 - is the sum
    /// of one per-shard result and cannot say which shard produced it. Read
    /// against the equally shard-tagged <c>orleans.lattice.wal.compactions</c>,
    /// the pair is the discriminator that separates "this shard trims and
    /// compacts" from "this shard trims and strands the bytes"; summed to the
    /// tree, an active minority of shards masks a stranded majority, which is
    /// the state #3206 measured on a live estate.
    /// </para>
    /// <para>
    /// <paramref name="count"/> is emitted even when it is zero, so every
    /// shard the pass actually scanned publishes a series. That is the same
    /// priming guarantee the trim-stop arms and the compaction counters carry:
    /// an absent series then means the shard was not scanned on this silo,
    /// rather than that it was scanned and reclaimed nothing.
    /// </para>
    /// </summary>
    private static void RecordEntriesTrimmed(
        KeyValuePair<string, object?> treeTag,
        KeyValuePair<string, object?> tenantTag,
        int shardIndex,
        long count)
        => LatticeMetrics.WalEntriesTrimmed.Add(
            count,
            treeTag,
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, shardIndex),
            tenantTag);

    /// <summary>
    /// Records one trim-scan stop for <paramref name="treeName"/>. Called with
    /// <paramref name="delta"/> zero to prime an arm, and with one to report an
    /// actual scan.
    /// </summary>
    private static void RecordTrimStop(string treeName, WalGcTrimStopReason reason, long delta = 1)
        => LatticeMetrics.WalGcTrimStops.Add(
            delta,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName),
            ClassifyTrimStop(reason),
            LatticeTenantLabel.ForTree(treeName));

    /// <summary>
    /// Zero-primes every <see cref="WalGcTrimStopReason"/> arm for
    /// <paramref name="treeName"/>, so an absent series means WAL GC is not
    /// running for this tree on this silo rather than that no scan ever stopped.
    /// <para>
    /// Called above every early return in <see cref="RunOnceAsync"/>, because the
    /// pass that reclaims nothing is exactly the pass a reader is investigating
    /// and it is the one most likely to return before reaching the trim loop.
    /// </para>
    /// </summary>
    private static void PrimeTrimStopSeries(string treeName)
    {
        RecordTrimStop(treeName, WalGcTrimStopReason.Exhausted, 0);
        RecordTrimStop(treeName, WalGcTrimStopReason.Empty, 0);
        RecordTrimStop(treeName, WalGcTrimStopReason.OffsetFloor, 0);
        RecordTrimStop(treeName, WalGcTrimStopReason.CursorFloor, 0);
        RecordTrimStop(treeName, WalGcTrimStopReason.CausalFrontier, 0);
        RecordTrimStop(treeName, WalGcTrimStopReason.BlockPin, 0);
    }

    /// <summary>
    /// Maps the clause that refused an entry onto the trim-stop reason it is
    /// reported under. One-to-one by construction, so a clause added to the
    /// predicate cannot inherit another clause's arm.
    /// </summary>
    private static WalGcTrimStopReason ClassifyIneligibility(WalGcTrimEligibility eligibility)
        => eligibility switch
        {
            WalGcTrimEligibility.CursorFloor => WalGcTrimStopReason.CursorFloor,
            WalGcTrimEligibility.CausalFrontier => WalGcTrimStopReason.CausalFrontier,
            WalGcTrimEligibility.BlockPin => WalGcTrimStopReason.BlockPin,
            _ => throw new ArgumentOutOfRangeException(
                nameof(eligibility), eligibility, "An eligible entry does not stop the scan."),
        };

    private static WalGcTrimEligibility ClassifyEligibility(
        LatticeMutation entry,
        long entryOffset,
        HybridLogicalClock? minCursor,
        HybridLogicalClock? ttlCeiling,
        VersionVector? causalStable,
        HybridLogicalClock? blockedFloor,
        WalGcOffsetAdmission? offsetAdmission)
        => WalGcTrimCore.ClassifyEntry(
            entry.Timestamp,
            entry.VectorClock,
            entryOffset,
            minCursor,
            ttlCeiling,
            causalStable,
            blockedFloor,
            offsetAdmission);
}

