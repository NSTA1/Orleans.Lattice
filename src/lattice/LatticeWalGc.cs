using System.Runtime.InteropServices;
using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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

    // Per-tree durable-floor progress, for the stall-age signal (issue #3300).
    //
    // Holds the highest offset floor this process has ever observed for a tree
    // and the timestamp at which that high-water mark was last RAISED. The
    // distinction from "the floor on the previous pass" matters: the floor is a
    // minimum over reporting leaves, so it can legitimately drop when a lagging
    // leaf starts reporting, and treating a drop as progress would reset the
    // stall clock on a tree that is not making any.
    //
    // `FirstObservedUtc` is what makes the never-advanced case measurable at
    // all. A tree whose floor has never moved has no advance timestamp to
    // subtract from, and reporting zero there would put the worst state on the
    // healthiest value - the precise confusion this whole instrument exists to
    // end - so the age is measured from the first pass that saw the tree
    // instead. In a process that has been up for hours, that yields an age
    // close to the uptime, which is the #3300 signature.
    //
    // Carried for the life of the silo, exactly like the byte-pressure latch
    // above, so the clock survives passes. It is deliberately NOT durable: the
    // question it answers is "is this process making data durable", and a
    // restart is precisely the event that resolves it.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, DurableFloorProgress> _durableFloorProgress
        = new(StringComparer.Ordinal);

    /// <summary>
    /// The highest durable materialiser offset floor observed for a tree in this
    /// process, when that high-water mark was last raised, and when the tree was
    /// first seen (issue #3300).
    /// </summary>
    private readonly record struct DurableFloorProgress(
        long? HighWaterFloor,
        DateTimeOffset? LastAdvanceUtc,
        DateTimeOffset FirstObservedUtc);

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
        // pass returns without reaching the trim loop still publishes
        // measured zeros rather than nothing at all (issue #3149). Primed per
        // partition, because the arm is shard-attributed (issue #3207) and a
        // tree-wide prime would leave the shard dimension absent on exactly
        // the early-return passes a reader is investigating.
        PrimeTrimStopSeries(treeName, partitions);
        WalGcBlockedConsumerCensus.Prime(treeName);

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
        RecordDurableFloorProgress(treeName, offsetFloor);
        // Floor the trim point under the durable leaf-materialiser pins for
        // any leaf MISSING from the in-memory registry. This survives a full
        // silo/cluster restart that wiped the registry: a forward consumer
        // (e.g. the replication shipper) re-reports its durably-advanced
        // cursor eagerly, but dormant leaves re-register only lazily, so
        // without this floor the GC would trim past a leaf's durable
        // checkpoint and lose its committed-but-not-yet-checkpointed WAL tail.
        var floorResult = await ApplyDurableMaterialiserFloorAsync(
            treeName, minCursor, partitions, offsetCoverage.CoveredConsumerIds,
            offsetCoverage.AbstainedConsumerIds, cancellationToken,
            ResolvePartitionProvider).ConfigureAwait(false);
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

        // The offset floor a given WAL partition trims against (issue #3178).
        //
        // Previously a single tree-wide minimum was applied to every partition.
        // That coupled each partition's retention to every other partition's
        // slowest pin, and because a leaf's pin for partition p only advances
        // when an entry is appended to partition p, a partition that had
        // converged and then fully drained held its terminal checkpoint
        // permanently and capped every sibling partition's scan forever. No
        // reactivation could lift it: the holding partition is empty by
        // construction, so there is nothing to replay and nothing to advance
        // over. See MaterialiserOffsetCoverage.FloorFor for the safety argument
        // and for the fail-closed cases that still fall back to the tree-wide
        // minimum.
        long? PartitionOffsetFloor(int partition) => offsetCoverage.FloorFor(partition);

        // The offset-space entitlement a given WAL partition trims against
        // (issue #3172), or null where the offset axis may not grant anything.
        //
        // Three independent conditions must all hold, and each is a fail-closed
        // gate rather than a refinement:
        //
        //   1. A durable offset floor was actually computed this pass for THIS
        //      partition. A null floor - an unreachable pin store, a host that
        //      reports no offsets, an all-"-1" pin set - admits NOTHING, leaving
        //      the predicate byte-identical to its pre-#3172 behaviour.
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
        // EXACT-COMPLEMENT INVARIANT - load-bearing, and pairwise. The Floor
        // carried here must be the SAME value the offset-floor STOP below
        // compares against for the SAME partition, because the two comparisons
        // are exact complements (stop on `> floor`, admit on `<= floor`). That
        // is what guarantees admission can never reach an entry the stop would
        // not already have walked past. Both sides therefore read
        // PartitionOffsetFloor(partition) and neither reads a tree-wide value.
        // Do not change one side without the other: admission GRANTS
        // entitlement rather than subtracting it (see the #3172 note above), so
        // an admission floor above the stop floor loses data rather than merely
        // over-retaining.
        WalGcOffsetAdmission? PartitionOffsetAdmission(int partition)
            => PartitionOffsetFloor(partition) is { } floor
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
        // the floor" (a defect state in which the tree cannot reclaim at all, so
        // the WAL it already holds is permanently unreleasable and no retention
        // or compaction setting can release it). Collapsing the two is issue
        // #2702; the scheduler reads this to schedule them differently.
        //
        // This value is the discriminator. Growth, absence of growth, byte count
        // and growth stopping are all unusable for it, because a tree only grows
        // while it is being written to and a blocked tree is flat the rest of the
        // time, so each of them reads benign on a tree that can never release a
        // byte. Purely diagnostic - the trim predicate below is unchanged.
        var cursorFloorState = cursorBlocked
            ? WalGcCursorFloorState.BlockedByUnusablePin
            : hasCursorPredicate
                ? WalGcCursorFloorState.Available
                : WalGcCursorFloorState.NoCursorReported;

        // Sample retained bytes once up front so a byte-pressure trigger is
        // decided against the pre-trim footprint. Returns null when the
        // policy is disabled or the provider does not support byte accounting.
        //
        // The ceiling is re-resolved per pass rather than read off the static
        // options, so a per-tree runtime override set through
        // ILatticeRegistry.SetWalMaxRetainedBytesAsync takes effect on the next
        // pass with no silo restart (issue #3333). The correct ceiling is a
        // function of the tree's live set, which grows, so a value calibrated at
        // deployment time goes stale with no code change and no misconfiguration
        // - and before this it could only be corrected by a restart. When the
        // resolver is unavailable (the bare-IServiceProvider construction a unit
        // test uses) fall back to the static option, which keeps the
        // no-override path byte-identical to the previous behaviour.
        var effectiveCeiling = OptionsResolver is { } ceilingResolver
            ? await ceilingResolver.GetWalMaxRetainedBytesAsync(treeName).ConfigureAwait(false)
            : resolved.WalMaxRetainedBytes;

        var (ceiling, retainedBefore, logicalBefore) = await SampleRetainedBytesAsync(
            ResolvePartitionProvider, resolved, effectiveCeiling, treeName, partitions, cancellationToken).ConfigureAwait(false);
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
                blockingConsumerIds, false, logicalBefore,
                EvaluateCeilingSatisfiability(ceiling, logicalBefore));
        }

        long totalTrimmed = 0;
        var retainedBacklog = false;
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName);
        var tenantTag = LatticeTenantLabel.ForTree(treeName);

        // Durability hold budget (issue #3300), decided once per pass against
        // the pre-trim footprint sampled above, exactly as the byte-pressure
        // trigger is.
        //
        // `holdConfigured` is TRUE by default as of the default-on change: the
        // hold applies only while the tree is UNDER the ceiling; at or above it
        // the hold yields and the pass trims as it always did, because an
        // unbounded WAL is the worse outage and a hold that can never end would
        // recreate issue #3094 on any tree with no materialiser wired. Setting
        // the ceiling non-positive disables the hold outright.
        //
        // A null `retainedBefore` - byte accounting unavailable from the
        // provider - now DECLINES the hold rather than holding through it, which
        // is the reverse of what this did while the knob was opt-in. The old
        // reading was that trimming on an unrun byte measurement is the same
        // reasoning error as trimming on an unrun durability check, and while an
        // operator had to switch the hold on deliberately that was right: they
        // had accepted the cost. Default-on changes who bears it. The ceiling is
        // the only thing bounding this hold, so with no bytes to measure the
        // hold never ends, and shipping that by default would put unbounded
        // retention on every deployment whose provider cannot report bytes -
        // issue #3094, arriving unannounced and on our initiative rather than an
        // operator's. Bounded retention is the entire safety property here, so
        // where the bound cannot exist the hold does not engage; the pass falls
        // back to DurabilityUnverified, which still names the condition loudly.
        var holdCeiling = resolved.WalDurabilityHoldCeilingBytes;

        // Consumer-identity predicate (issue #3300). The hold engages only when
        // every cursor admitting this trim is a leaf materialiser the durable
        // offset floor does not speak for - see WalGcCursorAuthority. Keying on
        // `offsetFloor is null` alone was not separable: a shipper-only tree and
        // the stalled tree this hold exists for present identically under it, so
        // that predicate held both (permanent retention on a correctly-wired
        // deployment) or neither (issue #3300 stays live). Classifying what the
        // cursor is EVIDENCE OF separates them, because a shipper's cursor
        // outlives this process and a materialiser's does not.
        var cursorAuthority = await ClassifyCursorAuthorityAsync(
            treeName, offsetCoverage.CoveredConsumerIds, cancellationToken).ConfigureAwait(false);
        var holdConfigured = holdCeiling is { } hc && hc > 0
            && cursorAuthority is WalGcCursorAuthority.Volatile
                or WalGcCursorAuthority.Unreadable;
        var holdHasBudget = holdConfigured
            && retainedBefore is { } rb && rb < holdCeiling!.Value;

        if (holdHasBudget)
        {
            // Which population this hold caught, recorded once per pass. Both
            // arms retain bytes and both stop on `durability_hold`, but they
            // call for opposite operator responses: `never_pinned` is a stalled
            // tree that will hold until someone repairs its materialiser, while
            // `pin_regressed` is a bounded transient - a rolling upgrade or leaf
            // churn - that clears itself when the leaves re-pin. An operator
            // seeing a hold mid-upgrade needs to know it will end without them,
            // and the stop reason alone cannot tell them.
            //
            // `cursor_unreadable` takes precedence over both because it is not a
            // statement about the pin history at all: the registry did not
            // answer, so neither of the other two arms has been measured and
            // reporting either would assert a fact this pass does not have.
            var holdEngagedReason = cursorAuthority == WalGcCursorAuthority.Unreadable
                ? LatticeMetrics.ReasonHoldEngagedCursorUnreadable
                : HasEverPinnedDurableFloor(treeName)
                    ? LatticeMetrics.ReasonHoldEngagedPinRegressed
                    : LatticeMetrics.ReasonHoldEngagedNeverPinned;
            LatticeMetrics.WalGcDurabilityHoldEngaged.Add(1, treeTag, holdEngagedReason, tenantTag);
        }

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

            var partitionOffsetFloor = PartitionOffsetFloor(partition);

            // Forced progress: the hold is enabled, this partition has no
            // durable floor to trim against, and the hold is not protecting it.
            // Count it before the scan rather than after, so the signal is
            // recorded even if the scan throws - a pass that died partway is
            // not a pass that decided not to discard anything.
            //
            // The reason arm is load-bearing, not decoration. `ceiling_exhausted`
            // is the hold working as designed and running out;
            // `unmeasurable_footprint` is the hold never having engaged, because
            // the provider reports no bytes and an unbounded hold must not be
            // the default. They call for different repairs - the first for a
            // materialiser, the second for a provider that can weigh itself -
            // and a single collapsed arm would say only "unprotected", which is
            // the conflation issue #3309 was raised to undo.
            if (holdConfigured && !holdHasBudget && partitionOffsetFloor is null)
            {
                LatticeMetrics.WalGcDurabilityHoldForced.Add(
                    1,
                    treeTag,
                    retainedBefore is null
                        ? LatticeMetrics.ReasonHoldForcedUnmeasurableFootprint
                        : LatticeMetrics.ReasonHoldForcedCeilingExhausted,
                    tenantTag);
            }

            var shardScan = await TrimShardAsync(partitionProvider, treeName, partition, PartitionCursor(partition), ttlCeiling, causalStable, blockedFloor, partitionOffsetFloor, PartitionOffsetAdmission(partition), holdHasBudget, cancellationToken).ConfigureAwait(false);
            totalTrimmed += shardScan.EligibleCount;
            retainedBacklog |= IsRetentionStop(shardScan.StopReason);
            RecordTrimStop(treeName, partition, shardScan.StopReason);
            RecordEntriesTrimmed(treeTag, tenantTag, partition, shardScan.EligibleCount);
        }

        var (_, retainedAfter, logicalAfter) = await SampleRetainedBytesAsync(
            ResolvePartitionProvider, resolved, effectiveCeiling, treeName, partitions, cancellationToken).ConfigureAwait(false);
        var overThreshold = FinishBytePressure(treeName, resolved, ceiling, retainedBefore, retainedAfter);

        return new LatticeWalGcReport(
            treeName, minCursor, ttlCeiling, causalStable, blockedFloor, partitions, totalTrimmed,
            ceiling, retainedBefore, retainedAfter, triggered, overThreshold, cursorFloorState, blockingConsumerId,
            blockingConsumerIds, retainedBacklog, logicalAfter,
            EvaluateCeilingSatisfiability(ceiling, logicalAfter));
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
    /// Written as an explicit member exclusion rather than a list of the four
    /// retention reasons so that a stop reason added later is treated as
    /// retention - the conservative reading - instead of silently joining the
    /// "nothing left behind" set and re-opening the defect.
    /// </para>
    /// <para>
    /// <see cref="WalGcTrimStopReason.DurabilityUnverified"/> is exempted
    /// EXPLICITLY, and the exemption is a statement about the scan rather than a
    /// relaxation of the rule above (issue #3300). That arm is selected only on
    /// the <c>!stop</c> path - the scan reached the end of the shard without
    /// meeting an entry it had to retain - so it leaves nothing behind for
    /// exactly the same structural reason <see cref="WalGcTrimStopReason.Exhausted"/>
    /// does, and differs from it only in whether a durable floor existed to
    /// judge the released entries against. Leaving it to the conservative
    /// default would assert that WAL outlived a pass which in fact consumed the
    /// whole log, manufacturing a false backlog signal on every tree that
    /// legitimately has no materialiser wired. Note the direction of the risk
    /// this exemption carries, because it is the opposite of the one the
    /// default guards: it cannot hide a stranded tree, since a stranded tree
    /// stops AT a retained entry and can never reach this arm.
    /// </para>
    /// </remarks>
    private static bool IsRetentionStop(WalGcTrimStopReason reason)
        => reason is not (WalGcTrimStopReason.Exhausted
            or WalGcTrimStopReason.Empty
            or WalGcTrimStopReason.DurabilityUnverified);

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
    /// <para>
    /// <b>Empty-WAL rule (issue #3453) - reporting and scheduling only.</b> A
    /// <c>(Zero, -1)</c> pin whose partition
    /// <paramref name="resolvePartitionProvider"/> proves empty still blocks
    /// that partition's trim, but is left out of <c>Blocked</c> and the
    /// blocking-consumer ids, so the scheduler neither reports nor drives it.
    /// It never feeds trim: <c>BlockedPartitions</c> is unchanged. A
    /// <see langword="null"/> resolver disables the rule.
    /// </para>
    /// </summary>
    private async Task<DurableMaterialiserFloor> ApplyDurableMaterialiserFloorAsync(
        string treeName,
        HybridLogicalClock? registryMin,
        int partitions,
        IReadOnlySet<string>? coveredConsumerIds,
        IReadOnlySet<string>? abstainedConsumerIds,
        CancellationToken cancellationToken,
        Func<int, IWalStorageProvider?>? resolvePartitionProvider)
    {
        var factory = GrainFactory;
        if (factory is null)
        {
            WalGcBlockedConsumerCensus.Record(treeName, 0);
            return DurableMaterialiserFloor.Unblocked(registryMin);
        }

        IReadOnlyDictionary<string, HybridLogicalClock> pins;
        try
        {
            pins = await ReadDurablePinsAsync(factory, treeName).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The durable pin store is unavailable on this pass; fall back to
            // the in-memory floor rather than failing the whole GC run. The
            // next pass retries; a missed floor never trims unsafely because
            // the present in-memory consumers still constrain the trim point.
            WalGcBlockedConsumerCensus.Record(treeName, -1);
            services.GetService<ILogger<LatticeWalGc>>()?.LogWarning(
                ex, "WAL GC blocked-consumer census unavailable for tree {Tree}: durable pins could not be read.", treeName);
            return DurableMaterialiserFloor.Unblocked(registryMin);
        }

        if (pins.Count == 0)
        {
            WalGcBlockedConsumerCensus.Record(treeName, 0);
            return DurableMaterialiserFloor.Unblocked(registryMin);
        }

        // ISSUE #2314: cross-check the offset-plane population against the pin
        // plane before trusting the floor computed from it.
        //
        // The durable offset floor is a minimum over the consumers that
        // REPORTED an offset, not over the consumers that OWE WAL entries. The
        // pin dictionary in hand here IS the independent census that
        // distinguishes those two populations: WalMaterialiserPinGrain.Merge
        // writes the pin and the offset in lockstep on every report, so a
        // consumer present here but in neither the coverage set nor the
        // abstained set never reported an offset at all. It has told us nothing,
        // and "nothing" was previously rendered byte-identically to a reported
        // "-1", which means "I owe nothing" and is a real answer.
        //
        // Why this must stop the CURSOR axis and not merely withhold offset
        // admission: the floor is a MINIMUM, so omitting a consumer can only
        // raise it, and the offset floor only ever REFUSES - TrimShardAsync
        // vetoes entries ABOVE it and grants nothing. A floor that is too high
        // therefore refuses fewer entries than it must and hands the decision
        // back to the HLC cursor axis, which will happily trim a low-HLC reap
        // sitting at a high offset: precisely the loss the offset floor exists
        // to prevent. Withholding admission alone would leave that untouched.
        //
        // Blocking the partitions the unreported consumer holds is the same
        // verdict the block-pin clause below reaches for a pin carrying no
        // usable frontier, arrived at from the offset plane instead: when the
        // floor demonstrably does not speak for a consumer, its partitions are
        // not trimmed. The cost is bounded over-retention that clears itself the
        // moment that consumer reports, never data loss, and the blocking ids
        // are carried out so the scheduler's blocked-leaf remedy can act.
        //
        // Gated on the offsets plane having reported SOMETHING on this pass -
        // a real offset (covered) or the "-1" sentinel (abstained). When it
        // reported nothing at all there is no partial floor to be misled by and
        // the tree collects on the HLC axis exactly as it did before the offset
        // plane existed; blocking there would stall every pre-offset deployment,
        // and every pass whose pin-store read threw (which the adjacent
        // offset_floor_unavailable counter already reports), for no safety gain.
        // Note the gate must NOT be conditioned on a real floor existing: a
        // population where every reporter abstained still proves the plane is
        // live, so a consumer missing from it is a genuine gap.
        DurableMaterialiserFloor? populationGap = null;
        long blockingPopulation = 0;
        if (coveredConsumerIds is not null || abstainedConsumerIds is not null)
        {
            bool[]? gapPartitions = null;
            string? firstUnreported = null;
            List<string>? unreportedConsumerIds = null;
            var unreportedCount = 0;

            foreach (var (consumerId, _) in pins)
            {
                if ((coveredConsumerIds is not null && coveredConsumerIds.Contains(consumerId))
                    || (abstainedConsumerIds is not null && abstainedConsumerIds.Contains(consumerId)))
                {
                    continue;
                }

                unreportedCount++;
                gapPartitions ??= new bool[partitions];
                firstUnreported ??= consumerId;
                unreportedConsumerIds ??= new List<string>(MaxReportedBlockingConsumers);
                if (unreportedConsumerIds.Count < MaxReportedBlockingConsumers)
                {
                    unreportedConsumerIds.Add(consumerId);
                }

                if (TryResolvePinPartition(consumerId, partitions) is { } gapPartition)
                {
                    gapPartitions[gapPartition] = true;
                }
                else
                {
                    // Unattributable: fail closed onto every partition, which is
                    // the same whole-tree block the block-pin clause takes.
                    for (var p = 0; p < partitions; p++)
                    {
                        gapPartitions[p] = true;
                    }
                }
            }

            if (unreportedCount > 0)
            {
                LatticeMetrics.WalGcOffsetFloorPopulationGap.Add(
                    unreportedCount,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName),
                    LatticeTenantLabel.ForTree(treeName));

                // Keep the original gap refusal, independently of the census
                // computed below. It must not acquire offset trim entitlement.
                populationGap = new DurableMaterialiserFloor(
                    null, gapPartitions, true, firstUnreported, unreportedConsumerIds, null, false);
                blockingPopulation = unreportedCount;
            }
        }

        IReadOnlyList<WalCursorSnapshot> snapshot;
        try
        {
            snapshot = await cursors.SnapshotAsync(treeName, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            // The pre-existing gap refusal did not need a registry snapshot.
            // A diagnostic read failure must not weaken or replace that refusal.
            WalGcBlockedConsumerCensus.Record(treeName, -1);
            services.GetService<ILogger<LatticeWalGc>>()?.LogWarning(
                ex, "WAL GC blocked-consumer census unavailable for tree {Tree}: cursor snapshot could not be read.", treeName);
            if (populationGap is { } preservedGap) return preservedGap;
            throw;
        }
        // Consumers whose registry cursor is a real (> Zero) frontier, and so
        // was folded into registryMin. Only these may have their durable pin
        // skipped by the pins loop below.
        var present = new HashSet<string>(snapshot.Count, StringComparer.Ordinal);
        // The lowest cursor held by a registry consumer the offset floor does
        // not speak for (issue #3172).
        HybridLogicalClock? uncovered = null;
        for (var i = 0; i < snapshot.Count; i++)
        {
            var entry = snapshot[i];

            // A Zero cursor is a block-pin-only registration: GetMinCursorAsync
            // skips it, so nothing was folded into registryMin for it, and
            // folding it here would pin this minimum at Zero and refuse every
            // entry. It is therefore NOT recorded as present either (issue
            // #3416). That is what makes the skip sound: the pins loop then reads
            // the consumer's durable pin exactly as it reads a registry-absent
            // consumer's - the block-pin clause for a Zero pin, the floor and
            // uncovered folds for a real one. Recording it as present, as this
            // loop once did, excluded it from BOTH guards, so the offset axis
            // admitted WAL a live, registered consumer still owed. A
            // non-materialiser Zero-cursor consumer holds no durable pin, so for
            // it this changes nothing: its buffer pin stays guarded by the
            // blocked-floor clause, as before.
            if (entry.Cursor <= HybridLogicalClock.Zero)
            {
                continue;
            }

            present.Add(entry.ConsumerId);
            if (coveredConsumerIds is not null && coveredConsumerIds.Contains(entry.ConsumerId))
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
        sbyte[]? walEmptyByPartition = null;

        foreach (var (consumerId, pin) in pins)
        {
            // A consumer present in the in-memory registry has a fresher
            // (>=) cursor already folded into registryMin; its durable pin
            // (possibly staler) must not raise the floor. "Present" means a
            // real cursor: a Zero-cursor registration is not in this set
            // (issue #3416), so its pin is read below like an absent one's.
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

                // Issue #3453: a (Zero, -1) pin on a partition whose WAL is
                // proven EMPTY (no entry was ever durably appended) is still
                // recorded against that partition's trim below - so trim is
                // byte-identical to before, and the first entry appended there
                // is retained by the next pass - but it is NOT reported as
                // blocking. There is nothing in an empty partition for it to
                // protect, and a never-written leaf whose replay had nothing to
                // scan has no checkpoint to publish, so reporting it only feeds
                // the scheduler a drive that must return NoAdvance for ever.
                //
                // Narrow and fail-closed: only a consumer the offset plane
                // proved abstained ("-1"), only on a partition the id names
                // (partition 0 on an unpartitioned tree; an unattributable id on
                // a partitioned tree keeps blocking), and only when the head
                // probe positively reads an empty partition. An unresolvable
                // provider or a probe that throws keeps the pin blocking.
                var reportExempt = resolvePartitionProvider is not null
                    && abstainedConsumerIds is not null
                    && abstainedConsumerIds.Contains(consumerId)
                    && (partitions <= 1 ? 0 : TryResolvePinPartition(consumerId, partitions)) is { } probePartition
                    && await IsPartitionWalProvenEmptyAsync(
                        treeName,
                        probePartition,
                        resolvePartitionProvider,
                        walEmptyByPartition ??= new sbyte[Math.Max(1, partitions)],
                        cancellationToken).ConfigureAwait(false);

                if (!reportExempt)
                {
                    // The unioned pin dictionary is already distinct by consumer.
                    // Population gaps were counted above, including usable pins.
                    if (populationGap is null
                        || coveredConsumerIds?.Contains(consumerId) == true
                        || abstainedConsumerIds?.Contains(consumerId) == true)
                    {
                        blockingPopulation++;
                    }
                    blockingConsumerId ??= consumerId;
                    blockingConsumerIds ??= new List<string>(MaxReportedBlockingConsumers);
                    if (blockingConsumerIds.Count < MaxReportedBlockingConsumers)
                    {
                        blockingConsumerIds.Add(consumerId);
                    }
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

                // Continue after the report fills: the census must include every
                // blocker, not just eight ids. No extra pin-store reads are needed;
                // empty-WAL probes remain memoised once per partition.

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

        WalGcBlockedConsumerCensus.Record(treeName, blockingPopulation);
        if (populationGap is { } gap)
        {
            return gap;
        }
        if (blockedCount >= partitions
            && blockingConsumerIds is { Count: >= MaxReportedBlockingConsumers })
        {
            // Preserve the former short-circuit's trim/report shape exactly.
            return new DurableMaterialiserFloor(
                null, blockedPartitions, true, blockingConsumerId, blockingConsumerIds, null, false);
        }
        return new DurableMaterialiserFloor(
            floor,
            blockedPartitions,
            blockingConsumerId is not null,
            blockingConsumerId,
            blockingConsumerIds,
            uncovered,
            true);
    }

    /// <summary>
    /// Whether WAL partition <paramref name="partition"/> of
    /// <paramref name="treeName"/> is proven empty - its provider reports no
    /// entry was ever durably appended (<c>GetHighestOffsetAsync &lt; 0</c>, the
    /// provider's monotonic high-water mark, so a trim never makes a written
    /// partition read empty) - memoised per pass in
    /// <paramref name="cache"/> (<c>0</c> unprobed, <c>1</c> empty,
    /// <c>-1</c> not proven empty). Fails closed: an unresolvable provider or a
    /// probe that throws reads as not empty (issue #3453).
    /// </summary>
    private static async ValueTask<bool> IsPartitionWalProvenEmptyAsync(
        string treeName,
        int partition,
        Func<int, IWalStorageProvider?> resolvePartitionProvider,
        sbyte[] cache,
        CancellationToken cancellationToken)
    {
        if ((uint)partition >= (uint)cache.Length)
        {
            return false;
        }

        if (cache[partition] != 0)
        {
            return cache[partition] > 0;
        }

        var empty = false;
        try
        {
            if (resolvePartitionProvider(partition) is { } provider)
            {
                var highest = await provider
                    .GetHighestOffsetAsync(treeName, partition, cancellationToken)
                    .ConfigureAwait(false);
                empty = highest < 0;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            empty = false;
        }

        cache[partition] = empty ? (sbyte)1 : (sbyte)-1;
        return empty;
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
    /// Whether any partition is blocked by a pin that is <em>reported</em> as
    /// blocking. The report's
    /// <see cref="WalGcCursorFloorState"/> is derived from this, so a tree with
    /// one blocked partition still reports
    /// <see cref="WalGcCursorFloorState.BlockedByUnusablePin"/> and still drives
    /// the scheduler's blocked-leaf remedy at its cadence floor. It can be
    /// <see langword="false"/> while <see cref="BlockedPartitions"/> is not:
    /// a <c>(Zero, -1)</c> pin on a partition whose WAL is proven empty still
    /// blocks that partition's trim but is not reported (issue #3453). Trim
    /// reads <see cref="IsPartitionBlocked"/> only, never this flag.
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
    /// Extracts the WAL partition a materialiser pin's consumer id speaks for
    /// (issue #3178). The multi-partition id shape is
    /// <c>_lattice_materialiser_{treeId}_{leafGrainId}_{partition}</c>; the
    /// single-partition shape omits the suffix entirely.
    /// </summary>
    /// <remarks>
    /// Deliberately strict, because the cost of a false positive is a
    /// relaxation of a retention floor. A trailing run of ASCII digits is only
    /// accepted when it is non-empty, parses as a non-negative
    /// <see cref="int"/>, and carries no redundant leading zero (so
    /// <c>"_07"</c> is refused rather than read as partition 7). Anything else
    /// - no underscore, an empty or non-numeric suffix, an overflowing value -
    /// returns <see langword="false"/>, and the caller then treats the pin as
    /// unattributable and lets it constrain every partition.
    /// </remarks>
    internal static bool TryParseConsumerPartition(string consumerId, out int partition)
    {
        partition = -1;
        if (string.IsNullOrEmpty(consumerId))
        {
            return false;
        }

        var separator = consumerId.LastIndexOf('_');
        if (separator < 0 || separator == consumerId.Length - 1)
        {
            return false;
        }

        var suffix = consumerId.AsSpan(separator + 1);
        foreach (var c in suffix)
        {
            if (c is < '0' or > '9')
            {
                return false;
            }
        }

        // "0" is legitimate; "00" or "07" is not a canonical partition suffix
        // and is far more likely to be part of a grain id than a partition.
        if (suffix.Length > 1 && suffix[0] == '0')
        {
            return false;
        }

        return int.TryParse(suffix, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out partition);
    }

    /// <summary>
    /// Records how long it has been since this tree's durable materialiser
    /// offset floor last advanced, once per garbage-collection pass
    /// (issue #3300).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Three outcomes, and keeping them separate is the entire point. A floor
    /// that rose records <c>advanced</c> at a true zero; a floor that exists and
    /// did not rise records <c>stalled</c> at its age; and a tree with NO floor
    /// at all records <c>absent</c> at the age since this process first saw it -
    /// never zero. Reporting the absent case as zero would give the state in
    /// which nothing is known to be durable the same reading as the healthiest
    /// possible one, which is the conflation that let issue #3300 run for
    /// eleven hours behind counters that all looked fine.
    /// </para>
    /// <para>
    /// Progress is measured against a HIGH-WATER MARK rather than against the
    /// previous pass's value. The floor is a minimum over the leaves that
    /// reported, so it can fall legitimately when a lagging leaf begins
    /// reporting; treating that fall as movement would restart the stall clock
    /// on a tree making no progress, which is the failure this signal exists to
    /// catch.
    /// </para>
    /// <para>
    /// Observation only. It reads no storage, issues no grain call, and never
    /// influences what a pass may trim.
    /// </para>
    /// </remarks>
    private void RecordDurableFloorProgress(string treeName, long? offsetFloor)
    {
        var now = _time.GetUtcNow();

        var updated = _durableFloorProgress.AddOrUpdate(
            treeName,
            _ => new DurableFloorProgress(
                offsetFloor,
                offsetFloor is null ? null : now,
                now),
            (_, prior) =>
            {
                if (offsetFloor is not { } floor)
                {
                    // No floor on this pass. Keep whatever high-water mark and
                    // advance timestamp we had: losing the floor is not
                    // progress, and it must not reset the clock.
                    return prior;
                }

                if (prior.HighWaterFloor is not { } high || floor > high)
                {
                    return prior with { HighWaterFloor = floor, LastAdvanceUtc = now };
                }

                return prior;
            });

        KeyValuePair<string, object?> status;
        DateTimeOffset since;

        if (offsetFloor is null)
        {
            status = LatticeMetrics.StatusDurableFloorAbsent;
            since = updated.LastAdvanceUtc ?? updated.FirstObservedUtc;
        }
        else if (updated.LastAdvanceUtc == now)
        {
            status = LatticeMetrics.StatusDurableFloorAdvanced;
            since = now;
        }
        else
        {
            status = LatticeMetrics.StatusDurableFloorStalled;
            since = updated.LastAdvanceUtc ?? updated.FirstObservedUtc;
        }

        // Clamped at zero so a non-monotonic or substituted TimeProvider can
        // never publish a negative age, which would read as nonsense on a
        // histogram and could not be distinguished from a unit error.
        var stallSeconds = (long)Math.Max(0d, (now - since).TotalSeconds);

        LatticeMetrics.WalGcDurableFloorStallSeconds.Record(
            stallSeconds,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName),
            status,
            LatticeTenantLabel.ForTree(treeName));
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

            // Per-partition attribution of the SAME minimum (issue #3178).
            // Built alongside the tree-wide floor, never instead of it: `floor`
            // and `covered` below are computed exactly as before, so every
            // consumer of them - ApplyDurableMaterialiserFloorAsync's
            // covered/uncovered split in particular - is unchanged.
            //
            // `unattributed` is the fail-closed channel. A consumer id with no
            // parseable partition suffix (the legacy single-partition shape,
            // `_lattice_materialiser_{treeId}_{leafGrainId}`) could speak for
            // any partition, so it is folded into EVERY partition's floor at
            // the end rather than being dropped.
            Dictionary<int, long>? byPartition = null;
            long? unattributed = null;
            // Issue #2314: the consumers that reported the "-1" no-dependency
            // sentinel, carried out rather than merely skipped.
            HashSet<string>? abstained = null;

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
                //
                // Note this sentinel does NOT catch the converged-then-drained
                // partition that issue #3178 is about. Such a partition was
                // written, was fully consumed, and was then trimmed to nothing;
                // its leaves hold a REAL durable checkpoint (offset >= 0) at the
                // partition's terminal offset, not -1. The gap is in the
                // predicate, not an oversight about empty partitions in general.
                if (offset < 0)
                {
                    // ISSUE #2314: carry the abstainers OUT rather than merely
                    // skipping them. Skipping made "this consumer told us it
                    // owes nothing" byte-identical to "this consumer never told
                    // us anything": both were simply absent from the coverage
                    // set. Only the second is a population gap, and only the
                    // second may stop a pass - conflating them forces a choice
                    // between blocking every legitimately empty partition
                    // forever and admitting a genuine gap in silence.
                    abstained ??= new HashSet<string>(StringComparer.Ordinal);
                    abstained.Add(consumerId);
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

                if (TryParseConsumerPartition(consumerId, out var partition))
                {
                    byPartition ??= new Dictionary<int, long>();
                    if (!byPartition.TryGetValue(partition, out var partitionFloor) || offset < partitionFloor)
                    {
                        byPartition[partition] = offset;
                    }
                }
                else if (unattributed is not { } currentUnattributed || offset < currentUnattributed)
                {
                    unattributed = offset;
                }
            }

            // Fold the unattributable minimum into every partition. Without
            // this an unsuffixed pin would constrain nothing once any suffixed
            // pin existed, which would be a relaxation this change does not
            // intend and cannot justify.
            if (byPartition is not null && unattributed is { } unattributedFloor)
            {
                foreach (var partition in byPartition.Keys.ToArray())
                {
                    if (unattributedFloor < byPartition[partition])
                    {
                        byPartition[partition] = unattributedFloor;
                    }
                }
            }

            return new MaterialiserOffsetCoverage(floor, covered, abstained, byPartition);
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
            // The deeper population caveat this counter does not cover is now
            // handled on the pin plane instead (issue #2314). The floor below
            // is a minimum over the leaves that REPORTED an offset, not over the
            // leaves that OWE entries, so a leaf absent from the offsets plane
            // does not constrain it at all - and absence is NOT the same state
            // as a reported -1. A reported -1 comes from a participating leaf
            // that has told us it owes nothing, and is covered either by a
            // paired Zero HLC block pin (the data-bearing, not-durably-
            // recoverable case) or by there being no committed prefix to lose at
            // all (the genuinely-empty case, which ResolveDurablePinForPartition
            // reports with the leaf's REAL frontier and so with no block pin -
            // it does not need one). An absent leaf - one whose birth block-pin
            // seed was swallowed, or whose state predates the offsets plane -
            // has told us nothing and carries neither cover.
            //
            // The two are now structurally separable: a reported -1 lands in
            // AbstainedConsumerIds, so absence from BOTH that set and the
            // coverage set means "never reported", and nothing else. The census
            // that distinguishes them is the durable PIN dictionary, which
            // WalMaterialiserPinGrain.Merge writes in lockstep with the offsets
            // dictionary on every report - so it enumerates every consumer the
            // store knows about, and ApplyDurableMaterialiserFloorAsync already
            // holds it. Treating absence as offset 0 is still NOT done, because
            // it would pin the WAL forever for a permanently-departed leaf;
            // ApplyDurableMaterialiserFloorAsync blocks the affected partitions
            // instead, which is bounded over-retention that clears itself the
            // moment the consumer reports.
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
    /// <param name="AbstainedConsumerIds">
    /// The consumers that reported the "-1" no-WAL-replay-dependency sentinel,
    /// or <see langword="null"/> when none did. They are deliberately absent
    /// from <paramref name="CoveredConsumerIds"/> - the floor is not evidence
    /// about them - but they are NOT unknown: they participated and told us they
    /// owe nothing. Carrying them makes absence from BOTH sets mean exactly one
    /// thing, "this consumer never reported an offset at all", which is the
    /// population gap issue #2314 is about and which was previously
    /// indistinguishable from a reported "-1".
    /// </param>
    private readonly record struct MaterialiserOffsetCoverage(
        long? Floor,
        IReadOnlySet<string>? CoveredConsumerIds,
        IReadOnlySet<string>? AbstainedConsumerIds = null,
        IReadOnlyDictionary<int, long>? FloorsByPartition = null)
    {
        /// <summary>
        /// No offset floor on this pass, and therefore no consumer covered by
        /// one. The fail-closed value.
        /// </summary>
        public static MaterialiserOffsetCoverage None => new(null, null, null, null);

        /// <summary>
        /// The offset floor WAL partition <paramref name="partition"/> trims
        /// against (issue #3178).
        /// <para>
        /// A leaf's materialiser pin for partition <c>p</c> only ever advances
        /// when an entry is appended to partition <c>p</c>, so a partition that
        /// has converged and then fully drained holds its terminal checkpoint
        /// permanently. Minimised across partitions, that terminal value caps
        /// every OTHER partition's trim scan forever, and no amount of
        /// reactivating the holding leaf can lift it - the partition is empty by
        /// construction, so there is nothing to replay and nothing to advance
        /// over. Attributing the minimum per partition removes that coupling.
        /// </para>
        /// <para>
        /// Safe because the seam's own safety argument quantifies over LEAVES,
        /// not partitions: an entry at offset <c>O</c> in partition <c>p</c> is
        /// only ever replayed by leaves reading partition <c>p</c>
        /// (<c>ReplayPartitionAsync</c> resolves an
        /// <c>ILeafReplayCoordinatorGrain</c> keyed <c>{treeId}/{partition}</c>
        /// and every slice read goes through it), gated by that leaf's
        /// checkpoint for partition <c>p</c> alone
        /// (<c>ProjectionCheckpointOffsetsByPartition[p]</c>). The leaf that
        /// owns <c>O</c> therefore still holds partition <c>p</c>'s minimum
        /// below <c>O</c> until it genuinely applies. The cross-partition term
        /// protected nothing, because no leaf reads partition <c>p</c> through
        /// partition <c>q</c>'s checkpoint.
        /// </para>
        /// <para>
        /// Fails closed to the tree-wide <see cref="Floor"/> in every case it
        /// cannot attribute: a partition no pin reported on, an unsuffixed
        /// (legacy single-partition) consumer id, or no per-partition map at
        /// all. Unattributable pins are additionally folded INTO every
        /// partition's floor by the builder, so they keep constraining the whole
        /// tree exactly as before.
        /// </para>
        /// </summary>
        public long? FloorFor(int partition)
            => FloorsByPartition is { } map && map.TryGetValue(partition, out var partitionFloor)
                ? partitionFloor
                : Floor;
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
    /// (<see cref="LatticeOptions.WalMaxRetainedBytes"/>), the occupancy total
    /// summed across every partition, and the <i>logical</i> retained payload
    /// summed across every partition. Returns all-null when the policy is
    /// disabled, and a null component when no partition's provider supports that
    /// form of accounting. The policy never trims past the safe frontier; the
    /// sampled totals only feed the advisory report and metrics.
    /// <para>
    /// Occupancy is sampled with
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
    /// <para>
    /// The logical total is sampled <i>as well as</i>, not instead of, the
    /// occupancy total, and that is the whole point of taking both (issue
    /// #3242). Occupancy is the quantity the ceiling <b>bounds</b>; the live set
    /// is the quantity the ceiling has to be <b>sized against</b>, because
    /// designed steady-state occupancy is a multiple of it
    /// (<see cref="LatticeOptions.WalMaxRetainedBytesWorkingSetMultiple"/>).
    /// Deriving the second from the first is not possible: occupancy oscillates
    /// between one and that multiple of the live set over a compaction cycle, so
    /// a satisfiability verdict read off occupancy alone would be a function of
    /// where in the sawtooth the pass happened to land. The extra probe is
    /// contractually O(1) - a provider must answer it from a running counter or
    /// a bounded metadata read and must never scan the log - and it is paid only
    /// by a deployment that configured a ceiling.
    /// </para>
    /// </summary>
    private static async Task<(long? Ceiling, long? Retained, long? Logical)> SampleRetainedBytesAsync(
        Func<int, IWalStorageProvider?> resolveProvider,
        LatticeOptions resolved,
        long? effectiveCeiling,
        string treeName,
        int partitions,
        CancellationToken cancellationToken)
    {
        if (effectiveCeiling is not { } ceiling || ceiling <= 0)
        {
            // Byte-pressure policy disabled - zero hot-path cost.
            //
            // The durability hold (issue #3300) also needs this sample, and for
            // a reason the byte-pressure policy does not share: its ceiling is
            // what BOUNDS the hold, so a hold running against an unsampled tree
            // never ends and grows the WAL without limit - the #3094 shape the
            // bound exists to avoid. So take the sample for the hold too, and
            // return a null Ceiling to keep the byte-pressure policy off. The
            // two ceilings stay independent; only the measurement is shared.
            //
            // Since the hold became default-on this branch is the common case
            // rather than the opt-in one, so a tree with byte pressure disabled
            // now pays one retained-bytes sample per GC pass where it previously
            // paid none. That is a per-pass, per-partition provider call, not a
            // per-entry one, and it is the measurement the hold is bounded by:
            // declining to take it would not save the work, it would disable the
            // bound and with it the hold (see the null-sample path in
            // CollectAsync).
            if (resolved.WalDurabilityHoldCeilingBytes is not { } holdCeiling || holdCeiling <= 0)
            {
                return (null, null, null);
            }

            var holdSample = await SampleRetainedBytesCoreAsync(
                resolveProvider, treeName, partitions, cancellationToken).ConfigureAwait(false);
            return (null, holdSample.Retained, holdSample.Logical);
        }

        var sample = await SampleRetainedBytesCoreAsync(
            resolveProvider, treeName, partitions, cancellationToken).ConfigureAwait(false);
        return (ceiling, sample.Retained, sample.Logical);
    }

    /// <summary>
    /// Sums a tree's retained occupancy and logical payload across partitions,
    /// independent of which policy asked for it.
    /// </summary>
    private static async Task<(long? Retained, long? Logical)> SampleRetainedBytesCoreAsync(
        Func<int, IWalStorageProvider?> resolveProvider,
        string treeName,
        int partitions,
        CancellationToken cancellationToken)
    {
        long retained = 0;
        long logical = 0;
        var anySupported = false;
        var anyLogicalSupported = false;
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

            // The live-payload sample, taken unconditionally so the
            // satisfiability verdict is decided on the same population the
            // occupancy total covers. It doubles as the occupancy fallback
            // below, so a provider without physical accounting is still probed
            // exactly once.
            var live = await provider
                .GetRetainedByteSizeAsync(treeName, partition, cancellationToken)
                .ConfigureAwait(false);
            if (live >= 0)
            {
                anyLogicalSupported = true;
                logical += live;
            }

            var bytes = await provider
                .GetPhysicalByteSizeAsync(treeName, partition, cancellationToken)
                .ConfigureAwait(false);
            if (bytes < 0)
            {
                // -1 sentinel: no physical accounting. Fall back to the
                // logical retained total, which is this backend's occupancy
                // when its trim deletes rather than marks dead.
                bytes = live;
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

        return (
            anySupported ? retained : null,
            anyLogicalSupported ? logical : null);
    }

    /// <summary>
    /// Whether the configured ceiling is <b>arithmetically unreachable</b> by a
    /// healthy tree holding the live set this pass measured (issue #3242).
    /// </summary>
    /// <remarks>
    /// <para>
    /// A log-structured provider reclaims dead bytes only by rewriting a
    /// segment, and it rewrites once dead bytes reach a configured fraction of
    /// total payload, so designed steady-state occupancy is
    /// <see cref="LatticeOptions.WalMaxRetainedBytesWorkingSetMultiple"/> times
    /// the live set. A ceiling below that is breached by a tree doing nothing
    /// wrong, and - because
    /// <see cref="LatticeOptions.WalBytePressureReclaimTarget"/> puts the disarm
    /// point below the natural floor of the same compaction cycle - it is
    /// breached permanently.
    /// </para>
    /// <para>
    /// Deliberately <b>false</b>, not "unknown", in all three of the shapes it
    /// cannot decide: policy disabled, no logical accounting, and an empty tree
    /// (<c>0</c> live bytes makes every positive ceiling satisfiable, which is
    /// true rather than merely undecided). The instrument this feeds is
    /// zero-primed, so those shapes read a measured zero; a false positive on a
    /// tree the library cannot measure would be strictly worse, because the
    /// remedy it names - raise the ceiling - is one an operator would act on.
    /// </para>
    /// <para>
    /// The comparison is made in <see cref="double"/> rather than by integer
    /// multiplication so that the multiple stays expressible as a ratio, and
    /// because the alternative overflows a <see cref="long"/> for live sets
    /// above 4 EiB while <see cref="double"/> is exact on byte counts below
    /// 8 PiB. It is a non-strict floor: a ceiling of exactly the multiple is
    /// satisfiable and does not report.
    /// </para>
    /// </remarks>
    private static bool EvaluateCeilingSatisfiability(long? ceiling, long? logicalRetained)
        => ceiling is { } cap
            && cap > 0
            && logicalRetained is { } live
            && live > 0
            && cap < live * LatticeOptions.WalMaxRetainedBytesWorkingSetMultiple;

    /// <summary>
    /// What the cursors admitting this pass's trim are evidence OF
    /// (issue #3300).
    /// </summary>
    /// <remarks>
    /// <para>
    /// The durability hold originally engaged on <c>offsetFloor is null</c>
    /// alone. That predicate is not separable: a shipper-only tree and the
    /// stalled <c>repo-context-memory</c> tree present identically under it -
    /// offset floor null, a positive cursor present, entries trimming - so a
    /// hold keyed on floor presence either holds both or neither. Holding a
    /// shipper-only tree is permanent retention on a correctly-configured
    /// deployment; holding neither leaves issue #3300 live. Neither is
    /// shippable.
    /// </para>
    /// <para>
    /// The separable question is not whether a floor exists but <b>what the
    /// cursor means</b>. A replication shipper's cursor is evidence that the
    /// data reached a peer, and it survives this process. A leaf materialiser's
    /// cursor is a claim about state held in that leaf's memory, and
    /// <c>BPlusLeafGrain.Activation.cs:2515-2519</c> publishes it <i>precisely
    /// on the path where the checkpoint was NOT advanced</i> - so on that path
    /// the cursor is emitted exactly when durability was not achieved, and the
    /// collector then reads it as permission to release. That claim dies with
    /// the process, which is what makes the trim data loss.
    /// </para>
    /// <para>
    /// Hence <see cref="WalGcCursorAuthority.Volatile"/>: every consumer
    /// admitting the trim is a leaf materialiser that the durable offset floor
    /// does not speak for, so nothing outside this process has attested to any
    /// of it. This is the only arm the hold engages on. The consumer
    /// populations are the ones the seam already distinguishes - see the
    /// <c>UncoveredCursor</c> paragraph above, which names view maintainers,
    /// log subscribers, the backup capture service and the shipper as consumers
    /// that report cursors and never offsets.
    /// </para>
    /// </remarks>
    internal enum WalGcCursorAuthority
    {
        /// <summary>
        /// No consumer holds a positive cursor, so no cursor admits anything and
        /// the scan stops on the cursor floor regardless. The hold would be a
        /// no-op here and does not engage - reporting a hold on a pass that was
        /// never going to trim would be the reassuring-value defect the rest of
        /// this work exists to remove.
        /// </summary>
        None = 0,

        /// <summary>
        /// At least one consumer admitting the trim holds durable evidence: it
        /// is either not a leaf materialiser at all (a shipper, view maintainer,
        /// log subscriber or backup capture), or it is a materialiser the
        /// durable offset floor already speaks for. The trim releases data
        /// something outside this process has attested to, so the hold does not
        /// engage.
        /// </summary>
        Durable = 1,

        /// <summary>
        /// Every consumer admitting the trim is a leaf materialiser the durable
        /// offset floor does NOT cover, so the sole attestation for the entries
        /// about to be released is in-process state that dies at the process
        /// boundary. This is the issue #3300 shape and the only arm that holds.
        /// </summary>
        Volatile = 2,

        /// <summary>
        /// The cursor registry could not be read, so what is watching this tree
        /// is unknown (issue #3366). Holds exactly as <see cref="Volatile"/>
        /// does, and is carried as its own value so that "could not measure" is
        /// never reported as a measurement.
        /// <para>
        /// This arm previously did not exist: a registry fault returned
        /// <see cref="Durable"/>, on the reasoning that an unread registry is
        /// not evidence that nothing durable is watching. That is true, and it
        /// is symmetric - an unread registry is equally not evidence that
        /// something durable <em>is</em> watching - so it does not select
        /// between the two answers. What breaks the tie is which way the answer
        /// fails, and the permissive one was chosen while being described as
        /// failing closed. It is not: <see cref="Durable"/> disables the hold,
        /// so the trim proceeds with no durability evidence at all. Retaining
        /// bytes on an unreadable registry costs bounded disk the hold ceiling
        /// already caps; releasing them costs acknowledged writes nothing can
        /// reconstruct. Those costs are not comparable, so the tie breaks
        /// toward the hold.
        /// </para>
        /// <para>
        /// Deliberately NOT folded into <see cref="Volatile"/>, though both
        /// engage the hold. They indict different subsystems and call for
        /// different repairs - <see cref="Volatile"/> is a correctly observed
        /// stalled materialiser, this is a registry that did not answer - and
        /// collapsing them would leave a tree holding because its registry is
        /// unreachable indistinguishable from one holding because its
        /// materialiser is stalled, sending an operator to repair the wrong
        /// thing.
        /// </para>
        /// </summary>
        Unreadable = 3,
    }

    /// <summary>
    /// Classifies what the cursors admitting this pass are evidence of
    /// (issue #3300). See <see cref="WalGcCursorAuthority"/>.
    /// </summary>
    /// <remarks>
    /// Takes its own registry snapshot rather than reusing the one
    /// <see cref="ApplyDurableMaterialiserFloorAsync"/> takes, because that
    /// method returns before snapshotting when the durable pin store is empty
    /// or unreachable - and an empty pin store is exactly the issue #3300 state
    /// this classification has to be correct in. Folding it in would blind the
    /// predicate on its own target. A registry error classifies as
    /// <see cref="WalGcCursorAuthority.Unreadable"/>, which engages the hold:
    /// an unread registry is not evidence that nothing durable is watching, but
    /// nor is it evidence that something is, and of the two answers only one
    /// can destroy acknowledged writes (issue #3366).
    /// </remarks>
    private async Task<WalGcCursorAuthority> ClassifyCursorAuthorityAsync(
        string treeName,
        IReadOnlySet<string>? coveredConsumerIds,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<WalCursorSnapshot> snapshot;
        try
        {
            snapshot = await cursors.SnapshotAsync(treeName, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Logged rather than swallowed: the previous bare catch made the
            // fault undiagnosable, so a registry that had been failing for
            // weeks was indistinguishable from one answering "durable". Resolved
            // from the service provider rather than injected, because this class
            // is otherwise metrics-only and widening its constructor would churn
            // every call site that builds one.
            services.GetService<ILogger<LatticeWalGc>>()?.LogWarning(
                ex,
                "WAL GC: cursor registry unreadable for tree {TreeName}; engaging durability hold (issue #3366).",
                treeName);
            return WalGcCursorAuthority.Unreadable;
        }

        var sawAdmittingCursor = false;
        for (var i = 0; i < snapshot.Count; i++)
        {
            var entry = snapshot[i];

            // Zero-cursor consumers admit nothing, for the same reason
            // GetMinCursorAsync skips them: a Zero cursor is a block-pin-only
            // registration. It is not evidence either way and must not make an
            // otherwise-volatile tree look durable.
            if (entry.Cursor <= HybridLogicalClock.Zero)
            {
                continue;
            }

            sawAdmittingCursor = true;

            var isMaterialiser = entry.ConsumerId.StartsWith(
                BPlusTree.Grains.ILeafCursorReporter.MaterialiserConsumerIdPrefix,
                StringComparison.Ordinal);
            if (!isMaterialiser)
            {
                return WalGcCursorAuthority.Durable;
            }

            if (coveredConsumerIds is not null && coveredConsumerIds.Contains(entry.ConsumerId))
            {
                return WalGcCursorAuthority.Durable;
            }
        }

        return sawAdmittingCursor
            ? WalGcCursorAuthority.Volatile
            : WalGcCursorAuthority.None;
    }

    /// <summary>
    /// Whether a durable materialiser offset floor has <em>ever</em> been
    /// observed for <paramref name="treeName"/> in this process (issue #3300).
    /// </summary>
    /// <remarks>
    /// This is what separates the two populations a durability hold can catch,
    /// and the distinction is the operator's, not the collector's. A tree that
    /// has never pinned is stalled and needs someone to wire or repair a
    /// materialiser; a tree whose floor existed and has gone is mid-upgrade or
    /// mid-leaf-churn and will clear itself when the leaves re-pin. Both hold,
    /// both retain bytes, and they are indistinguishable from the stop reason
    /// alone - so the engagement counter carries the arm. Reads the high-water
    /// mark <see cref="RecordDurableFloorProgress"/> maintains, which by
    /// construction never regresses on floor loss.
    /// </remarks>
    private bool HasEverPinnedDurableFloor(string treeName)
        => _durableFloorProgress.TryGetValue(treeName, out var progress)
            && progress.HighWaterFloor is not null;

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
        bool durabilityHold,
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

                // Durability hold (issue #3300). No durable materialiser offset
                // floor exists for this tree, so there is no evidence that any
                // leaf has applied any of what follows; retain it rather than
                // release it on the strength of an absent check.
                //
                // Placed after entriesSeen++ so an EMPTY shard never reaches it
                // and still reports Empty: a shard with nothing in it has
                // nothing to protect, and putting it on a retention arm would
                // make every idle shard in a hold-configured fleet look like a
                // stranded one.
                //
                // Placed before the offset-floor gate below because that gate
                // is a no-op when the floor is null - `offsetFloor is { } floor`
                // is exactly the fail-open through which #3300 released eleven
                // hours of writes. This is the branch that closes it, and the
                // caller has already decided the hold has budget left.
                if (durabilityHold && offsetFloor is null)
                {
                    stopReason = WalGcTrimStopReason.DurabilityHold;
                    stop = true;
                    break;
                }

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
        else if (!stop && offsetFloor is null)
        {
            // The scan walked a NON-EMPTY shard to its end while no durable
            // materialiser offset floor existed for this tree, so every entry
            // it released was released without any check that the data had been
            // durably applied anywhere (issue #3300).
            //
            // Reported separately from Exhausted, which it would otherwise be
            // indistinguishable from. The two differ in the only way that
            // matters here: Exhausted means the scan HAD a durable floor and
            // cleared it, this means it had none to clear. Collapsed together -
            // as they were - a tree discarding live, never-checkpointed records
            // on every pass published the arm documented as healthy, which is
            // precisely how this stayed invisible.
            //
            // Ordered AFTER the empty check on purpose. An empty shard has
            // nothing to lose, so a missing floor over it is uninteresting and
            // stays on the Empty arm; the state worth naming is the one where
            // entries were actually released.
            //
            // Diagnostic only, exactly like every other arm. The scan above has
            // already finished and this selects a label for what it did; it
            // does not and must not change which entries were eligible. Making
            // trim fail closed here is a separate behavioural change, and an
            // unconditional one would grow the WAL without bound on every tree
            // that legitimately has no materialiser wired (issue #3094).
            stopReason = WalGcTrimStopReason.DurabilityUnverified;
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
            // becomes Exhausted, and reclamation silently stops again.
            //
            // SCOPE - stated exactly, because an earlier revision of this
            // comment overstated it and claimed the dead-byte quantity "is
            // independent of every stop reason". It is not: it is causally
            // downstream of one. This call reaches the evaluation; it cannot
            // create the quantity the evaluation tests. Dead bytes rise only
            // inside the provider's trim, or on the replay of a marker that
            // trim wrote, so a shard that has released nothing since its very
            // first entry holds no dead bytes at all, and every arm correctly
            // declines on an operand pinned at zero. Its retained bytes are
            // LIVE, not dead, so no threshold and no rewrite would return one
            // of them - only the floor advancing does. What this call
            // completes is the other population: a shard that HAS trimmed
            // before and is floored now, whose accumulated dead bytes were
            // previously measured against no threshold at all and are now
            // measured against the same policy a trim would have applied.
            //
            // The never-released population is instead made nameable one
            // level up, by the shard-attributed trim-stop arm the caller
            // records: a stop arm advancing for a shard whose entries-trimmed
            // counter stays flat is a shard asked on every pass that releases
            // nothing. That is the signal which does not read through the
            // dead-byte accounting, and it is the only kind that can work
            // here, because every dead-byte arm reads a quantity this stop
            // prevents from ever being written.
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
            WalGcTrimStopReason.DurabilityUnverified => LatticeMetrics.ReasonTrimDurabilityUnverified,
            WalGcTrimStopReason.DurabilityHold => LatticeMetrics.ReasonTrimDurabilityHold,
            WalGcTrimStopReason.DurableOffsetRefusal => LatticeMetrics.ReasonTrimDurableOffsetRefusal,
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
    /// Records one trim-scan stop for partition <paramref name="shardIndex"/> of
    /// <paramref name="treeName"/>. Called with <paramref name="delta"/> zero to
    /// prime an arm, and with one to report an actual scan.
    /// <para>
    /// The shard dimension is what makes the arm joinable against the equally
    /// shard-attributed <see cref="LatticeMetrics.WalEntriesTrimmed"/>, and that
    /// join is the only signal that names a shard which is asked on every pass
    /// and releases nothing (issue #3207). Summed to the tree the arm cannot
    /// distinguish the two estates: a shard that trims thousands of entries and
    /// then stops at the floor publishes the same <c>offset_floor</c> arm as a
    /// shard that has never released an entry in its life, so the healthy tree
    /// and the wedged one are indistinguishable on it. Per shard they separate
    /// exactly - a stop arm advancing while that shard's entries-trimmed counter
    /// stays flat is a shard releasing nothing - and no dead-byte arm can report
    /// that state, because dead bytes only rise as a consequence of the release
    /// that is not happening.
    /// </para>
    /// </summary>
    private static void RecordTrimStop(
        string treeName, int shardIndex, WalGcTrimStopReason reason, long delta = 1)
        => LatticeMetrics.WalGcTrimStops.Add(
            delta,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, shardIndex),
            ClassifyTrimStop(reason),
            LatticeTenantLabel.ForTree(treeName));

    /// <summary>
    /// Zero-primes every <see cref="WalGcTrimStopReason"/> arm for every one of
    /// <paramref name="treeName"/>'s <paramref name="partitions"/> partitions, so
    /// an absent series means WAL GC is not running for this tree on this silo
    /// rather than that no scan ever stopped.
    /// <para>
    /// Called above every early return in <see cref="RunOnceAsync"/>, because the
    /// pass that reclaims nothing is exactly the pass a reader is investigating
    /// and it is the one most likely to return before reaching the trim loop.
    /// </para>
    /// <para>
    /// Primed across the whole partition range rather than only the partitions
    /// this silo resolves a provider for, because a partition pinned to a
    /// provider key this silo cannot resolve is skipped inside the loop and
    /// would otherwise publish no arm at all. Primed, it publishes nine flat
    /// zeros and no entries-trimmed series, which is a distinguishable and
    /// honest reading; a shard this silo does scan and cannot release advances
    /// an arm instead.
    /// </para>
    /// </summary>
    private static void PrimeTrimStopSeries(string treeName, int partitions)
    {
        for (var partition = 0; partition < partitions; partition++)
        {
            RecordTrimStop(treeName, partition, WalGcTrimStopReason.Exhausted, 0);
            RecordTrimStop(treeName, partition, WalGcTrimStopReason.Empty, 0);
            RecordTrimStop(treeName, partition, WalGcTrimStopReason.OffsetFloor, 0);
            RecordTrimStop(treeName, partition, WalGcTrimStopReason.CursorFloor, 0);
            RecordTrimStop(treeName, partition, WalGcTrimStopReason.CausalFrontier, 0);
            RecordTrimStop(treeName, partition, WalGcTrimStopReason.BlockPin, 0);
            RecordTrimStop(treeName, partition, WalGcTrimStopReason.DurabilityUnverified, 0);
            RecordTrimStop(treeName, partition, WalGcTrimStopReason.DurabilityHold, 0);
            RecordTrimStop(treeName, partition, WalGcTrimStopReason.DurableOffsetRefusal, 0);
        }
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
            WalGcTrimEligibility.DurableOffsetRefusal => WalGcTrimStopReason.DurableOffsetRefusal,
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
