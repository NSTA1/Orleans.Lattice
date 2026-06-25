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
        // Floor the trim point under the durable leaf-materialiser pins for
        // any leaf MISSING from the in-memory registry. This survives a full
        // silo/cluster restart that wiped the registry: a forward consumer
        // (e.g. the replication shipper) re-reports its durably-advanced
        // cursor eagerly, but dormant leaves re-register only lazily, so
        // without this floor the GC would trim past a leaf's durable
        // checkpoint and lose its committed-but-not-yet-checkpointed WAL tail.
        minCursor = await ApplyDurableMaterialiserFloorAsync(treeName, minCursor, cancellationToken).ConfigureAwait(false);
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
            ResolvePartitionProvider, resolved, treeName, partitions, cancellationToken).ConfigureAwait(false);
        var triggered = EvaluateBytePressureTrigger(treeName, resolved, ceiling, retainedBefore);
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
            var over0 = FinishBytePressure(treeName, resolved, ceiling, retainedBefore, retainedBefore);
            return new LatticeWalGcReport(
                treeName, minCursor, ttlCeiling, causalStable, blockedFloor, partitions, 0,
                ceiling, retainedBefore, retainedBefore, triggered, over0);
        }

        long totalTrimmed = 0;
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
            totalTrimmed += await TrimShardAsync(partitionProvider, treeName, partition, minCursor, ttlCeiling, causalStable, blockedFloor, cancellationToken).ConfigureAwait(false);
        }

        if (totalTrimmed > 0)
        {
            LatticeMetrics.WalEntriesTrimmed.Add(
                totalTrimmed,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName));
        }

        var (_, retainedAfter) = await SampleRetainedBytesAsync(
            ResolvePartitionProvider, resolved, treeName, partitions, cancellationToken).ConfigureAwait(false);
        var overThreshold = FinishBytePressure(treeName, resolved, ceiling, retainedBefore, retainedAfter);

        return new LatticeWalGcReport(
            treeName, minCursor, ttlCeiling, causalStable, blockedFloor, partitions, totalTrimmed,
            ceiling, retainedBefore, retainedAfter, triggered, overThreshold);
    }

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
    /// <see cref="HybridLogicalClock.Zero"/> - a leaf that activated but never
    /// checkpointed - returns <see langword="null"/>, disabling the cursor
    /// branch of the GC predicate entirely so the WAL head is retained for
    /// that leaf (the TTL ceiling still bounds growth). When the grain factory
    /// is unavailable (a bare-IServiceProvider unit-test construction) or no
    /// durable pins exist, the registry minimum is returned unchanged.
    /// </para>
    /// </summary>
    private async Task<HybridLogicalClock?> ApplyDurableMaterialiserFloorAsync(
        string treeName,
        HybridLogicalClock? registryMin,
        CancellationToken cancellationToken)
    {
        var factory = GrainFactory;
        if (factory is null)
        {
            return registryMin;
        }

        IReadOnlyDictionary<string, HybridLogicalClock> pins;
        try
        {
            pins = await factory.GetGrain<IWalMaterialiserPinGrain>(treeName)
                .GetPinsAsync()
                .ConfigureAwait(false);
        }
        catch
        {
            // The durable pin store is unavailable on this pass; fall back to
            // the in-memory floor rather than failing the whole GC run. The
            // next pass retries; a missed floor never trims unsafely because
            // the present in-memory consumers still constrain the trim point.
            return registryMin;
        }

        if (pins.Count == 0)
        {
            return registryMin;
        }

        var snapshot = await cursors.SnapshotAsync(treeName, cancellationToken).ConfigureAwait(false);
        var present = new HashSet<string>(snapshot.Count, StringComparer.Ordinal);
        for (var i = 0; i < snapshot.Count; i++)
        {
            present.Add(snapshot[i].ConsumerId);
        }

        var floor = registryMin;
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
                // Never-checkpointed dormant leaf: block the cursor branch
                // entirely so nothing is trimmed by cursor for this tree.
                // Zero is the strongest possible floor, so short-circuit.
                return null;
            }

            floor = floor is { } current
                ? (pin < current ? pin : current)
                : pin;
        }

        return floor;
    }

    /// <summary>
    /// Emits the byte-pressure reclaim and over-threshold signals after a
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
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeName));
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
    /// (<see cref="LatticeOptions.WalMaxRetainedBytes"/>) and the retained-byte
    /// total summed across every partition. Returns <c>(null, null)</c> when
    /// the policy is disabled and <c>(ceiling, null)</c> when the provider does
    /// not support byte accounting (every partition returned the <c>-1</c>
    /// sentinel). The policy never trims past the safe frontier; the sampled
    /// total only feeds the advisory report and metrics.
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

