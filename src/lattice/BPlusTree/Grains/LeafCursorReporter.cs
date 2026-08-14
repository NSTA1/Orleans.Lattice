using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILeafCursorReporter"/> implementation that forwards
/// every report and unregister to the silo-registered
/// <see cref="IWalCursorRegistry"/>, and additionally mirrors each leaf's
/// durable checkpoint frontier into the cluster-wide
/// <see cref="IWalMaterialiserPinGrain"/> so the WAL GC's trim floor survives
/// a full silo/cluster restart. Wired up by
/// <see cref="LatticeServiceCollectionExtensions.AddWalCursorRegistry"/>
/// so a host that opts into the cursor registry automatically promotes
/// every leaf grain to a first-class WAL consumer; a host without the
/// registry leaves the registration absent and the leaf grain skips the
/// report path entirely (the partial <c>BPlusLeafGrain.CursorRegistry</c>
/// resolves the reporter as a nullable service and no-ops when null).
/// <para>
/// The durable-pin mirror is fire-and-forget and coalesced: the leaf calls
/// <see cref="NoteDurableMaterialiserFrontier"/> off its foreground/checkpoint
/// path, and this reporter debounces per <c>(treeName, consumerId)</c> so a
/// busy every-write-checkpoint leaf does not issue a durable grain write per
/// write. A debounce that drops an intermediate frontier only ever leaves the
/// durable pin <i>older</i> than the leaf's true frontier, which is GC-safe.
/// </para>
/// </summary>
internal sealed class LeafCursorReporter(
    IWalCursorRegistry registry,
    IGrainFactory? grainFactory = null,
    IOptionsMonitor<LatticeOptions>? options = null,
    ILogger<LeafCursorReporter>? logger = null,
    IGrainStorage? pinStorage = null,
    Func<string, GrainId>? pinGrainIdResolver = null) : ILeafCursorReporter
{
    /// <summary>
    /// Minimum wall-clock spacing between durable pin writes for a single
    /// <c>(treeName, consumerId)</c>. A leaf that advances faster than this
    /// coalesces its durable reports; the in-memory registry still tracks
    /// every advance at full fidelity, so only the durable restart-backstop
    /// is debounced. The first real (non-Zero) frontier always writes
    /// through regardless of spacing so the pin leaves its seeded Zero
    /// "block" value promptly.
    /// </summary>
    private const long MinDurableWriteSpacingMs = 1000;

    /// <summary>
    /// Per-<c>(treeName, consumerId)</c> debounce state for the durable pin
    /// mirror. Holds the last frontier and last checkpoint offset written
    /// through and the wall-clock tick at which they were written. The offset
    /// is tracked alongside the HLC so an offset-only advance (a
    /// tombstone-compaction reap moves the applied offset while the HLC stays
    /// flat) is not coalesced away by the HLC-only comparison.
    /// </summary>
    private readonly ConcurrentDictionary<(string TreeName, string ConsumerId), (HybridLogicalClock LastWritten, long LastWrittenOffset, long LastWriteTickMs)> _durableDebounce =
        new();

    /// <summary>
    /// Per-durable-pin-shard-key mutual-exclusion gates for the teardown
    /// direct-store fallback (<see cref="DirectStorePinAsync"/>). During a
    /// full-silo graceful shutdown several deactivating leaves whose consumer
    /// ids route to the same pin shard can take the direct-store path
    /// concurrently; the read-modify-write of a shard's durable
    /// <see cref="WalMaterialiserPinState"/> must be serialized per shard key so
    /// concurrent fallbacks monotonic-max merge instead of clobbering one
    /// another. (There is no live pin activation to race on this path - the
    /// grain call was rejected precisely because none could be created.)
    /// </summary>
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _directStoreLocks =
        new(StringComparer.Ordinal);

    /// <inheritdoc />
    public Task ReportAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        CancellationToken cancellationToken)
        => registry.ReportCursorAsync(treeName, consumerId, cursor, cancellationToken);

    /// <inheritdoc />
    public async Task UnregisterAsync(
        string treeName,
        string consumerId,
        CancellationToken cancellationToken)
    {
        await registry.UnregisterAsync(treeName, consumerId, cancellationToken).ConfigureAwait(false);
        await RemoveDurablePinAsync(treeName, consumerId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task UnregisterTreeAsync(
        string treeName,
        CancellationToken cancellationToken)
    {
        // Snapshot under the registry's own lock (cheap O(consumers
        // per tree)) and filter to the leaf-materialiser prefix so
        // peer / custom consumers registered against the tree are
        // left alone. Only runs at terminal lifecycle events
        // (tree-deletion purge), so the snapshot+iterate cost is
        // amortised over the lifetime of the tree.
        var snapshot = await registry.SnapshotAsync(treeName, cancellationToken).ConfigureAwait(false);

        var prefix = ILeafCursorReporter.MaterialiserConsumerIdPrefix + treeName + "_";
        for (var i = 0; i < snapshot.Count; i++)
        {
            var consumerId = snapshot[i].ConsumerId;
            if (consumerId.StartsWith(prefix, StringComparison.Ordinal))
            {
                await registry.UnregisterAsync(treeName, consumerId, cancellationToken).ConfigureAwait(false);
            }
        }

        // Clear the durable pin store for this tree regardless of the
        // in-memory snapshot: a leaf whose durable pin outlived its
        // in-memory registration (post-restart, never re-activated) is
        // only visible in the durable grain. Clear every shard plus the
        // legacy unsuffixed key so no orphaned pin survives the purge.
        if (grainFactory is not null)
        {
            var shardCount = WalMaterialiserPinRouting.ResolveShardCount(options);
            var keys = WalMaterialiserPinRouting.EnumerateReadKeys(treeName, shardCount);
            for (var i = 0; i < keys.Count; i++)
            {
                try
                {
                    await grainFactory.GetGrain<IWalMaterialiserPinGrain>(keys[i]).ClearAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(
                        ex,
                        "Failed to clear durable WAL materialiser pins for tree {TreeId} shard key {GrainKey} during tree-deletion purge.",
                        treeName,
                        keys[i]);
                }
            }
        }

        // Drop any debounce state for this tree so a future re-creation of
        // the same tree id starts from a clean slate.
        foreach (var key in _durableDebounce.Keys)
        {
            if (string.Equals(key.TreeName, treeName, StringComparison.Ordinal))
            {
                _durableDebounce.TryRemove(key, out _);
            }
        }
    }

    /// <inheritdoc />
    public void NoteDurableMaterialiserFrontier(
        string treeName,
        string consumerId,
        HybridLogicalClock frontier,
        long checkpointOffset)
    {
        if (grainFactory is null)
        {
            return;
        }

        // The debounce key is owned by exactly one leaf activation (the
        // consumer id is per-leaf-per-partition and a leaf is single-threaded
        // under the Orleans turn), so the read-modify-write below is race-free
        // for a given key even though the dictionary is shared across leaves.
        // A value-tuple key avoids a per-call string-concat allocation, and
        // the explicit branch avoids the closure allocations an AddOrUpdate
        // factory pair would incur on this per-checkpoint (and, in every-write
        // mode, per-write) path.
        var key = (treeName, consumerId);
        var now = Environment.TickCount64;
        bool shouldWrite;

        if (_durableDebounce.TryGetValue(key, out var current))
        {
            // Only advances matter, but an advance of EITHER the HLC frontier
            // or the applied offset counts. A reap advances the offset while
            // the HLC stays flat, so an HLC-only coalesce would drop the very
            // advance the offset floor needs; a stale/equal report on both
            // axes is coalesced.
            if (frontier <= current.LastWritten && checkpointOffset <= current.LastWrittenOffset)
            {
                return;
            }

            // Always publish the first real frontier (leaving the seeded
            // Zero block pin), otherwise debounce by wall-clock spacing.
            var crossingZero = current.LastWritten <= HybridLogicalClock.Zero;
            shouldWrite = crossingZero || now - current.LastWriteTickMs >= MinDurableWriteSpacingMs;
            if (!shouldWrite)
            {
                return;
            }

            _durableDebounce[key] = (
                frontier > current.LastWritten ? frontier : current.LastWritten,
                Math.Max(checkpointOffset, current.LastWrittenOffset),
                now);
        }
        else
        {
            // First note for this consumer: always write through so the
            // durable pin is seeded (including a Zero "block" pin).
            shouldWrite = true;
            _durableDebounce[key] = (frontier, checkpointOffset, now);
        }

        if (shouldWrite)
        {
            // Fire-and-forget: the durable write must never add synchronous
            // latency to the leaf's checkpoint/foreground path. A lagging or
            // dropped durable pin only retains more WAL, which is safe.
            _ = WriteDurablePinAsync(treeName, consumerId, frontier, checkpointOffset);
        }
    }

    /// <inheritdoc />
    public async Task SeedDurableMaterialiserBlockAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock frontier,
        CancellationToken cancellationToken)
    {
        if (grainFactory is null)
        {
            // No durable backing (pre-WAL host / bare-IServiceProvider): the
            // block pin has no store to land in, so this is a no-op.
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            // Await the durable write so the block pin is persisted BEFORE the
            // caller (a leaf at birth) lets any inherited/routed data become
            // reachable in the WAL. The pin store's monotonic-max merge makes a
            // Zero (or stale) seed a no-op once a real frontier has landed, so
            // this is idempotent and safe on a recovery-path re-call.
            await PinGrain(treeName, consumerId)
                .ReportManyAsync(new[] { new MaterialiserPinReport(consumerId, frontier, -1) })
                .ConfigureAwait(false);

            // Record the seed in the debounce state so a subsequent
            // NoteDurableMaterialiserFrontier treats this consumer as already
            // seeded (its first real frontier still writes through via the
            // crossing-zero branch) rather than issuing a redundant durable
            // write of the same value.
            var key = (treeName, consumerId);
            if (_durableDebounce.TryGetValue(key, out var current))
            {
                if (frontier > current.LastWritten)
                {
                    _durableDebounce[key] = (frontier, Math.Max(-1, current.LastWrittenOffset), Environment.TickCount64);
                }
            }
            else
            {
                _durableDebounce[key] = (frontier, -1, Environment.TickCount64);
            }
        }
        catch (Exception ex)
        {
            // Swallow-and-log: the birth/create path must not fail because the
            // durable pin store had a transient hiccup. A missed seed only
            // narrows the protection window; the leaf's first checkpoint flush
            // re-seeds via NoteDurableMaterialiserFrontier.
            logger?.LogWarning(
                ex,
                "Failed to seed durable WAL materialiser block pin for tree {TreeId} consumer {ConsumerId} at {Frontier}; will re-seed on next checkpoint.",
                treeName,
                consumerId,
                frontier);
        }
    }

    /// <inheritdoc />
    public async Task SeedDurableMaterialiserBlockManyAsync(
        string treeName,
        IReadOnlyList<MaterialiserPinReport> reports,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reports);
        if (grainFactory is null || reports.Count == 0)
        {
            // No durable backing (pre-WAL host / bare-IServiceProvider) or
            // nothing to seed: no-op.
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();
        await PersistPinBatchDurablyAsync(treeName, reports, seed: true).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task FlushDurableMaterialiserFrontierAsync(
        string treeName,
        IReadOnlyList<MaterialiserPinReport> reports,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reports);
        if (grainFactory is null || reports.Count == 0)
        {
            // No durable backing (pre-WAL host / bare-IServiceProvider) or
            // nothing to flush: no-op.
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();
        await PersistPinBatchDurablyAsync(treeName, reports, seed: false).ConfigureAwait(false);
    }

    /// <summary>
    /// Shared awaited-durable core for the birth block-pin seed
    /// (<see cref="SeedDurableMaterialiserBlockManyAsync"/>) and the
    /// real-frontier retention flush
    /// (<see cref="FlushDurableMaterialiserFrontierAsync"/>). Groups the
    /// per-partition pins by their routed durable-pin shard so each distinct
    /// shard takes a single batched, awaited <c>SeedManyAsync</c> (monotonic-max
    /// merge + durable persist) and pre-seeds the debounce state so a subsequent
    /// coalesced <see cref="NoteDurableMaterialiserFrontier"/> does not re-issue
    /// the same value. Transient failures are swallowed-and-logged so neither the
    /// birth path nor deactivation is ever blocked; <paramref name="seed"/> only
    /// selects the log wording.
    /// </summary>
    private async Task PersistPinBatchDurablyAsync(
        string treeName,
        IReadOnlyList<MaterialiserPinReport> reports,
        bool seed)
    {
        var shardCount = WalMaterialiserPinRouting.ResolveShardCount(options);

        // Group the per-partition pins by their routed shard key so each
        // distinct shard takes a single batched durable write. A single leaf's
        // partition-consumers can hash to several shards; issuing one
        // SeedManyAsync per shard concurrently spreads the write load that
        // would otherwise be O(partitions) serialized writes through one hot
        // grain.
        Dictionary<string, List<MaterialiserPinReport>>? byShard = null;
        for (var i = 0; i < reports.Count; i++)
        {
            var report = reports[i];
            ArgumentException.ThrowIfNullOrWhiteSpace(report.ConsumerId);
            var key = WalMaterialiserPinRouting.ShardKey(treeName, report.ConsumerId, shardCount);
            byShard ??= new Dictionary<string, List<MaterialiserPinReport>>(StringComparer.Ordinal);
            if (!byShard.TryGetValue(key, out var bucket))
            {
                bucket = new List<MaterialiserPinReport>();
                byShard[key] = bucket;
            }

            bucket.Add(report);

            // Pre-seed the debounce state so a subsequent
            // NoteDurableMaterialiserFrontier treats this consumer as already
            // written through at this frontier rather than issuing a redundant
            // durable write of the same value.
            var debounceKey = (treeName, report.ConsumerId);
            if (_durableDebounce.TryGetValue(debounceKey, out var current))
            {
                if (report.Frontier > current.LastWritten || report.CheckpointOffset > current.LastWrittenOffset)
                {
                    _durableDebounce[debounceKey] = (
                        report.Frontier > current.LastWritten ? report.Frontier : current.LastWritten,
                        Math.Max(report.CheckpointOffset, current.LastWrittenOffset),
                        Environment.TickCount64);
                }
            }
            else
            {
                _durableDebounce[debounceKey] = (report.Frontier, report.CheckpointOffset, Environment.TickCount64);
            }
        }

        if (byShard is null)
        {
            return;
        }

        var writes = new List<Task>(byShard.Count);
        foreach (var (key, bucket) in byShard)
        {
            writes.Add(SeedShardAsync(key, bucket));
        }

        try
        {
            await Task.WhenAll(writes).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Swallow-and-log: neither the birth/create path nor deactivation
            // must fail because the durable pin store had a transient hiccup on
            // one shard. A missed write only narrows the protection window; the
            // leaf's next checkpoint flush (or the next deactivation) re-writes.
            if (seed)
            {
                logger?.LogWarning(
                    ex,
                    "Failed to seed one or more durable WAL materialiser block pins for tree {TreeId}; will re-seed on next checkpoint.",
                    treeName);
            }
            else
            {
                logger?.LogWarning(
                    ex,
                    "Failed to flush one or more durable WAL materialiser frontier pins for tree {TreeId}; will re-flush on next checkpoint or deactivation.",
                    treeName);
            }
        }
    }

    private async Task SeedShardAsync(string grainKey, IReadOnlyList<MaterialiserPinReport> bucket)
    {
        try
        {
            await grainFactory!.GetGrain<IWalMaterialiserPinGrain>(grainKey)
                .SeedManyAsync(bucket)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (pinStorage is not null && IsActivationCollectionRejection(ex))
        {
            // Full-silo graceful shutdown (issue #1464): the pin-store grain is
            // itself deactivating and the stopping silo refuses to create its
            // activation, so this durable retention barrier's grain call is
            // rejected mid-teardown. That defeats the "fall off the log" floor -
            // and, when a leaf's FIRST real-frontier checkpoint is produced by
            // the deactivation flush, defeats BOTH barriers, leaving no durable
            // floor at all and reintroducing LeafProjectionStaleException on cold
            // restart. Persist the pins by writing directly to the identical
            // durable slot the grain would have written, so the floor still
            // advances to the final frontier during teardown. A genuine
            // (non-shutdown) transient fault is rethrown so the outer catch keeps
            // swallowing-and-logging it for re-flush on the next checkpoint.
            await DirectStorePinAsync(grainKey, bucket).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Teardown fallback for <see cref="SeedShardAsync"/>: writes
    /// <paramref name="bucket"/>'s pins straight to the durable
    /// <see cref="WalMaterialiserPinState"/> slot the shard's
    /// <see cref="IWalMaterialiserPinGrain"/> would own, replicating the grain's
    /// monotonic-max merge, when the grain call is rejected because the silo is
    /// stopping. Serialized per shard key so concurrent deactivating leaves that
    /// route to the same shard converge instead of clobbering one another.
    /// </summary>
    private async Task DirectStorePinAsync(string grainKey, IReadOnlyList<MaterialiserPinReport> bucket)
    {
        var grainId = ResolvePinGrainId(grainKey);
        var gate = _directStoreLocks.GetOrAdd(grainKey, static _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            var grainState = new GrainState<WalMaterialiserPinState>(new WalMaterialiserPinState());
            await pinStorage!.ReadStateAsync(WalMaterialiserPinState.StateName, grainId, grainState)
                .ConfigureAwait(false);

            var state = grainState.State ??= new WalMaterialiserPinState();
            var pins = state.Pins;
            var offsets = state.Offsets;
            var changed = false;
            for (var i = 0; i < bucket.Count; i++)
            {
                var report = bucket[i];
                // Monotonic-max merge, identical to WalMaterialiserPinGrain.Merge:
                // a report at or below the stored frontier/offset is coalesced,
                // and each axis advances independently.
                if (!pins.TryGetValue(report.ConsumerId, out var existing) || report.Frontier > existing)
                {
                    pins[report.ConsumerId] = report.Frontier;
                    changed = true;
                }

                if (!offsets.TryGetValue(report.ConsumerId, out var existingOffset) || report.CheckpointOffset > existingOffset)
                {
                    offsets[report.ConsumerId] = report.CheckpointOffset;
                    changed = true;
                }
            }

            if (changed)
            {
                await pinStorage!.WriteStateAsync(WalMaterialiserPinState.StateName, grainId, grainState)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Resolves the durable-state <see cref="GrainId"/> for a pin shard key.
    /// Production uses the grain factory (the reference's grain id is the exact
    /// key the storage bridge writes under); tests inject
    /// <c>pinGrainIdResolver</c> because <c>GetGrainId()</c> requires a real
    /// grain reference, which a substituted factory does not return.
    /// </summary>
    private GrainId ResolvePinGrainId(string grainKey) =>
        pinGrainIdResolver is not null
            ? pinGrainIdResolver(grainKey)
            : grainFactory!.GetGrain<IWalMaterialiserPinGrain>(grainKey).GetGrainId();

    /// <summary>
    /// True when <paramref name="exception"/> (or any of its inner or aggregated
    /// causes) is Orleans rejecting a grain call because the target activation
    /// could not be created on a stopping silo - the signature of a durable-pin
    /// grain call issued during full-silo graceful shutdown. Matched by the
    /// rejection type name and the canonical rejection message fragments rather
    /// than a hard dependency on an Orleans.Runtime type, so the detection is
    /// portable and unit-testable with a plain exception carrying the marker.
    /// </summary>
    private static bool IsActivationCollectionRejection(Exception exception)
    {
        for (var ex = exception; ex is not null; ex = ex.InnerException!)
        {
            if (ex.GetType().FullName is { } typeName &&
                typeName.Contains("MessageRejectionException", StringComparison.Ordinal))
            {
                return true;
            }

            if (ex.Message is { } message &&
                (message.Contains("Unable to create local activation", StringComparison.Ordinal) ||
                 message.Contains("invalid activation", StringComparison.Ordinal)))
            {
                return true;
            }

            if (ex is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    if (IsActivationCollectionRejection(inner))
                    {
                        return true;
                    }
                }
            }

            if (ex.InnerException is null)
            {
                break;
            }
        }

        return false;
    }

    /// <summary>
    /// under <paramref name="treeName"/>. Routing is by a stable hash of the
    /// consumer id so the same consumer always reads and writes the same shard.
    /// </summary>
    private IWalMaterialiserPinGrain PinGrain(string treeName, string consumerId)
    {
        var shardCount = WalMaterialiserPinRouting.ResolveShardCount(options);
        var key = WalMaterialiserPinRouting.ShardKey(treeName, consumerId, shardCount);
        return grainFactory!.GetGrain<IWalMaterialiserPinGrain>(key);
    }

    private async Task WriteDurablePinAsync(string treeName, string consumerId, HybridLogicalClock frontier, long checkpointOffset)
    {
        try
        {
            await PinGrain(treeName, consumerId)
                .ReportManyAsync(new[] { new MaterialiserPinReport(consumerId, frontier, checkpointOffset) })
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Roll the debounce state back so a subsequent note retries
            // rather than treating the failed write as durably landed.
            _durableDebounce.TryRemove((treeName, consumerId), out _);
            logger?.LogWarning(
                ex,
                "Failed to persist durable WAL materialiser pin for tree {TreeId} consumer {ConsumerId} at {Frontier}; will retry on next checkpoint.",
                treeName,
                consumerId,
                frontier);
        }
    }

    private async Task RemoveDurablePinAsync(string treeName, string consumerId, CancellationToken cancellationToken)
    {
        if (grainFactory is null)
        {
            return;
        }

        // Only leaf-materialiser consumer ids own a durable pin; a peer or
        // custom consumer routed through this reporter must not touch the
        // pin store.
        if (!consumerId.StartsWith(ILeafCursorReporter.MaterialiserConsumerIdPrefix, StringComparison.Ordinal))
        {
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            await PinGrain(treeName, consumerId).RemoveAsync(consumerId).ConfigureAwait(false);
            _durableDebounce.TryRemove((treeName, consumerId), out _);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(
                ex,
                "Failed to remove durable WAL materialiser pin for tree {TreeId} consumer {ConsumerId}.",
                treeName,
                consumerId);
        }
    }
}
