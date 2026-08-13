using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;

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
    ILogger<LeafCursorReporter>? logger = null) : ILeafCursorReporter
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
    /// mirror. Holds the last frontier written through and the wall-clock
    /// tick at which it was written.
    /// </summary>
    private readonly ConcurrentDictionary<(string TreeName, string ConsumerId), (HybridLogicalClock LastWritten, long LastWriteTickMs)> _durableDebounce =
        new();

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
        HybridLogicalClock frontier)
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
            // Only advances matter; a stale/equal frontier is coalesced.
            if (frontier <= current.LastWritten)
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

            _durableDebounce[key] = (frontier, now);
        }
        else
        {
            // First note for this consumer: always write through so the
            // durable pin is seeded (including a Zero "block" pin).
            shouldWrite = true;
            _durableDebounce[key] = (frontier, now);
        }

        if (shouldWrite)
        {
            // Fire-and-forget: the durable write must never add synchronous
            // latency to the leaf's checkpoint/foreground path. A lagging or
            // dropped durable pin only retains more WAL, which is safe.
            _ = WriteDurablePinAsync(treeName, consumerId, frontier);
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
                .ReportAsync(consumerId, frontier)
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
                    _durableDebounce[key] = (frontier, Environment.TickCount64);
                }
            }
            else
            {
                _durableDebounce[key] = (frontier, Environment.TickCount64);
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
                if (report.Frontier > current.LastWritten)
                {
                    _durableDebounce[debounceKey] = (report.Frontier, Environment.TickCount64);
                }
            }
            else
            {
                _durableDebounce[debounceKey] = (report.Frontier, Environment.TickCount64);
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

    private Task SeedShardAsync(string grainKey, IReadOnlyList<MaterialiserPinReport> bucket) =>
        grainFactory!.GetGrain<IWalMaterialiserPinGrain>(grainKey).SeedManyAsync(bucket);

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

    private async Task WriteDurablePinAsync(string treeName, string consumerId, HybridLogicalClock frontier)
    {
        try
        {
            await PinGrain(treeName, consumerId)
                .ReportAsync(consumerId, frontier)
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
