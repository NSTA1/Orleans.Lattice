using System.Collections.Immutable;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// WAL placement and managed-move surface of <see cref="LatticeAdminGrain"/>.
/// Implements the read-only inspection methods
/// (<see cref="GetWalPlacementAsync"/>, <see cref="AuditWalPlacementAsync"/>,
/// <see cref="PlanWalMoveAsync"/>) and the mutating move saga
/// (<see cref="ExecuteWalMoveAsync"/>, <see cref="ReclaimMovedWalSourceAsync"/>).
/// </summary>
internal sealed partial class LatticeAdminGrain
{
    private LatticeOptionsResolver RequireResolver() => optionsResolver
        ?? throw new InvalidOperationException(
            "WAL placement administration requires a LatticeOptionsResolver; this admin grain was constructed without one.");

    private IWalStorageProviderCatalog RequireCatalog() => walProviderCatalog
        ?? throw new InvalidOperationException(
            "WAL placement administration requires an IWalStorageProviderCatalog; this admin grain was constructed without one.");

    private IWalRecordEncoder RequireEncoder() => walRecordEncoder
        ?? throw new InvalidOperationException(
            "WAL placement moves require an IWalRecordEncoder; this admin grain was constructed without one.");

    private async Task<(string PhysicalTreeId, int WalPartitions)> ResolveTopologyAsync(
        string treeId, CancellationToken cancellationToken)
    {
        var lattice = grainFactory.GetGrain<ILattice>(treeId);
        var routing = await lattice.GetRoutingAsync(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        var walPartitions = await RequireResolver().GetWalPartitionsAsync(routing.PhysicalTreeId);
        return (routing.PhysicalTreeId, walPartitions);
    }

    private static void ValidatePartition(int partition, int walPartitions)
    {
        if (partition < 0 || partition >= walPartitions)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition),
                partition,
                $"WAL partition must be in [0, {walPartitions}); the tree has {walPartitions} WAL partition(s).");
        }
    }

    /// <inheritdoc />
    public async Task<WalPlacement> GetWalPlacementAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        var catalog = RequireCatalog();

        var (physicalTreeId, walPartitions) = await ResolveTopologyAsync(treeId, cancellationToken);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var pin = await registry.GetWalPlacementAsync(physicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();

        var partitions = BuildPartitionPlacements(pin, walPartitions, catalog);
        return new WalPlacement
        {
            TreeId = treeId,
            Version = pin.Version,
            DefaultProviderKey = pin.DefaultProviderKey,
            Partitions = partitions,
        };
    }

    /// <inheritdoc />
    public async Task<WalPlacementAudit> AuditWalPlacementAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        var catalog = RequireCatalog();

        var (physicalTreeId, walPartitions) = await ResolveTopologyAsync(treeId, cancellationToken);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var pin = await registry.GetWalPlacementAsync(physicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();

        var partitions = BuildPartitionPlacements(pin, walPartitions, catalog);
        var allResolvable = true;
        foreach (var placement in partitions)
        {
            allResolvable &= placement.ResolvableOnThisSilo;
        }

        var knownKeys = catalog.Keys.OrderBy(static k => k, StringComparer.Ordinal).ToImmutableArray();
        return new WalPlacementAudit
        {
            TreeId = treeId,
            Version = pin.Version,
            PartitionCount = walPartitions,
            Partitions = partitions,
            AllResolvableOnThisSilo = allResolvable,
            KnownProviderKeys = knownKeys,
        };
    }

    private static ImmutableArray<WalPartitionPlacement> BuildPartitionPlacements(
        State.WalPlacementPin pin, int walPartitions, IWalStorageProviderCatalog catalog)
    {
        var builder = ImmutableArray.CreateBuilder<WalPartitionPlacement>(walPartitions);
        for (var partition = 0; partition < walPartitions; partition++)
        {
            var key = pin.ResolveKey(partition);
            builder.Add(new WalPartitionPlacement
            {
                Partition = partition,
                ProviderKey = key,
                ResolvableOnThisSilo = catalog.TryGet(key, out _),
            });
        }
        return builder.MoveToImmutable();
    }

    /// <inheritdoc />
    public async Task<WalMovePlan> PlanWalMoveAsync(
        string treeId, int partition, string targetProviderKey, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(targetProviderKey);
        cancellationToken.ThrowIfCancellationRequested();
        var resolver = RequireResolver();
        var catalog = RequireCatalog();

        var (physicalTreeId, walPartitions) = await ResolveTopologyAsync(treeId, cancellationToken);
        ValidatePartition(partition, walPartitions);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var pin = await registry.GetWalPlacementAsync(physicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();

        return await BuildPartitionPlanAsync(
            treeId, physicalTreeId, pin, partition, targetProviderKey, resolver, catalog, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<WalMoveBatchPlan> PlanWalMoveAsync(
        string treeId, IEnumerable<(int Partition, string TargetProviderKey)> moves, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(moves);
        cancellationToken.ThrowIfCancellationRequested();
        var resolver = RequireResolver();
        var catalog = RequireCatalog();
        var requested = NormalizeMoves(moves);

        var (physicalTreeId, walPartitions) = await ResolveTopologyAsync(treeId, cancellationToken);
        foreach (var (partition, _) in requested)
        {
            ValidatePartition(partition, walPartitions);
        }

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var pin = await registry.GetWalPlacementAsync(physicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();

        var builder = ImmutableArray.CreateBuilder<WalMovePlan>(requested.Count);
        var allResolvable = true;
        foreach (var (partition, targetKey) in requested)
        {
            var plan = await BuildPartitionPlanAsync(
                treeId, physicalTreeId, pin, partition, targetKey, resolver, catalog, cancellationToken);
            allResolvable &= plan.TargetResolvableOnThisSilo;
            builder.Add(plan);
        }

        return new WalMoveBatchPlan
        {
            TreeId = treeId,
            PlacementVersion = pin.Version,
            Moves = builder.MoveToImmutable(),
            AllTargetsResolvableOnThisSilo = allResolvable,
        };
    }

    /// <summary>
    /// Builds a single partition's <see cref="WalMovePlan"/> against an already-read
    /// placement <paramref name="pin"/>. Shared by the single- and batch-partition
    /// planning overloads. Read-only: quiesces nothing and changes no placement.
    /// </summary>
    private async Task<WalMovePlan> BuildPartitionPlanAsync(
        string treeId,
        string physicalTreeId,
        State.WalPlacementPin pin,
        int partition,
        string targetProviderKey,
        LatticeOptionsResolver resolver,
        IWalStorageProviderCatalog catalog,
        CancellationToken cancellationToken)
    {
        var currentKey = pin.ResolveKey(partition);
        var targetResolvable = catalog.TryGet(targetProviderKey, out _);
        var alreadyAtTarget = string.Equals(currentKey, targetProviderKey, StringComparison.Ordinal);

        var (srcProvider, _) = resolver.ResolveWalProvider(physicalTreeId, pin, partition);
        var srcLowest = await srcProvider.GetLowestOffsetAsync(physicalTreeId, partition, cancellationToken);
        var srcHighest = await srcProvider.GetHighestOffsetAsync(physicalTreeId, partition, cancellationToken);
        long entriesToCopy = 0;
        if (srcHighest >= 0)
        {
            var floor = srcLowest < 0 ? 0 : srcLowest;
            entriesToCopy = srcHighest - floor + 1;
        }

        return new WalMovePlan
        {
            TreeId = treeId,
            Partition = partition,
            FromProviderKey = currentKey,
            ToProviderKey = targetProviderKey,
            PlacementVersion = pin.Version,
            SourceLowestOffset = srcLowest,
            SourceHighestOffset = srcHighest,
            EntriesToCopy = entriesToCopy,
            TargetResolvableOnThisSilo = targetResolvable,
            AlreadyAtTarget = alreadyAtTarget,
        };
    }

    /// <inheritdoc />
    public async Task<WalMoveReceipt> ExecuteWalMoveAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        WalMoveOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(targetProviderKey);
        cancellationToken.ThrowIfCancellationRequested();
        var resolver = RequireResolver();
        var catalog = RequireCatalog();
        var encoder = RequireEncoder();
        var opts = options ?? WalMoveOptions.Default;

        var (physicalTreeId, walPartitions) = await ResolveTopologyAsync(treeId, cancellationToken);
        ValidatePartition(partition, walPartitions);

        // Fail closed before touching any log if the target key is unknown on
        // this silo: a move whose target cannot be resolved would wedge the
        // partition the moment the pin flipped.
        if (!catalog.TryGet(targetProviderKey, out _))
        {
            throw new LatticeWalProviderMissingException(physicalTreeId, partition, targetProviderKey);
        }

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var pin = await registry.GetWalPlacementAsync(physicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();

        var currentKey = pin.ResolveKey(partition);
        var (srcProvider, _) = resolver.ResolveWalProvider(physicalTreeId, pin, partition);
        var wal = grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{partition}");

        // Idempotent fast path: the pin already routes the partition to the
        // requested key. Re-run the post-flip repair (force deactivation so the
        // shard re-resolves the live placement) and report no copy.
        if (string.Equals(currentKey, targetProviderKey, StringComparison.Ordinal))
        {
            await wal.DeactivateForMoveAsync(cancellationToken);
            var highest = await srcProvider.GetHighestOffsetAsync(physicalTreeId, partition, cancellationToken);
            return new WalMoveReceipt
            {
                TreeId = treeId,
                Partition = partition,
                FromProviderKey = currentKey,
                ToProviderKey = targetProviderKey,
                PreviousPlacementVersion = pin.Version,
                NewPlacementVersion = pin.Version,
                CopiedFromOffset = -1,
                CopiedThroughOffset = -1,
                SourceHighestOffset = highest,
                TargetHighestOffset = highest,
                SourceRetained = true,
                Outcome = WalMoveOutcome.AlreadyAtTarget,
            };
        }

        var copy = await RunMoveCopyPhasesAsync(
            physicalTreeId, pin, partition, targetProviderKey, resolver, encoder, opts, cancellationToken);

        // 5. Atomically flip the placement pin (compare-and-swap on version).
        var flipped = await registry.UpdateWalPlacementAsync(physicalTreeId, pin.Version, partition, targetProviderKey);

        // 6. Force the source activation to deactivate so the next activation
        //    (on any silo) re-resolves placement and routes to the target.
        await ForceDeactivateAfterFlipAsync(physicalTreeId, partition);

        logger.LogInformation(
            "WAL partition {TreeId}/{Partition} moved from '{From}' to '{To}' (placement {OldVer} -> {NewVer}); source retained for reclaim.",
            physicalTreeId, partition, currentKey, targetProviderKey, pin.Version, flipped.Version);

        return new WalMoveReceipt
        {
            TreeId = treeId,
            Partition = partition,
            FromProviderKey = currentKey,
            ToProviderKey = targetProviderKey,
            PreviousPlacementVersion = pin.Version,
            NewPlacementVersion = flipped.Version,
            CopiedFromOffset = copy.CopiedFrom,
            CopiedThroughOffset = copy.CopiedThrough,
            SourceHighestOffset = copy.SrcHighest,
            TargetHighestOffset = copy.DstHighest,
            SourceRetained = true,
            Outcome = WalMoveOutcome.Moved,
        };
    }

    /// <summary>
    /// The intermediate result of the per-partition copy phases (quiesce, copy,
    /// converge, verify) run by <see cref="RunMoveCopyPhasesAsync"/> before the
    /// placement pin is flipped.
    /// </summary>
    private readonly record struct MoveCopyResult(long CopiedFrom, long CopiedThrough, long SrcHighest, long DstHighest);

    /// <summary>
    /// Runs phases 1-4 of a single partition's move against an already-read
    /// placement <paramref name="basePin"/>: quiesce + fence the source, copy its
    /// retained tail to the target preserving offsets, re-converge on any appends
    /// that slipped in, and verify the target tail. Does <b>not</b> flip the pin
    /// or deactivate the source on success - the caller flips (single CAS for one
    /// partition, or one batched CAS for many) and then deactivates. On any
    /// failure the fenced source is deactivated best-effort so it resumes service
    /// without waiting out the quiesce lease, and the exception is rethrown with
    /// the partial target copy retained for a resumable retry.
    /// <para>
    /// The caller must already have validated that the target key resolves and
    /// that the partition is not already at the target.
    /// </para>
    /// </summary>
    private async Task<MoveCopyResult> RunMoveCopyPhasesAsync(
        string physicalTreeId,
        State.WalPlacementPin basePin,
        int partition,
        string targetProviderKey,
        LatticeOptionsResolver resolver,
        IWalRecordEncoder encoder,
        WalMoveOptions opts,
        CancellationToken cancellationToken)
    {
        var (srcProvider, _) = resolver.ResolveWalProvider(physicalTreeId, basePin, partition);
        var movedPin = basePin.WithPartition(partition, targetProviderKey, basePin.Version);
        var (dstProvider, _) = resolver.ResolveWalProvider(physicalTreeId, movedPin, partition);
        var wal = grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{partition}");

        // 1. Quiesce + fence the source activation at the pin version we read.
        var quiesce = await wal.QuiesceForMoveAsync(basePin.Version, opts.EffectiveQuiesceLease, cancellationToken);
        if (!quiesce.Quiesced)
        {
            throw new InvalidOperationException(
                $"WAL move of {physicalTreeId}/{partition} aborted: the source activation resolved placement version "
                + $"{quiesce.ObservedPlacementVersion}, but the coordinator expected {basePin.Version}. The placement changed "
                + "underneath the move; re-read placement and retry.");
        }

        var srcHighest = quiesce.HighestOffsetInclusive;
        var srcLowest = await srcProvider.GetLowestOffsetAsync(physicalTreeId, partition, cancellationToken);

        long copiedFrom = -1, copiedThrough = -1;

        // Copies source entries with offset in (fromExclusive, throughInclusive]
        // to the target, preserving offsets. Returns the new exclusive cursor.
        async Task<long> CopyRangeAsync(long fromExclusive, long throughInclusive)
        {
            var cursor = fromExclusive;
            while (cursor < throughInclusive)
            {
                var page = await srcProvider.ReadEncodedAsync(
                    physicalTreeId, partition, cursor, opts.EffectiveCopyPageSize, encoder, cancellationToken);
                if (page.Offsets.Length == 0)
                {
                    break;
                }
                await dstProvider.AppendEncodedBatchAsync(
                    physicalTreeId, partition, page.EncodedEntries, page.Offsets, encoder, cancellationToken);

                var offsets = page.Offsets.Span;
                if (copiedFrom < 0)
                {
                    copiedFrom = offsets[0];
                }
                copiedThrough = offsets[^1];
                cursor = offsets[^1];
            }
            return cursor;
        }

        long dstHighest;
        try
        {
            // 2. Copy the retained tail [srcLowest..srcHighest] to the target,
            //    preserving offsets and the source trim floor. Resumable: if a
            //    prior attempt copied a prefix, continue past the target's tail.
            if (srcHighest >= 0)
            {
                var dstHighestBefore = await dstProvider.GetHighestOffsetAsync(physicalTreeId, partition, cancellationToken);
                if (!WalMoveResumeCore.IsTargetCleanPrefix(dstHighestBefore, srcHighest))
                {
                    throw new InvalidOperationException(
                        $"WAL move of {physicalTreeId}/{partition} aborted: the target already holds offset "
                        + $"{dstHighestBefore}, beyond the source highest {srcHighest}. The target is not a clean "
                        + "prefix of the source; resolve the divergence before retrying.");
                }
                if (WalMoveResumeCore.NeedsFloorReserve(dstHighestBefore, srcLowest))
                {
                    // Reserve the destination trim floor so the first append's
                    // offset (srcLowest) is contiguous with the reserved point.
                    await dstProvider.TrimAsync(physicalTreeId, partition, srcLowest - 1, cancellationToken);
                }

                await CopyRangeAsync(WalMoveResumeCore.ResumeCursor(srcLowest, dstHighestBefore), srcHighest);
            }

            // 3. Convergence: re-quiesce with a fresh lease right before the
            //    cutover. This (a) resets the source's self-heal deadline so the
            //    fence is guaranteed to outlast the compare-and-swap below, and
            //    (b) catches any appends that slipped onto the source if the
            //    first lease lapsed during a slow copy. Loop until the source
            //    tail is stable, then flip immediately while the lease holds.
            while (true)
            {
                var recheck = await wal.QuiesceForMoveAsync(basePin.Version, opts.EffectiveQuiesceLease, cancellationToken);
                if (!recheck.Quiesced)
                {
                    throw new InvalidOperationException(
                        $"WAL move of {physicalTreeId}/{partition} aborted: the source activation resolved placement "
                        + $"version {recheck.ObservedPlacementVersion} during convergence, but the coordinator expected "
                        + $"{basePin.Version}. The placement changed underneath the move; retry.");
                }
                if (recheck.HighestOffsetInclusive <= srcHighest)
                {
                    break;
                }
                // New appends landed on the source while copying: copy the delta.
                await CopyRangeAsync(srcHighest, recheck.HighestOffsetInclusive);
                srcHighest = recheck.HighestOffsetInclusive;
            }

            // 4. Verify the target tail before the irreversible cutover. The
            //    overshoot guard runs even when content verification is off.
            dstHighest = await dstProvider.GetHighestOffsetAsync(physicalTreeId, partition, cancellationToken);
            if (srcHighest >= 0 && dstHighest > srcHighest)
            {
                throw new InvalidOperationException(
                    $"WAL move of {physicalTreeId}/{partition} aborted: target highest offset {dstHighest} overshot "
                    + $"source highest {srcHighest} after copy.");
            }
            if (opts.VerifyAfterCopy && srcHighest >= 0 && dstHighest != srcHighest)
            {
                throw new InvalidOperationException(
                    $"WAL move of {physicalTreeId}/{partition} failed verification: source highest offset {srcHighest} "
                    + $"but target highest offset {dstHighest} after copy. The pin was not flipped; the source remains live.");
            }
        }
        catch
        {
            // The pin was never flipped, so the partition's durable placement
            // still points at the source. Force the fenced source activation to
            // deactivate so the next activation resumes service on the source
            // immediately instead of waiting out the quiesce lease.
            try
            {
                await wal.DeactivateForMoveAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Best-effort source deactivation after aborted WAL move of {TreeId}/{Partition} failed.", physicalTreeId, partition);
            }
            throw;
        }

        return new MoveCopyResult(copiedFrom, copiedThrough, srcHighest, dstHighest);
    }

    /// <summary>
    /// Forces a moved partition's source activation to deactivate after the
    /// placement pin has flipped, so the next activation (on any silo) re-resolves
    /// placement and routes to the target. Best-effort: a failure is logged but
    /// not propagated because the pin is already durably flipped and an idempotent
    /// re-execute repairs the deactivation.
    /// </summary>
    private async Task ForceDeactivateAfterFlipAsync(string physicalTreeId, int partition)
    {
        try
        {
            await grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{partition}")
                .DeactivateForMoveAsync(CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "WAL move of {TreeId}/{Partition} flipped the pin but failed to deactivate the source; re-execute to repair.", physicalTreeId, partition);
        }
    }

    /// <inheritdoc />
    public async Task<WalMoveBatchReceipt> ExecuteWalMoveAsync(
        string treeId,
        IEnumerable<(int Partition, string TargetProviderKey)> moves,
        WalMoveOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(moves);
        cancellationToken.ThrowIfCancellationRequested();
        var resolver = RequireResolver();
        var catalog = RequireCatalog();
        var encoder = RequireEncoder();
        var opts = options ?? WalMoveOptions.Default;

        var requested = NormalizeMoves(moves);
        var (physicalTreeId, walPartitions) = await ResolveTopologyAsync(treeId, cancellationToken);

        // Fail closed before touching any log if any target key is unknown on
        // this silo: partial success is never exposed, so the whole batch must be
        // resolvable up front.
        foreach (var (partition, targetKey) in requested)
        {
            ValidatePartition(partition, walPartitions);
            if (!catalog.TryGet(targetKey, out _))
            {
                throw new LatticeWalProviderMissingException(physicalTreeId, partition, targetKey);
            }
        }

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var pin = await registry.GetWalPlacementAsync(physicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();

        // Split into real moves (need copy + flip) and idempotent no-copy repairs
        // (already pinned to the requested target). Preserve request order so the
        // receipt array lines up with the caller's input.
        var realMoveIndexes = new List<int>();
        var currentKeys = new string[requested.Count];
        for (var i = 0; i < requested.Count; i++)
        {
            var currentKey = pin.ResolveKey(requested[i].Partition);
            currentKeys[i] = currentKey;
            if (!string.Equals(currentKey, requested[i].TargetProviderKey, StringComparison.Ordinal))
            {
                realMoveIndexes.Add(i);
            }
        }

        var copyResults = new MoveCopyResult[requested.Count];
        if (realMoveIndexes.Count > 0)
        {
            // Phases 1-4 for every real move, bounded by the configured ceiling.
            // Task.WhenAll waits for all phases to settle even on failure, so the
            // catch can release every fenced source deterministically.
            try
            {
                await RunBoundedAsync(realMoveIndexes.Count, opts.EffectiveMaxConcurrentPartitionMoves, async slot =>
                {
                    var i = realMoveIndexes[slot];
                    copyResults[i] = await RunMoveCopyPhasesAsync(
                        physicalTreeId, pin, requested[i].Partition, requested[i].TargetProviderKey,
                        resolver, encoder, opts, cancellationToken);
                });
            }
            catch
            {
                // Any per-partition failure aborts the whole batch: the pin was
                // never flipped, so release every fenced source (the failed ones
                // were already deactivated by the copy helper; re-requesting is an
                // idempotent no-op) and retain partial target copies for a
                // resumable retry.
                foreach (var slot in realMoveIndexes)
                {
                    try
                    {
                        await grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{requested[slot].Partition}")
                            .DeactivateForMoveAsync(CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        logger.LogDebug(ex, "Best-effort source deactivation after aborted batch WAL move of {TreeId}/{Partition} failed.", physicalTreeId, requested[slot].Partition);
                    }
                }
                throw;
            }
        }

        // 5. Flip every real move together under a single compare-and-swap. When
        //    no partition needed moving the placement is left untouched.
        var previousVersion = pin.Version;
        var newVersion = pin.Version;
        if (realMoveIndexes.Count > 0)
        {
            var batched = new (int Partition, string ProviderKey)[realMoveIndexes.Count];
            for (var slot = 0; slot < realMoveIndexes.Count; slot++)
            {
                var i = realMoveIndexes[slot];
                batched[slot] = (requested[i].Partition, requested[i].TargetProviderKey);
            }
            var flipped = await registry.UpdateWalPlacementAsync(physicalTreeId, pin.Version, batched);
            newVersion = flipped.Version;
        }

        // 6. Force every requested partition's source activation to deactivate so
        //    the next activation re-resolves placement. Real moves route to the
        //    new target; already-at-target repairs complete their cutover.
        foreach (var (partition, _) in requested)
        {
            await ForceDeactivateAfterFlipAsync(physicalTreeId, partition);
        }

        if (realMoveIndexes.Count > 0)
        {
            logger.LogInformation(
                "Batch WAL move of tree {TreeId} relocated {MovedCount} partition(s) (placement {OldVer} -> {NewVer}); sources retained for reclaim.",
                physicalTreeId, realMoveIndexes.Count, previousVersion, newVersion);
        }

        var receipts = ImmutableArray.CreateBuilder<WalMoveReceipt>(requested.Count);
        for (var i = 0; i < requested.Count; i++)
        {
            var (partition, targetKey) = requested[i];
            var isRealMove = !string.Equals(currentKeys[i], targetKey, StringComparison.Ordinal);
            if (isRealMove)
            {
                var copy = copyResults[i];
                receipts.Add(new WalMoveReceipt
                {
                    TreeId = treeId,
                    Partition = partition,
                    FromProviderKey = currentKeys[i],
                    ToProviderKey = targetKey,
                    PreviousPlacementVersion = previousVersion,
                    NewPlacementVersion = newVersion,
                    CopiedFromOffset = copy.CopiedFrom,
                    CopiedThroughOffset = copy.CopiedThrough,
                    SourceHighestOffset = copy.SrcHighest,
                    TargetHighestOffset = copy.DstHighest,
                    SourceRetained = true,
                    Outcome = WalMoveOutcome.Moved,
                });
            }
            else
            {
                receipts.Add(new WalMoveReceipt
                {
                    TreeId = treeId,
                    Partition = partition,
                    FromProviderKey = currentKeys[i],
                    ToProviderKey = targetKey,
                    PreviousPlacementVersion = previousVersion,
                    NewPlacementVersion = newVersion,
                    CopiedFromOffset = -1,
                    CopiedThroughOffset = -1,
                    SourceHighestOffset = -1,
                    TargetHighestOffset = -1,
                    SourceRetained = true,
                    Outcome = WalMoveOutcome.AlreadyAtTarget,
                });
            }
        }

        return new WalMoveBatchReceipt
        {
            TreeId = treeId,
            PreviousPlacementVersion = previousVersion,
            NewPlacementVersion = newVersion,
            Moves = receipts.MoveToImmutable(),
            Outcome = realMoveIndexes.Count > 0 ? WalMoveOutcome.Moved : WalMoveOutcome.AlreadyAtTarget,
        };
    }

    /// <summary>
    /// Validates and materialises a batch of requested moves: rejects a null
    /// target key, a null/empty batch, or a partition named more than once
    /// (ambiguous), preserving request order.
    /// </summary>
    private static IReadOnlyList<(int Partition, string TargetProviderKey)> NormalizeMoves(
        IEnumerable<(int Partition, string TargetProviderKey)> moves)
    {
        var list = new List<(int, string)>();
        var seen = new HashSet<int>();
        foreach (var (partition, targetProviderKey) in moves)
        {
            if (targetProviderKey is null)
            {
                throw new ArgumentNullException(nameof(moves), "A move's target provider key must not be null.");
            }
            if (!seen.Add(partition))
            {
                throw new ArgumentException(
                    $"Duplicate partition {partition} in the move batch; each partition may appear at most once.", nameof(moves));
            }
            list.Add((partition, targetProviderKey));
        }
        if (list.Count == 0)
        {
            throw new ArgumentException(
                "The move batch must contain at least one (partition, targetProviderKey) pair.", nameof(moves));
        }
        return list;
    }

    /// <summary>
    /// Runs <paramref name="body"/> for each slot in <c>[0, count)</c> with at most
    /// <paramref name="maxConcurrency"/> in flight at once. <see cref="Task.WhenAll(Task[])"/>
    /// completes only after every slot settles, so the caller's catch can act on a
    /// fully-quiesced batch even when one slot faulted.
    /// </summary>
    private static async Task RunBoundedAsync(int count, int maxConcurrency, Func<int, Task> body)
    {
        if (count <= 0)
        {
            return;
        }
        if (maxConcurrency >= count)
        {
            var all = new Task[count];
            for (var i = 0; i < count; i++)
            {
                all[i] = body(i);
            }
            await Task.WhenAll(all);
            return;
        }

        using var gate = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        var tasks = new Task[count];
        for (var i = 0; i < count; i++)
        {
            tasks[i] = RunOneAsync(i);
        }
        await Task.WhenAll(tasks);

        async Task RunOneAsync(int slot)
        {
            await gate.WaitAsync();
            try
            {
                await body(slot);
            }
            finally
            {
                gate.Release();
            }
        }
    }

    /// <inheritdoc />
    public async Task<WalMoveReceipt> ReclaimMovedWalSourceAsync(
        string treeId, int partition, string sourceProviderKey, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(sourceProviderKey);
        cancellationToken.ThrowIfCancellationRequested();
        var catalog = RequireCatalog();

        var (physicalTreeId, walPartitions) = await ResolveTopologyAsync(treeId, cancellationToken);
        ValidatePartition(partition, walPartitions);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var pin = await registry.GetWalPlacementAsync(physicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();

        var currentKey = pin.ResolveKey(partition);
        if (string.Equals(currentKey, sourceProviderKey, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Refusing to reclaim WAL partition {physicalTreeId}/{partition} from provider key '{sourceProviderKey}': "
                + "it is the partition's live placement. Move the partition to a different provider first.");
        }

        if (!catalog.TryGet(sourceProviderKey, out var sourceProvider))
        {
            throw new LatticeWalProviderMissingException(physicalTreeId, partition, sourceProviderKey);
        }

        var highest = await sourceProvider.GetHighestOffsetAsync(physicalTreeId, partition, cancellationToken);
        var outcome = WalMoveOutcome.NoOp;
        if (highest >= 0)
        {
            await sourceProvider.TrimAsync(physicalTreeId, partition, highest, cancellationToken);
            outcome = WalMoveOutcome.SourceReclaimed;
            logger.LogInformation(
                "Reclaimed orphaned WAL source {TreeId}/{Partition} on provider '{Key}' through offset {Offset}.",
                physicalTreeId, partition, sourceProviderKey, highest);
        }

        return new WalMoveReceipt
        {
            TreeId = treeId,
            Partition = partition,
            FromProviderKey = sourceProviderKey,
            ToProviderKey = currentKey,
            PreviousPlacementVersion = pin.Version,
            NewPlacementVersion = pin.Version,
            CopiedFromOffset = -1,
            CopiedThroughOffset = -1,
            SourceHighestOffset = -1,
            TargetHighestOffset = -1,
            SourceRetained = false,
            Outcome = outcome,
        };
    }
}
