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

        var currentKey = pin.ResolveKey(partition);
        var targetResolvable = catalog.TryGet(targetProviderKey, out _);
        var alreadyAtTarget = string.Equals(currentKey, targetProviderKey, StringComparison.Ordinal);

        long srcLowest = -1, srcHighest = -1, entriesToCopy = 0;
        var (srcProvider, _) = resolver.ResolveWalProvider(physicalTreeId, pin, partition);
        srcLowest = await srcProvider.GetLowestOffsetAsync(physicalTreeId, partition, cancellationToken);
        srcHighest = await srcProvider.GetHighestOffsetAsync(physicalTreeId, partition, cancellationToken);
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

        var newPin = pin.WithPartition(partition, targetProviderKey, pin.Version + 1);
        var (dstProvider, _) = resolver.ResolveWalProvider(physicalTreeId, newPin, partition);

        // 1. Quiesce + fence the source activation at the pin version we read.
        var quiesce = await wal.QuiesceForMoveAsync(pin.Version, opts.EffectiveQuiesceLease, cancellationToken);
        if (!quiesce.Quiesced)
        {
            throw new InvalidOperationException(
                $"WAL move of {physicalTreeId}/{partition} aborted: the source activation resolved placement version "
                + $"{quiesce.ObservedPlacementVersion}, but the coordinator expected {pin.Version}. The placement changed "
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
                if (dstHighestBefore > srcHighest)
                {
                    throw new InvalidOperationException(
                        $"WAL move of {physicalTreeId}/{partition} aborted: the target already holds offset "
                        + $"{dstHighestBefore}, beyond the source highest {srcHighest}. The target is not a clean "
                        + "prefix of the source; resolve the divergence before retrying.");
                }
                if (dstHighestBefore < srcLowest - 1 && srcLowest > 0)
                {
                    // Reserve the destination trim floor so the first append's
                    // offset (srcLowest) is contiguous with the reserved point.
                    await dstProvider.TrimAsync(physicalTreeId, partition, srcLowest - 1, cancellationToken);
                }

                await CopyRangeAsync(Math.Max(srcLowest - 1, dstHighestBefore), srcHighest);
            }

            // 3. Convergence: re-quiesce with a fresh lease right before the
            //    cutover. This (a) resets the source's self-heal deadline so the
            //    fence is guaranteed to outlast the compare-and-swap below, and
            //    (b) catches any appends that slipped onto the source if the
            //    first lease lapsed during a slow copy. Loop until the source
            //    tail is stable, then flip immediately while the lease holds.
            while (true)
            {
                var recheck = await wal.QuiesceForMoveAsync(pin.Version, opts.EffectiveQuiesceLease, cancellationToken);
                if (!recheck.Quiesced)
                {
                    throw new InvalidOperationException(
                        $"WAL move of {physicalTreeId}/{partition} aborted: the source activation resolved placement "
                        + $"version {recheck.ObservedPlacementVersion} during convergence, but the coordinator expected "
                        + $"{pin.Version}. The placement changed underneath the move; retry.");
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

        // 5. Atomically flip the placement pin (compare-and-swap on version).
        var flipped = await registry.UpdateWalPlacementAsync(physicalTreeId, pin.Version, partition, targetProviderKey);

        // 6. Force the source activation to deactivate so the next activation
        //    (on any silo) re-resolves placement and routes to the target. Use a
        //    non-cancelable token: once the pin has flipped the stale activation
        //    must be retired regardless of the caller's cancellation.
        try
        {
            await wal.DeactivateForMoveAsync(CancellationToken.None);
        }
        catch (Exception ex)
        {
            // The pin is already flipped; an idempotent re-execute will repair
            // the deactivation via the AlreadyAtTarget fast path.
            logger.LogWarning(ex, "WAL move of {TreeId}/{Partition} flipped the pin but failed to deactivate the source; re-execute to repair.", physicalTreeId, partition);
        }

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
            CopiedFromOffset = copiedFrom,
            CopiedThroughOffset = copiedThrough,
            SourceHighestOffset = srcHighest,
            TargetHighestOffset = dstHighest,
            SourceRetained = true,
            Outcome = WalMoveOutcome.Moved,
        };
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
