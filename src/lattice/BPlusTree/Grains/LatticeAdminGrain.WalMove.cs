using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// WAL placement and managed-move surface of <see cref="LatticeAdminGrain"/>.
/// Implements the read-only inspection methods
/// (<see cref="GetWalPlacementAsync"/>, <see cref="AuditWalPlacementAsync"/>,
/// <see cref="Orleans.Lattice.BPlusTree.Grains.LatticeAdminGrain.PlanWalMoveAsync(string, int, string, System.Threading.CancellationToken)"/>) and the mutating move saga
/// (<see cref="Orleans.Lattice.BPlusTree.Grains.LatticeAdminGrain.ExecuteWalMoveAsync(string, int, string, System.Nullable{Orleans.Lattice.WalMoveOptions}, System.Threading.CancellationToken)"/>, <see cref="ReclaimMovedWalSourceAsync"/>).
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
        // Forced: a WAL move names a partition, not a key, so a cached alias would
        // plan or move the retired physical tree's log after a resize (#4180).
        var routing = await lattice.GetRoutingAsync(forceRefresh: true, cancellationToken);
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
        var registry = grainFactory.GetLatticeRegistry();
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
        var registry = grainFactory.GetLatticeRegistry();
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

        var registry = grainFactory.GetLatticeRegistry();
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

        var registry = grainFactory.GetLatticeRegistry();
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
        if (srcLowest >= 0 && srcHighest >= srcLowest)
        {
            // Count the LIVE range only. srcHighest is a monotonic high-water
            // mark that survives a trim (see IWalStorageProvider.
            // GetHighestOffsetAsync), so a fully-trimmed shard reports a
            // positive tail with no retained entries; srcLowest is the
            // authority on whether anything is actually there to copy.
            entriesToCopy = srcHighest - srcLowest + 1;
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
    public async Task<WalMoveReceipt> ExecuteWalMoveTrackedAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        WalMoveOptions? options,
        LatticeOperationTicket ticket,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(ticket);
        using var relay = LatticeOperationRelay.Open(grainFactory, ticket, cancellationToken);
        try
        {
            var receipt = await ExecuteWalMoveAsync(treeId, partition, targetProviderKey, options, relay.Token);
            await relay.FlushAsync();
            return receipt;
        }
        catch
        {
            await BankRelayedProgressAsync(relay);
            throw;
        }
    }

    /// <summary>
    /// Banks a tracked move's coalesced copy progress on its fault and
    /// cancellation path, so the entries copied before the fault stay recorded
    /// (#2545).
    /// </summary>
    private static Task BankRelayedProgressAsync(LatticeOperationRelay relay) => relay.BankProgressAsync();

    /// <summary>
    /// Reports progress to the coordinated operation running this call, if any; a
    /// single null check otherwise.
    /// </summary>
    private static ValueTask ReportMoveProgressAsync(
        string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null)
    {
        var progress = LatticeOperationProgress.Current;
        return progress is null
            ? ValueTask.CompletedTask
            : progress.ReportAsync(phase, completedUnits, totalUnits, unitName);
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

        var registry = grainFactory.GetLatticeRegistry();
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

        // Raise the durable move fence before the first quiesce (issue #4525).
        // From here until the flip clears it, every activation of the source -
        // including one that replaces a lost fenced activation - comes up fenced.
        var moveId = NewWalMoveId();
        await registry.RaiseWalMoveFencesAsync(
            physicalTreeId, pin.Version, [partition], moveId, opts.EffectiveQuiesceLease, renew: false);

        var copy = await RunMoveCopyPhasesAsync(
            physicalTreeId, pin, partition, targetProviderKey, resolver, encoder, opts, moveId, cancellationToken);

        // 5. Atomically flip the placement pin: a compare-and-swap on the version
        //    that also requires the partition to still carry this move's fence,
        //    and clears it in the same write. A fence that lapsed and was released
        //    may have let the source acknowledge appends the copy never saw, so
        //    the flip is refused and the move aborts with the source still live.
        State.WalPlacementPin flipped;
        try
        {
            flipped = await registry.FlipFencedWalPlacementAsync(
                physicalTreeId, pin.Version, [(partition, targetProviderKey)], moveId);
        }
        catch
        {
            await ReleaseFenceAndDeactivateSourceAsync(physicalTreeId, partition, moveId);
            throw;
        }

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
    /// failure the move's durable fence is released and the fenced source is
    /// deactivated best-effort so it resumes service without waiting out the
    /// quiesce lease, and the exception is rethrown with
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
        string moveId,
        CancellationToken cancellationToken)
    {
        var (srcProvider, _) = resolver.ResolveWalProvider(physicalTreeId, basePin, partition);
        var movedPin = basePin.WithPartition(partition, targetProviderKey, basePin.Version);
        var (dstProvider, _) = resolver.ResolveWalProvider(physicalTreeId, movedPin, partition);
        var wal = grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{partition}");
        var registry = grainFactory.GetLatticeRegistry();

        long srcHighest = -1, srcLowest = -1;
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

                // Units are offsets of the live tail: a resumed copy counts the
                // prefix an earlier attempt already landed as done.
                var floor = srcLowest >= 0 && srcLowest <= copiedFrom ? srcLowest : copiedFrom;
                await ReportMoveProgressAsync(
                    LatticeMaintenanceProgress.Copying,
                    copiedThrough - floor + 1,
                    Math.Max(throughInclusive, copiedThrough) - floor + 1,
                    LatticeMaintenanceProgress.Entries);
            }
            return cursor;
        }

        long dstHighest;
        var hasLiveRange = false;
        try
        {
            // 1. Quiesce + fence the source activation at the pin version we read.
            //    The durable fence the caller raised already holds, so whichever
            //    activation answers is fenced; this drains it and reads its tail.
            var quiesce = await wal.QuiesceForMoveAsync(basePin.Version, opts.EffectiveQuiesceLease, cancellationToken);
            ThrowIfNotQuiesced(quiesce, physicalTreeId, partition, basePin.Version, "");

            srcHighest = quiesce.HighestOffsetInclusive;
            srcLowest = await srcProvider.GetLowestOffsetAsync(physicalTreeId, partition, cancellationToken);

            // True when the source actually holds live entries to copy. srcHighest
            // is a monotonic high-water mark that survives a trim, so it is NOT a
            // safe proxy: a fully-trimmed shard reports a positive tail with
            // nothing retained. Gating the copy - and the post-copy verification -
            // on the live range keeps a fully-trimmed partition movable instead of
            // failing verification against a tail that no longer has entries.
            hasLiveRange = srcLowest >= 0 && srcHighest >= srcLowest;

            // 2. Copy the retained tail [srcLowest..srcHighest] to the target,
            //    preserving offsets and the source trim floor. Resumable: if a
            //    prior attempt copied a prefix, continue past the target's tail.
            if (hasLiveRange)
            {
                await ReportMoveProgressAsync(
                    LatticeMaintenanceProgress.Copying, 0, srcHighest - srcLowest + 1, LatticeMaintenanceProgress.Entries);
                var dstHighestBefore = await dstProvider.GetHighestOffsetAsync(physicalTreeId, partition, cancellationToken);
                if (!WalMoveResumeCore.IsTargetCleanPrefix(dstHighestBefore, srcHighest))
                {
                    throw new InvalidOperationException(
                        $"WAL move of {physicalTreeId}/{partition} aborted: the target already holds offset "
                        + $"{dstHighestBefore}, beyond the source highest {srcHighest}. The target is not a clean "
                        + "prefix of the source; resolve the divergence before retrying.");
                }
                if (dstHighestBefore >= srcLowest)
                {
                    var dstLowestBefore = await dstProvider.GetLowestOffsetAsync(physicalTreeId, partition, cancellationToken);
                    if (!WalMoveResumeCore.TargetHoldsResumedPrefix(dstHighestBefore, dstLowestBefore, srcLowest))
                    {
                        throw new InvalidOperationException(
                            $"WAL move of {physicalTreeId}/{partition} aborted: the target's high-water mark "
                            + $"{dstHighestBefore} covers source offsets from {srcLowest} that the target no longer "
                            + $"holds (its lowest live offset is {dstLowestBefore}), typically because it was reclaimed "
                            + "after an earlier placement. Resuming past that mark would skip live entries. Move to a "
                            + $"different provider key, or retry once the source's retained range starts above {dstHighestBefore}.");
                    }
                }
                if (WalMoveResumeCore.NeedsFloorReserve(dstHighestBefore, srcLowest))
                {
                    // Reserve the destination trim floor so the first append's
                    // offset (srcLowest) is contiguous with the reserved point.
                    await dstProvider.TrimAsync(physicalTreeId, partition, srcLowest - 1, cancellationToken);
                }

                await CopyRangeAsync(WalMoveResumeCore.ResumeCursor(srcLowest, dstHighestBefore), srcHighest);
            }
            else if (srcHighest >= 0)
            {
                // Fully trimmed source: nothing to copy, but the target must still
                // carry the source's high-water mark, because the WAL grain that
                // activates on the target allocates its next offset from the
                // target's highest. Reserve it as a trim point; a provider that does
                // not raise its mark for a reservation is refused at step 4.
                var dstHighestBefore = await dstProvider.GetHighestOffsetAsync(physicalTreeId, partition, cancellationToken);
                if (dstHighestBefore < srcHighest)
                {
                    await dstProvider.TrimAsync(physicalTreeId, partition, srcHighest, cancellationToken);
                }
            }

            // 3. Convergence: renew the durable fence and re-quiesce with a fresh
            //    lease right before the cutover. This (a) resets the source's
            //    self-heal deadline, and (b) catches any appends that slipped onto
            //    the source if the first lease lapsed during a slow copy. Loop until
            //    the source tail is stable, then flip immediately. The lease alone
            //    does not make the cutover safe: the activation holding it can be
            //    lost. What does is the durable fence, which every later activation
            //    honours and which the flip requires to still be held (issue #4525).
            while (true)
            {
                // Renew the durable fence before every re-quiesce. A renewal never
                // re-creates a fence that lapsed and was released - the source may
                // have served appends since - so a lost fence aborts the move here.
                await registry.RaiseWalMoveFencesAsync(
                    physicalTreeId, basePin.Version, [partition], moveId, opts.EffectiveQuiesceLease, renew: true);
                var recheck = await wal.QuiesceForMoveAsync(basePin.Version, opts.EffectiveQuiesceLease, cancellationToken);
                ThrowIfNotQuiesced(recheck, physicalTreeId, partition, basePin.Version, " during convergence");
                if (recheck.HighestOffsetInclusive <= srcHighest)
                {
                    break;
                }
                // New appends landed on the source while copying: copy the delta.
                await CopyRangeAsync(srcHighest, recheck.HighestOffsetInclusive);
                srcHighest = recheck.HighestOffsetInclusive;
                hasLiveRange = true;
            }

            // 4. Verify the target tail before the irreversible cutover. The
            //    overshoot guard runs even when content verification is off.
            await ReportMoveProgressAsync(LatticeMaintenanceProgress.Verifying);
            dstHighest = await dstProvider.GetHighestOffsetAsync(physicalTreeId, partition, cancellationToken);
            if (srcHighest >= 0 && dstHighest > srcHighest)
            {
                throw new InvalidOperationException(
                    $"WAL move of {physicalTreeId}/{partition} aborted: target highest offset {dstHighest} overshot "
                    + $"source highest {srcHighest} after copy.");
            }
            if (opts.VerifyAfterCopy && hasLiveRange && dstHighest != srcHighest)
            {
                throw new InvalidOperationException(
                    $"WAL move of {physicalTreeId}/{partition} failed verification: source highest offset {srcHighest} "
                    + $"but target highest offset {dstHighest} after copy. The pin was not flipped; the source remains live.");
            }
            if (!hasLiveRange && srcHighest >= 0 && dstHighest < srcHighest)
            {
                // Runs even when content verification is off: this is not about
                // content (there is none to copy) but about allocation. Flipping
                // here would restart the partition's offsets beneath offsets its
                // consumers have already passed.
                throw new InvalidOperationException(
                    $"WAL move of {physicalTreeId}/{partition} aborted: the source holds no live entries but has assigned "
                    + $"offsets through {srcHighest}, and the target did not record that high-water mark (target highest "
                    + $"{dstHighest}). After the cutover the target would reuse offsets from {dstHighest + 1}. The pin was "
                    + "not flipped; the source remains live. Retry once the partition holds live entries.");
            }

            // Defence in depth for issue #4525: re-read the source's durable tail
            // directly. The fenced flip is what makes the cutover safe, but an
            // append that slipped past the final quiesce is caught here, before
            // the irreversible step, with a precise diagnosis.
            var srcDurableHighest = await srcProvider.GetHighestOffsetAsync(physicalTreeId, partition, cancellationToken);
            if (srcDurableHighest > srcHighest)
            {
                throw new InvalidOperationException(
                    $"WAL move of {physicalTreeId}/{partition} aborted: the source's durable highest offset is "
                    + $"{srcDurableHighest}, beyond the quiesced highest {srcHighest} the copy was taken at. An append "
                    + "reached the source after the final quiesce. The pin was not flipped; the source remains live; retry.");
            }

            // The last cancellation point of a tracked move: a cancel observed here
            // still lands in the catch below, which unfences the source. The report
            // itself can carry the stop signal back, so check the token after it.
            await ReportMoveProgressAsync(LatticeMaintenanceProgress.Flipping);
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch
        {
            // The pin was never flipped, so the partition's durable placement
            // still points at the source. Release the durable fence first, then
            // force the fenced source activation to deactivate, so the next
            // activation resumes service on the source immediately instead of
            // waiting out the quiesce lease. In that order: an activation that
            // came up before the release would stay fenced for the whole lease.
            await ReleaseFenceAndDeactivateSourceAsync(physicalTreeId, partition, moveId);
            throw;
        }

        return new MoveCopyResult(copiedFrom, copiedThrough, srcHighest, dstHighest);
    }

    /// <summary>
    /// Throws when a <see cref="IWalShardGrain.QuiesceForMoveAsync"/> did not
    /// quiesce the source: either the activation resolved a newer placement, or
    /// provider writes it stopped waiting for may still land (issue #4525).
    /// </summary>
    private static void ThrowIfNotQuiesced(
        WalMoveQuiesceResult result, string physicalTreeId, int partition, long expectedVersion, string phase)
    {
        if (result.Quiesced)
        {
            return;
        }
        if (result.DrainIncomplete)
        {
            throw new InvalidOperationException(
                $"WAL move of {physicalTreeId}/{partition} aborted{phase}: the source could not drain within its drain "
                + "budget and provider writes it stopped waiting for may still land, so its tail is not stable. The pin "
                + "was not flipped; the source remains live; retry once the source's provider recovers.");
        }
        throw new InvalidOperationException(
            $"WAL move of {physicalTreeId}/{partition} aborted{phase}: the source activation resolved placement version "
            + $"{result.ObservedPlacementVersion}, but the coordinator expected {expectedVersion}. The placement changed "
            + "underneath the move; re-read placement and retry.");
    }

    /// <summary>A fresh identity for one WAL move's durable fence.</summary>
    private static string NewWalMoveId() => Guid.NewGuid().ToString("N");

    /// <summary>
    /// Best-effort abort cleanup for a batch move: releases the fence and
    /// deactivates the source of every partition the batch fenced.
    /// </summary>
    private async Task ReleaseBatchFencesAsync(string physicalTreeId, IEnumerable<int> partitions, string moveId)
    {
        foreach (var partition in partitions)
        {
            await ReleaseFenceAndDeactivateSourceAsync(physicalTreeId, partition, moveId);
        }
    }

    /// <summary>
    /// Best-effort abort cleanup for a fenced move of one partition: releases the
    /// move's durable fence, then deactivates the source activation so the next
    /// one re-resolves placement unfenced. Each step is logged rather than
    /// propagated; a fence that cannot be released lapses with its lease and is
    /// released by the next activation of the source. A no-op for a fence the
    /// flip already cleared.
    /// </summary>
    private async Task ReleaseFenceAndDeactivateSourceAsync(string physicalTreeId, int partition, string moveId)
    {
        try
        {
            await grainFactory.GetLatticeRegistry()
                .ReleaseWalMoveFenceAsync(physicalTreeId, partition, moveId, onlyIfExpired: false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Releasing the fence of aborted WAL move {MoveId} of {TreeId}/{Partition} failed; it lapses with its lease.", moveId, physicalTreeId, partition);
        }
        try
        {
            await grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{partition}")
                .DeactivateForMoveAsync(CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Best-effort source deactivation after aborted WAL move of {TreeId}/{Partition} failed.", physicalTreeId, partition);
        }
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

        var registry = grainFactory.GetLatticeRegistry();
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
        var moveId = NewWalMoveId();
        if (realMoveIndexes.Count > 0)
        {
            // Raise every real move's durable fence in one registry write before
            // any source is quiesced (issue #4525). A conflict with another move
            // refuses the whole batch before any log is touched.
            var fencedPartitions = new int[realMoveIndexes.Count];
            for (var slot = 0; slot < realMoveIndexes.Count; slot++)
            {
                fencedPartitions[slot] = requested[realMoveIndexes[slot]].Partition;
            }
            await registry.RaiseWalMoveFencesAsync(
                physicalTreeId, pin.Version, fencedPartitions, moveId, opts.EffectiveQuiesceLease, renew: false);

            // Phases 1-4 for every real move, bounded by the configured ceiling.
            // Task.WhenAll waits for all phases to settle even on failure, so the
            // catch can release every fenced source deterministically.
            try
            {
                await BoundedFanOut.RunAsync(realMoveIndexes.Count, opts.EffectiveMaxConcurrentPartitionMoves, async slot =>
                {
                    var i = realMoveIndexes[slot];
                    copyResults[i] = await RunMoveCopyPhasesAsync(
                        physicalTreeId, pin, requested[i].Partition, requested[i].TargetProviderKey,
                        resolver, encoder, opts, moveId, cancellationToken);
                });
            }
            catch
            {
                // Any per-partition failure aborts the whole batch: the pin was
                // never flipped, so release every fence and fenced source (the
                // failed ones were already released by the copy helper;
                // re-requesting is an idempotent no-op) and retain partial target
                // copies for a resumable retry.
                await ReleaseBatchFencesAsync(physicalTreeId, fencedPartitions, moveId);
                throw;
            }
        }

        // 5. Flip every real move together under a single compare-and-swap that
        //    requires every moved partition to still carry this move's fence and
        //    clears them in the same write. When no partition needed moving the
        //    placement is left untouched.
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
            try
            {
                var flipped = await registry.FlipFencedWalPlacementAsync(physicalTreeId, pin.Version, batched, moveId);
                newVersion = flipped.Version;
            }
            catch
            {
                await ReleaseBatchFencesAsync(physicalTreeId, batched.Select(static m => m.Partition), moveId);
                throw;
            }
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

        var registry = grainFactory.GetLatticeRegistry();
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
        var lowest = await sourceProvider.GetLowestOffsetAsync(physicalTreeId, partition, cancellationToken);
        var outcome = WalMoveOutcome.NoOp;

        // Gate on the LIVE range, not on the high-water mark alone: a trim never
        // lowers GetHighestOffsetAsync, so an already-reclaimed source still
        // reports a non-negative highest while holding nothing to reclaim.
        if (lowest >= 0 && highest >= lowest)
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
