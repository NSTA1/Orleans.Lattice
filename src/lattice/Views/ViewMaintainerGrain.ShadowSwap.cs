using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Views;

/// <summary>
/// Shadow-swap rebuild, view anti-entropy (<see cref="ReconcileAsync"/>), and the
/// order-independent drift digest (Phase 5) of the view maintainer.
/// <para>
/// <b>Generation-addressed view tree.</b> The live view tree id is resolved
/// through the durable <see cref="ViewCheckpointState.ActiveGeneration"/>:
/// generation <c>0</c> is the legacy <c>view-{name}</c> id (so an
/// already-materialised view keeps its tree across an upgrade) and every
/// generation greater than <c>0</c> is the suffixed <c>view-{name}#g{N}</c>. A
/// rebuild targets generation <c>ActiveGeneration + 1</c>, builds it fully in the
/// background, then flips the active generation - together with the resume
/// checkpoint - in a single durable <c>WriteStateAsync</c>:
/// the atomic swap. Readers resolve the active generation, so they move from the
/// old fully-built tree to the new fully-built tree with no empty window.
/// </para>
/// <para>
/// <b>Crash safety.</b> A crash before the swap leaves
/// <see cref="ViewCheckpointState.ActiveGeneration"/> unchanged, so the prior
/// generation keeps serving; the orphaned shadow under
/// <c>view-{name}#g{old+1}</c> is exactly the next rebuild attempt's target and
/// is cleared before that attempt re-builds, so it never leaks. The swap is a
/// single write, so it either fully happens or not at all.
/// </para>
/// <para>
/// <b>Deferred reclaim.</b> The swapped-out generation tree is not deleted inline
/// with the swap; instead the swap records it for reclamation after a grace
/// (<see cref="LatticeViewOptions.OldGenerationReclaimGrace"/>) that exceeds the
/// read handle's active-tree cache lifetime, so a reader still holding the prior
/// generation during the brief post-swap staleness window reads a fully-built
/// (if slightly stale) tree rather than a deleted one. The reclaim runs on the
/// regular drain cadence and is itself durable.
/// </para>
/// </summary>
internal sealed partial class ViewMaintainerGrain
{
    /// <summary>The captured per-partition resume floor and highest HLC of a completed shadow build.</summary>
    private readonly record struct ShadowBuildResult(Dictionary<int, long> Offsets, HybridLogicalClock Highest);

    /// <summary>
    /// Resolves a generation number to its view tree id. Generation <c>0</c> maps
    /// to the legacy <c>view-{name}</c> id for backward compatibility; higher
    /// generations are suffixed <c>#g{N}</c>.
    /// </summary>
    private string GenerationTreeId(long generation) =>
        generation <= 0
            ? $"{LatticeConstants.ViewTreePrefix}{ViewName}"
            : $"{LatticeConstants.ViewTreePrefix}{ViewName}#g{generation}";

    private TimeSpan ReclaimGrace
    {
        get
        {
            var grace = Options.OldGenerationReclaimGrace;
            return grace > TimeSpan.Zero ? grace : LatticeViewOptions.DefaultOldGenerationReclaimGrace;
        }
    }

    /// <inheritdoc />
    public Task<string> GetActiveTreeIdAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(GenerationTreeId(state.State.ActiveGeneration));

    /// <inheritdoc />
    public async Task<ViewDigest> ComputeViewDigestAsync(CancellationToken cancellationToken = default)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var registration = catalog.TryGet(ViewName);
        var isAggregation = registration?.IsAggregation ?? false;
        var activeTree = grainFactory.GetGrain<ILattice>(ViewTreeId);
        return await ComputeTreeDigestAsync(activeTree, isAggregation, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> ReconcileAsync(CancellationToken cancellationToken = default)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return false;
        }

        // ShipView consumer (Decision A): a suppressed consumer has no local source
        // to re-derive from, so source-digest drift repair is producer-only. Drift
        // on a consumer's replicated view tree is repaired by the existing
        // replication anti-entropy against the producer, not by a local reconcile.
        if (_shipViewSuppressed)
        {
            return false;
        }

        await TryReclaimPendingGenerationAsync(cancellationToken);

        // Digest the live active view before touching anything. The shadow build
        // writes only into the shadow generation, so the active tree (and this
        // digest) stays valid across it.
        var activeTree = grainFactory.GetGrain<ILattice>(ViewTreeId);
        var liveDigest = await ComputeTreeDigestAsync(activeTree, registration.IsAggregation, cancellationToken);

        // Build the expected view from current source state into the shadow.
        var built = await BuildShadowAsync(registration, cancellationToken);
        var shadowTree = grainFactory.GetGrain<ILattice>(GenerationTreeId(state.State.ActiveGeneration + 1));
        var shadowDigest = await ComputeTreeDigestAsync(shadowTree, registration.IsAggregation, cancellationToken);

        if (shadowDigest.ContentEquals(liveDigest))
        {
            // In sync: discard the shadow, keep serving the active generation, and
            // leave the checkpoint untouched so the live tail resumes where it was.
            await ClearTreeAsync(shadowTree, cancellationToken);
            return false;
        }

        // Drift: the freshly-built shadow is the repaired view; swap it in.
        await SwapToShadowAsync(registration, built.Offsets, built.Highest, cancellationToken);
        return true;
    }

    /// <summary>
    /// In-place rebuild for a <see cref="LatticeViewReplicationMode.ShipView"/> view
    /// (Decision B): reprojects current committed source state directly into the
    /// stable generation-0 <c>view-{name}</c> tree, keeping the active generation at
    /// <c>0</c> so the replicated tree id never cycles and matches the operator's
    /// replicated-trees entry. Unlike the shadow-swap rebuild it clears and
    /// re-derives the live tree in place (a brief transient divergence on the
    /// producer that heals on consumers via replication anti-entropy), then advances
    /// the durable checkpoint and re-pins the source WAL cursor at the rebuilt head
    /// in a single state write.
    /// </summary>
    private async Task InPlaceRebuildAsync(ViewRegistration registration, CancellationToken cancellationToken)
    {
        var sourceTreeId = registration.SourceTreeId;
        var partitions = await optionsResolver.GetWalPartitionsAsync(sourceTreeId);

        // Capture the source head per partition BEFORE clearing/scanning so any
        // source mutation committed during the build is picked up by the resumed
        // tail. Hold each partition's resume floor back below the lowest
        // still-staged offset (computed while staging is still populated) so an
        // in-flight (committed-but-not-yet-flushed) atomic batch whose prepares sit
        // below head is re-read and re-staged by the resumed tail rather than
        // skipped past - otherwise its committed batch would be permanently lost
        // until a later reconcile.
        var capturedOffsets = new Dictionary<int, long>();
        for (var partition = 0; partition < partitions; partition++)
        {
            var head = await commitLogReader.GetHeadOffsetAsync(sourceTreeId, partition, cancellationToken);
            var floor = head - 1;
            var stagedFloor = HeldFloorForPartition(partition);
            if (stagedFloor != long.MaxValue && stagedFloor - 1 < floor)
            {
                floor = stagedFloor - 1;
            }

            capturedOffsets[partition] = floor;
        }

        // A rebuild reconverges from current committed source state, so abandon any
        // partially-staged atomic batch (its uncommitted prepares are not part of
        // committed source state); an in-flight batch held back above is re-staged
        // by the resumed tail when it commits.
        _staging.Clear();
        _stagedSourceKeyRefCount.Clear();
        _ordinaryHlcOverStagedKey.Clear();

        // Freshen the aggregation idempotency keys (see BuildShadowAsync) so the
        // re-accumulation mints fresh sagas rather than re-attaching to the retained
        // sagas of the now-cleared rows.
        if (registration.IsAggregation)
        {
            state.State.RebuildGeneration++;
            await state.WriteStateAsync();
        }

        var viewTree = grainFactory.GetGrain<ILattice>(GenerationTreeId(0));
        await ClearTreeAsync(viewTree, cancellationToken);

        var sourceTree = grainFactory.GetGrain<ILattice>(sourceTreeId);
        var highest = HybridLogicalClock.Zero;
        var aggregationApplier = registration.IsAggregation ? CreateAggregationApplier(viewTree) : null;

        await foreach (var key in sourceTree.KeysAsync(cancellationToken: cancellationToken))
        {
            var versioned = await sourceTree.GetWithVersionAsync(key, cancellationToken);
            if (versioned.Value is null)
            {
                continue;
            }

            var synthetic = new LatticeMutation
            {
                TreeId = sourceTreeId,
                Kind = MutationKind.Set,
                Key = key,
                Value = versioned.Value,
                Timestamp = versioned.Version,
                ExpiresAtTicks = versioned.ExpiresAtTicks,
                Category = MutationCategory.User,
            };

            if (aggregationApplier is not null)
            {
                foreach (var contribution in registration.AggregationProjection!.Project(synthetic))
                {
                    await aggregationApplier.ApplyAsync(contribution, cancellationToken);
                }
            }
            else
            {
                foreach (var write in registration.Projection!.Project(synthetic))
                {
                    await ApplyAsync(viewTree, write, cancellationToken);
                }
            }

            if (versioned.Version > highest)
            {
                highest = versioned.Version;
            }
        }

        // Keep the active generation at 0 (stable tree id) and advance the resume
        // checkpoint to the captured source heads in one durable write.
        state.State.ActiveGeneration = 0;
        state.State.AppliedOffsets = capturedOffsets;
        state.State.HighestAppliedTimestamp = highest;
        state.State.ProjectionVersion = registration.ProjectionVersion;
        await state.WriteStateAsync();

        if (highest > HybridLogicalClock.Zero)
        {
            await cursorRegistry.ReportCursorAsync(sourceTreeId, ConsumerId, highest, cancellationToken);
        }
    }

    /// <summary>
    /// Builds the next generation's view tree from current committed source state:
    /// captures each source partition head as the resume floor before scanning,
    /// clears the (normally empty) shadow tree, then range-scans the source
    /// re-projecting every live entry through the same filter / re-key /
    /// aggregation projection the tail path uses - carrying each entry's
    /// <see cref="LatticeMutation.ExpiresAtTicks"/> through so rebuilt entries keep
    /// their TTL. Does not advance the checkpoint or flip the active generation;
    /// the caller swaps.
    /// </summary>
    private async Task<ShadowBuildResult> BuildShadowAsync(ViewRegistration registration, CancellationToken cancellationToken)
    {
        var sourceTreeId = registration.SourceTreeId;
        var partitions = await optionsResolver.GetWalPartitionsAsync(sourceTreeId);

        // Capture the source head per partition BEFORE clearing/scanning so any
        // source mutation committed during the build is picked up by the resumed
        // tail (and re-applied idempotently if it was also seen in the scan). Hold
        // each partition's resume floor back below the lowest still-staged offset
        // (computed while staging is still populated) so an in-flight atomic batch
        // whose prepares sit below head is re-read and re-staged by the resumed
        // tail rather than skipped past - otherwise a committed batch whose
        // terminal had not yet arrived at scan time would be permanently lost until
        // a later reconcile.
        var capturedOffsets = new Dictionary<int, long>();
        for (var partition = 0; partition < partitions; partition++)
        {
            var head = await commitLogReader.GetHeadOffsetAsync(sourceTreeId, partition, cancellationToken);
            var floor = head - 1;
            var stagedFloor = HeldFloorForPartition(partition);
            if (stagedFloor != long.MaxValue && stagedFloor - 1 < floor)
            {
                floor = stagedFloor - 1;
            }

            capturedOffsets[partition] = floor;
        }

        // A rebuild reconverges the view from current committed source state, so
        // any partially-staged atomic batch is abandoned: its uncommitted prepares
        // are not part of committed source state and must not leak into the view;
        // a committed batch is re-derived from the source rows (and an in-flight
        // batch held back above is re-staged by the resumed tail when it commits).
        _staging.Clear();
        _stagedSourceKeyRefCount.Clear();
        _ordinaryHlcOverStagedKey.Clear();

        // Bump and durably persist the rebuild generation BEFORE re-applying so the
        // aggregation flip's idempotency keys are freshened: re-using the prior
        // build's operation ids would re-attach to its retained sagas and apply
        // nothing. A build that crashes and retries persists yet another
        // generation, so it never collides with a partially-applied attempt.
        if (registration.IsAggregation)
        {
            state.State.RebuildGeneration++;
            await state.WriteStateAsync();
        }

        var shadowGeneration = state.State.ActiveGeneration + 1;
        var shadowTree = grainFactory.GetGrain<ILattice>(GenerationTreeId(shadowGeneration));

        // Clear the shadow: it is empty in the steady state, but an earlier crashed
        // build may have left a partial generation under the same id.
        await ClearTreeAsync(shadowTree, cancellationToken);

        var sourceTree = grainFactory.GetGrain<ILattice>(sourceTreeId);
        var highest = HybridLogicalClock.Zero;
        var aggregationApplier = registration.IsAggregation ? CreateAggregationApplier(shadowTree) : null;

        // An accumulative (history) view shapes every rebuilt row under the source
        // tree's live retention policy, exactly as the incremental drain does, so a
        // rebuild (initial backfill, fall-off-log recovery, or an explicit operator
        // rebuild) never stores unshaped value bytes. The policy and the apply clock
        // are read once for the whole build.
        HistoryRetentionPolicy historyPolicy = default;
        var historyNowTicks = 0L;
        if (registration.Accumulative)
        {
            historyPolicy = await optionsResolver
                .GetHistoryRetentionAsync(sourceTreeId, Options.HistoryHybridFullValueWindow);
            historyNowTicks = DateTime.UtcNow.Ticks;
        }

        await foreach (var key in sourceTree.KeysAsync(cancellationToken: cancellationToken))
        {
            var versioned = await sourceTree.GetWithVersionAsync(key, cancellationToken);
            if (versioned.Value is null)
            {
                continue;
            }

            // Synthesize a Set mutation so the build reuses the exact projection
            // logic the tail path uses, carrying the entry's absolute expiry so a
            // rebuilt entry expires in lockstep with its source (TTL preserved).
            var synthetic = new LatticeMutation
            {
                TreeId = sourceTreeId,
                Kind = MutationKind.Set,
                Key = key,
                Value = versioned.Value,
                Timestamp = versioned.Version,
                ExpiresAtTicks = versioned.ExpiresAtTicks,
                Category = MutationCategory.User,
            };

            if (aggregationApplier is not null)
            {
                foreach (var contribution in registration.AggregationProjection!.Project(synthetic))
                {
                    await aggregationApplier.ApplyAsync(contribution, cancellationToken);
                }
            }
            else if (registration.Accumulative)
            {
                foreach (var write in registration.Projection!.Project(synthetic))
                {
                    await ApplyAsync(shadowTree, ShapeHistoryWrite(write, historyPolicy, historyNowTicks), cancellationToken);
                }
            }
            else
            {
                foreach (var write in registration.Projection!.Project(synthetic))
                {
                    await ApplyAsync(shadowTree, write, cancellationToken);
                }
            }

            if (versioned.Version > highest)
            {
                highest = versioned.Version;
            }
        }

        return new ShadowBuildResult(capturedOffsets, highest);
    }

    /// <summary>
    /// The atomic swap: flips the active generation to the freshly-built shadow and
    /// advances the resume checkpoint to the captured source heads in one durable
    /// write, and records the swapped-out generation for deferred reclamation.
    /// </summary>
    private async Task SwapToShadowAsync(
        ViewRegistration registration,
        Dictionary<int, long> capturedOffsets,
        HybridLogicalClock highest,
        CancellationToken cancellationToken)
    {
        var oldGeneration = state.State.ActiveGeneration;
        var newGeneration = oldGeneration + 1;

        // Flush any still-pending older reclaim so the new pending does not clobber
        // it (only reachable when two rebuilds happen within one reclaim grace,
        // already beyond the single post-swap staleness window).
        await TryReclaimPendingGenerationAsync(cancellationToken, force: true);

        state.State.ActiveGeneration = newGeneration;
        state.State.AppliedOffsets = capturedOffsets;
        state.State.HighestAppliedTimestamp = highest;
        state.State.ProjectionVersion = registration.ProjectionVersion;
        state.State.HasPendingReclaim = true;
        state.State.PendingReclaimGeneration = oldGeneration;
        state.State.ReclaimEligibleAtTicks = DateTime.UtcNow.Ticks + ReclaimGrace.Ticks;

        // Single durable commit: readers resolving the active generation flip from
        // the old fully-built tree to the new fully-built tree with no empty window.
        await state.WriteStateAsync();

        if (highest > HybridLogicalClock.Zero)
        {
            await cursorRegistry.ReportCursorAsync(registration.SourceTreeId, ConsumerId, highest, cancellationToken);
        }
    }

    /// <summary>
    /// Reclaims (deletes) the swapped-out generation tree once its post-swap reader
    /// grace has elapsed, or immediately when <paramref name="force"/> is set.
    /// Never reclaims the active generation. Durable, so a crash mid-reclaim simply
    /// retries on the next drain.
    /// </summary>
    private async Task TryReclaimPendingGenerationAsync(CancellationToken cancellationToken, bool force = false)
    {
        if (!state.State.HasPendingReclaim)
        {
            return;
        }

        if (!force && DateTime.UtcNow.Ticks < state.State.ReclaimEligibleAtTicks)
        {
            return;
        }

        var generation = state.State.PendingReclaimGeneration;
        if (generation != state.State.ActiveGeneration)
        {
            var stale = grainFactory.GetGrain<ILattice>(GenerationTreeId(generation));
            await ClearTreeAsync(stale, cancellationToken);
        }

        state.State.HasPendingReclaim = false;
        state.State.PendingReclaimGeneration = 0;
        state.State.ReclaimEligibleAtTicks = 0;
        await state.WriteStateAsync();
    }

    /// <summary>Deletes every key in <paramref name="tree"/> (including reserved aggregation rows).</summary>
    private static async Task ClearTreeAsync(ILattice tree, CancellationToken cancellationToken)
    {
        var keys = new List<string>();
        await foreach (var key in tree.KeysAsync(cancellationToken: cancellationToken))
        {
            keys.Add(key);
        }

        foreach (var key in keys)
        {
            await tree.DeleteAsync(key, cancellationToken);
        }
    }

    /// <summary>
    /// Computes the order-independent <see cref="ViewDigest"/> over a tree's
    /// materialised (key, value) entries. For an aggregation view the scan starts
    /// above the reserved internal-row range so only the materialised group values
    /// are folded. Each entry contributes <c>XxHash128(keyLen || key || value)</c>;
    /// the contributions are XOR-folded (commutative, self-inverse) and the final
    /// hash is <c>XxHash128(xor || entryCount)</c>.
    /// </summary>
    private static async Task<ViewDigest> ComputeTreeDigestAsync(ILattice tree, bool isAggregation, CancellationToken cancellationToken)
    {
        var floor = isAggregation ? AggregationRowCodec.FirstNonReservedKey : null;
        var accumulator = new byte[16];
        long count = 0;

        var hasher = new XxHash128();
        var entryHash = new byte[16];
        var lengthPrefix = new byte[4];

        await foreach (var entry in tree.EntriesAsync(floor, cancellationToken: cancellationToken))
        {
            var keyBytes = Encoding.UTF8.GetBytes(entry.Key);
            BinaryPrimitives.WriteInt32LittleEndian(lengthPrefix, keyBytes.Length);
            hasher.Append(lengthPrefix);
            hasher.Append(keyBytes);
            hasher.Append(entry.Value);
            hasher.GetHashAndReset(entryHash);

            for (var i = 0; i < 16; i++)
            {
                accumulator[i] ^= entryHash[i];
            }

            count++;
        }

        var countBytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(countBytes, count);
        hasher.Append(accumulator);
        hasher.Append(countBytes);
        var finalHash = new byte[16];
        hasher.GetHashAndReset(finalHash);

        return new ViewDigest { Hash = finalHash, EntryCount = count };
    }
}
