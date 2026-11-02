using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The WAL append fence on the partition (epic #5605, design note
/// <c>spec/wal/design/LiveAdvanceDesign.md</c>, section 1): the leaf epoch table,
/// the retired-epoch floor, the routing-generation table, the unstamped
/// watermark, and the stamped-append admission check.
/// </summary>
/// <remarks>
/// <para>
/// <b>Where the rows live.</b> Every durable value is a row of its own under a
/// reserved grain type (<see cref="WalAppendFenceRows"/>), written through the
/// tree's grain storage provider and keyed by tree and partition, never by
/// provider. A WAL move switches the stream's provider and touches none of them;
/// its cutover reactivates this grain, which drops only the in-memory copies.
/// Those reload through re-presented claims, and nothing is admitted from an
/// unloaded entry in the meantime.
/// </para>
/// <para>
/// <b>Cost on the append path.</b> One dictionary lookup and two comparisons under
/// <see cref="_stateGate"/>, which the append already holds, for a stamped append;
/// one field store for an unstamped one. No storage read, no grain call and no
/// allocation.
/// </para>
/// <para>
/// <b>Mutable state.</b> Every field below is read and written under
/// <see cref="_stateGate"/>. The claim and retirement methods do their storage I/O
/// outside it and take it only to decide and to raise.
/// </para>
/// </remarks>
internal sealed partial class WalShardGrain : IWalAppendFenceCapable
{
    /// <summary>How many times a claim or retirement re-reads after losing an ETag race on a row.</summary>
    internal const int MaxFenceRowAttempts = 8;

    /// <summary>
    /// The highest offset allocated to an unstamped append. Starts at the
    /// recovered allocator head minus one, treating everything before this
    /// activation as possibly unstamped (<c>ReadPositionHonestRestartForgetsUnstamped</c>).
    /// </summary>
    private long _unstampedWatermark = -1;

    /// <summary>The loaded leaf epoch entries, by leaf; created on the first claim.</summary>
    private Dictionary<Guid, FenceEntry>? _epochEntries;

    /// <summary>The loaded routing-generation entries, by initiator; created on the first re-claim.</summary>
    private Dictionary<WalRoutingInitiator, FenceEntry>? _generationEntries;

    /// <summary>The partition's retired-epoch floor, as far as this activation has read or raised it.</summary>
    private long _epochFloor;

    /// <summary>Set once the durable floor row has been read into <see cref="_epochFloor"/>.</summary>
    private bool _epochFloorLoaded;

    /// <summary>Waiters for a leaf's multi-entry stamped batches to finish assigning; created on demand.</summary>
    private Dictionary<Guid, TaskCompletionSource>? _batchDrains;

    /// <summary>Per-leaf claim/retirement gates; created on the first claim.</summary>
    private Dictionary<Guid, LeafGate>? _leafGates;

    /// <summary>The tree's grain storage provider, resolved on first use.</summary>
    private IGrainStorage? _fenceStorage;

    /// <summary>
    /// The highest unstamped offset this partition has allocated; test seam.
    /// </summary>
    internal long UnstampedWatermarkForTesting
    {
        get
        {
            lock (_stateGate)
            {
                return _unstampedWatermark;
            }
        }
    }

    /// <summary>The retired-epoch floor as this activation holds it; test seam.</summary>
    internal long EpochFloorForTesting
    {
        get
        {
            lock (_stateGate)
            {
                return _epochFloor;
            }
        }
    }

    /// <summary>Overrides the grain storage provider the fence rows go through; test seam.</summary>
    internal void UseFenceStorageForTesting(IGrainStorage storage) => _fenceStorage = storage;

    /// <inheritdoc />
    public async Task<WalAppendFenceClaimResult> ClaimAppendEpochAsync(Guid leafId, long epoch, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInternalOrigin(LatticeOperation.Write);
        EnsureInitialized();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(epoch);

        var storage = FenceStorage();
        await EnsureEpochFloorLoadedAsync(storage).ConfigureAwait(true);
        var key = WalAppendFenceRows.EntryKey(_treeId, _shardIndex, leafId);
        WalAppendFenceClaimResult result;
        await EnterLeafGateAsync(leafId).ConfigureAwait(true);
        try
        {
            result = await ClaimCoreAsync(
                storage,
                WalAppendFenceRows.EntryGrainType,
                key,
                () => WalAppendFenceRows.ReadEntryAsync(storage, _treeId, _shardIndex, leafId),
                static row => row.State.Epoch,
                static (row, value) => row.State.Epoch = value,
                EpochEntries(),
                leafId,
                epoch,
                useFloor: true,
                batchLeaf: leafId).ConfigureAwait(true);
        }
        finally
        {
            ExitLeafGate(leafId);
        }

        RecordFenceClaim(LatticeMetrics.AppendFenceKindEpoch, result.Accepted);
        return result;
    }

    /// <inheritdoc />
    public async Task<WalAppendFenceClaimResult> ClaimRoutingGenerationAsync(
        WalRoutingInitiator initiator,
        long generation,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInternalOrigin(LatticeOperation.Write);
        EnsureInitialized();
        if (!initiator.IsSet)
        {
            throw new ArgumentException("A routing generation is claimed for a named initiator.", nameof(initiator));
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(generation);

        var storage = FenceStorage();
        var key = WalAppendFenceRows.GenerationKey(_treeId, _shardIndex, initiator.Kind, initiator.Index);
        var result = await ClaimCoreAsync(
            storage,
            WalAppendFenceRows.GenerationGrainType,
            key,
            () => WalAppendFenceRows.ReadGenerationAsync(storage, _treeId, _shardIndex, initiator),
            static row => row.State.Generation,
            static (row, value) => row.State.Generation = value,
            GenerationEntries(),
            initiator,
            generation,
            useFloor: false,
            batchLeaf: null).ConfigureAwait(true);
        RecordFenceClaim(LatticeMetrics.AppendFenceKindGeneration, result.Accepted);
        return result;
    }

    /// <inheritdoc />
    public async Task<long> RetireAppendEpochAsync(Guid leafId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInternalOrigin(LatticeOperation.Write);
        EnsureInitialized();

        var storage = FenceStorage();
        await EnsureEpochFloorLoadedAsync(storage).ConfigureAwait(true);

        // Serialised with this leaf's claims (#5635): a claim that read the entry
        // absent and is about to create it finishes first, so this retirement reads
        // what it wrote and counts it in the floor.
        await EnterLeafGateAsync(leafId).ConfigureAwait(true);
        try
        {
            return await RetireUnderLeafGateAsync(storage, leafId).ConfigureAwait(true);
        }
        finally
        {
            ExitLeafGate(leafId);
        }
    }

    private async Task<long> RetireUnderLeafGateAsync(IGrainStorage storage, Guid leafId)
    {
        var key = WalAppendFenceRows.EntryKey(_treeId, _shardIndex, leafId);
        for (var attempt = 1; ; attempt++)
        {
            var row = await WalAppendFenceRows.ReadEntryAsync(storage, _treeId, _shardIndex, leafId).ConfigureAwait(true);
            long retired;
            lock (_stateGate)
            {
                retired = row.State.Epoch;
                if (_epochEntries is not null && _epochEntries.TryGetValue(leafId, out var loaded))
                {
                    retired = Math.Max(retired, Math.Max(loaded.Memory, loaded.DurableSeen));
                }
            }

            // The floor is raised durably BEFORE the entry is deleted, so no
            // interleaving can observe the entry gone with the floor still below
            // it: a surviving duplicate of the retired leaf stays outranked
            // (ReadPositionHonestRecreateDropsFloor).
            if (retired > 0)
            {
                await RaiseEpochFloorAsync(storage, retired).ConfigureAwait(true);
            }

            if (row.RecordExists)
            {
                try
                {
                    await WalAppendFenceRows.ClearAsync(storage, WalAppendFenceRows.EntryGrainType, key, row).ConfigureAwait(true);
                }
                catch (InconsistentStateException) when (attempt < MaxFenceRowAttempts)
                {
                    // A claim wrote the entry since the read; retire what it wrote.
                    continue;
                }
            }

            // A multi-entry stamped batch of this leaf finishes assigning before its
            // entry goes, so retirement never leaves it partially admitted.
            while (true)
            {
                Task? drain;
                lock (_stateGate)
                {
                    drain = BatchDrainLocked(leafId);
                    if (drain is null)
                    {
                        // RED-PROOF: loaded entry left in place
                        return _epochFloor;
                    }
                }

                await drain.ConfigureAwait(true);
            }
        }
    }

    /// <summary>
    /// The append fence's admission check for <paramref name="stamp"/>, run under
    /// <see cref="_stateGate"/> in the step that would assign the offset.
    /// </summary>
    private WalAppendFenceOutcome AdmitStampLocked(in WalAppendStamp stamp)
    {
        if (_epochEntries is null
            || !_epochEntries.TryGetValue(stamp.LeafId, out var entry)
            || entry.Memory == WalAppendFenceCore.Unloaded)
        {
            return WalAppendFenceOutcome.UnloadedEntry;
        }

        if (!WalAppendFenceCore.IsEpochAdmitted(stamp.Epoch, entry.Memory) || stamp.Epoch <= _epochFloor) // RED-PROOF: #5635
        {
            return WalAppendFenceOutcome.StaleEpoch;
        }

        if (stamp.Initiator.IsSet)
        {
            if (_generationEntries is null
                || !_generationEntries.TryGetValue(stamp.Initiator, out var generation)
                || generation.Memory == WalAppendFenceCore.Unloaded)
            {
                return WalAppendFenceOutcome.UnloadedGeneration;
            }

            if (!WalAppendFenceCore.IsGenerationAdmitted(stamp.Generation, generation.Memory))
            {
                return WalAppendFenceOutcome.StaleGeneration;
            }
        }

        return WalAppendFenceOutcome.Admitted;
    }

    /// <summary>
    /// The three-step claim's second and third steps, shared by the epoch and
    /// routing-generation tables (step 1, the leaf's own epoch row, is the
    /// claimant's). The durable row is written as a conditional max-merge, and an
    /// ETag conflict re-reads and re-decides; only then is the in-memory entry
    /// raised, under the offset lock, so a restart can never lose a raise whose
    /// head was already returned.
    /// </summary>
    private async Task<WalAppendFenceClaimResult> ClaimCoreAsync<TId, TRow>(
        IGrainStorage storage,
        string grainType,
        string key,
        Func<Task<GrainState<TRow>>> read,
        Func<GrainState<TRow>, long> getValue,
        Action<GrainState<TRow>, long> setValue,
        Dictionary<TId, FenceEntry> table,
        TId id,
        long value,
        bool useFloor,
        Guid? batchLeaf)
        where TId : notnull
    {
        for (var attempt = 1; ; attempt++)
        {
            var row = await read().ConfigureAwait(true);
            var durable = 0L; // RED-PROOF: durable table not carried
            WalAppendFenceCore.ClaimWriteDecision decision;
            long threshold;
            lock (_stateGate)
            {
                var entry = NoteDurableLocked(table, id, durable);
                var floor = useFloor ? _epochFloor : 0;
                decision = WalAppendFenceCore.DecideClaimWrite(value, entry.Memory, durable, floor);
                threshold = WalAppendFenceCore.ClaimThreshold(entry.Memory, Math.Max(durable, entry.DurableSeen), floor);
            }

            if (decision == WalAppendFenceCore.ClaimWriteDecision.Refuse)
            {
                return WalAppendFenceClaimResult.Refuse(threshold);
            }

            if (decision == WalAppendFenceCore.ClaimWriteDecision.Write)
            {
                setValue(row, value);
                try
                {
                    await WalAppendFenceRows.WriteAsync(storage, grainType, key, row).ConfigureAwait(true);
                }
                catch (InconsistentStateException)
                {
                    if (attempt < MaxFenceRowAttempts)
                    {
                        continue;
                    }

                    // Persistently losing the row to other claims of the same
                    // claimant id: refuse rather than surface a storage fault. The
                    // claimant re-persists its own row, which fails if it is stale.
                    return WalAppendFenceClaimResult.Refuse(value);
                }
            }

            // The raise waits out any multi-entry stamped batch of this leaf that is
            // still assigning offsets, so it never falls between two of its entries
            // and leaves the batch partially admitted.
            while (true)
            {
                Task? drain = null;
                lock (_stateGate)
                {
                    if (batchLeaf is { } leaf)
                    {
                        drain = BatchDrainLocked(leaf);
                    }

                    if (drain is null)
                    {
                        var entry = NoteDurableLocked(table, id, value);
                        var floor = useFloor ? _epochFloor : 0;
                        if (!WalAppendFenceCore.IsRaiseAdmitted(value, entry.Memory, entry.DurableSeen, floor))
                        {
                            return WalAppendFenceClaimResult.Refuse(
                                WalAppendFenceCore.ClaimThreshold(entry.Memory, entry.DurableSeen, floor));
                        }

                        entry.Memory = value;
                        table[id] = entry;
                        return WalAppendFenceClaimResult.Accept(_nextOffset);
                    }
                }

                await drain.ConfigureAwait(true);
            }
        }
    }

    /// <summary>
    /// Marks a multi-entry stamped batch of <paramref name="leafId"/> as assigning
    /// offsets. Called under <see cref="_stateGate"/> in the step that admitted its
    /// first entry; no allocation.
    /// </summary>
    private void RegisterStampedBatchLocked(Guid leafId)
    {
        var entry = _epochEntries![leafId];
        entry.InFlightBatches++;
        _epochEntries[leafId] = entry;
    }

    /// <summary>
    /// Ends a multi-entry stamped batch of <paramref name="leafId"/> once it has
    /// assigned or rejected every entry, releasing a raise or retirement waiting
    /// for it.
    /// </summary>
    private void ReleaseStampedBatch(Guid leafId)
    {
        TaskCompletionSource? drained = null;
        lock (_stateGate)
        {
            if (_epochEntries is not null && _epochEntries.TryGetValue(leafId, out var entry))
            {
                entry.InFlightBatches--;
                _epochEntries[leafId] = entry;
                if (entry.InFlightBatches > 0)
                {
                    return;
                }
            }

            _batchDrains?.Remove(leafId, out drained);
        }

        drained?.TrySetResult();
    }

    /// <summary>
    /// A task that completes when <paramref name="leafId"/> has no multi-entry
    /// stamped batch assigning offsets, or <see langword="null"/> when it has none
    /// now. Called under <see cref="_stateGate"/>.
    /// </summary>
    private Task? BatchDrainLocked(Guid leafId)
    {
        if (_epochEntries is null
            || !_epochEntries.TryGetValue(leafId, out var entry)
            || entry.InFlightBatches <= 0)
        {
            return null;
        }

        _batchDrains ??= new Dictionary<Guid, TaskCompletionSource>();
        if (!_batchDrains.TryGetValue(leafId, out var drained))
        {
            drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _batchDrains[leafId] = drained;
        }

        return drained.Task;
    }

    /// <summary>
    /// Serialises this activation's claims and retirement of one leaf (#5635).
    /// Different leaves claim concurrently.
    /// </summary>
    private async Task EnterLeafGateAsync(Guid leafId)
    {
        SemaphoreSlim gate;
        lock (_stateGate)
        {
            _leafGates ??= new Dictionary<Guid, LeafGate>();
            if (!_leafGates.TryGetValue(leafId, out var leafGate))
            {
                leafGate = new LeafGate();
                _leafGates[leafId] = leafGate;
            }

            leafGate.Users++;
            gate = leafGate.Gate;
        }

        await gate.WaitAsync().ConfigureAwait(true);
    }

    private void ExitLeafGate(Guid leafId)
    {
        lock (_stateGate)
        {
            var leafGate = _leafGates![leafId];
            leafGate.Gate.Release();
            if (--leafGate.Users == 0)
            {
                _leafGates.Remove(leafId);
            }
        }
    }

    /// <summary>One leaf's claim/retirement gate and the number of operations holding or awaiting it.</summary>
    private sealed class LeafGate
    {
        public SemaphoreSlim Gate { get; } = new(1, 1);

        public int Users { get; set; }
    }

    /// <summary>
    /// Records that <paramref name="durable"/> has been read from or written to the
    /// durable row of <paramref name="id"/>, creating an unloaded entry when there
    /// is none, and returns the entry.
    /// </summary>
    private static FenceEntry NoteDurableLocked<TId>(Dictionary<TId, FenceEntry> table, TId id, long durable)
        where TId : notnull
    {
        if (!table.TryGetValue(id, out var entry))
        {
            entry = new FenceEntry(WalAppendFenceCore.Unloaded, 0);
        }

        if (durable > entry.DurableSeen)
        {
            entry.DurableSeen = durable;
        }

        table[id] = entry;
        return entry;
    }

    private Dictionary<Guid, FenceEntry> EpochEntries()
    {
        lock (_stateGate)
        {
            return _epochEntries ??= new Dictionary<Guid, FenceEntry>();
        }
    }

    private Dictionary<WalRoutingInitiator, FenceEntry> GenerationEntries()
    {
        lock (_stateGate)
        {
            return _generationEntries ??= new Dictionary<WalRoutingInitiator, FenceEntry>();
        }
    }

    private IGrainStorage FenceStorage() =>
        _fenceStorage ??= WalAppendFenceStorage.Resolve(context.ActivationServices)
            ?? throw new InvalidOperationException(
                $"WAL shard {_treeId}/{_shardIndex} cannot serve the append fence: no grain storage provider is "
                + $"registered under '{LatticeOptions.StorageProviderName}'.");

    private async Task EnsureEpochFloorLoadedAsync(IGrainStorage storage)
    {
        lock (_stateGate)
        {
            if (_epochFloorLoaded)
            {
                return;
            }
        }

        var row = await WalAppendFenceRows.ReadFloorAsync(storage, _treeId, _shardIndex).ConfigureAwait(true);
        lock (_stateGate)
        {
            _epochFloor = Math.Max(_epochFloor, row.State.Floor);
            _epochFloorLoaded = true;
        }
    }

    /// <summary>
    /// Raises the durable retired floor to at least <paramref name="atLeast"/> as a
    /// conditional max-merge, then the in-memory floor. The floor never moves down.
    /// </summary>
    private async Task RaiseEpochFloorAsync(IGrainStorage storage, long atLeast)
    {
        var key = WalAppendFenceRows.FloorKey(_treeId, _shardIndex);
        for (var attempt = 1; ; attempt++)
        {
            var row = await WalAppendFenceRows.ReadFloorAsync(storage, _treeId, _shardIndex).ConfigureAwait(true);
            if (row.State.Floor < atLeast)
            {
                row.State.Floor = atLeast;
                try
                {
                    await WalAppendFenceRows.WriteAsync(storage, WalAppendFenceRows.FloorGrainType, key, row).ConfigureAwait(true);
                }
                catch (InconsistentStateException) when (attempt < MaxFenceRowAttempts)
                {
                    continue;
                }
            }

            lock (_stateGate)
            {
                _epochFloor = Math.Max(_epochFloor, row.State.Floor);
                _epochFloorLoaded = true;
            }

            return;
        }
    }

    private void RecordFenceClaim(string kind, bool accepted) =>
        LatticeMetrics.WalAppendFenceClaims.Add(
            1,
            _treeTag,
            _tenantTag,
            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, kind),
            accepted ? LatticeMetrics.AppendFenceClaimAccepted : LatticeMetrics.AppendFenceClaimRefused);

    private void RecordFenceRejection(WalAppendFenceOutcome outcome) =>
        LatticeMetrics.WalAppendFenceRejections.Add(
            1,
            _treeTag,
            _tenantTag,
            LatticeMetrics.AppendFenceRejectionReason(outcome));

    /// <summary>
    /// One entry of a fence table: the in-memory value appends are checked against
    /// (<see cref="WalAppendFenceCore.Unloaded"/> until a claim raises it), and the
    /// highest durable value this activation has read or written for it.
    /// </summary>
    private record struct FenceEntry(long Memory, long DurableSeen)
    {
        /// <summary>Multi-entry stamped batches of this leaf still assigning offsets.</summary>
        public int InFlightBatches { get; set; }
    }

    /// <summary>Shapes the result of the single-entry append for its caller.</summary>
    private interface ISingleAppendShape<TResult>
    {
        static abstract bool Stamped { get; }

        static abstract TResult Admitted(long offset, long unstampedWatermark);

        static abstract TResult Rejected(WalAppendFenceOutcome outcome, long unstampedWatermark);
    }

    /// <summary>Shapes the result of the batched append for its caller.</summary>
    private interface IBatchAppendShape<TResult>
    {
        static abstract bool Stamped { get; }

        static abstract TResult Empty();

        static abstract TResult Complete(long[] offsets, int admittedCount, long unstampedWatermark, WalAppendFenceOutcome outcome);
    }

    private readonly struct UnstampedSingleShape : ISingleAppendShape<long>
    {
        public static bool Stamped => false;

        public static long Admitted(long offset, long unstampedWatermark) => offset;

        public static long Rejected(WalAppendFenceOutcome outcome, long unstampedWatermark) =>
            throw new InvalidOperationException("An unstamped append is never rejected by the append fence.");
    }

    private readonly struct StampedSingleShape : ISingleAppendShape<WalStampedAppendResult>
    {
        public static bool Stamped => true;

        public static WalStampedAppendResult Admitted(long offset, long unstampedWatermark) =>
            new(offset, unstampedWatermark, WalAppendFenceOutcome.Admitted);

        public static WalStampedAppendResult Rejected(WalAppendFenceOutcome outcome, long unstampedWatermark) =>
            new(WalStampedBatchResult.Rejected, unstampedWatermark, outcome);
    }

    private readonly struct UnstampedBatchShape : IBatchAppendShape<IReadOnlyList<long>>
    {
        public static bool Stamped => false;

        public static IReadOnlyList<long> Empty() => Array.Empty<long>();

        public static IReadOnlyList<long> Complete(long[] offsets, int admittedCount, long unstampedWatermark, WalAppendFenceOutcome outcome) =>
            offsets;
    }

    private readonly struct StampedBatchShape : IBatchAppendShape<WalStampedBatchResult>
    {
        public static bool Stamped => true;

        public static WalStampedBatchResult Empty() =>
            new(Array.Empty<long>(), 0, -1, WalAppendFenceOutcome.Admitted);

        public static WalStampedBatchResult Complete(long[] offsets, int admittedCount, long unstampedWatermark, WalAppendFenceOutcome outcome) =>
            new(offsets, admittedCount, unstampedWatermark, outcome);
    }
}
