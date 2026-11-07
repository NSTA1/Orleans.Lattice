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
internal sealed partial class WalShardGrain : IWalAppendFenceCapable, IWalAppendFencePreArmCapable
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

    /// <summary>
    /// The tree id's routing floors, one per initiator (#5668), as far as this
    /// activation has read or raised them: a generation claim at or below its
    /// initiator's floor is refused. Load-bearing: it is the one seam every routing
    /// generation passes before a stamped append under it is admitted.
    /// </summary>
    /// <remarks>Keyed by initiator, not by its stored tag, so a claim's lookup allocates nothing.</remarks>
    private Dictionary<WalRoutingInitiator, long>? _generationFloors;

    /// <summary>Set once the durable routing floor row has been read into <see cref="_generationFloors"/>.</summary>
    private bool _generationFloorLoaded;

    /// <summary>The in-flight read of the retired-epoch floor row, shared by concurrent first loads (#5987).</summary>
    private Task? _epochFloorLoad;

    /// <summary>The in-flight read of the routing floor row, shared by concurrent first loads (#5987).</summary>
    private Task? _generationFloorLoad;

    /// <summary>
    /// This activation's read of the partition's routing-generation table (#5987), shared
    /// by every initiator's cold load: a new tree pays one read per partition, not one per
    /// partition and initiator. A cold load takes the slot it holds, max-merged with every
    /// value this activation has seen durable for the initiator since
    /// (<see cref="FenceEntry.DurableSeen"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Two writers can raise a slot after the read: this grain's own claims, which record
    /// the value in <see cref="FenceEntry.DurableSeen"/>, and a duplicate activation of
    /// the partition, whose raise this activation does not see (#5988 review, B1). A
    /// cached slot can therefore be stale-low, which is no weaker than the per-initiator
    /// rows' read at load time and is covered by the model's lost-update run (a slot
    /// dropped to 0 at any time). It is still narrowed: every claim's fresh read of the
    /// row is max-merged into the cache, an ETag conflict drops it, and it is re-read once
    /// older than <see cref="GenerationRowCacheMaxAge"/>, so a first-touch burst shares
    /// one read while a stale value lives a bounded time.
    /// </para>
    /// <para>
    /// The retirement's storage fallback is the one writer outside every activation. It
    /// runs only when the partition is hosted by a build without the retirement call, and
    /// it never raises a slot: it raises the floors, then deletes the row, so it can only
    /// leave the cache stale-high.
    /// </para>
    /// </remarks>
    private Task<IReadOnlyDictionary<string, long>>? _generationRowLoad;

    /// <summary>When <see cref="_generationRowLoad"/> was filled, in <see cref="GenerationRowCacheClock"/> UTC ticks.</summary>
    private long _generationRowLoadedUtcTicks;

    /// <summary>How long a cached routing-generation table serves cold loads before they re-read it (#5988 review, B1).</summary>
    internal static readonly TimeSpan GenerationRowCacheMaxAge = TimeSpan.FromSeconds(5);

    /// <summary>The clock <see cref="GenerationRowCacheMaxAge"/> is measured on; replaced only by tests.</summary>
    internal TimeProvider GenerationRowCacheClock { get; set; } = TimeProvider.System;

    /// <summary>
    /// Serialises this activation's conditional read-modify-writes of the routing-generation
    /// table (#5987): claims of different initiators write the same row, and each holds only
    /// its own initiator's gate.
    /// </summary>
    private readonly SemaphoreSlim _generationRowGate = new(1, 1);

    /// <summary>Waiters for a leaf's multi-entry stamped batches to finish assigning; created on demand.</summary>
    private Dictionary<Guid, TaskCompletionSource>? _batchDrains;

    /// <summary>
    /// Waiters for an initiator's multi-entry generation-stamped batches to finish
    /// assigning (member 2, Q4); created on demand.
    /// </summary>
    private Dictionary<WalRoutingInitiator, TaskCompletionSource>? _generationBatchDrains;

    /// <summary>Per-leaf claim/retirement gates; created on the first claim.</summary>
    private Dictionary<Guid, LeafGate>? _leafGates;

    /// <summary>Per-initiator routing-generation claim gates (#5831, F1); created on the first claim.</summary>
    private Dictionary<WalRoutingInitiator, SemaphoreSlim>? _generationGates;
    private SemaphoreSlim?[]? _directoryGates;

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

    /// <summary><paramref name="initiator"/>'s routing floor as this activation holds it; test seam.</summary>
    internal long GenerationFloorForTesting(WalRoutingInitiator initiator)
    {
        lock (_stateGate)
        {
            return GenerationFloorLocked(initiator);
        }
    }

    /// <summary><paramref name="initiator"/>'s routing floor as this activation holds it; called under <see cref="_stateGate"/>. No allocation.</summary>
    private long GenerationFloorLocked(WalRoutingInitiator initiator) =>
        _generationFloors is not null && _generationFloors.TryGetValue(initiator, out var floor) ? floor : 0;

    /// <summary>Merges <paramref name="stored"/> into the floors this activation holds; called under <see cref="_stateGate"/>.</summary>
    private void MergeGenerationFloorsLocked(TreeRoutingFloorState stored)
    {
        foreach (var (tag, value) in stored.Floors)
        {
            if (!WalAppendFenceRows.TryParseInitiatorTag(tag, out var initiator))
            {
                continue;
            }

            _generationFloors ??= new Dictionary<WalRoutingInitiator, long>();
            if (!_generationFloors.TryGetValue(initiator, out var current) || current < value)
            {
                _generationFloors[initiator] = value;
            }
        }

        _generationFloorLoaded = true;
    }

    /// <summary>Whether a routing-generation entry for <paramref name="initiator"/> is loaded; test seam.</summary>
    internal bool HasGenerationEntryForTesting(WalRoutingInitiator initiator)
    {
        lock (_stateGate)
        {
            return _generationEntries is not null && _generationEntries.ContainsKey(initiator);
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
        await EnsureFloorsLoadedAsync(storage).ConfigureAwait(true);
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
                epochTable: true,
                batchLeaf: leafId,
                batchInitiator: null).ConfigureAwait(true);
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
        if (!initiator.IsSet || initiator.Kind == WalRoutingInitiatorKind.CoordinatorTerminal)
        {
            throw new ArgumentException("A routing generation is claimed for a named initiator.", nameof(initiator));
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(generation);

        var result = await ClaimRoutingGenerationGatedAsync(initiator, generation, cancellationToken).ConfigureAwait(true);
        RecordFenceClaim(LatticeMetrics.AppendFenceKindGeneration, result.Accepted);
        return result;
    }

    /// <summary>
    /// A routing-generation claim, one at a time per initiator (#5831, F1). Every
    /// concurrent write of a fresh tree that meets its shard root's generation unloaded
    /// presents the same claim; serialised, the first reads and creates the row and each
    /// later one is decided from what it loaded, so none races another to create it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The gate is waiter-driven: each claim takes it in turn and decides for itself, so
    /// no claim shares another's result, fault or cancellation. A waiter waits with its
    /// own token and the gate is released in <c>finally</c>. The gate wait and each storage
    /// call are bounded by <see cref="LatticeOptions.WalFlushTimeout"/>, so a hung storage
    /// call delays the claims queued behind it by at most that long; the raise's drain of
    /// in-flight batches is not, as each batch bounds itself. A storage call that times
    /// out fails its claim without raising anything, and may still land; the next claim's
    /// read sees it, and its ETag condition orders it.
    /// </para>
    /// <para>
    /// No circular wait: a claim's raise waits for the initiator's multi-entry stamped
    /// batches while it holds the gate, so no registered batch may ever wait on it. The
    /// stamped append's own load (F2) claims before its batch registers, and never holds
    /// <see cref="_stateGate"/> across the claim; see <see cref="RegisterStampedBatchLocked"/>.
    /// </para>
    /// </remarks>
    private async Task<WalAppendFenceClaimResult> ClaimRoutingGenerationGatedAsync(
        WalRoutingInitiator initiator,
        long generation,
        CancellationToken cancellationToken)
    {
        var storage = FenceStorage();
        await EnsureFloorsLoadedAsync(storage).ConfigureAwait(true);
        var key = WalAppendFenceRows.GenerationsKey(_treeId, _shardIndex);
        var gate = GenerationGate(initiator);
        var deadline = Options.WalFlushTimeout;
        TimeSpan? bound = deadline > TimeSpan.Zero && deadline != Timeout.InfiniteTimeSpan ? deadline : null;
        if (!await gate.WaitAsync(bound ?? Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(true))
        {
            throw new TimeoutException(
                $"WAL shard {_treeId}/{_shardIndex}: a routing-generation claim for {WalAppendFenceRows.InitiatorTag(initiator)} waited "
                + $"{deadline} for the claim ahead of it.");
        }

        try
        {
            // Only the gate wait and the storage I/O are bounded (#5831 review, S1): a
            // raise's drain of the initiator's in-flight batches is bounded per batch
            // already, and a close's re-claim waiting on a slow batch must finish late,
            // never fail.
            return await ClaimCoreAsync(
                storage,
                WalAppendFenceRows.GenerationGrainType,
                key,
                async () =>
                {
                    var row = await Bound(WalAppendFenceRows.ReadGenerationsAsync(storage, _treeId, _shardIndex), bound).ConfigureAwait(true);
                    lock (_stateGate)
                    {
                        RefreshGenerationRowLocked(row.State);
                    }

                    return row;
                },
                row => WalAppendFenceRows.SlotOf(row.State, initiator),
                (row, value) => WalAppendFenceRows.RaiseSlot(row.State, initiator, value),
                GenerationEntries(),
                initiator,
                generation,
                epochTable: false,
                batchLeaf: null,
                batchInitiator: initiator,
                ioBound: bound,
                rowGate: _generationRowGate,
                onConflict: DropGenerationRowCache).ConfigureAwait(true);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Drops the cached table after an ETag conflict on it: another writer moved the row (#5988 review, B1).</summary>
    private void DropGenerationRowCache()
    {
        lock (_stateGate)
        {
            _generationRowLoad = null;
        }
    }

    private static Task<T> Bound<T>(Task<T> io, TimeSpan? bound) => bound is { } b ? io.WaitAsync(b) : io;

    private static Task Bound(Task io, TimeSpan? bound) => bound is { } b ? io.WaitAsync(b) : io;

    /// <summary>The claim gate of <paramref name="initiator"/>; at most one per shard-level initiator of the tree.</summary>
    private SemaphoreSlim GenerationGate(WalRoutingInitiator initiator)
    {
        lock (_stateGate)
        {
            _generationGates ??= new Dictionary<WalRoutingInitiator, SemaphoreSlim>();
            if (!_generationGates.TryGetValue(initiator, out var gate))
            {
                gate = new SemaphoreSlim(1, 1);
                _generationGates[initiator] = gate;
            }

            return gate;
        }
    }

    /// <summary>
    /// The F2 load (#5831): makes <paramref name="stamp"/>'s initiator loaded for a stamped
    /// append that met it unloaded. Returns <see langword="true"/> when the caller should
    /// run admission again, or <see langword="null"/> when the load could not be made, in
    /// which case the append reports the generation unloaded as before and the writer
    /// re-presents it. The caller re-runs admission afterwards and never admits on this
    /// result: a close may re-claim a higher generation in between. A bulk seed never
    /// reaches it (#5812 review C2).
    /// </summary>
    /// <remarks>
    /// #5833 (revision 2, C1): the load is read-only (<see cref="LoadRoutingGenerationAsync"/>).
    /// It never raises the entry to the presented generation, so a write carrying a
    /// generation a close raised but has not re-claimed here does not fence the
    /// initiator's in-flight writes at an arbitrary moment; only an arm and the close
    /// paths re-claim.
    /// </remarks>
    private async Task<bool?> LoadStampGenerationAsync(WalAppendStamp stamp, CancellationToken cancellationToken)
    {
        try
        {
            await LoadRoutingGenerationAsync(stamp.Initiator, cancellationToken).ConfigureAwait(true);
            if (AfterStampGenerationLoadedForTesting is { } hook)
            {
                await hook().ConfigureAwait(true);
            }

            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// The read-only generation load (#5833, revision 2, C1 and C7): reads
    /// <paramref name="initiator"/>'s durable row and loads <c>max(row, floor)</c>, marked floor-only when the row is not above the floor, into
    /// its entry, max-merged so it never lowers a value a claim raised. It writes nothing.
    /// It runs under the initiator's claim gate, so it is ordered with every claim of that
    /// initiator and never interleaves with a re-claim's read and raise. A purged
    /// incarnation's generation, at or below the floor, stays refused, as a claim at the
    /// floor is.
    /// </summary>
    private async Task LoadRoutingGenerationAsync(WalRoutingInitiator initiator, CancellationToken cancellationToken)
    {
        var storage = FenceStorage();
        await EnsureFloorsLoadedAsync(storage).ConfigureAwait(true);
        var gate = GenerationGate(initiator);
        var deadline = Options.WalFlushTimeout;
        TimeSpan? bound = deadline > TimeSpan.Zero && deadline != Timeout.InfiniteTimeSpan ? deadline : null;
        if (!await gate.WaitAsync(bound ?? Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(true))
        {
            throw new TimeoutException(
                $"WAL shard {_treeId}/{_shardIndex}: a routing-generation load for {WalAppendFenceRows.InitiatorTag(initiator)} waited "
                + $"{deadline} for the claim ahead of it.");
        }

        try
        {
            if (IsLoaded(GenerationEntries(), initiator))
            {
                return;
            }

            var slot = await GenerationSlotAsync(storage, initiator, bound).ConfigureAwait(true);
            lock (_stateGate)
            {
                var table = GenerationEntries();

                // The row as a fresh read would return it (#5987): the activation's one read
                // of the table, max-merged with every value seen durable since.
                var entry = NoteDurableLocked(table, initiator, slot);
                var durable = entry.DurableSeen;
                var floor = GenerationFloorLocked(initiator);
                var loaded = WalAppendFenceCore.LoadedGeneration(durable, floor);
                if (entry.Memory == WalAppendFenceCore.Unloaded)
                {
                    // The floor is read here, when the load lands, not when the row was
                    // read: a purge that retired the entry in between raised it (R). Only a
                    // load from unloaded marks the entry floor-only.
                    entry.Memory = loaded;
                    entry.FloorOnly = durable <= floor;
                    table[initiator] = entry;
                }
                else if (entry.Memory < loaded)
                {
                    // max(existing, row, floor), never newly floor-only over a value a
                    // concurrent claim installed.
                    entry.Memory = loaded;
                    entry.FloorOnly &= durable <= floor;
                    table[initiator] = entry;
                }
            }
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Invoked after the F2 load accepts and before admission re-runs; test seam (#5831).</summary>
    internal Func<Task>? AfterStampGenerationLoadedForTesting { get; set; }

    /// <inheritdoc />
    public async Task<long> RetireAppendEpochAsync(Guid leafId, long rowEpoch, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInternalOrigin(LatticeOperation.Write);
        EnsureInitialized();

        var storage = FenceStorage();
        if (rowEpoch == WalAppendFenceRows.RowEpochUnread)
        {
            // The WAL GC's orphan-pin retirement (#5605 review, S4): one row read,
            // and nothing more for a leaf that never claimed or was fully retired.
            // A leaf writes its epoch row before it claims anywhere, and its clear
            // deletes the row only after every partition retired its entry.
            var row = await WalAppendFenceRows.ReadLeafEpochAsync(storage, leafId).ConfigureAwait(true);
            if (!row.RecordExists && !HasLoadedEpochEntry(leafId))
            {
                return 0;
            }

            rowEpoch = row.State.Epoch;
        }

        await EnsureEpochFloorLoadedAsync(storage).ConfigureAwait(true);

        // Serialised with this leaf's claims (#5635): a claim that read the entry
        // absent and is about to create it finishes first, so this retirement reads
        // what it wrote and counts it in the floor.
        await EnterLeafGateAsync(leafId).ConfigureAwait(true);
        try
        {
            return await RetireUnderLeafGateAsync(storage, leafId, rowEpoch).ConfigureAwait(true);
        }
        finally
        {
            ExitLeafGate(leafId);
        }
    }

    /// <inheritdoc />
    public async Task<long> RetireRoutingGenerationsAsync(int shardCount, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInternalOrigin(LatticeOperation.Write);
        EnsureInitialized();
        ArgumentOutOfRangeException.ThrowIfNegative(shardCount);

        var storage = FenceStorage();
        var initiators = new HashSet<WalRoutingInitiator>(TreeRoutingFenceRetirement.Initiators(shardCount));
        var loaded = new Dictionary<WalRoutingInitiator, long>();
        lock (_stateGate)
        {
            if (_generationEntries is not null)
            {
                foreach (var (initiator, entry) in _generationEntries)
                {
                    initiators.Add(initiator);
                    loaded[initiator] = Math.Max(entry.Memory, entry.DurableSeen);
                }
            }
        }

        // The floors are raised in memory, and the loaded entries dropped, before the row
        // is deleted: from then on a claim at or below its initiator's floor is refused at
        // its row write and at its raise, and a stamped append meets an unloaded entry and
        // re-presents a claim that is refused. The table's own slots are covered too.
        var stored = await TreeRoutingFenceRetirement.RetireGenerationsRowAsync(
            storage,
            _treeId,
            _shardIndex,
            loaded,
            async (floors, covered) =>
            {
                lock (_stateGate)
                {
                    MergeGenerationFloorsLocked(floors);
                    _generationRowLoad = null;
                }

                initiators.UnionWith(covered);
                foreach (var initiator in initiators)
                {
                    await RemoveLoadedGenerationAsync(initiator).ConfigureAwait(true);
                }
            }).ConfigureAwait(true);
        return stored.Floors.Values.DefaultIfEmpty(0).Max();
    }

    /// <summary>
    /// Removes <paramref name="initiator"/>'s loaded generation entry once any
    /// multi-entry batch stamped with its generation has finished assigning.
    /// </summary>
    private async Task RemoveLoadedGenerationAsync(WalRoutingInitiator initiator)
    {
        while (true)
        {
            Task? drain;
            lock (_stateGate)
            {
                drain = GenerationBatchDrainLocked(initiator);
                if (drain is null)
                {
                    _generationEntries?.Remove(initiator);
                    return;
                }
            }

            await drain.ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Loads the partition's retired-epoch floor and its tree's routing floors, in
    /// parallel when both are missing (#5831, F3): both are floors, raised before any row
    /// they cover is deleted, so neither read's order matters to the other, and both
    /// complete before any entry or generation row is read.
    /// </summary>
    private Task EnsureFloorsLoadedAsync(IGrainStorage storage)
    {
        bool epoch;
        bool generation;
        lock (_stateGate)
        {
            epoch = !_epochFloorLoaded;
            generation = !_generationFloorLoaded;
        }

        return (epoch, generation) switch
        {
            (true, true) => Task.WhenAll(EnsureEpochFloorLoadedAsync(storage), EnsureGenerationFloorLoadedAsync(storage)),
            (true, false) => EnsureEpochFloorLoadedAsync(storage),
            (false, true) => EnsureGenerationFloorLoadedAsync(storage),
            _ => Task.CompletedTask,
        };
    }

    /// <summary>
    /// Loads the routing floor row once (#5987): concurrent first loads share one read, and
    /// a load that failed is retried by the next caller.
    /// </summary>
    private Task EnsureGenerationFloorLoadedAsync(IGrainStorage storage)
    {
        lock (_stateGate)
        {
            if (_generationFloorLoaded)
            {
                return Task.CompletedTask;
            }

            if (_generationFloorLoad is { IsFaulted: false, IsCanceled: false } inFlight)
            {
                return inFlight;
            }
        }

        var load = LoadGenerationFloorCoreAsync(storage);
        lock (_stateGate)
        {
            if (_generationFloorLoad is not { IsFaulted: false, IsCanceled: false })
            {
                _generationFloorLoad = load;
            }

            return _generationFloorLoad;
        }
    }

    private async Task LoadGenerationFloorCoreAsync(IGrainStorage storage)
    {
        var row = await WalAppendFenceRows.ReadRoutingFloorAsync(storage, _treeId).ConfigureAwait(true);
        lock (_stateGate)
        {
            MergeGenerationFloorsLocked(row.State);
        }
    }

    /// <summary>
    /// <paramref name="initiator"/>'s slot in the partition's routing-generation table, from
    /// this activation's one read of it (#5987): concurrent cold loads of every initiator
    /// share the read, and a read that failed is retried by the next caller.
    /// </summary>
    private async Task<long> GenerationSlotAsync(IGrainStorage storage, WalRoutingInitiator initiator, TimeSpan? bound)
    {
        Task<IReadOnlyDictionary<string, long>>? load;
        lock (_stateGate)
        {
            load = CachedGenerationRowLocked();
        }

        if (load is null)
        {
            var created = ReadGenerationRowAsync(storage);
            lock (_stateGate)
            {
                if (CachedGenerationRowLocked() is null)
                {
                    _generationRowLoad = created;
                    _generationRowLoadedUtcTicks = GenerationRowCacheClock.GetUtcNow().UtcTicks;
                }

                load = _generationRowLoad!;
            }
        }

        var slots = await Bound(load, bound).ConfigureAwait(true);
        return slots.TryGetValue(WalAppendFenceRows.InitiatorTag(initiator), out var slot) ? slot : 0;
    }

    /// <summary>The cached table, when it is neither faulted nor older than <see cref="GenerationRowCacheMaxAge"/>.</summary>
    private Task<IReadOnlyDictionary<string, long>>? CachedGenerationRowLocked() =>
        _generationRowLoad is { IsFaulted: false, IsCanceled: false } cached
        && GenerationRowCacheClock.GetUtcNow().UtcTicks - _generationRowLoadedUtcTicks <= GenerationRowCacheMaxAge.Ticks
            ? cached
            : null;

    /// <summary>
    /// Max-merges a claim's fresh read of the table into the cache and restarts its age
    /// (#5988 review, B1), so a slot a duplicate activation raised reaches this
    /// activation's next cold load. A value cached above the read (a row the fallback
    /// deleted) is kept: stale-high only refuses more.
    /// </summary>
    private void RefreshGenerationRowLocked(WalRoutingGenerationsState fresh)
    {
        var merged = new Dictionary<string, long>(fresh.Generations, StringComparer.Ordinal);
        if (_generationRowLoad is { IsCompletedSuccessfully: true } cached)
        {
            foreach (var (tag, slot) in cached.Result)
            {
                if (!merged.TryGetValue(tag, out var current) || current < slot)
                {
                    merged[tag] = slot;
                }
            }
        }

        _generationRowLoad = Task.FromResult<IReadOnlyDictionary<string, long>>(merged);
        _generationRowLoadedUtcTicks = GenerationRowCacheClock.GetUtcNow().UtcTicks;
    }

    private async Task<IReadOnlyDictionary<string, long>> ReadGenerationRowAsync(IGrainStorage storage)
    {
        var row = await WalAppendFenceRows.ReadGenerationsAsync(storage, _treeId, _shardIndex).ConfigureAwait(true);
        return new Dictionary<string, long>(row.State.Generations, StringComparer.Ordinal);
    }

    private bool HasLoadedEpochEntry(Guid leafId)
    {
        lock (_stateGate)
        {
            return _epochEntries is not null && _epochEntries.ContainsKey(leafId);
        }
    }

    private async Task<long> RetireUnderLeafGateAsync(IGrainStorage storage, Guid leafId, long rowEpoch)
    {
        var key = WalAppendFenceRows.EntryKey(_treeId, _shardIndex, leafId);
        var floorCovers = false;
        try
        {
            for (var attempt = 1; ; attempt++)
            {
                var row = await WalAppendFenceRows.ReadEntryAsync(storage, _treeId, _shardIndex, leafId).ConfigureAwait(true);
                long retired;
                lock (_stateGate)
                {
                    // #5641: the epoch row's value as well as the entry. A duplicate that
                    // persisted its row but has not yet written its entry here must be
                    // refused at its entry write, or it would share the re-created
                    // incarnation's epoch once the clear deletes the row.
                    retired = Math.Max(row.State.Epoch, Math.Max(rowEpoch, 0));
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

                // From here the floor covers every epoch this leaf held here, so its loaded
                // entry is removed below whatever fails next (#5657): a surviving duplicate
                // must not keep appending at a partition whose clear it outlived (#5635).
                floorCovers = true;

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

                break;
            }

            // #5634: delisted only once the entry is gone. This leaf's claims wait on
            // the leaf gate this retirement holds, so none can list it in between. Best
            // effort: a listing left behind is harmless, and the sweep retires it again.
            try
            {
                await DirectoryOpAsync(leafId, () => WalAppendEpochDirectory.RemoveAsync(storage, _treeId, _shardIndex, leafId)).ConfigureAwait(true);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Left listed; the sweep finds the leaf gone and retires it again.
            }
        }
        finally
        {
            if (floorCovers)
            {
                await RemoveLoadedEntryAsync(leafId).ConfigureAwait(true);
            }
        }

        lock (_stateGate)
        {
            return _epochFloor;
        }
    }

    /// <summary>
    /// Removes <paramref name="leafId"/>'s loaded entry once any multi-entry stamped
    /// batch of it has finished assigning, so retirement never leaves a batch
    /// partially admitted.
    /// </summary>
    private async Task RemoveLoadedEntryAsync(Guid leafId)
    {
        while (true)
        {
            Task? drain;
            lock (_stateGate)
            {
                drain = BatchDrainLocked(leafId);
                if (drain is null)
                {
                    _epochEntries?.Remove(leafId);
                    return;
                }
            }

            await drain.ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Runs a directory listing or delisting under its bucket's in-process gate
    /// (#5605 review of #5653, S2), so concurrent first claims on this partition list
    /// one at a time per bucket and an ETag conflict means only a duplicate activation
    /// of this WAL shard. Bounded: <see cref="WalAppendEpochDirectory.Buckets"/> gates.
    /// </summary>
    private async Task DirectoryOpAsync(Guid leafId, Func<Task> op)
    {
        SemaphoreSlim gate;
        lock (_stateGate)
        {
            _directoryGates ??= new SemaphoreSlim[WalAppendEpochDirectory.Buckets];
            gate = _directoryGates[WalAppendEpochDirectory.BucketOf(leafId)] ??= new SemaphoreSlim(1, 1);
        }

        await gate.WaitAsync().ConfigureAwait(true);
        try
        {
            await op().ConfigureAwait(true);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// The append fence's admission check for <paramref name="stamp"/>, run under
    /// <see cref="_stateGate"/> in the step that would assign the offset.
    /// </summary>
    private WalAppendFenceOutcome AdmitStampLocked(in WalAppendStamp stamp)
    {
        if (stamp.IsCoordinatorTerminal)
        {
            // Member 2: a coordinator's saga terminal is authored by no leaf, so no
            // entry is checked; whether it raises the watermark is decided per record
            // at allocation (RaisesUnstampedWatermark).
            return WalAppendFenceOutcome.Admitted;
        }

        if (stamp.IsBulkSeed)
        {
            return AdmitBulkSeedLocked(stamp);
        }

        if (stamp.IsPreArm)
        {
            // #5833, revision 2: no entry is checked and no row is needed. The routing
            // generation is checked as for a stamped append, so a purged incarnation's
            // straddler and a write admitted before a re-claim stay fenced. Without one
            // the record is admitted unstamped and reported (C4).
            return stamp.Initiator.IsSet ? AdmitGenerationLocked(stamp) : WalAppendFenceOutcome.Admitted;
        }

        if (_epochEntries is null
            || !_epochEntries.TryGetValue(stamp.LeafId, out var entry)
            || entry.Memory == WalAppendFenceCore.Unloaded)
        {
            return WalAppendFenceOutcome.UnloadedEntry;
        }

        if (!WalAppendFenceCore.IsEpochAdmitted(stamp.Epoch, entry.Memory))
        {
            return WalAppendFenceOutcome.StaleEpoch;
        }

        return stamp.Initiator.IsSet ? AdmitGenerationLocked(stamp) : WalAppendFenceOutcome.Admitted;
    }

    /// <summary>The routing-generation half of <see cref="AdmitStampLocked"/>, for a stamp whose initiator is set.</summary>
    private WalAppendFenceOutcome AdmitGenerationLocked(in WalAppendStamp stamp)
    {
        if (_generationEntries is null
            || !_generationEntries.TryGetValue(stamp.Initiator, out var generation)
            || generation.Memory == WalAppendFenceCore.Unloaded)
        {
            return WalAppendFenceOutcome.UnloadedGeneration;
        }

        return WalAppendFenceCore.IsLoadedGenerationAdmitted(stamp.Generation, generation.Memory, generation.FloorOnly)
            ? WalAppendFenceOutcome.Admitted
            : WalAppendFenceOutcome.StaleGeneration;
    }

    /// <summary>
    /// The bulk-seed class's admission (#5812, review C2): only on a positive match
    /// against the shard root's routing generation as this partition has loaded it. An
    /// entry not loaded, or loaded below the seed's generation, is
    /// <see cref="WalAppendFenceOutcome.UnloadedGeneration"/>: the seed re-presents its
    /// generation, which loads it while it is current and is refused once a close has
    /// re-claimed above it. An entry above it is stale: the switch closed. Admission is
    /// never decided on an absent entry.
    /// </summary>
    private WalAppendFenceOutcome AdmitBulkSeedLocked(in WalAppendStamp stamp)
    {
        if (_generationEntries is null
            || !_generationEntries.TryGetValue(stamp.Initiator, out var generation)
            || generation.Memory == WalAppendFenceCore.Unloaded
            || generation.FloorOnly
            || generation.Memory < stamp.Generation)
        {
            return WalAppendFenceOutcome.UnloadedGeneration;
        }

        return generation.Memory == stamp.Generation
            ? WalAppendFenceOutcome.Admitted
            : WalAppendFenceOutcome.StaleGeneration;
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
        bool epochTable,
        Guid? batchLeaf,
        WalRoutingInitiator? batchInitiator,
        TimeSpan? ioBound = null,
        SemaphoreSlim? rowGate = null,
        Action? onConflict = null)
        where TId : notnull
    {
        var listed = false;
        for (var attempt = 1; ; attempt++)
        {
            GrainState<TRow>? row = null;
            long durable;
            var rowGateHeld = false;
            try
            {
                if (attempt == 1 && batchInitiator is not null && !DisableLoadedClaimFastPathForTesting && TryLandedLocked(table, id, value, out var landed))
                {
                    // #5831, F1: a re-presentation of a value this activation has loaded and
                    // knows durable - read back or written by a write known to have landed -
                    // is decided from that value with no storage op. It cannot need a write
                    // (value <= durable), and the decision below is the one the I/O path
                    // reaches when its read returns that value.
                    durable = landed;
                }
                else if (attempt == 1 && batchLeaf is { } leafToList && value == 1 && !IsLoaded(table, id))
                {
                    // #5831, F3: a leaf's first activation (epoch 1) has no entry at any
                    // partition, so its first claim lists it in parallel with reading the
                    // entry. A later activation re-claims an entry that exists and is listed,
                    // so it reads the entry alone and lists only if absent, as before, rather
                    // than paying a page read on every re-claim. Listing is idempotent and
                    // needs no decision: one left listed by a refused claim is harmless and
                    // swept. The entry is still written only after the listing has landed
                    // (#5634), conditionally on the ETag read, or as a create of an absent row.
                    var listing = DirectoryOpAsync(leafToList, () => WalAppendEpochDirectory.AddAsync(storage, _treeId, _shardIndex, leafToList, DateTime.UtcNow.Ticks));
                    var reading = read();
                    await Task.WhenAll(listing, reading).ConfigureAwait(true);
                    listed = true;
                    row = reading.Result;
                    durable = getValue(row);
                }
                else
                {
                    if (rowGate is not null)
                    {
                        // #5987: the row is shared by every initiator's slot, so its
                        // read-modify-write is serialised in this activation; the ETag
                        // condition still orders it against any writer outside it.
                        if (!await rowGate.WaitAsync(ioBound ?? Timeout.InfiniteTimeSpan).ConfigureAwait(true))
                        {
                            throw new TimeoutException(
                                $"WAL shard {_treeId}/{_shardIndex}: a routing-generation table write waited {ioBound} for the one ahead of it.");
                        }

                        rowGateHeld = true;
                    }

                    row = await read().ConfigureAwait(true);
                    durable = getValue(row);
                }

                WalAppendFenceCore.ClaimWriteDecision decision;
                long threshold;
                lock (_stateGate)
                {
                    var entry = NoteDurableLocked(table, id, durable);
                    var floor = epochTable ? _epochFloor : GenerationFloorLocked(batchInitiator!.Value);
                    decision = WalAppendFenceCore.DecideClaimWrite(value, entry.Memory, durable, floor);
                    threshold = WalAppendFenceCore.ClaimThreshold(entry.Memory, Math.Max(durable, entry.DurableSeen), floor);
                }

                if (decision == WalAppendFenceCore.ClaimWriteDecision.Refuse)
                {
                    return WalAppendFenceClaimResult.Refuse(threshold);
                }

                if (decision == WalAppendFenceCore.ClaimWriteDecision.Write)
                {
                    if (row is null)
                    {
                        // Unreachable: the memory decision never writes. Fail safe to the read path.
                        continue;
                    }

                    if (batchLeaf is { } listedLeaf && !row.RecordExists && !listed)
                    {
                        // #5634: listed before the entry row is first written, so every
                        // entry is enumerable by the sweep. Idempotent across retries.
                        await DirectoryOpAsync(listedLeaf, () => WalAppendEpochDirectory.AddAsync(storage, _treeId, _shardIndex, listedLeaf, DateTime.UtcNow.Ticks)).ConfigureAwait(true);
                    }

                    setValue(row, value);
                    try
                    {
                        await Bound(WalAppendFenceRows.WriteAsync(storage, grainType, key, row), ioBound).ConfigureAwait(true);
                    }
                    catch (InconsistentStateException)
                    {
                        onConflict?.Invoke();
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
            }
            finally
            {
                if (rowGateHeld)
                {
                    rowGate!.Release();
                }
            }

            // The raise waits out any multi-entry stamped batch of this leaf, or carrying
            // this initiator's generation (member 2, Q4), that is still assigning
            // offsets, so it never falls between two of its entries and leaves the batch
            // partially admitted above the head it returns.
            while (true)
            {
                Task? drain = null;
                lock (_stateGate)
                {
                    if (batchLeaf is { } leaf)
                    {
                        drain = BatchDrainLocked(leaf);
                    }
                    else if (batchInitiator is { } batchedFor)
                    {
                        drain = GenerationBatchDrainLocked(batchedFor);
                    }

                    if (drain is null)
                    {
                        var entry = NoteDurableLocked(table, id, value);
                        var floor = epochTable ? _epochFloor : GenerationFloorLocked(batchInitiator!.Value);
                        if (!WalAppendFenceCore.IsRaiseAdmitted(value, entry.Memory, entry.DurableSeen, floor))
                        {
                            return WalAppendFenceClaimResult.Refuse(
                                WalAppendFenceCore.ClaimThreshold(entry.Memory, entry.DurableSeen, floor));
                        }

                        entry.Memory = value;
                        entry.FloorOnly = false;
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
    /// <remarks>
    /// Invariant (#5831, F1): a batch registered here never waits on a routing-generation
    /// claim gate, because a claim holding that gate waits for this batch to drain. The
    /// stamped append's generation load runs, and releases the gate, before the batch's
    /// first entry is admitted and registered.
    /// </remarks>
    private void RegisterStampedBatchLocked(in WalAppendStamp stamp)
    {
        // A bulk seed names no leaf (#5812), and a pre-arm record's leaf holds no claim
        // (#5833): only the generation is held, so a close's re-claim waits for the batch
        // and its records land below the close heads.
        if (!stamp.IsBulkSeed && !stamp.IsPreArm)
        {
            var entry = _epochEntries![stamp.LeafId];
            entry.InFlightBatches++;
            _epochEntries[stamp.LeafId] = entry;
        }

        if (stamp.Initiator.IsSet)
        {
            // Admission found the initiator's entry loaded in this same step.
            var generation = _generationEntries![stamp.Initiator];
            generation.InFlightBatches++;
            _generationEntries[stamp.Initiator] = generation;
        }
    }

    /// <summary>
    /// Ends a multi-entry stamped batch once it has assigned or rejected every
    /// entry, releasing a raise or retirement waiting for it on the leaf, and a
    /// generation re-claim waiting for it on its initiator.
    /// </summary>
    private void ReleaseStampedBatch(in WalAppendStamp stamp)
    {
        var leafId = stamp.LeafId;
        TaskCompletionSource? drained = null;
        TaskCompletionSource? generationDrained = null;
        lock (_stateGate)
        {
            if (stamp.Initiator.IsSet
                && _generationEntries is not null
                && _generationEntries.TryGetValue(stamp.Initiator, out var generation))
            {
                generation.InFlightBatches--;
                _generationEntries[stamp.Initiator] = generation;
                if (generation.InFlightBatches <= 0)
                {
                    _generationBatchDrains?.Remove(stamp.Initiator, out generationDrained);
                }
            }

            var leafDone = !stamp.IsPreArm;
            if (!stamp.IsPreArm && _epochEntries is not null && _epochEntries.TryGetValue(leafId, out var entry))
            {
                entry.InFlightBatches--;
                _epochEntries[leafId] = entry;
                leafDone = entry.InFlightBatches <= 0;
            }

            if (leafDone)
            {
                _batchDrains?.Remove(leafId, out drained);
            }
        }

        drained?.TrySetResult();
        generationDrained?.TrySetResult();
    }

    /// <summary>
    /// A task that completes when <paramref name="initiator"/> has no multi-entry
    /// generation-stamped batch assigning offsets, or <see langword="null"/> when it
    /// has none now. Called under <see cref="_stateGate"/>.
    /// </summary>
    private Task? GenerationBatchDrainLocked(WalRoutingInitiator initiator)
    {
        if (_generationEntries is null
            || !_generationEntries.TryGetValue(initiator, out var entry)
            || entry.InFlightBatches <= 0)
        {
            return null;
        }

        _generationBatchDrains ??= new Dictionary<WalRoutingInitiator, TaskCompletionSource>();
        if (!_generationBatchDrains.TryGetValue(initiator, out var drained))
        {
            drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _generationBatchDrains[initiator] = drained;
        }

        return drained.Task;
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

    /// <summary>
    /// Whether <paramref name="id"/>'s entry is loaded and <paramref name="value"/> is at
    /// or below the durable value this activation has seen for it, returning that value.
    /// <see cref="FenceEntry.DurableSeen"/> is raised only by a read, or by a write that
    /// returned, so it is a value known to have landed (#5831, F1).
    /// </summary>
    private bool TryLandedLocked<TId>(Dictionary<TId, FenceEntry> table, TId id, long value, out long landed)
        where TId : notnull
    {
        lock (_stateGate)
        {
            if (table.TryGetValue(id, out var entry)
                && entry.Memory != WalAppendFenceCore.Unloaded
                && value <= entry.DurableSeen)
            {
                landed = entry.DurableSeen;
                return true;
            }

            landed = 0;
            return false;
        }
    }

    /// <summary>Whether <paramref name="id"/>'s entry is loaded.</summary>
    private bool IsLoaded<TId>(Dictionary<TId, FenceEntry> table, TId id)
        where TId : notnull
    {
        lock (_stateGate)
        {
            return table.TryGetValue(id, out var entry) && entry.Memory != WalAppendFenceCore.Unloaded;
        }
    }

    /// <summary>Forces every routing-generation claim to read its row; test seam (#5831, F1 exactness).</summary>
    internal bool DisableLoadedClaimFastPathForTesting { get; set; }

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

    /// <summary>
    /// Loads the retired-epoch floor row once (#5987): concurrent first loads share one
    /// read, and a load that failed is retried by the next caller.
    /// </summary>
    private Task EnsureEpochFloorLoadedAsync(IGrainStorage storage)
    {
        lock (_stateGate)
        {
            if (_epochFloorLoaded)
            {
                return Task.CompletedTask;
            }

            if (_epochFloorLoad is { IsFaulted: false, IsCanceled: false } inFlight)
            {
                return inFlight;
            }
        }

        var load = LoadEpochFloorCoreAsync(storage);
        lock (_stateGate)
        {
            if (_epochFloorLoad is not { IsFaulted: false, IsCanceled: false })
            {
                _epochFloorLoad = load;
            }

            return _epochFloorLoad;
        }
    }

    private async Task LoadEpochFloorCoreAsync(IGrainStorage storage)
    {
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

    /// <summary>
    /// Whether allocating <paramref name="entry"/> under <paramref name="stamp"/>
    /// raises the partition's unstamped watermark: every unstamped append does, and so
    /// does a record sent under the coordinator-terminal class that is not a
    /// <c>TxCommit</c> or <c>TxAbort</c> (member 2, review S2), which is therefore
    /// admitted unstamped and reported, and a pre-arm record sent without a routing
    /// generation (#5833, revision 2, C4). A routed pre-arm record never does: it is
    /// reported only to its own leaf, through <see cref="ForeignHighLocked"/>. Called
    /// under <see cref="_stateGate"/>.
    /// </summary>
    private static bool RaisesUnstampedWatermark(bool stamped, in WalAppendStamp stamp, in WalRecord entry) =>
        !stamped
        || (stamp.IsCoordinatorTerminal && !WalAppendFenceCore.IsCoordinatorTerminalOp(entry.Op))
        || (stamp.IsPreArm && !stamp.Initiator.IsSet);

    /// <summary>
    /// Counts records sent under the coordinator-terminal class that were not
    /// terminals, and so were admitted unstamped and reported (member 2).
    /// </summary>
    private void RecordTerminalClassMisuse(int count) =>
        LatticeMetrics.WalAppendFenceUnstampedAppends.Add(
            count,
            _treeTag,
            _tenantTag,
            LatticeMetrics.UnstampedSourceTerminalClassMisuse);

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

        /// <summary>
        /// A routing-generation entry whose <see cref="Memory"/> is the floor a read-only
        /// load installed, not a claimed value (#5833): admission is then strictly above
        /// it. Cleared by any raise.
        /// </summary>
        public bool FloorOnly { get; set; }
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
