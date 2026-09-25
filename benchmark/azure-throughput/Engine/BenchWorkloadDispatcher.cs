using Orleans;
using Orleans.Lattice;

namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// Workload-mode dispatcher used by <c>TcpIngestService.FlushAsync</c> to
/// route each producer batch through the <c>ILattice</c> operation
/// selected by <see cref="BenchWorkloadMode"/>. Extracted as a static
/// helper so the per-mode dispatch logic is independently testable
/// (see throughput-capture-plan.md step 7) without exposing the
/// <c>TcpIngestService</c> internals via <c>[InternalsVisibleTo]</c>.
/// </summary>
public static class BenchWorkloadDispatcher
{
    /// <summary>
    /// Dispatches <paramref name="batch"/> through the <c>ILattice</c>
    /// operation selected by <paramref name="mode"/>. Returns the number
    /// of <c>ILattice</c>-visible entries the silo has issued
    /// (always <c>batch.Count</c>; the count is mode-independent so the
    /// existing per-second "Entries written per second" counter remains
    /// directly comparable across modes when the offered load is held
    /// constant).
    /// </summary>
    /// <param name="mode">Workload selector resolved from the
    /// <c>BENCH_WORKLOAD_MODE</c> env-var at silo startup.</param>
    /// <param name="lattice">The lattice instance, already warmed up
    /// and (for read modes) already pre-seeded.</param>
    /// <param name="batch">One producer batch's worth of
    /// key/value pairs. For read modes the values are ignored; for
    /// <see cref="BenchWorkloadMode.GetMany"/> the entire batch's key
    /// list becomes the single <c>ILattice.GetManyAsync</c> argument.</param>
    /// <param name="atomicBatchSize">Saga slice size for
    /// <see cref="BenchWorkloadMode.SetManyAtomic"/>; ignored
    /// otherwise.</param>
    /// <param name="parallelism">In-flight cap for the per-entry fan-out
    /// modes (<see cref="BenchWorkloadMode.SetPoint"/>,
    /// <see cref="BenchWorkloadMode.GetPoint"/>); ignored
    /// otherwise.</param>
    /// <param name="ct">Propagates shutdown / producer-disconnect.</param>
    /// <param name="grainFactory">Grain factory used by the cross-tree
    /// modes (<see cref="BenchWorkloadMode.CrossTreeAtomic2"/>,
    /// <see cref="BenchWorkloadMode.CrossTreeAtomic64"/>) to open the
    /// cross-tree atomic-write builder; ignored by the single-tree
    /// modes.</param>
    /// <param name="treeId">Primary tree id; the cross-tree modes derive the
    /// sibling tree id (<c>{treeId}-b</c>) from it and split each saga's keys
    /// across the two trees. Ignored by the single-tree modes.</param>
    public static async Task<int> DispatchAsync(
        BenchWorkloadMode mode,
        ILattice lattice,
        List<KeyValuePair<string, byte[]>> batch,
        int atomicBatchSize,
        int parallelism,
        CancellationToken ct,
        IGrainFactory? grainFactory = null,
        string? treeId = null)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Count == 0) return 0;

        switch (mode)
        {
            case BenchWorkloadMode.SetMany:
                await lattice.SetManyAsync(batch, ct).ConfigureAwait(false);
                return batch.Count;

            case BenchWorkloadMode.SetManyAtomic:
                {
                    // Slice the batch into atomicBatchSize-sized sagas,
                    // awaited in order. The ingest engine hands this method
                    // one saga per call (SliceIntoFlushUnits), so saga
                    // concurrency comes from its flush gate; the loop only
                    // matters for a caller that passes a larger batch.
                    var sliceSize = Math.Max(1, atomicBatchSize);
                    var i = 0;
                    while (i < batch.Count)
                    {
                        var len = Math.Min(sliceSize, batch.Count - i);
                        // GetRange returns a fresh List<KeyValuePair<,>>
                        // so the slice is a self-contained value the
                        // SetManyAtomicAsync seam can pin without
                        // worrying about aliasing the outer batch.
                        var slice = batch.GetRange(i, len);
                        await lattice.SetManyAtomicAsync(slice, ct).ConfigureAwait(false);
                        i += len;
                    }
                    return batch.Count;
                }

            case BenchWorkloadMode.SetManyAtomic2:
                {
                    // Fixed 2-key single-tree sagas (the single-tree
                    // counterpart to CrossTreeAtomic2 at a matched batch
                    // size). Pinned to 2 keys regardless of atomicBatchSize.
                    var i = 0;
                    while (i < batch.Count)
                    {
                        var len = Math.Min(2, batch.Count - i);
                        var slice = batch.GetRange(i, len);
                        await lattice.SetManyAtomicAsync(slice, ct).ConfigureAwait(false);
                        i += len;
                    }
                    return batch.Count;
                }

            case BenchWorkloadMode.CrossTreeAtomic2:
                await DispatchCrossTreeAsync(grainFactory, treeId, batch, keysPerSaga: 2, ct).ConfigureAwait(false);
                return batch.Count;

            case BenchWorkloadMode.CrossTreeAtomic64:
                await DispatchCrossTreeAsync(grainFactory, treeId, batch, keysPerSaga: 64, ct).ConfigureAwait(false);
                return batch.Count;

            // set-point-mv shares the identical foreground write path as
            // set-point (one SetAsync per key); the only difference is the
            // silo-side materialised view attached at startup, which the
            // maintainer derives asynchronously off this hot path. Keeping the
            // dispatch identical is what makes the two cohorts a clean A/B.
            case BenchWorkloadMode.SetPoint:
            case BenchWorkloadMode.SetPointMv:
                await FanOutAsync(
                    batch,
                    parallelism,
                    (kvp, token) => lattice.SetAsync(kvp.Key, kvp.Value, token),
                    ct).ConfigureAwait(false);
                return batch.Count;

            case BenchWorkloadMode.GetPoint:
                await FanOutAsync(
                    batch,
                    parallelism,
                    async (kvp, token) =>
                    {
                        _ = await lattice.GetAsync(kvp.Key, token).ConfigureAwait(false);
                    },
                    ct).ConfigureAwait(false);
                return batch.Count;

            case BenchWorkloadMode.GetMany:
                {
                    // Project the producer batch to its key list. Capacity-
                    // hinted so the List grows zero times on the hot path.
                    var keys = new List<string>(batch.Count);
                    for (var i = 0; i < batch.Count; i++) keys.Add(batch[i].Key);
                    _ = await lattice.GetManyAsync(keys, ct).ConfigureAwait(false);
                    return batch.Count;
                }

            default:
                throw new InvalidOperationException($"Unhandled BenchWorkloadMode: {mode}");
        }
    }

    /// <summary>
    /// Splits one producer batch into the units the ingest engine flushes,
    /// retries and accounts independently. A non-atomic mode returns the batch
    /// unchanged as a single unit. An atomic mode returns one unit per saga:
    /// <see cref="BenchWorkloadMode.SetManyAtomic2"/> and
    /// <see cref="BenchWorkloadMode.CrossTreeAtomic2"/> slice into 2-key units,
    /// <see cref="BenchWorkloadMode.CrossTreeAtomic64"/> into 64-key units, and
    /// <see cref="BenchWorkloadMode.SetManyAtomic"/> into
    /// <paramref name="atomicBatchSize"/>-key units. The final unit carries any
    /// remainder.
    /// </summary>
    /// <remarks>
    /// Each unit is dispatched through <see cref="DispatchAsync"/> on its own
    /// flush slot, so the unit dispatches exactly one saga. Handing a whole
    /// producer batch of several thousand keys to one slot instead ran its
    /// sagas as a sequential chain: ops only moved when the last saga of the
    /// chain returned, a saturation retry re-committed every saga that had
    /// already landed, and one rolled-back saga booked the whole batch as
    /// failed (#3581).
    /// </remarks>
    /// <param name="mode">Workload selector.</param>
    /// <param name="batch">One producer batch.</param>
    /// <param name="atomicBatchSize">Saga size for
    /// <see cref="BenchWorkloadMode.SetManyAtomic"/>; values below 1 are
    /// treated as 1. Ignored by every other mode.</param>
    /// <returns>The flush units, in batch order. Empty when
    /// <paramref name="batch"/> is empty.</returns>
    public static IReadOnlyList<List<KeyValuePair<string, byte[]>>> SliceIntoFlushUnits(
        BenchWorkloadMode mode,
        List<KeyValuePair<string, byte[]>> batch,
        int atomicBatchSize)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Count == 0)
        {
            return Array.Empty<List<KeyValuePair<string, byte[]>>>();
        }

        var sagaSize = mode switch
        {
            BenchWorkloadMode.SetManyAtomic => Math.Max(1, atomicBatchSize),
            BenchWorkloadMode.SetManyAtomic2 => 2,
            BenchWorkloadMode.CrossTreeAtomic2 => 2,
            BenchWorkloadMode.CrossTreeAtomic64 => 64,
            _ => 0,
        };
        if (sagaSize == 0 || batch.Count <= sagaSize)
        {
            return new[] { batch };
        }

        var units = new List<List<KeyValuePair<string, byte[]>>>((batch.Count + sagaSize - 1) / sagaSize);
        for (var i = 0; i < batch.Count; i += sagaSize)
        {
            units.Add(batch.GetRange(i, Math.Min(sagaSize, batch.Count - i)));
        }

        return units;
    }

    /// <summary>
    /// Bounded-parallelism fan-out over <paramref name="batch"/>: at
    /// most <paramref name="parallelism"/> calls to
    /// <paramref name="action"/> are in flight at any time. Used by the
    /// per-entry point-write and point-read modes so concurrency is
    /// capped at the caller-supplied flush concurrency rather than
    /// thrashing the threadpool with one Task per entry.
    /// </summary>
    private static async Task FanOutAsync(
        List<KeyValuePair<string, byte[]>> batch,
        int parallelism,
        Func<KeyValuePair<string, byte[]>, CancellationToken, Task> action,
        CancellationToken ct)
    {
        var maxInFlight = Math.Max(1, parallelism);
        using var gate = new SemaphoreSlim(maxInFlight, maxInFlight);
        // Track every issued task so a single failure surfaces via
        // Task.WhenAll rather than escaping into the threadpool. The
        // FlushAsync caller wraps DispatchAsync in retry/shutdown
        // handling that treats a thrown exception as a transient
        // failure or as a real fault; either way a fan-out task that
        // faulted must propagate.
        var tasks = new List<Task>(batch.Count);
        for (var i = 0; i < batch.Count; i++)
        {
            await gate.WaitAsync(ct).ConfigureAwait(false);
            var kvp = batch[i];
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await action(kvp, ct).ConfigureAwait(false);
                }
                finally
                {
                    gate.Release();
                }
            }, ct));
        }
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    /// <summary>
    /// Commits the producer <paramref name="batch"/> as a sequence of
    /// cross-tree atomic writes, each spanning two trees
    /// (<paramref name="treeId"/> and <c>{treeId}-b</c>) and committed
    /// all-or-nothing through
    /// <see cref="LatticeCrossTreeAtomicWriteExtensions.BeginAtomicWrite(IGrainFactory, string)"/>.
    /// The batch is sliced into <paramref name="keysPerSaga"/>-key sagas; within
    /// each saga the first half of the keys target the primary tree and the
    /// second half target the sibling <c>-b</c> tree, so a 2-key saga writes
    /// 1 key per tree and a 64-key saga writes 32 keys per tree. Each saga mints
    /// a fresh operationId (a stable idempotency key is mandatory for a
    /// multi-registry cross-tree saga). Bounded by the outer FlushConcurrency
    /// gate the caller already holds; the ingest engine passes one saga per
    /// call (see <see cref="SliceIntoFlushUnits"/>).
    /// </summary>
    private static async Task DispatchCrossTreeAsync(
        IGrainFactory? grainFactory,
        string? treeId,
        List<KeyValuePair<string, byte[]>> batch,
        int keysPerSaga,
        CancellationToken ct)
    {
        if (grainFactory is null || string.IsNullOrEmpty(treeId))
        {
            throw new InvalidOperationException(
                "Cross-tree workload modes require an IGrainFactory and a tree id; "
                + "wire them through BenchWorkloadDispatcher.DispatchAsync.");
        }

        var secondTreeId = treeId + "-b";
        var i = 0;
        while (i < batch.Count)
        {
            var len = Math.Min(keysPerSaga, batch.Count - i);
            // Split the saga's keys evenly across the two trees: the first
            // ceil(len/2) keys go to the primary tree, the rest to the
            // sibling -b tree. A trailing odd-length tail saga puts its lone
            // last key on the primary tree (a single-tree cross-tree commit
            // is valid); steady-state batches divide evenly.
            var half = (len + 1) / 2;
            var operationId = Guid.NewGuid().ToString("N");
            var builder = grainFactory.BeginAtomicWrite(operationId).ForTree(treeId);
            for (var j = 0; j < len; j++)
            {
                if (j == half)
                {
                    builder.ForTree(secondTreeId);
                }
                var entry = batch[i + j];
                builder.Set(entry.Key, entry.Value);
            }
            await builder.CommitAsync(ct).ConfigureAwait(false);
            i += len;
        }
    }
}
