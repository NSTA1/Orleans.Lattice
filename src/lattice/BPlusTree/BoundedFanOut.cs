namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Ordered, concurrency-bounded fan-out helper. Runs one unit of work per slot
/// in <c>[0, count)</c> with at most <c>maxConcurrency</c> in flight at once and
/// returns the per-slot results <b>in slot order</b>, so a caller whose input
/// list carries an ordering guarantee (for example the sorted tree ids the tree
/// registry returns) keeps that guarantee across the fan-out.
/// <para>
/// <b>Why bound at all.</b> Nested unbounded <see cref="Task.WhenAll(Task[])"/>
/// levels multiply: a cluster-wide roll-up that issues one task per tree, each of
/// which issues one task per shard and WAL partition, dispatches
/// <c>trees x (shards + partitions)</c> grain calls in a single burst that all
/// race one Orleans response deadline. Bounding each level makes the work degrade
/// in <i>latency</i> instead of collapsing into deadline failures (see issue
/// #1728).
/// </para>
/// <para>
/// <b>Settle-before-return.</b> Every launched slot is awaited through a single
/// <see cref="Task.WhenAll{TResult}(Task{TResult}[])"/>, which observes every
/// child's fault even when several fail, so an abandoned fan-out can never leave
/// an unobserved faulted <see cref="Task"/> behind. If <c>body</c> throws
/// synchronously part-way through the launch loop (call sites pass <c>async</c>
/// delegates, so this is defensive), the slots already launched are settled
/// before the synchronous fault propagates.
/// </para>
/// <para>
/// <b>Cancellation.</b> The token is observed on the gate acquisition, so a slot
/// that has not started yet is never dispatched once cancellation is requested;
/// slots already in flight settle normally and the aggregate then surfaces the
/// <see cref="OperationCanceledException"/>.
/// </para>
/// <para>
/// <b>Scheduler affinity.</b> This helper deliberately never calls
/// <c>ConfigureAwait(false)</c>: it is invoked from Orleans grain code, where
/// dropping the continuation off the grain's task scheduler would lose
/// <c>RequestContext</c> (and with it the active-tenant and system-origin
/// scopes) and break the single-threaded activation contract.
/// </para>
/// </summary>
internal static class BoundedFanOut
{
    /// <summary>
    /// Runs <paramref name="body"/> for each slot in <c>[0, count)</c> with at
    /// most <paramref name="maxConcurrency"/> in flight, returning the per-slot
    /// results in slot order.
    /// </summary>
    /// <typeparam name="T">The per-slot result type.</typeparam>
    /// <param name="count">Number of slots to run. A non-positive value yields an empty result.</param>
    /// <param name="maxConcurrency">Maximum slots in flight at once. Values below 1 are clamped to 1.</param>
    /// <param name="body">Per-slot work. Must not throw synchronously; faults belong in the returned task.</param>
    /// <param name="cancellationToken">Observed when acquiring a concurrency slot.</param>
    public static async Task<T[]> RunAsync<T>(
        int count,
        int maxConcurrency,
        Func<int, Task<T>> body,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(body);
        if (count <= 0)
        {
            return [];
        }

        // A caller-supplied bound below 1 would otherwise build a permanently
        // empty semaphore and deadlock the fan-out; clamp exactly as the other
        // MaxConcurrent* knobs are clamped at their use sites.
        var bound = Math.Max(1, maxConcurrency);
        if (bound >= count)
        {
            // Every slot fits inside the bound, so the gate would never block:
            // skip the semaphore (and its allocation) entirely. This is the
            // dominant case for a small cluster or a narrow tree.
            var all = new Task<T>[count];
            await LaunchAsync(all, count, body);
            return await Task.WhenAll(all);
        }

        using var gate = new SemaphoreSlim(bound, bound);
        var tasks = new Task<T>[count];
        await LaunchAsync(tasks, count, GatedAsync);
        // WhenAll settles every slot before the gate is disposed, and observes
        // every fault, so no task is abandoned unobserved.
        return await Task.WhenAll(tasks);

        async Task<T> GatedAsync(int slot)
        {
            await gate.WaitAsync(cancellationToken);
            try
            {
                return await body(slot);
            }
            finally
            {
                gate.Release();
            }
        }
    }

    /// <summary>
    /// Result-free counterpart to
    /// <see cref="RunAsync{T}(int, int, Func{int, Task{T}}, CancellationToken)"/>.
    /// <see cref="Task.WhenAll(Task[])"/> completes only after every slot settles,
    /// so a caller's catch can act on a fully-quiesced batch even when one slot
    /// faulted.
    /// </summary>
    /// <param name="count">Number of slots to run. A non-positive value is a no-op.</param>
    /// <param name="maxConcurrency">Maximum slots in flight at once. Values below 1 are clamped to 1.</param>
    /// <param name="body">Per-slot work. Must not throw synchronously; faults belong in the returned task.</param>
    /// <param name="cancellationToken">Observed when acquiring a concurrency slot.</param>
    public static async Task RunAsync(
        int count,
        int maxConcurrency,
        Func<int, Task> body,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(body);
        if (count <= 0)
        {
            return;
        }

        var bound = Math.Max(1, maxConcurrency);
        if (bound >= count)
        {
            var all = new Task[count];
            await LaunchAsync(all, count, body);
            await Task.WhenAll(all);
            return;
        }

        using var gate = new SemaphoreSlim(bound, bound);
        var tasks = new Task[count];
        await LaunchAsync(tasks, count, GatedAsync);
        await Task.WhenAll(tasks);

        async Task GatedAsync(int slot)
        {
            await gate.WaitAsync(cancellationToken);
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

    /// <summary>
    /// Fills <paramref name="tasks"/> by invoking <paramref name="body"/> once per
    /// slot. A synchronous throw from <paramref name="body"/> would otherwise
    /// abandon the slots already launched; this settles them first so none is
    /// left unobserved, then re-throws the original fault.
    /// </summary>
    private static async Task LaunchAsync<TTask>(TTask[] tasks, int count, Func<int, TTask> body)
        where TTask : Task
    {
        var launched = 0;
        try
        {
            for (; launched < count; launched++)
            {
                tasks[launched] = body(launched);
            }
        }
        catch
        {
            for (var i = 0; i < launched; i++)
            {
                try
                {
                    await tasks[i];
                }
                catch
                {
                    // Observed deliberately: the synchronous launch fault is the
                    // one the caller must see, and an already-launched slot's own
                    // fault must not mask it or surface as unobserved.
                }
            }
            throw;
        }
    }
}
