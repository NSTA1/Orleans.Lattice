using System.Runtime.CompilerServices;

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
    /// Default width for the corpus-sized fan-out forms
    /// (<see cref="ForEachAsync{TItem}"/> and <see cref="ReadAheadAsync{TItem, TResult}"/>).
    /// </summary>
    /// <remarks>
    /// Sized to the <see cref="ILattice"/> router grain's
    /// <c>[StatelessWorker(maxLocalWorkers: 32)]</c>: calls issued together are
    /// serviced by separate local workers rather than queued behind one another,
    /// but only up to that worker count, so a wider window would buy no further
    /// overlap while bursting more outbound calls. It is the same bound the
    /// shipped leaf-seal fan-out and tag-index row removal already use.
    /// </remarks>
    public const int DefaultWidth = 32;

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
    /// Corpus-sized counterpart to
    /// <see cref="RunAsync(int, int, Func{int, Task}, CancellationToken)"/>:
    /// applies <paramref name="body"/> to every item in <paramref name="items"/>
    /// in bounded overlapped waves, holding at most
    /// <paramref name="maxConcurrency"/> tasks alive at any moment.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why a second shape.</b> <c>RunAsync</c> launches one gated task per slot
    /// up front so it can return the results in slot order, which is exactly right
    /// for the cluster-roll-up fan-outs it serves (trees x shards - hundreds of
    /// slots at most). It is the wrong shape when the slot count is a function of
    /// the stored corpus rather than of the topology: clearing an N-key view
    /// generation would allocate an N-element task array and park N-bound gated
    /// tasks on a semaphore. This form walks the input a wave at a time instead,
    /// so its live task set is O(maxConcurrency) however large the input is.
    /// </para>
    /// <para>
    /// <b>What callers must guarantee.</b> The items must be independent of one
    /// another and the work order-insensitive: completion order within a wave is
    /// not defined, and no result is returned. Each wave settles through
    /// <see cref="Task.WhenAll(IEnumerable{Task})"/>, which observes every fault
    /// in that wave, so an abandoned fan-out leaves no unobserved faulted task.
    /// Like the rest of this type it never calls <c>ConfigureAwait(false)</c>, so
    /// it is safe to call from grain code.
    /// </para>
    /// </remarks>
    /// <param name="items">The items to apply <paramref name="body"/> to. An empty list is a no-op.</param>
    /// <param name="maxConcurrency">Maximum items in flight at once. Values below 1 are clamped to 1.</param>
    /// <param name="body">Per-item work. Must not throw synchronously; faults belong in the returned task.</param>
    public static async Task ForEachAsync<TItem>(
        IReadOnlyList<TItem> items,
        int maxConcurrency,
        Func<TItem, Task> body)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(body);

        if (items.Count == 0)
        {
            return;
        }

        // A single item stays on the direct await, so the common shallow case
        // pays no task-list allocation at all.
        if (items.Count == 1)
        {
            await body(items[0]);
            return;
        }

        var bound = Math.Max(1, maxConcurrency);
        var wave = new List<Task>(Math.Min(items.Count, bound));
        for (var i = 0; i < items.Count; i++)
        {
            wave.Add(body(items[i]));
            if (wave.Count >= bound)
            {
                await Task.WhenAll(wave);
                wave.Clear();
            }
        }

        if (wave.Count > 0)
        {
            await Task.WhenAll(wave);
        }
    }

    /// <summary>
    /// Reads every item in <paramref name="items"/> through
    /// <paramref name="read"/> with at most <paramref name="maxConcurrency"/>
    /// reads in flight, yielding the results strictly in input order.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the shape for a loop whose <i>body</i> must stay sequential over
    /// reads that need not - a projection rebuild that reads one source key and
    /// then writes the view rows it derives, say. Only the reads overlap; the
    /// consumer still sees exactly the sequence a serial loop produced, so
    /// nothing downstream has to become order-insensitive to benefit. That is
    /// safe precisely when the reads are pure and disjoint from whatever the loop
    /// body writes.
    /// </para>
    /// <para>
    /// <b>Ring window.</b> The read for input index <c>j</c> occupies slot
    /// <c>j % width</c>. Only indices in <c>[consumed, consumed + width)</c> are
    /// ever outstanding, and those map to distinct slots, so no in-flight read is
    /// overwritten. Like <see cref="ForEachAsync"/>, the live task set is
    /// O(maxConcurrency) regardless of input size.
    /// </para>
    /// <para>
    /// <b>Abandonment.</b> If the consumer breaks out, or its body throws, the
    /// reads still outstanding are observed on disposal, so a faulted one cannot
    /// resurface later as an unobserved task exception.
    /// </para>
    /// </remarks>
    /// <param name="items">The items to read. An empty list yields nothing.</param>
    /// <param name="maxConcurrency">Maximum reads in flight at once. Values below 1 are clamped to 1.</param>
    /// <param name="read">Per-item read. Must be free of side effects the consumer's body depends on.</param>
    /// <param name="cancellationToken">Observed before each result is awaited.</param>
    public static async IAsyncEnumerable<TResult> ReadAheadAsync<TItem, TResult>(
        IReadOnlyList<TItem> items,
        int maxConcurrency,
        Func<TItem, Task<TResult>> read,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(read);

        if (items.Count == 0)
        {
            yield break;
        }

        var window = new Task<TResult>[Math.Min(items.Count, Math.Max(1, maxConcurrency))];
        var issued = 0;
        var consumed = 0;
        try
        {
            for (; consumed < items.Count; consumed++)
            {
                while (issued < items.Count && issued - consumed < window.Length)
                {
                    window[issued % window.Length] = read(items[issued]);
                    issued++;
                }

                cancellationToken.ThrowIfCancellationRequested();
                yield return await window[consumed % window.Length];
            }
        }
        finally
        {
            // Everything from consumed up to issued is either the read this loop
            // stopped on or one issued ahead of it. Starting at consumed rather
            // than past it covers the case where cancellation threw before the
            // await; re-observing an already-awaited slot is harmless, because a
            // completed non-faulted task never runs the fault continuation.
            for (var j = consumed; j < issued; j++)
            {
                Observe(window[j % window.Length]);
            }
        }
    }

    /// <summary>
    /// Attaches a no-op fault continuation so an abandoned read's exception is
    /// retrieved rather than left unobserved.
    /// </summary>
    private static void Observe(Task task) =>
        _ = task.ContinueWith(
            static t => _ = t.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

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
