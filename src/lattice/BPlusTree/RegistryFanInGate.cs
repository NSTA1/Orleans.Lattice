using System.Diagnostics;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Bounds the number of registry round trips a silo may have in flight at once,
/// and folds whatever is waiting behind that bound into a single batched read.
/// <para>
/// <b>The scaling law this exists to break.</b> Every registered tree births its
/// own set of reminder-driven background services - the hot-shard monitor, shard
/// healing, tree merge, reshard, snapshot - and each of them resolves through the
/// one cluster-singleton <see cref="ILatticeRegistry"/> activation. Nothing in
/// that path was bounded, so the concurrent cold-start fan-in onto that single
/// activation was the product <c>(trees) x (per-tree background services)</c>:
/// add a tree and every silo's peak in-flight registry concurrency rises with
/// it. A bound that is itself a function of tree count is not a bound, and until
/// this type there was none that was not.
/// </para>
/// <para>
/// <b>The bound.</b> At most <see cref="GlobalMaxConcurrentReads"/> registry
/// round trips are in flight across the <em>whole cluster</em> at any instant,
/// each carrying at most <see cref="MaxBatchSize"/> tree ids. Every silo derives
/// its own share of that one cluster-wide ceiling by dividing it by the live silo
/// count, which it reads locally from cluster membership. So the invariant is
/// "at most <c>GlobalMaxConcurrentReads</c> in flight at the activation", not
/// "at most N per caller" - neither term mentions the tree count.
/// </para>
/// <para>
/// <b>Why the budget is divided by the silo count rather than fixed per silo.</b>
/// There is exactly one <see cref="ILatticeRegistry"/> activation cluster-wide -
/// the interface carries no <c>[StatelessWorker]</c> and no placement attribute,
/// and no placement director is registered anywhere in the library - so a
/// per-silo bound of C would put <c>M x C</c> round trips onto that one
/// activation across M silos. That is a large constant factor, not a bound: a
/// cluster grows its silo count precisely because it grew its tree count, so M
/// is itself a function of the estate and a fixed per-silo share would smuggle
/// the tree count back in through M. Dividing one global ceiling by the live silo
/// count is what makes the bound hold at the target rather than at each caller.
/// </para>
/// <para>
/// <b>The one irreducible term.</b> Every silo keeps a floor of one round trip,
/// because a silo that may issue none cannot make progress at all. So cluster
/// in-flight is <c>max(GlobalMaxConcurrentReads, M)</c>: flat at the ceiling
/// while <c>M &lt;= GlobalMaxConcurrentReads</c>, and equal to M beyond it, which
/// is the floor any design permitting every silo to proceed must pay. Neither
/// branch contains the tree count, which is the property this type is for.
/// </para>
/// <para>
/// <b>Why membership and not a coordinating grain.</b> A shared counter grain
/// would be a second cluster singleton, reached by the same unbounded fan-in this
/// type exists to bound, and would double the round trips to boot. Cluster
/// membership is already globally agreed, already maintained by Orleans, and is
/// read from a local in-memory snapshot with no RPC - so it coordinates the bound
/// globally while adding no chokepoint and no call.
/// </para>
/// <para>
/// <b>Why it adds no latency to a quiet silo.</b> There is no timer and no
/// accumulation window, deliberately: a request dispatches immediately whenever
/// the concurrency bound has room, and a batch forms only out of requests that
/// were already waiting for capacity. So the batching is pure salvage of time
/// callers were going to spend queued anyway, and it needs no window constant
/// fitted to a particular host. This follows the reasoning already recorded on
/// <see cref="LatticeOptionsResolver"/>'s in-flight read coalescer, which
/// likewise refused a cache in order to avoid owning an expiry constant.
/// </para>
/// <para>
/// <b>Why a batch of one is not a batch.</b> A lone waiting id is read with
/// <see cref="ILatticeRegistry.GetEntryAsync"/>, exactly the call the caller
/// would have made unbounded. Only two or more distinct ids waiting together are
/// read with <see cref="ILatticeRegistry.GetEntriesAsync"/>. An estate small
/// enough never to queue therefore issues byte-for-byte the same registry
/// traffic it always did, and the batched member is reached only by the fan-in
/// this type exists to bound.
/// </para>
/// <para>
/// <b>Why batching is safe here.</b> <see cref="ILatticeRegistry.GetEntriesAsync"/>
/// is <c>[AlwaysInterleave]</c> and reads each id through the same single-key
/// read <see cref="ILatticeRegistry.GetEntryAsync"/> uses, so a batched read
/// returns exactly what the per-id reads would have returned and is not an
/// authorization boundary of any kind - it grants nothing that reading the ids
/// one at a time would not.
/// </para>
/// <para>
/// This gate is a per-silo object rather than a static, so a silo owns its own
/// share of the cluster budget and a fixture that constructs one starts clean.
/// </para>
/// </summary>
/// <param name="grainFactory">The grain factory used to reach the registry.</param>
/// <param name="siloStatusOracle">
/// The local silo's membership snapshot, used to divide the cluster-wide budget.
/// <c>null</c> outside a silo (a client host, or a unit-test fixture), in which
/// case the gate assumes a single silo and takes the whole budget.
/// </param>
internal sealed class RegistryFanInGate(
    IGrainFactory grainFactory,
    ISiloStatusOracle? siloStatusOracle = null)
{
    /// <summary>
    /// The most registry round trips that may be in flight <em>across the whole
    /// cluster</em> at once.
    /// <para>
    /// This is the constant that makes the bound a bound: it is not derived from
    /// the tree count, the shard count, or any configured value, so no estate can
    /// widen it. It is well above one so that a single slow round trip cannot
    /// stall every other tree's resolution behind it, and small enough that the
    /// singleton registry activation sees a fan-in width it can serve.
    /// </para>
    /// <para>
    /// <strong>What kind of number this is.</strong> It bounds the width of the
    /// interleave admitted at the registry activation. Every read member this
    /// gate calls carries <c>[AlwaysInterleave]</c>, so the registry admits
    /// concurrent calls <em>without any limit of its own</em>; that is precisely
    /// why the observed storm was never-served rather than served-slowly, since
    /// every call was admitted, every call awaited work further down, and none
    /// completed. A target that admits without limit supplies no backpressure,
    /// so the bound has to be imposed by the caller, because there is nowhere
    /// else in the path to impose it.
    /// </para>
    /// <para>
    /// <strong>Why it is host-independent.</strong> Not because one activation
    /// can only run one turn at a time; that is true of a non-reentrant
    /// activation but is exactly what <c>[AlwaysInterleave]</c> removes here, so
    /// it cannot be the reason. It is host-independent because what the width
    /// ultimately protects is the <em>terminal</em> activation of the path, and
    /// that one is non-reentrant: a registry read resolves to
    /// <c>ISystemLattice</c> and thence to <c>IShardRootGrain.GetAsync</c> on a
    /// shard of the registry tree, which carries no <c>[AlwaysInterleave]</c>
    /// and so executes one turn at a time on a single activation per shard.
    /// Turn-at-a-time execution is a property of how the runtime schedules an
    /// activation, not of the machine underneath it. A faster host finishes each
    /// turn sooner; it does not run two. That is what makes a compile-time
    /// constant defensible rather than something that ought to scale with the
    /// hardware.
    /// </para>
    /// <para>
    /// <strong>It is not fitted to a benchmark.</strong> The value was fixed
    /// before the measurement rig was exercised and no measurement was tuned
    /// against. This is a claim about provenance, not about validation: it means
    /// the constant does not encode the rig's failure signature (which is in any
    /// case a different signature from the production one), not that the value
    /// has been empirically confirmed.
    /// </para>
    /// <para>
    /// <strong>What bounds the choice.</strong> Above: the production timeout
    /// census counted roughly one hundred timed-out calls across a two-minute
    /// window. That is a count of failures in a window, not a measured
    /// concurrency; it implies heavy overlap given the response timeout in force,
    /// but the concurrency figure is an inference drawn from it rather than an
    /// observation, so it supports "well below one hundred" and no sharper claim.
    /// Below: single digits serialise every cold start for little gain, because
    /// the batched read already coalesces up to <see cref="MaxBatchSize"/> ids
    /// into one round trip, so narrowing the permit count buys far less than
    /// widening the batch does. Sixteen sits between those walls with room on
    /// either side. The claim being made is only that the value is the right
    /// order of magnitude, which is the property the bound needs; nothing here
    /// depends on sixteen rather than twelve or twenty.
    /// </para>
    /// <para>
    /// <strong>This constant multiplies with <see cref="MaxBatchSize"/>.</strong>
    /// See the remarks on that field before changing either. The product, not
    /// this value alone, is what reaches the backing tree.
    /// </para>
    /// <para>
    /// <strong>The asymmetry to know about.</strong> Setting this too high fails
    /// loudly and harmlessly: the bound simply stops binding and the original
    /// saturation returns, which every registry-side signal already reports.
    /// Setting it too low fails <em>quietly</em>, because admission here has no
    /// timeout and no queue cap by design, so the symptom is unbounded waiting
    /// rather than an error, and nothing in the registry's own telemetry moves.
    /// The <c>orleans.lattice.registry.admission.wait</c> histogram is the only
    /// signal that distinguishes the two, which is why it is emitted
    /// unconditionally on every dequeue rather than behind a threshold. Evidence
    /// that would justify changing this constant is a sustained rise in that
    /// histogram's upper percentiles without a matching rise in registry-side
    /// latency: that combination means the gate, not the registry, is the
    /// constraint.
    /// </para>
    /// </summary>
    internal const int GlobalMaxConcurrentReads = 16;

    /// <summary>
    /// How long a computed per-silo budget is reused before membership is
    /// consulted again. Bounds how long the gate can run on a stale silo count
    /// after a membership change, and exists only to keep a membership lookup off
    /// the path of every single read. It is not a data cache: no registry content
    /// is retained for any period, ever.
    /// </summary>
    private const int BudgetRefreshMillis = 5_000;

    /// <summary>
    /// The most tree ids one batched round trip will carry. Caps the size of a
    /// single registry message so that a very large estate widens the number of
    /// batches rather than producing one unboundedly large request.
    /// <para>
    /// <strong>This is not only a message-size cap: it multiplies with
    /// <see cref="GlobalMaxConcurrentReads"/> to set the concurrency that
    /// reaches the backing tree.</strong> <c>ILatticeRegistry.GetEntriesAsync</c>
    /// does not perform one read per batch. It issues one concurrent
    /// <c>ISystemLattice.GetAsync</c> per id in the batch and awaits the whole
    /// wave, so a batch of <c>B</c> ids becomes <c>B</c> concurrent key reads
    /// downstream. With <c>P</c> permits in flight the ceiling reaching the tree
    /// is therefore <c>P * B</c>, currently 16 * 64 = 1024, not 16.
    /// </para>
    /// <para>
    /// <strong>Read this before widening the batch to buy fewer round trips.</strong>
    /// Batching reduces the number of messages arriving at the registry
    /// singleton; it does not reduce the concurrent work, it relocates and
    /// re-expands it one-for-one further down. Doubling this constant halves the
    /// round trips and doubles the downstream fan-out. The two constants are a
    /// single budget expressed as a product, and
    /// <c>RegistryFanInGateTests.The_downstream_ceiling_is_the_product_of_the_two_constants</c>
    /// exists to fail if either is changed without that being considered.
    /// </para>
    /// <para>
    /// <strong>Why the product is nonetheless safe here.</strong> A batch only
    /// ever carries ids that are actually waiting, and each waiting id is
    /// distinct, so the downstream concurrency is
    /// <c>min(distinct queued ids, P * B)</c>. Without this gate every one of
    /// those callers issues its own single-key read concurrently, so the
    /// downstream concurrency is the unbounded caller count itself. The gate
    /// therefore never raises downstream concurrency above what the ungated path
    /// already produces; it is identical below the product and capped above it,
    /// where the ungated path is capped by nothing. The amplification inside
    /// <c>GetEntriesAsync</c> is pre-existing behaviour that this gate inherits
    /// rather than introduces. What has <em>not</em> been measured is whether the
    /// product is comfortable for the terminal shard-root activation; the claim
    /// made here is only the comparative one, that this path is never worse than
    /// the path it replaces.
    /// </para>
    /// </summary>
    internal const int MaxBatchSize = 64;

    private long _budgetComputedAt;
    private bool _budgetComputed;
    private int _budget = GlobalMaxConcurrentReads;

    /// <summary>
    /// This silo's current share of <see cref="GlobalMaxConcurrentReads"/>: the
    /// cluster ceiling divided by the live silo count, floored at one. Read under
    /// <see cref="_sync"/>.
    /// </summary>
    private int CurrentBudget()
    {
        var now = Environment.TickCount64;

        // Tracked with an explicit flag rather than a sentinel timestamp: a
        // sentinel of long.MinValue would make the elapsed subtraction overflow
        // and read as "just computed", so the budget would never be taken from
        // membership at all and every silo would silently keep the whole cluster
        // ceiling - which is exactly the per-silo bound this type rejects.
        if (_budgetComputed && now - _budgetComputedAt < BudgetRefreshMillis)
        {
            return _budget;
        }

        var silos = 1;
        try
        {
            // Local in-memory membership snapshot - no RPC, no grain call.
            var active = siloStatusOracle?.GetApproximateSiloStatuses(onlyActive: true);
            if (active is { Count: > 0 })
            {
                silos = active.Count;
            }
        }
        catch (Exception)
        {
            // Membership is an optimisation for sizing the share, never a
            // correctness input. A silo that cannot read it falls back to the
            // single-silo assumption rather than failing a registry read.
            silos = 1;
        }

        _budget = Math.Max(1, GlobalMaxConcurrentReads / silos);
        _budgetComputedAt = now;
        _budgetComputed = true;
        return _budget;
    }

    private readonly object _sync = new();

    /// <summary>Ids waiting for capacity, each with the callers awaiting it.</summary>
    private readonly Dictionary<string, TaskCompletionSource<TreeRegistryEntry?>> _waiting =
        new(StringComparer.Ordinal);

    /// <summary>Arrival order of <see cref="_waiting"/>, so no id can be starved.</summary>
    private readonly Queue<string> _arrivals = new();

    /// <summary>
    /// When each waiting id was enqueued, so the admission wait the bound imposes
    /// can be measured rather than inferred. See
    /// <see cref="LatticeMetrics.RegistryAdmissionWait"/> for why this must be
    /// observable: bounding fan-in queues work rather than removing it, and every
    /// other registry signal is scoped to the registry grain and so cannot see a
    /// stall that relocated to the caller.
    /// </summary>
    private readonly Dictionary<string, long> _enqueuedAt = new(StringComparer.Ordinal);

    /// <summary>Ids whose round trip has been dispatched and not yet completed.</summary>
    private readonly Dictionary<string, Task<TreeRegistryEntry?>> _dispatched =
        new(StringComparer.Ordinal);

    private int _inFlight;

    /// <summary>
    /// Reads <paramref name="treeId"/>'s registry entry under the gate's
    /// concurrency bound, joining a read already waiting or already in flight for
    /// the same id rather than adding another.
    /// </summary>
    /// <param name="treeId">The tree id to read.</param>
    /// <returns>The registry entry, or <c>null</c> when the tree is not registered.</returns>
    internal Task<TreeRegistryEntry?> GetEntryAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        TaskCompletionSource<TreeRegistryEntry?> waiter;
        int depthAtArrival;
        lock (_sync)
        {
            if (_waiting.TryGetValue(treeId, out var queued))
            {
                return queued.Task;
            }

            if (_dispatched.TryGetValue(treeId, out var running))
            {
                return running;
            }

            waiter = new TaskCompletionSource<TreeRegistryEntry?>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            _waiting.Add(treeId, waiter);
            _arrivals.Enqueue(treeId);
            _enqueuedAt[treeId] = Stopwatch.GetTimestamp();

            // Recorded inside the lock, and counting this arrival, so the value
            // is the offered fan-in at a single consistent instant. This is the
            // one gate instrument that moves when the bound is NOT binding, which
            // is what lets a flat wait / width / batch-size reading be attributed
            // to absent demand rather than to comfortable headroom.
            depthAtArrival = _waiting.Count;
        }

        LatticeMetrics.RegistryAdmissionQueueDepth.Record(depthAtArrival, LatticeTenantLabel.Platform);
        Pump();
        return waiter.Task;
    }

    /// <summary>
    /// Resolves the physical tree id for <paramref name="treeId"/> under the same
    /// bound, matching <see cref="ILatticeRegistry.ResolveAsync"/> exactly: the
    /// pinned physical id when one is set, otherwise the logical id itself.
    /// </summary>
    /// <param name="treeId">The logical tree id to resolve.</param>
    /// <returns>The physical tree id.</returns>
    internal async Task<string> ResolveAsync(string treeId)
    {
        var entry = await GetEntryAsync(treeId).ConfigureAwait(false);
        return entry?.PhysicalTreeId ?? treeId;
    }

    /// <summary>
    /// Reads the persisted <see cref="ShardMap"/> for <paramref name="treeId"/>
    /// under the same bound, matching
    /// <see cref="ILatticeRegistry.GetShardMapAsync"/>.
    /// <para>
    /// The map is returned as a copy. <see cref="ShardMap"/> is a mutable class,
    /// and a grain call would have handed each caller its own deep copy, so
    /// returning the shared entry's instance to two callers that joined the same
    /// read would silently alias state that used to be private to each. Copying
    /// keeps the caller-visible contract identical to the un-gated call.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree whose shard map to read.</param>
    /// <returns>The shard map, or <c>null</c> when the tree uses the default identity map.</returns>
    internal async Task<ShardMap?> GetShardMapAsync(string treeId)
    {
        var entry = await GetEntryAsync(treeId).ConfigureAwait(false);
        if (entry?.ShardMap is not { } map)
        {
            return null;
        }

        return new ShardMap
        {
            Slots = (int[])map.Slots.Clone(),
            Version = map.Version,
        };
    }

    /// <summary>
    /// Dispatches as many batches as the concurrency bound currently allows,
    /// draining <see cref="_waiting"/> in arrival order.
    /// </summary>
    private void Pump()
    {
        while (true)
        {
            List<string> ids;
            List<TaskCompletionSource<TreeRegistryEntry?>> waiters;
            int widthAtDispatch;

            lock (_sync)
            {
                if (_inFlight >= CurrentBudget() || _arrivals.Count == 0)
                {
                    return;
                }

                var take = Math.Min(MaxBatchSize, _arrivals.Count);
                ids = new List<string>(take);
                waiters = new List<TaskCompletionSource<TreeRegistryEntry?>>(take);

                while (ids.Count < take && _arrivals.Count > 0)
                {
                    var id = _arrivals.Dequeue();
                    if (!_waiting.Remove(id, out var waiter))
                    {
                        _enqueuedAt.Remove(id);
                        continue;
                    }

                    if (_enqueuedAt.Remove(id, out var enqueuedAt))
                    {
                        LatticeMetrics.RegistryAdmissionWait.Record(
                            Stopwatch.GetElapsedTime(enqueuedAt).TotalMilliseconds,
                            LatticeTenantLabel.Platform);
                    }

                    ids.Add(id);
                    waiters.Add(waiter);
                    _dispatched[id] = waiter.Task;
                }

                if (ids.Count == 0)
                {
                    continue;
                }

                _inFlight++;
                widthAtDispatch = _inFlight;
            }

            // Recorded outside the lock, but from values captured inside it, so
            // the pair is consistent without holding the lock across a metric
            // call. Width counts THIS dispatch, so it reaches
            // GlobalMaxConcurrentReads exactly when the ceiling is reached; see
            // LatticeMetrics.RegistryAdmissionInFlight for why that convention
            // differs from its neighbours on purpose.
            LatticeMetrics.RegistryAdmissionInFlight.Record(widthAtDispatch, LatticeTenantLabel.Platform);
            LatticeMetrics.RegistryAdmissionBatchSize.Record(ids.Count, LatticeTenantLabel.Platform);

            _ = DispatchAsync(ids, waiters);
        }
    }

    /// <summary>
    /// Runs one round trip for <paramref name="ids"/> and publishes its result to
    /// <paramref name="waiters"/>, then releases the capacity it held.
    /// </summary>
    private async Task DispatchAsync(
        List<string> ids,
        List<TaskCompletionSource<TreeRegistryEntry?>> waiters)
    {
        TreeRegistryEntry?[]? results = null;
        Exception? failure = null;

        try
        {
            var registry = grainFactory.GetLatticeRegistry();
            results = new TreeRegistryEntry?[ids.Count];

            if (ids.Count == 1)
            {
                // A batch of one is not a batch. Issuing the single-key read keeps
                // an un-contended silo on byte-for-byte the traffic it had before
                // the gate existed.
                results[0] = await registry.GetEntryAsync(ids[0]).ConfigureAwait(false);
            }
            else
            {
                var entries = await registry.GetEntriesAsync(ids).ConfigureAwait(false);
                for (var i = 0; i < ids.Count; i++)
                {
                    // Unregistered ids are absent from the batched result, which is
                    // the same null the single-key read returns for them.
                    results[i] = entries.TryGetValue(ids[i], out var entry) ? entry : null;
                }
            }
        }
        catch (Exception ex)
        {
            failure = ex;
        }

        lock (_sync)
        {
            foreach (var id in ids)
            {
                _dispatched.Remove(id);
            }

            _inFlight--;
        }

        // Publish after the bookkeeping above, so a continuation that immediately
        // asks for the same id starts a fresh read rather than joining a finished
        // one. This gate shares round trips in progress; it is not a cache, and it
        // must not become one by accident.
        for (var i = 0; i < waiters.Count; i++)
        {
            if (failure is not null)
            {
                // Every joined caller observes the fault it would have observed on
                // its own read. Sharing one failure is not new exposure: the
                // alternative is N identical failures against a registry that is
                // already not answering.
                waiters[i].TrySetException(failure);
            }
            else
            {
                waiters[i].TrySetResult(results![i]);
            }
        }

        Pump();
    }
}
