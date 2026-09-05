using System.Collections.Concurrent;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Process-wide, caller-side instrumentation of the <b>durable</b>
/// leaf-materialiser pin store: how long each pin write actually took and
/// whether it faulted, recorded by <see cref="LeafCursorReporter"/> at the call
/// site rather than inside <see cref="IWalMaterialiserPinGrain"/>.
/// <para>
/// This closes the blind spot behind issue #2015. Every other materialiser
/// input to the WAL saturation signal is derived from the <i>in-memory</i>
/// cursor registry, so a durable pin store that has stopped keeping up - the
/// exact condition of issue #2012, where the retention floor stalled and the
/// WAL grew without bound - reads perfectly healthy. Measuring the durable
/// write itself is the only way that condition becomes observable.
/// </para>
/// <para>
/// The measurement is deliberately <b>caller-side</b>. A pin grain activation
/// lives on exactly one silo while the leaves reporting into it are spread
/// across the whole cluster, so a counter incremented inside the grain would
/// only ever be sampled by whichever silo happens to host that activation. The
/// saturation sampler reads process statics, so the increment has to happen in
/// the calling process for the signal to be visible where it is consumed.
/// Recording at the call site also measures what callers actually experience -
/// queueing ahead of the grain included - which is precisely what a saturation
/// signal should reflect.
/// </para>
/// <para>
/// Nothing here changes the grain contract. There is no new grain method and no
/// change to an existing signature, so a cluster running mixed builds through a
/// rolling upgrade is unaffected: an old silo simply does not record, and a new
/// silo records against whatever activation answers it.
/// </para>
/// </summary>
internal static class WalMaterialiserPinPressure
{
    /// <summary>
    /// Multiple of the previous durable pin write's own measured duration for
    /// which subsequent <i>coalescible</i> reports to the same shard are shed
    /// rather than enqueued. Mirrors the grain-side write amortisation added for
    /// issue #2012, but applied at the caller: when a shard's durable write is
    /// taking seconds, every leaf that enqueues another report during that
    /// window simply lengthens the non-reentrancy queue every other leaf is
    /// already waiting behind, and none of those reports can be serviced any
    /// sooner for having been enqueued.
    /// <para>
    /// Shedding here - and not inside the grain - is what actually reduces the
    /// queue. A grain-side shed still requires the call to reach the front of
    /// the queue before it can be refused, so it removes none of the queueing
    /// delay that made issue #2012 time out; declining to make the call removes
    /// all of it.
    /// </para>
    /// <para>
    /// Always safe: a shed report leaves the durable pin staler than the leaf's
    /// true frontier, and a stale pin only ever retains <i>more</i> WAL. The
    /// in-memory registry the WAL GC reads for its live floor is untouched, so
    /// shedding cannot advance the trim point. Explicit durability points (the
    /// birth block-pin seed and the teardown flush) are never shed.
    /// </para>
    /// </summary>
    private const long ShedAmortisationFactor = 4;

    /// <summary>
    /// Minimum measured duration, in milliseconds, that a durable pin write must
    /// take before it opens a shed window at all. Below this floor the write has
    /// demonstrated no pressure worth shedding for, so the gate stays inert.
    /// <para>
    /// The floor is load-bearing, not a tuning nicety. A pin write can fail
    /// <i>cheaply</i> - a synchronous rejection, a missing activation, a
    /// serialization fault - in a millisecond or two, and
    /// <see cref="LeafCursorReporter.NoteDurableMaterialiserFrontier"/> rolls the
    /// debounce back on exactly that failure so the next checkpoint retries. A
    /// window opened by a cheap fault would shed that retry, silently defeating
    /// the rollback contract for a write that cost nothing to attempt. Gating on
    /// demonstrated cost keeps the two mechanisms from fighting: cheap failures
    /// retry immediately, and only a write that genuinely ran long - the issue
    /// #2012 condition, where attempts cost seconds apiece and piling on more
    /// lengthens the queue everyone else waits behind - suppresses its
    /// successors.
    /// </para>
    /// </summary>
    private const long ShedTriggerFloorMs = 100;

    /// <summary>
    /// Cumulative count, per <c>(treeId, shard)</c>, of durable pin writes that
    /// either faulted or exceeded
    /// <see cref="LatticeOptions.WalSaturationMaterialiserPinLatencyThreshold"/>.
    /// Sampled by <see cref="WalSaturationSampler"/> as a delta from the prior
    /// tick, exactly like the writer-side dispatch-timeout and flush-latency
    /// counters. Bounded by the live <c>(tree, shard)</c> cardinality.
    /// </summary>
    internal static readonly ConcurrentDictionary<(string TreeId, int Shard), long> _latencyTrips = new();

    /// <summary>
    /// Per pin-shard-key shed deadline as an <see cref="Environment.TickCount64"/>
    /// value. Coalescible reports routed to a shard are shed until this instant.
    /// Bounded by the live <c>(tree, shard)</c> cardinality.
    /// </summary>
    private static readonly ConcurrentDictionary<string, long> _shedUntilTickMs =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Records the outcome of one durable pin write against
    /// <paramref name="shardKey"/>.
    /// </summary>
    /// <param name="shardKey">The pin grain key the write was issued to.</param>
    /// <param name="elapsedMs">Measured wall-clock duration of the call.</param>
    /// <param name="faulted">Whether the call threw.</param>
    /// <param name="latencyThresholdMs">
    /// The configured saturation latency threshold in milliseconds, or
    /// <c>null</c> when the input is disabled. When <c>null</c> no trip is
    /// counted, matching how the writer-side flush-latency increment site is
    /// gated on its own option; the shed gate below still runs, because shedding
    /// is a self-tuning safety behaviour rather than a reported signal.
    /// </param>
    internal static void RecordWrite(string shardKey, long elapsedMs, bool faulted, long? latencyThresholdMs)
    {
        // Self-tuning shed window: hold off coalescible reports to this shard
        // for a multiple of the duration the write just demonstrated it costs.
        // Only a write that cleared ShedTriggerFloorMs counts as evidence of
        // pressure; a fast write - healthy store, small tree, or a cheap
        // synchronous failure - leaves the gate inert so the caller's own
        // debounce rollback can retry it on the very next checkpoint.
        if (elapsedMs >= ShedTriggerFloorMs)
        {
            var until = Environment.TickCount64 + (elapsedMs * ShedAmortisationFactor);
            _shedUntilTickMs.AddOrUpdate(shardKey, until, (_, existing) => Math.Max(existing, until));
        }

        if (latencyThresholdMs is not { } thresholdMs)
        {
            return;
        }

        if (!faulted && elapsedMs < thresholdMs)
        {
            return;
        }

        var treeId = WalMaterialiserPinRouting.TreeNameFromKey(shardKey);
        var shard = WalMaterialiserPinRouting.ShardIndexFromKey(shardKey);
        _latencyTrips.AddOrUpdate((treeId, shard), 1L, static (_, existing) => existing + 1);

        LatticeMetrics.MaterialiserPinDurableWriteLatency.Record(
            elapsedMs,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
            LatticeTenantLabel.ForTree(treeId));
    }

    /// <summary>
    /// True when steady-state per-checkpoint reports routed to
    /// <paramref name="shardKey"/> should be shed because a recent durable write
    /// to that shard demonstrated the store is not keeping up. Never consulted
    /// for the birth block-pin seed or the deactivation flush, which are
    /// last-chance writes and therefore correctness-bearing.
    /// </summary>
    internal static bool ShouldShed(string shardKey)
        => _shedUntilTickMs.TryGetValue(shardKey, out var until)
            && Environment.TickCount64 < until;

    /// <summary>
    /// Clears all recorded pressure. Test seam only; production state is
    /// process-lifetime and bounded by the live <c>(tree, shard)</c> cardinality.
    /// </summary>
    internal static void ResetForTests()
    {
        _latencyTrips.Clear();
        _shedUntilTickMs.Clear();
    }

    /// <summary>
    /// Forces <paramref name="shardKey"/> into its shed window for
    /// <paramref name="durationMs"/>. Test seam only.
    /// </summary>
    internal static void ForceShedForTests(string shardKey, long durationMs)
        => _shedUntilTickMs[shardKey] = Environment.TickCount64 + durationMs;
}
