using System.Collections.Concurrent;
using System.Diagnostics;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// A silo-wide <see cref="IOutgoingGrainCallFilter"/> that observes, per grain
/// type, how many calls to a target activation were already outstanding from
/// this silo at the moment a further call was dispatched to it, and how long
/// each call took to complete. Registered by
/// <see cref="LatticeServiceCollectionExtensions.AddLatticeGrainCallObservation(Hosting.ISiloBuilder)"/>,
/// which is opt-in, so a host that does not call it pays nothing.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists.</b> Orleans' per-activation non-reentrancy queue is
/// described by exactly one channel out of the box: the
/// <c>Response did not arrive on time</c> timeout diagnostic, which prints a
/// <c>NonReentrancyQueueSize=</c> clause from the target activation's diagnostic
/// dump. That channel is <b>censored</b> in two compounding ways, and the
/// censoring is invisible in the data it produces:
/// </para>
/// <list type="number">
/// <item>
/// It is emitted only for a request that is already approaching the message
/// timeout, so the population it describes is truncated at that threshold -
/// every sample sits within a few seconds of the timeout and the distribution
/// carries no discriminating variance.
/// </item>
/// <item>
/// The clause describes the <em>emitting request's own</em> wait, so a request
/// slow for any other reason (a long grain turn, storage latency, a cold
/// activation) contributes a sample whose queue delay is exactly zero. A grain
/// type whose calls are genuinely queued but which never itself trips the
/// timeout contributes <b>no samples at all</b>.
/// </item>
/// </list>
/// <para>
/// The observable consequence is that a grain type can carry the deepest
/// non-reentrancy queues in a cluster and contribute zero rows to the only
/// channel that mentions queueing, so an extraction from that channel reports
/// no queueing and is internally consistent. Two independent extractions
/// agreeing is not evidence of correctness when both apply the same selection
/// predicate: reproducibility validates the arithmetic, not the sampling frame.
/// </para>
/// <para>
/// <b>What this filter measures instead.</b> The depth is recorded at
/// <em>dispatch</em>, before the call is awaited and unconditionally on every
/// outgoing call, so no timeout, fault, or slow turn is required for a sample to
/// exist and the population is not truncated by any threshold. That is the
/// property <see cref="LatticeMetrics.GrainCallOutstandingDepth"/> is for, and
/// it is why the seam is the <em>outgoing</em> filter.
/// </para>
/// <para>
/// <b>Why not the incoming seam.</b> An <see cref="IIncomingGrainCallFilter"/>
/// runs after the scheduler has dequeued the request, so on a non-reentrant
/// grain the in-flight count it can observe is always exactly one and a depth
/// derived from it pins at zero no matter how deep the real queue is. That is
/// the same censoring as the timeout diagnostic wearing different clothes, and
/// it is already demonstrated in production by
/// <see cref="LatticeMetrics.LeafCommitInFlight"/>. Re-siting this measurement
/// at the incoming seam therefore reintroduces the defect it exists to remove;
/// <c>LatticeGrainCallObservationTests</c> fails if that happens.
/// </para>
/// <para>
/// <b>Precise semantics, and what the number is not.</b> The recorded value is
/// the count of calls to the same target activation that were <em>issued from
/// this silo and had not yet completed</em> when this call was dispatched. For a
/// non-reentrant grain that is <c>running + waiting</c> minus the call being
/// recorded, so it relates to Orleans' own <c>NonReentrancyQueueSize</c> as
/// approximately <c>NonReentrancyQueueSize == value</c> when the target is
/// executing one of them, but the two are not the same quantity and must not be
/// presented as equal. Three limits are load-bearing:
/// </para>
/// <list type="bullet">
/// <item>
/// <b>Per-silo.</b> Only calls issued from this silo are counted. Calls to the
/// same activation from another silo or from an external Orleans client are
/// invisible here and make the value an <em>under</em>-estimate. It is a floor
/// on contention, never a ceiling.
/// </item>
/// <item>
/// <b>Reentrancy changes the meaning, not the value.</b> For a
/// <c>[Reentrant]</c> grain, or a method marked
/// <c>[AlwaysInterleave]</c>, outstanding calls interleave rather than queue, so
/// a high value means pipelining and not contention. Read the value against the
/// target grain type's reentrancy, which this filter cannot determine.
/// </item>
/// <item>
/// <b>It counts dispatch, not admission.</b> A call is counted from the moment
/// this filter runs until its task completes, which includes network transit and
/// the response hop, not only time on the target's queue.
/// </item>
/// </list>
/// </remarks>
internal sealed class LatticeGrainCallObservationFilter : IOutgoingGrainCallFilter
{
    /// <summary>
    /// Outstanding calls per target activation. An entry exists only while at
    /// least one call to that activation is in flight, so the dictionary is
    /// bounded by live concurrency rather than by the number of activations the
    /// silo has ever addressed.
    /// </summary>
    private readonly ConcurrentDictionary<GrainId, Outstanding> _outstanding = new();

    /// <summary>
    /// Frozen per-grain-type tag pairs, so the hot path does not allocate a
    /// grain-type string per call. Bounded by the number of distinct grain types
    /// this silo calls, which is a property of the deployed application and not
    /// of its traffic.
    /// </summary>
    private readonly ConcurrentDictionary<GrainType, KeyValuePair<string, object?>> _grainTypeTags = new();

    /// <inheritdoc />
    public async Task Invoke(IOutgoingGrainCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var target = context.TargetId;
        var tag = GrainTypeTag(target.Type);
        var slot = Enter(target, out var depth);

        // Recorded before the call is awaited, so a sample exists for every
        // dispatched call whatever becomes of it. Recording on completion
        // instead would drop exactly the calls that never complete, which is
        // the population a saturation investigation is looking for.
        LatticeMetrics.GrainCallOutstandingDepth.Record(depth, tag, LatticeTenantLabel.Platform);

        var start = Stopwatch.GetTimestamp();
        var faulted = true;
        try
        {
            await context.Invoke().ConfigureAwait(false);
            faulted = false;
        }
        finally
        {
            Exit(target, slot);
            LatticeMetrics.GrainCallDuration.Record(
                Stopwatch.GetElapsedTime(start).TotalMilliseconds,
                tag,
                faulted ? FaultedOutcome : CompletedOutcome,
                LatticeTenantLabel.Platform);
        }
    }

    /// <summary>
    /// The <see cref="LatticeMetrics.TagOutcome"/> value for a call that
    /// returned a response, however slowly.
    /// </summary>
    private static readonly KeyValuePair<string, object?> CompletedOutcome =
        new(LatticeMetrics.TagOutcome, "completed");

    /// <summary>
    /// The <see cref="LatticeMetrics.TagOutcome"/> value for a call that threw,
    /// which includes the message timeout. Separating the two is what keeps the
    /// duration histogram from being contaminated by the timeout population that
    /// censors the near-timeout diagnostic: a reader wanting completion latency
    /// selects <c>completed</c> and is not silently handed a distribution pinned
    /// at the timeout threshold.
    /// </summary>
    private static readonly KeyValuePair<string, object?> FaultedOutcome =
        new(LatticeMetrics.TagOutcome, "faulted");

    /// <summary>
    /// Registers one more outstanding call against <paramref name="target"/> and
    /// reports how many were already outstanding.
    /// </summary>
    /// <param name="target">The target activation.</param>
    /// <param name="depth">The count outstanding immediately before this call.</param>
    /// <returns>The counter this call must later release.</returns>
    private Outstanding Enter(GrainId target, out int depth)
    {
        while (true)
        {
            var slot = _outstanding.GetOrAdd(target, static _ => new Outstanding());
            lock (slot)
            {
                // A counter that reached zero on another thread is retired and
                // about to leave the dictionary. Joining it would split one
                // activation's concurrency across two counters and undercount,
                // so retry and take (or create) the live one instead.
                if (slot.Retired)
                {
                    continue;
                }

                depth = slot.Count;
                slot.Count++;
                return slot;
            }
        }
    }

    /// <summary>
    /// Releases one outstanding call, retiring and removing the counter when it
    /// reaches zero so the dictionary stays bounded by live concurrency.
    /// </summary>
    /// <param name="target">The target activation.</param>
    /// <param name="slot">The counter returned by <see cref="Enter"/>.</param>
    private void Exit(GrainId target, Outstanding slot)
    {
        lock (slot)
        {
            if (--slot.Count > 0)
            {
                return;
            }

            slot.Retired = true;
        }

        // Keyed on the pair, so a counter that was already replaced by a
        // concurrent Enter is never removed out from under its new owner.
        _outstanding.TryRemove(new KeyValuePair<GrainId, Outstanding>(target, slot));
    }

    /// <summary>Resolves the cached tag pair naming <paramref name="type"/>.</summary>
    /// <param name="type">The target grain type.</param>
    /// <returns>The frozen <see cref="LatticeMetrics.TagGrainType"/> tag.</returns>
    private KeyValuePair<string, object?> GrainTypeTag(GrainType type) =>
        _grainTypeTags.GetOrAdd(
            type,
            static t => new KeyValuePair<string, object?>(LatticeMetrics.TagGrainType, t.ToString()));

    /// <summary>
    /// The mutable outstanding-call count for one target activation. Guarded by
    /// its own monitor rather than by <see cref="Interlocked"/> so that reaching
    /// zero, retiring, and removal are one atomic decision - a lock-free
    /// decrement would let a concurrent arrival increment a counter that is
    /// already on its way out of the dictionary.
    /// </summary>
    private sealed class Outstanding
    {
        /// <summary>Calls dispatched to this activation and not yet completed.</summary>
        public int Count;

        /// <summary>Set when the count reached zero and the counter is leaving.</summary>
        public bool Retired;
    }
}
