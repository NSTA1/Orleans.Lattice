using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the replay permit queue-wait histogram (issue #2873).
/// <para>
/// <b>What this fixture exists to establish.</b>
/// <c>leaf_activation_failures_total{reason="canceled_awaiting_permit"}</c> is
/// the dominant activation failure in the deployment - acceptance run 12
/// measured roughly 946 cancelled activations in a thirty-minute window - and it
/// cannot discriminate. The reason tag is assigned from the admission phase, so
/// it is honest about <i>where</i> an activation died and silent about
/// <i>why</i>: the Orleans request deadline spans the whole grain call, so an
/// activation that burned its budget upstream arrives at the gate already doomed
/// and is cancelled there within seconds. The count is identical whether the
/// gate was saturated for the full budget or idle throughout. Only the duration
/// separates the two, which is what this histogram records.
/// </para>
/// <para>
/// Every test here touches the process-wide gate, so each restores what it took
/// and is <see cref="NonParallelizableAttribute"/>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Collects every queue-wait sample recorded while the scope is open.
    /// </summary>
    private static IDisposable ListenForQueueWaitSamples(
        ConcurrentBag<(double Value, KeyValuePair<string, object?>[] Tags)> sink) =>
        MeterListening.StartForInstrument(
            LatticeMetrics.WalReplayPermitQueueWait,
            l => l.SetMeasurementEventCallback<double>(
                (_, value, tags, _) => sink.Add((value, tags.ToArray()))));

    /// <summary>
    /// Takes every permit out of the gate and returns how many it removed, so a
    /// following activation is forced to queue rather than admitted immediately.
    /// </summary>
    private static int DrainEveryReplayPermit(SemaphoreSlim gate)
    {
        var taken = 0;
        while (gate.Wait(0))
            taken++;
        return taken;
    }

    /// <summary>
    /// Reads the single sample carrying <paramref name="outcome"/>, asserting
    /// first that exactly one was recorded.
    /// </summary>
    private static (double Value, KeyValuePair<string, object?>[] Tags) SingleSampleWithOutcome(
        ConcurrentBag<(double Value, KeyValuePair<string, object?>[] Tags)> samples,
        string outcome)
    {
        var matching = samples
            .Where(s => s.Tags.Any(t =>
                t.Key == LatticeMetrics.TagOutcome && (string?)t.Value == outcome))
            .ToArray();

        Assert.That(matching, Has.Length.EqualTo(1),
            $"expected exactly one '{outcome}' queue-wait sample, got {matching.Length}. "
            + $"All observed outcomes: [{string.Join(", ", samples.SelectMany(s => s.Tags)
                .Where(t => t.Key == LatticeMetrics.TagOutcome)
                .Select(t => t.Value))}]");

        return matching[0];
    }

    [Test]
    [NonParallelizable]
    public async Task Acquiring_a_replay_permit_records_a_queue_wait_sample_tagged_acquired()
    {
        // The hot arm: it fires on every replay admission, so if it were missing
        // the instrument would only ever describe failures and could never
        // establish the "gate was idle" half of the discrimination.
        var gate = await QuiescentReplayGateAsync();
        var samples = new ConcurrentBag<(double Value, KeyValuePair<string, object?>[] Tags)>();

        using (ListenForQueueWaitSamples(samples))
        {
            await ActivateWithCleanReplayAsync();
        }

        Assert.That(samples, Is.Not.Empty,
            "an admitted replay must record a queue-wait sample. Without the acquired arm the "
            + "histogram describes only cancellations, and a reading could never distinguish a "
            + "saturated gate from an idle one");

        var sample = SingleSampleWithOutcome(samples, "acquired");
        Assert.That(sample.Value, Is.GreaterThanOrEqualTo(0));
        Assert.That(gate.CurrentCount, Is.EqualTo(BPlusLeafGrain.ReplayConcurrencyCeilingForTest),
            "the activation must have returned its permit, or a later test starts depressed");
    }

    [Test]
    [NonParallelizable]
    public async Task A_queue_wait_sample_carries_the_tree_so_a_reading_can_be_scoped_to_one_tree()
    {
        // Not decoration. A corpus-wide aggregate read as though it were
        // tree-scoped is exactly how an acceptance run scored a pass whose true
        // tree-scoped value was a fail, so the tag that makes a per-tree reading
        // possible is part of the fix rather than a convenience. Note the
        // sibling adaptation counter is deliberately NOT tree-tagged - its
        // quantity is a process-wide ceiling - so this cannot be inferred from
        // the neighbouring instrument and has to be pinned here.
        var gate = await QuiescentReplayGateAsync();
        var samples = new ConcurrentBag<(double Value, KeyValuePair<string, object?>[] Tags)>();

        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 0,
            walHead: 0);
        var treeId = UniqueReplayPermitTree();
        state.State.TreeId = treeId;

        using (ListenForQueueWaitSamples(samples))
        {
            await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        }

        var sample = SingleSampleWithOutcome(samples, "acquired");

        Assert.Multiple(() =>
        {
            Assert.That(
                sample.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == treeId),
                Is.True,
                $"the sample must name the tree it belongs to. Tags were: "
                + $"[{string.Join(", ", sample.Tags.Select(t => $"{t.Key}={t.Value}"))}]");

            Assert.That(
                sample.Tags.Any(t => t.Key == LatticeTenantLabel.TagTenant),
                Is.True,
                "the derived tenant dimension must be present on every emission site");
        });

        Assert.That(gate.CurrentCount, Is.EqualTo(BPlusLeafGrain.ReplayConcurrencyCeilingForTest));
    }

    [Test]
    [NonParallelizable]
    public async Task A_replay_that_actually_queued_records_the_time_it_spent_queued()
    {
        // THE CLAUSE THAT GIVES THE INSTRUMENT ITS MEANING. Every other test here
        // is satisfied by a build that records a constant zero: the sample exists,
        // it carries the right tags, and it is filed under the right outcome. Only
        // this one fails against that build, and a constant zero is precisely the
        // failure that would matter, because the whole reading rule is "a
        // distribution near the request budget means real starvation; a
        // distribution of a fraction of a second means the gate is innocent". An
        // instrument stuck at zero answers "innocent" every time.
        var gate = await QuiescentReplayGateAsync();
        var samples = new ConcurrentBag<(double Value, KeyValuePair<string, object?>[] Tags)>();
        var heldPermits = 0;
        var queuedFor = TimeSpan.FromMilliseconds(250);

        try
        {
            heldPermits = DrainEveryReplayPermit(gate);
            Assert.That(heldPermits, Is.GreaterThan(0),
                "instrument validation: the gate must have had permits to take, or the activation "
                + "below is never forced to queue and this test measures nothing");

            using (ListenForQueueWaitSamples(samples))
            {
                var activation = ActivateWithCleanReplayAsync();

                // Hold the gate shut for a known interval, then admit.
                await Task.Delay(queuedFor);
                gate.Release();
                heldPermits--;

                // Bounded rather than awaited outright: if admission regressed
                // this would otherwise hang, burn the blame-hang timeout, and
                // abort the whole run instead of reporting one failed clause.
                var completed = await Task.WhenAny(activation, Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.That(completed, Is.SameAs(activation),
                    "the activation never acquired a permit after one was released");
                await activation;
            }

            var sample = SingleSampleWithOutcome(samples, "acquired");

            // The tolerance is one-sided and generous downward: Task.Delay may
            // overshoot but never undershoots by much, and the assertion that
            // matters is that the recorded value tracks REAL elapsed time rather
            // than being a constant.
            Assert.That(sample.Value, Is.GreaterThanOrEqualTo(queuedFor.TotalMilliseconds * 0.5),
                $"a replay held out of the gate for {queuedFor.TotalMilliseconds} ms recorded a "
                + $"wait of {sample.Value} ms. A value at or near zero means the histogram is not "
                + "measuring the queue at all, which would make every reading say 'the gate is "
                + "innocent' regardless of what the gate did");
        }
        finally
        {
            gate.Release(heldPermits);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_cancellation_while_queued_records_a_queue_wait_sample_tagged_canceled()
    {
        // The arm that does the actual discriminating. This is the population
        // counted by `canceled_awaiting_permit`, and the duration recorded here is
        // what separates "waited out its whole budget on a saturated gate" from
        // "arrived with no budget left and was cancelled immediately".
        var gate = await QuiescentReplayGateAsync();
        var samples = new ConcurrentBag<(double Value, KeyValuePair<string, object?>[] Tags)>();
        var heldPermits = 0;

        try
        {
            heldPermits = DrainEveryReplayPermit(gate);
            Assert.That(heldPermits, Is.GreaterThan(0),
                "instrument validation: with permits still available the activation would be "
                + "admitted rather than queued, and no cancellation could be observed");

            using var cts = new CancellationTokenSource();
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null,
                persistedCheckpoint: 0,
                walHead: 0);
            state.State.TreeId = UniqueReplayPermitTree();

            using (ListenForQueueWaitSamples(samples))
            {
                var activation = ((IGrainBase)grain).OnActivateAsync(cts.Token);

                // Let it reach the queue, then cancel it there.
                await Task.Delay(TimeSpan.FromMilliseconds(150));
                await cts.CancelAsync();

                var completed = await Task.WhenAny(activation, Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.That(completed, Is.SameAs(activation),
                    "the cancelled activation never returned. A cancellation that cannot unwind is "
                    + "a hang, not a failure, and would abort the run rather than report");

                Assert.That(async () => await activation, Throws.InstanceOf<OperationCanceledException>(),
                    "a cancellation while queued must surface, not be swallowed");
            }

            var sample = SingleSampleWithOutcome(samples, "canceled");
            Assert.That(sample.Value, Is.GreaterThanOrEqualTo(0));
        }
        finally
        {
            gate.Release(heldPermits);
        }
    }

    [Test]
    [NonParallelizable]
    public void Recording_a_queue_wait_sample_allocates_nothing_per_call()
    {
        // This instrument fires on EVERY replay admission - the mass-reactivation
        // path the replay gate exists to relieve - so an allocation here is a
        // measuring instrument degrading the thing it measures, in the exact
        // regime where it matters.
        //
        // The assertion is made against measured bytes rather than against any
        // proxy for them. Tag COUNT in particular is not a proxy: on this runtime
        // `params ReadOnlySpan<KeyValuePair<string, object?>>` is stack-allocated
        // at every arity, so one tag and five tags both cost nothing. What costs
        // is a tag VALUE that has to be created per call - a boxed value type, a
        // computed string, or a materialised array. Both controls below are
        // therefore about values, not counts.
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, "alloc-probe");
        var tenantTag = LatticeTenantLabel.ForTree("alloc-probe");

        // A listener must be attached for the measurement path to run at all. With
        // no listener the instrument is disabled and Record returns before it
        // touches its tags, so this test would pass for the wrong reason -
        // vacuously, having measured a disabled no-op.
        var observed = 0;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalReplayPermitQueueWait,
            l => l.SetMeasurementEventCallback<double>((_, _, _, _) => observed++));

        Assert.That(LatticeMetrics.WalReplayPermitQueueWait.Enabled, Is.True,
            "instrument validation: the instrument must be enabled, or Record short-circuits and "
            + "this test measures a disabled no-op rather than the emission path");

        var emissionBytes = MeasureAllocationsPerCall(
            () => LatticeMetrics.WalReplayPermitQueueWait.Record(
                1.0, treeTag, LatticeMetrics.PermitQueueWaitAcquired, tenantTag));

        // Known-positive control 1: a materialised tag array, the cost of handing
        // Record a real array instead of letting the params span stack-allocate.
        var arrayBytes = MeasureAllocationsPerCall(
            () => LatticeMetrics.WalReplayPermitQueueWait.Record(
                1.0, new[] { treeTag, LatticeMetrics.PermitQueueWaitAcquired, tenantTag }));

        // Known-positive control 2: a value-typed tag boxed inline. This is the
        // regression a future editor is actually liable to introduce here - adding
        // a shard number, a retry count, or a bool - and it is invisible at the
        // call site, because it looks exactly like the string-valued tags beside
        // it. Hoisting the pair into a static field boxes once and costs nothing.
        var boxedBytes = MeasureAllocationsPerCall(
            () => LatticeMetrics.WalReplayPermitQueueWait.Record(
                1.0, treeTag, LatticeMetrics.PermitQueueWaitAcquired,
                new KeyValuePair<string, object?>(LatticeMetrics.TagShard, 0)));

        Assert.That(observed, Is.GreaterThan(0),
            "instrument validation: the listener must actually have received measurements, or "
            + "none of the figures below describe the emission path");

        Assert.Multiple(() =>
        {
            // The detector proves itself before it is believed. An apparatus
            // reporting zero because it cannot see allocation is byte-identical to
            // one reporting zero because there is none, so both known-allocating
            // shapes are measured on the same harness in the same run.
            Assert.That(arrayBytes, Is.GreaterThan(0),
                "detector validation: passing a materialised array is known to allocate it. "
                + "Measuring zero here means this test cannot detect allocation at all, so its "
                + "verdict on the real emission shape is worthless");

            Assert.That(boxedBytes, Is.GreaterThan(0),
                "detector validation: boxing a value-typed tag is known to allocate the box. "
                + "Measuring zero here means this test cannot detect the one regression shape it "
                + "most needs to catch");

            Assert.That(emissionBytes, Is.Zero,
                $"the queue-wait emission must allocate nothing per call, but allocated "
                + $"{emissionBytes} bytes. This fires on every replay admission, so a per-call "
                + "allocation lands on the mass-reactivation path this gate exists to relieve. "
                + "Note the cause will not be the number of tags - it will be a tag VALUE that is "
                + "built per call. Hoist it into a static field, or derive it from a cache as "
                + "LatticeTenantLabel.ForTree does");
        });
    }

    [Test]
    [NonParallelizable]
    public async Task No_queue_wait_tag_emitted_by_an_activation_carries_a_boxed_value()
    {
        // The companion to the allocation test above, and it exists because that
        // test alone does not cover this. It measures a hand-built replica of the
        // emission shape, so it stays green against a production site that added a
        // boxed tag - the replica is not the call site. Substituting a boxed shard
        // tag into RecordReplayPermitQueueWait was confirmed to leave it green,
        // which is an uncovered sibling site rather than a hypothetical.
        //
        // This clause closes it from the other end: it reads the tags a REAL
        // activation actually emitted, and requires every value to be a reference
        // type. A boxed value type is invisible at the call site - it looks exactly
        // like the string-valued tags beside it - and costs 24 bytes on every
        // replay admission.
        var gate = await QuiescentReplayGateAsync();
        var samples = new ConcurrentBag<(double Value, KeyValuePair<string, object?>[] Tags)>();

        using (ListenForQueueWaitSamples(samples))
        {
            await ActivateWithCleanReplayAsync();
        }

        var sample = SingleSampleWithOutcome(samples, "acquired");

        Assert.That(sample.Tags, Is.Not.Empty,
            "instrument validation: a sample with no tags would satisfy the loop below vacuously");

        Assert.Multiple(() =>
        {
            foreach (var tag in sample.Tags)
            {
                Assert.That(tag.Value, Is.Not.Null.And.InstanceOf<string>(),
                    $"tag '{tag.Key}' carries a {tag.Value?.GetType().Name ?? "null"} value. Every "
                    + "queue-wait tag value must be a string: this emission runs on every replay "
                    + "admission, and a value type boxed into object? allocates per call on the "
                    + "mass-reactivation path the gate exists to relieve. Hoist the pair into a "
                    + "static field so the box is created once");
            }
        });

        Assert.That(gate.CurrentCount, Is.EqualTo(BPlusLeafGrain.ReplayConcurrencyCeilingForTest));
    }

    /// <summary>
    /// Mean bytes allocated per invocation of <paramref name="action"/>, after a
    /// warm-up long enough to settle tiered JIT and any first-call caching.
    /// </summary>
    /// <remarks>
    /// <see cref="GC.GetAllocatedBytesForCurrentThread"/> is per-thread, so
    /// unrelated background activity cannot pollute the reading.
    /// </remarks>
    private static long MeasureAllocationsPerCall(Action action)
    {
        const int Warmup = 512;
        const int Iterations = 512;

        for (var i = 0; i < Warmup; i++)
            action();

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Iterations; i++)
            action();
        var after = GC.GetAllocatedBytesForCurrentThread();

        return (after - before) / Iterations;
    }
}
