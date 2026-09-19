using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Orleans.Hosting;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Anti-censoring tests for the grain-call observation channel installed by
/// <see cref="LatticeServiceCollectionExtensions.AddLatticeGrainCallObservation"/>.
/// <para>
/// <b>What these tests are guarding against is not an absent instrument but a
/// present and useless one.</b> The channel this replaces - the
/// <c>NonReentrancyQueueSize=</c> clause of Orleans' near-timeout diagnostic -
/// is not missing data; it reports queue depth confidently and consistently, and
/// its population is selected so that the deepest queues in a cluster
/// contribute nothing to it. Two independent extractions from it agreed exactly
/// and were both wrong, because reproducibility validates the arithmetic and
/// says nothing about the sampling frame.
/// </para>
/// <para>
/// So a test that merely asserts "the histogram recorded something" would pass
/// against a faithful reimplementation of that same defect. These tests instead
/// drive a grain that is <em>genuinely</em> queued and assert the channel
/// reports a non-empty depth, and simultaneously assert that the post-dequeue
/// view of the same activation stays pinned at one - the reading the censored
/// seam produces. If the channel is ever re-sited at that seam, the first
/// assertion fails while the second still holds, and the failure names the
/// cause.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeGrainCallObservationTests
{
    private const int Concurrency = 5;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUpAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDownAsync()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    /// <summary>
    /// The load-bearing test. A grain type whose calls are genuinely queued must
    /// be visible on the channel <b>while the cluster is healthy</b>: no
    /// timeout, no fault, and no dependence on the queued request being the one
    /// that reports.
    /// </summary>
    [Test]
    public async Task GrainCallOutstandingDepth_when_calls_are_queued_behind_one_another_reports_a_non_empty_depth()
    {
        var key = "probe-" + Guid.NewGuid().ToString("N");

        var depths = new ConcurrentBag<int>();
        using var depthListener = MeterListening.StartForInstrument(
            LatticeMetrics.GrainCallOutstandingDepth,
            listener => listener.SetMeasurementEventCallback<int>(
                (_, value, tags, _) =>
                {
                    if (IsProbe(tags))
                    {
                        depths.Add(value);
                    }
                }));

        var durations = new ConcurrentBag<double>();
        using var durationListener = MeterListening.StartForInstrument(
            LatticeMetrics.GrainCallDuration,
            listener => listener.SetMeasurementEventCallback<double>(
                (_, value, tags, _) =>
                {
                    if (IsProbe(tags) && HasOutcome(tags, "completed"))
                    {
                        durations.Add(value);
                    }
                }));

        var fanOut = _cluster.GrainFactory.GetGrain<IQueueDepthFanOutGrain>("driver-" + key);
        var fanOutTask = fanOut.FanOutAsync(key, Concurrency);

        try
        {
            await WaitUntilAsync(
                () => depths.Count >= Concurrency && QueueDepthProbeGrain.EnteredCount(key) >= 1,
                "the fan-out to dispatch every call and the probe to occupy its activation");

            // The channel sees the queue. A depth of Concurrency - 1 means the
            // last call was dispatched with every earlier one still outstanding,
            // which is precisely the state the near-timeout diagnostic cannot
            // describe unless the cluster is already failing.
            Assert.That(
                depths.Max(),
                Is.GreaterThanOrEqualTo(Concurrency - 1),
                "the observation channel must report the depth of a genuinely queued activation");

            // The contrast. This is what an incoming-call filter, or any counter
            // inside the grain body, is able to see: the scheduler admits one
            // call at a time to a non-reentrant grain, so concurrency there is
            // one and a depth derived from it is zero however deep the queue is.
            // Asserting it here is what makes this test fail - rather than
            // quietly still pass - if the channel is re-sited post-dequeue.
            Assert.That(
                QueueDepthProbeGrain.MaxConcurrentExecutions(key),
                Is.EqualTo(1),
                "the post-dequeue view must stay pinned at one, which is why it cannot carry this signal");
        }
        finally
        {
            QueueDepthProbeGrain.Release(key);
        }

        await fanOutTask;

        // Nothing timed out or faulted, and yet the depth above was observed:
        // the channel does not condition on failure the way the diagnostic it
        // replaces does.
        Assert.That(fanOutTask.IsCompletedSuccessfully, Is.True, "the fan-out must complete normally");
        Assert.That(
            durations.Count,
            Is.GreaterThanOrEqualTo(Concurrency),
            "every completed call must contribute a completion-latency sample");
    }

    /// <summary>
    /// A call that is dispatched to an idle activation must record a depth of
    /// zero rather than being omitted. Without this, an implementation could
    /// record only non-zero depths and produce a series whose every sample shows
    /// contention - the mirror image of the censoring defect, and equally
    /// misleading.
    /// </summary>
    [Test]
    public async Task GrainCallOutstandingDepth_when_the_target_is_idle_records_a_zero_depth_sample()
    {
        var key = "idle-" + Guid.NewGuid().ToString("N");

        var depths = new ConcurrentBag<int>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.GrainCallOutstandingDepth,
            l => l.SetMeasurementEventCallback<int>(
                (_, value, tags, _) =>
                {
                    if (IsProbe(tags))
                    {
                        depths.Add(value);
                    }
                }));

        QueueDepthProbeGrain.Release(key);
        var fanOut = _cluster.GrainFactory.GetGrain<IQueueDepthFanOutGrain>("driver-" + key);
        await fanOut.FanOutAsync(key, 1);

        Assert.That(depths, Does.Contain(0), "an uncontended dispatch must still contribute a sample");
    }

    /// <summary>
    /// Completion latency must be recorded on its own outcome arm, separately
    /// from faults. A single undifferentiated duration series would mix
    /// completion latency with a population pinned at the message timeout, which
    /// is how the diagnostic's execution-duration clause came to have no
    /// discriminating variance at all.
    /// </summary>
    [Test]
    public async Task GrainCallDuration_when_a_call_completes_normally_records_a_completed_outcome_sample()
    {
        var key = "duration-" + Guid.NewGuid().ToString("N");

        var completed = new ConcurrentBag<double>();
        var faulted = new ConcurrentBag<double>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.GrainCallDuration,
            l => l.SetMeasurementEventCallback<double>(
                (_, value, tags, _) =>
                {
                    if (!IsProbe(tags))
                    {
                        return;
                    }

                    if (HasOutcome(tags, "completed"))
                    {
                        completed.Add(value);
                    }
                    else if (HasOutcome(tags, "faulted"))
                    {
                        faulted.Add(value);
                    }
                }));

        QueueDepthProbeGrain.Release(key);
        var fanOut = _cluster.GrainFactory.GetGrain<IQueueDepthFanOutGrain>("driver-" + key);
        await fanOut.FanOutAsync(key, 1);

        Assert.That(completed, Is.Not.Empty, "a normal completion must record on the completed arm");
        Assert.That(faulted, Is.Empty, "a normal completion must not record on the faulted arm");
    }

    private static bool IsProbe(ReadOnlySpan<KeyValuePair<string, object?>> tags) =>
        HasTag(tags, LatticeMetrics.TagGrainType, QueueDepthProbeGrain.GrainTypeName);

    private static bool HasOutcome(ReadOnlySpan<KeyValuePair<string, object?>> tags, string outcome) =>
        HasTag(tags, LatticeMetrics.TagOutcome, outcome);

    private static bool HasTag(
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        string key,
        string value)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, key, StringComparison.Ordinal)
                && string.Equals(tag.Value as string, value, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string what)
    {
        var deadline = Stopwatch.GetTimestamp() + (long)(Stopwatch.Frequency * 20);
        while (!condition())
        {
            if (Stopwatch.GetTimestamp() > deadline)
            {
                Assert.Fail("Timed out waiting for " + what + ".");
            }

            await Task.Delay(25);
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Called twice on purpose: the registration is documented as
            // idempotent, and a second filter instance would double every
            // recorded sample and halve every reported depth.
            siloBuilder.AddLatticeGrainCallObservation();
            siloBuilder.AddLatticeGrainCallObservation();
        }
    }
}
