using System.Diagnostics;
using System.Text;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Efficiency and overhead guardrail tests for the live metrics surface. These
/// prove the two properties the add-on promises operationally: concurrent
/// observers coalesce onto a single shared sampling loop (server cost is
/// O(trees + shards) per tick, never O(observers)), and foreground writes stay
/// prompt while many readers and subscribers are active.
/// </summary>
[Category("Integration")]
[TestFixture]
public class LatticeMetricsEfficiencyGuardrailTests
{
    // Generous wall-clock budget for a batch of foreground writes performed
    // while the dashboard surface is under concurrent read + subscribe load.
    // Sized well above observed local timings so the assertion only trips on a
    // genuine regression (e.g. the metrics feed back-pressuring the write path),
    // not on CI jitter.
    private const double WriteBudgetSeconds = 30.0;

    private const int WriteBatch = 50;

    private MetricsObservationClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new MetricsObservationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task many_subscribers_share_a_single_sampling_loop()
    {
        await _fixture.CreatePopulatedTreeAsync("guardrail-coalesce", keyCount: 16, shardCount: 2);

        const int subscriberCount = 8;
        var request = new TreeMetricsRequest
        {
            IncludeShardHotness = true,
            IncludeViewLag = true,
        };

        var pumps = new List<(Task Pump, List<TreeMetricsSnapshot> Snapshots, CancellationTokenSource Cts)>();
        for (var i = 0; i < subscriberCount; i++)
        {
            pumps.Add(_fixture.ObserveInBackground(request));
        }

        try
        {
            // All subscribers share one request signature, so exactly one loop
            // must service them regardless of how many attach.
            var oneLoop = await MetricsObservationClusterFixture.WaitUntilAsync(
                () => _fixture.Sampler.ActiveSamplerCount == 1 && _fixture.Sampler.TotalSampleCount > 0,
                TimeSpan.FromSeconds(10));
            Assert.That(oneLoop, Is.True, "subscribers should coalesce onto a single sampling loop");

            // Over a fixed window the shared loop samples about once per cadence
            // tick (100ms here). If each subscriber drove its own loop the count
            // would grow ~subscriberCount-fold; assert it stays far below that.
            var start = _fixture.Sampler.TotalSampleCount;
            await Task.Delay(TimeSpan.FromMilliseconds(600));
            var delta = _fixture.Sampler.TotalSampleCount - start;

            Assert.Multiple(() =>
            {
                Assert.That(_fixture.Sampler.ActiveSamplerCount, Is.EqualTo(1));
                Assert.That(delta, Is.GreaterThan(0), "the shared loop should keep sampling");
                Assert.That(
                    delta,
                    Is.LessThan(subscriberCount * 4),
                    "sampling passes must track ticks, not the subscriber count");
            });
        }
        finally
        {
            await CancelAllAsync(pumps);
        }

        // Once every subscriber detaches the shared loop must be torn down, so
        // an idle dashboard leaves no residual sampling timer running.
        var drained = await MetricsObservationClusterFixture.WaitUntilAsync(
            () => _fixture.Sampler.ActiveSamplerCount == 0,
            TimeSpan.FromSeconds(5));
        Assert.That(drained, Is.True, "the loop should stop when the last subscriber detaches");

        // Draining the sampler count is necessary but not sufficient: prove the
        // loop is genuinely halted by confirming the cumulative sample counter
        // stops advancing once the last subscriber has gone.
        var quiescedCount = _fixture.Sampler.TotalSampleCount;
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        Assert.That(_fixture.Sampler.TotalSampleCount, Is.EqualTo(quiescedCount),
            "no further sampling passes may run after the last subscriber detaches");
    }

    [Test]
    public async Task writes_stay_prompt_while_many_readers_and_subscribers_are_active()
    {
        var tree = await _fixture.CreatePopulatedTreeAsync("guardrail-load", keyCount: 32, shardCount: 2);

        using var loadCts = new CancellationTokenSource();
        var request = new TreeMetricsRequest { IncludeShardHotness = true };

        var subscribers = new List<(Task Pump, List<TreeMetricsSnapshot> Snapshots, CancellationTokenSource Cts)>();
        for (var i = 0; i < 6; i++)
        {
            subscribers.Add(_fixture.ObserveInBackground(request));
        }

        // Hammer the read facade concurrently with the metrics subscriptions so
        // the write path below contends with the full dashboard surface.
        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            while (!loadCts.IsCancellationRequested)
            {
                await _fixture.Query.GetTreeSummaryAsync("guardrail-load", deep: true, loadCts.Token);
                await _fixture.Query.GetShardSummariesAsync("guardrail-load", deep: false, loadCts.Token);
            }
        })).ToArray();

        try
        {
            // Let the sampler and readers warm up so writes are timed against a
            // genuinely loaded silo.
            await MetricsObservationClusterFixture.WaitUntilAsync(
                () => _fixture.Sampler.TotalSampleCount > 2,
                TimeSpan.FromSeconds(5));

            var stopwatch = Stopwatch.StartNew();
            for (var i = 0; i < WriteBatch; i++)
            {
                await tree.SetAsync(
                    MetricsObservationClusterFixture.KeyAt(1000 + i),
                    Encoding.UTF8.GetBytes($"load-{i:D5}"));
            }

            stopwatch.Stop();

            Assert.That(
                stopwatch.Elapsed.TotalSeconds,
                Is.LessThan(WriteBudgetSeconds),
                "foreground writes must stay prompt under concurrent read + subscribe load");
        }
        finally
        {
            loadCts.Cancel();
            try
            {
                await Task.WhenAll(readers);
            }
            catch (OperationCanceledException)
            {
            }

            await CancelAllAsync(subscribers);
        }
    }

    private static async Task CancelAllAsync(
        List<(Task Pump, List<TreeMetricsSnapshot> Snapshots, CancellationTokenSource Cts)> pumps)
    {
        foreach (var pump in pumps)
        {
            pump.Cts.Cancel();
        }

        try
        {
            await Task.WhenAll(pumps.Select(p => p.Pump));
        }
        catch (OperationCanceledException)
        {
        }

        foreach (var pump in pumps)
        {
            pump.Cts.Dispose();
        }
    }
}
