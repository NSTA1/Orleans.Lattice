using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Dashboard;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Tests.Federation;
using NUnit.Framework;
using Orleans.Runtime;

namespace MultiSiteManufacturing.Tests.Dashboard;

/// <summary>
/// Covers the failure-aware back-off added to the
/// <see cref="DashboardBroadcaster"/> rebuild loop (issue #1069): when summary
/// upserts start failing - as they do when the shared summary-view WAL
/// partition saturates and Phase-2 commits time out - the loop must shed load
/// (exponentially growing the delay, capped) instead of retrying at full rate
/// and congestion-collapsing the silo, and must snap back to its normal cadence
/// the moment drains succeed again.
/// </summary>
/// <remarks>
/// The live saturation (a real storage tier timing out under a 5000-part
/// burst) cannot be reproduced in the in-memory test cluster, so these tests
/// drive the back-off state machine directly through its test seams. The
/// end-to-end effect (a saturated silo recovering) is verified operationally.
/// </remarks>
[TestFixture]
public sealed class DashboardBroadcasterBackpressureTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public Task SetUp() => (_fixture = new FederationTestClusterFixture()).InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    private DashboardBroadcaster NewBroadcaster(TimeSpan rebuildInterval)
    {
        var (router, _, _) = _fixture.NewRouter();
        var streamId = StreamId.Create(
            DashboardBroadcaster.StreamNamespace,
            $"broadcast-{Guid.NewGuid():N}");
        var broadcaster = new DashboardBroadcaster(
            router,
            _fixture.Cluster.Client,
            _fixture.NewPartCrdtStore(),
            NullLogger<DashboardBroadcaster>.Instance,
            streamId,
            // A long interval keeps the background loop from racing the
            // deterministic seam-driven drive below.
            partRebuildInterval: rebuildInterval);
        broadcaster.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return broadcaster;
    }

    [Test]
    public async Task Steady_state_uses_the_base_rebuild_interval()
    {
        var interval = TimeSpan.FromSeconds(1);
        await using var broadcaster = NewBroadcaster(interval);

        Assert.Multiple(() =>
        {
            Assert.That(broadcaster.ConsecutiveFailedDrainsForTest, Is.EqualTo(0));
            Assert.That(broadcaster.ComputeRebuildDelayForTest(), Is.EqualTo(interval),
                "with no failed drains the loop waits exactly the configured interval");
        });
    }

    [Test]
    public async Task Repeated_failed_drains_grow_the_delay_exponentially_up_to_the_cap()
    {
        var interval = TimeSpan.FromSeconds(1);
        await using var broadcaster = NewBroadcaster(interval);

        // First failure doubles the delay; each further failure doubles again.
        broadcaster.RecordDrainOutcomeForTest(failures: 3);
        Assert.That(broadcaster.ConsecutiveFailedDrainsForTest, Is.EqualTo(1));
        Assert.That(broadcaster.ComputeRebuildDelayForTest(), Is.EqualTo(interval * 2));

        broadcaster.RecordDrainOutcomeForTest(failures: 1);
        Assert.That(broadcaster.ComputeRebuildDelayForTest(), Is.EqualTo(interval * 4));

        broadcaster.RecordDrainOutcomeForTest(failures: 1);
        Assert.That(broadcaster.ComputeRebuildDelayForTest(), Is.EqualTo(interval * 8));

        // Keep failing well past the point the geometric series would exceed
        // the ceiling; the delay must clamp at MaxRebuildBackoff and never grow
        // beyond it (nor overflow).
        for (var i = 0; i < 40; i++)
        {
            broadcaster.RecordDrainOutcomeForTest(failures: 1);
        }

        var capped = broadcaster.ComputeRebuildDelayForTest();
        Assert.That(capped, Is.EqualTo(DashboardBroadcaster.MaxRebuildBackoffForTest),
            "a sustained failure streak must clamp the delay at the configured ceiling");
        Assert.That(capped, Is.LessThanOrEqualTo(DashboardBroadcaster.MaxRebuildBackoffForTest));
    }

    [Test]
    public async Task A_successful_drain_resets_the_back_off_to_the_base_interval()
    {
        var interval = TimeSpan.FromSeconds(1);
        await using var broadcaster = NewBroadcaster(interval);

        // Accumulate a back-off, then a clean drain (zero failures) must snap
        // the loop back to its normal cadence immediately.
        broadcaster.RecordDrainOutcomeForTest(failures: 2);
        broadcaster.RecordDrainOutcomeForTest(failures: 2);
        Assert.That(broadcaster.ComputeRebuildDelayForTest(), Is.GreaterThan(interval));

        broadcaster.RecordDrainOutcomeForTest(failures: 0);

        Assert.Multiple(() =>
        {
            Assert.That(broadcaster.ConsecutiveFailedDrainsForTest, Is.EqualTo(0));
            Assert.That(broadcaster.ComputeRebuildDelayForTest(), Is.EqualTo(interval),
                "a clean drain must clear the back-off so the loop resumes full cadence");
        });
    }

    [Test]
    public async Task A_healthy_drain_reports_no_failures()
    {
        // Against the in-memory cluster every upsert succeeds, so a real drain
        // (via the public seam) must report zero failures and leave the loop at
        // its base cadence - the success path that keeps steady state fast.
        var interval = TimeSpan.FromMilliseconds(50);
        await using var broadcaster = NewBroadcaster(interval);

        var failures = await broadcaster.DrainDirtyForTestAsync();
        Assert.That(failures, Is.EqualTo(0),
            "a drain with a healthy storage tier must report no upsert failures");
    }
}
