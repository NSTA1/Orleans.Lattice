using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// <c>/cluster</c>'s re-measure of every shard as a tracked cluster operation
/// (#4126): it starts once the cluster id is typed, its progress in trees is
/// followed, it can be stopped, a success re-reads the refreshed figures, a
/// failed start or failed re-measure says why, and a re-measure already running
/// is picked up on arrival.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterRemeasureTests : ClusterTestContext
{
    private static readonly DateTimeOffset Sampled = new(2026, 9, 30, 8, 0, 0, TimeSpan.Zero);

    /// <summary>Registers the re-measure operations and a cached summary.</summary>
    public ClusterRemeasureTests()
    {
        Refreshes = new FakeStorageUsageOperations();
        Services.AddKeyedSingleton<ILatticeStorageUsageOperations>(ShellFacades.Key, Refreshes);
        Admin.GetStorageUsageAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ClusterStorageUsageSummary { TreeCount = 12, TotalBytes = Remeasured ? 4096 : 1024, SampledAt = Sampled });
    }

    private FakeStorageUsageOperations Refreshes { get; }

    private bool Remeasured { get; set; }

    private IRenderedComponent<ClusterPage> StartRemeasure()
    {
        var cut = RenderAt("/cluster");
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Re-measure every shard..."), Is.True));
        Button(cut, "Re-measure every shard...").Click();
        ConfirmTyping(cut, "lattice-prod");
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Stop re-measuring"), Is.True));
        return cut;
    }

    private void Tick()
    {
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers >= 1, TimeSpan.FromSeconds(10)), Is.True, "the follower re-arms");
        Time.Advance(ClusterStatusPoller.Interval);
    }

    [Test]
    public void A_re_measure_runs_on_the_cluster_with_its_progress_followed_and_then_re_reads_the_figures()
    {
        var cut = StartRemeasure();
        var id = Refreshes.Latest!;

        Assert.That(Refreshes.CountOf(nameof(ILatticeStorageUsageOperations.StartStorageUsageRefreshAsync)), Is.EqualTo(1));
        Admin.DidNotReceive().GetStorageUsageAsync(true, Arg.Any<CancellationToken>());
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-cluster=remeasure-progress] [role=progressbar]").GetAttribute("aria-label"), Is.EqualTo("Storage re-measure progress")));

        Refreshes.Progress(id, StorageUsageRefreshOperation.MeasuringPhase, 3, 12, StorageUsageRefreshOperation.TreesUnit);
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Measuring"));
            Assert.That(cut.Find("[role=progressbar]").GetAttribute("aria-valuenow"), Is.EqualTo("25"));
        });

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("1.0 KiB")));
        Remeasured = true;
        Refreshes.Succeed(id, new ClusterStorageUsageSummary { TreeCount = 12, TotalBytes = 4096, Deep = true, SampledAt = Sampled });
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[data-lt-cluster=remeasure-progress]"), Is.Empty);
            Assert.That(cut.Markup, Does.Contain("4.0 KiB"), "the refreshed figures are read again");
            Assert.That(cut.Markup, Does.Contain("Re-measured by a leaf walk"));
            Assert.That(HasButton(cut, "Re-measure every shard..."), Is.True);
        });
        Admin.DidNotReceive().GetStorageUsageAsync(true, Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_re_measure_can_be_stopped()
    {
        var cut = StartRemeasure();
        var id = Refreshes.Latest!;

        cut.Find("[data-lt-cluster=stop-measure]").Click();

        cut.WaitUntil(() => Assert.That(Refreshes.Calls, Does.Contain((nameof(ILatticeOperations.CancelOperationAsync), (object?)id))));
        Refreshes.Cancel(id);
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-cluster=remeasure-error]").TextContent, Is.EqualTo("The re-measure was stopped before it finished."));
            Assert.That(cut.Markup, Does.Not.Contain("Re-measured by a leaf walk"));
            Assert.That(HasButton(cut, "Re-measure every shard..."), Is.True);
        });
    }

    [Test]
    public void A_re_measure_that_failed_on_the_cluster_gives_its_reason()
    {
        var cut = StartRemeasure();

        Refreshes.Fail(Refreshes.Latest!, "The registry could not be read.");
        Tick();

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-cluster=remeasure-error]").TextContent, Is.EqualTo("The re-measure failed. The registry could not be read.")));
    }

    [Test]
    public void A_re_measure_that_could_not_start_says_why()
    {
        Refreshes.StartFault = new UnauthorizedAccessException();
        var cut = RenderAt("/cluster");
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Re-measure every shard..."), Is.True));

        Button(cut, "Re-measure every shard...").Click();
        ConfirmTyping(cut, "lattice-prod");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-cluster=remeasure-error]").TextContent, Is.EqualTo(ClusterFaults.Describe(new UnauthorizedAccessException())));
            Assert.That(HasButton(cut, "Re-measure every shard..."), Is.True);
        });
    }

    [Test]
    public void A_stop_the_cluster_refuses_says_why_while_the_re_measure_runs_on()
    {
        Refreshes.CancelFault = new UnauthorizedAccessException();
        var cut = StartRemeasure();

        cut.Find("[data-lt-cluster=stop-measure]").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-cluster=remeasure-error]").TextContent, Is.EqualTo(ClusterFaults.Describe(new UnauthorizedAccessException())));
            Assert.That(HasButton(cut, "Stop re-measuring"), Is.True);
        });
    }

    [Test]
    public void A_re_measure_already_running_is_followed_on_arrival()
    {
        Refreshes.Statuses["elsewhere"] = Refreshes.Queued("elsewhere") with
        {
            State = LatticeOperationState.Running,
            Phase = StorageUsageRefreshOperation.MeasuringPhase,
            CompletedUnits = 6,
            TotalUnits = 12,
        };

        var cut = RenderAt("/cluster");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-cluster=remeasure-progress] [role=progressbar]").GetAttribute("aria-valuenow"), Is.EqualTo("50"));
            Assert.That(HasButton(cut, "Stop re-measuring"), Is.True);
        });
        Assert.That(Refreshes.CountOf(nameof(ILatticeStorageUsageOperations.StartStorageUsageRefreshAsync)), Is.Zero);
    }

    [Test]
    public void A_finished_re_measure_is_not_followed_on_arrival()
    {
        Refreshes.Statuses["earlier"] = Refreshes.Queued("earlier");
        Refreshes.Fail("earlier", "old");

        var cut = RenderAt("/cluster");

        cut.WaitUntil(() =>
        {
            Assert.That(Refreshes.CountOf(nameof(ILatticeOperations.ListOperationsAsync)), Is.EqualTo(1));
            Assert.That(HasButton(cut, "Re-measure every shard..."), Is.True);
            Assert.That(cut.FindAll("[data-lt-cluster=remeasure-progress]"), Is.Empty);
            Assert.That(cut.FindAll("[data-lt-cluster=remeasure-error]"), Is.Empty, "an old failure is not reported as this page's");
        });
    }
}
