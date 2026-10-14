using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>Independent operation reads and the refresh after settlement survive partial failures.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterSummaryResilienceTests : ClusterTestContext
{
    private const string TreeId = "orders";

    [Test]
    public async Task A_denied_operation_does_not_stop_following_another_running_operation()
    {
        Seed();
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new TreeReshardStatus { TreeId = TreeId, InProgress = true }),
                _ => Task.FromException<TreeReshardStatus>(new UnauthorizedAccessException()));
        Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeSnapshotStatus { TreeId = TreeId, InProgress = true },
                new TreeSnapshotStatus { TreeId = TreeId, InProgress = true, CopiedShardCount = 1, ShardCount = 2 },
                new TreeSnapshotStatus { TreeId = TreeId });
        var cut = RenderAt("/cluster/trees/orders");
        cut.WaitUntil(() => Assert.That(Time.ArmedTimers, Is.EqualTo(1)));

        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("1 of 2 shards copied.")));
        cut.WaitUntil(() => Assert.That(Time.ArmedTimers, Is.EqualTo(1)));
        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=progressbar]"), Is.Empty));
        Assert.That(cut.Markup, Does.Contain(ClusterFaults.Describe(new UnauthorizedAccessException())));
        await Admin.Received(2).GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_failed_settlement_read_is_retried_without_losing_the_finished_operation(bool aliasFails)
    {
        Seed();
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeReshardStatus { TreeId = TreeId, InProgress = true },
                new TreeReshardStatus { TreeId = TreeId });
        if (aliasFails)
        {
            Admin.ResolveTreeAliasAsync(TreeId, Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new TreeAliasResolution { TreeId = TreeId, PhysicalTreeId = TreeId }),
                    _ => Task.FromException<TreeAliasResolution>(new TimeoutException()),
                    _ => Task.FromResult(new TreeAliasResolution { TreeId = TreeId, PhysicalTreeId = "replacement", IsAliased = true }));
        }
        else
        {
            Admin.GetTreeStatsAsync(TreeId, Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new TreeStatsReport { TreeId = TreeId, TotalLiveKeys = 1 }),
                    _ => Task.FromException<TreeStatsReport>(new TimeoutException()),
                    _ => Task.FromResult(new TreeStatsReport { TreeId = TreeId, TotalLiveKeys = 987 }));
        }

        var cut = RenderAt("/cluster/trees/orders");
        cut.WaitUntil(() => Assert.That(Time.ArmedTimers, Is.EqualTo(1)));
        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=progressbar]"), Is.Empty));
        cut.WaitUntil(() => Assert.That(Time.ArmedTimers, Is.EqualTo(1), "a failed settlement refresh must be retried"));
        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Delay(1)));

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain(aliasFails ? "Aliased: this name" : "987")));
        cut.WaitUntil(() => Assert.That(Time.ArmedTimers, Is.Zero));
        await Admin.Received(2).GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>());
        await Admin.Received(2).GetTreeConfigAsync(TreeId, Arg.Any<CancellationToken>());
    }

    private void Seed()
    {
        Admin.GetTreeStatsAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeStatsReport { TreeId = TreeId });
        Admin.ResolveTreeAliasAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeAliasResolution { TreeId = TreeId, PhysicalTreeId = TreeId });
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeResizeStatus { TreeId = TreeId });
        Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeSnapshotStatus { TreeId = TreeId });
    }
}
