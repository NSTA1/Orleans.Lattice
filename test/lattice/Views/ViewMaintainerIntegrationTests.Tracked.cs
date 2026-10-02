using Orleans.Lattice.Operations;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// The tracked view rebuild and reconcile (#4124): <see cref="IViewMaintainerGrain.RebuildTrackedAsync"/>
/// and <see cref="IViewMaintainerGrain.ReconcileTrackedAsync"/> relay their phases to the
/// coordinated operation they name, and a cancelled operation stops the rebuild before
/// it swaps, leaving the active generation serving.
/// </summary>
public partial class ViewMaintainerIntegrationTests
{
    private async Task<(ILatticeOperationGrain Grain, LatticeOperationTicket Ticket)> BeginViewOperationAsync(params string[] phases)
    {
        var ticket = LatticeOperationTicket.For("default", LatticeOperationKey.NewId());
        var grain = _fixture.Cluster.Client.GetGrain<ILatticeOperationGrain>(ticket.OperationKey);
        await grain.BeginAsync(new LatticeOperationBeginRequest { Kind = "treeadmin.view-rebuild", Phases = phases });
        return (grain, ticket);
    }

    private IViewMaintainerGrain Maintainer(string viewName) =>
        _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);

    [Test]
    public async Task RebuildTrackedAsync_relays_its_phases_and_preserves_the_view()
    {
        var src = _fixture.Source(ViewClusterFixture.CountSource);
        await src.SetAsync("trb-1", ViewClusterFixture.AggValue("trackedg"));
        await src.SetAsync("trb-2", ViewClusterFixture.AggValue("trackedg"));
        var view = await ViewAsync(ViewClusterFixture.CountView);
        await view.WaitForSourceHeadAsync(Barrier);
        var (operation, ticket) = await BeginViewOperationAsync(
            LatticeMaintenanceProgress.Scanning, LatticeMaintenanceProgress.Projecting, LatticeMaintenanceProgress.Swapping);

        await Maintainer(ViewClusterFixture.CountView).RebuildTrackedAsync(ticket);
        var record = await operation.GetAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(record!.Phase, Is.EqualTo(LatticeMaintenanceProgress.Swapping));
            Assert.That(record.PhaseIndex, Is.EqualTo(2));
            Assert.That(await view.GetAggregateInt64Async("trackedg"), Is.EqualTo(2L));
        });
    }

    [Test]
    public async Task ReconcileTrackedAsync_on_a_matching_view_ends_comparing_without_a_swap()
    {
        var src = _fixture.Source(ViewClusterFixture.MaxSource);
        await src.SetAsync("trec-1", ViewClusterFixture.AggValue("trecg", 3));
        var view = await ViewAsync(ViewClusterFixture.MaxView);
        await view.WaitForSourceHeadAsync(Barrier);
        var (operation, ticket) = await BeginViewOperationAsync(
            LatticeMaintenanceProgress.Digesting,
            LatticeMaintenanceProgress.Scanning,
            LatticeMaintenanceProgress.Projecting,
            LatticeMaintenanceProgress.Comparing,
            LatticeMaintenanceProgress.Swapping);

        var repaired = await Maintainer(ViewClusterFixture.MaxView).ReconcileTrackedAsync(ticket);
        var record = await operation.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(repaired, Is.False);
            Assert.That(record!.Phase, Is.EqualTo(LatticeMaintenanceProgress.Comparing));
        });
    }

    [Test]
    public async Task A_cancelled_tracked_rebuild_stops_before_the_swap_and_keeps_serving()
    {
        var src = _fixture.Source(ViewClusterFixture.SumSource);
        await src.SetAsync("tcx-1", ViewClusterFixture.AggValue("tcxg", 4));
        var view = await ViewAsync(ViewClusterFixture.SumView);
        await view.WaitForSourceHeadAsync(Barrier);
        var (operation, ticket) = await BeginViewOperationAsync(
            LatticeMaintenanceProgress.Scanning, LatticeMaintenanceProgress.Projecting, LatticeMaintenanceProgress.Swapping);
        await operation.RequestCancelAsync();

        Assert.That(
            async () => await Maintainer(ViewClusterFixture.SumView).RebuildTrackedAsync(ticket),
            Throws.InstanceOf<OperationCanceledException>());

        var record = await operation.GetAsync();
        Assert.Multiple(async () =>
        {
            Assert.That(record!.Phase, Is.Not.EqualTo(LatticeMaintenanceProgress.Swapping));
            Assert.That(await view.GetAggregateDoubleAsync("tcxg"), Is.EqualTo(4d));
        });
    }
}
