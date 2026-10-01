using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The tracked tag-index sweep (#4124): <see cref="ITagIndexReconcileGrain.RunTrackedSweepAsync"/>
/// relays its probe and repair progress to the coordinated operation it names, and a
/// cancelled operation abandons the sweep so the coordinator goes idle.
/// </summary>
public partial class LatticeTagIndexReconcileIntegrationTests
{
    private async Task<(ILatticeOperationGrain Grain, LatticeOperationTicket Ticket)> BeginOperationAsync()
    {
        var operationId = LatticeOperationKey.NewId();
        var ticket = LatticeOperationTicket.For("default", operationId);
        var grain = _cluster.GrainFactory.GetGrain<ILatticeOperationGrain>(ticket.OperationKey);
        await grain.BeginAsync(new LatticeOperationBeginRequest
        {
            Kind = "treeadmin.tag-index-reconcile",
            TreeIds = ["tag-test"],
            Phases = [LatticeMaintenanceProgress.Probing, LatticeMaintenanceProgress.Repairing],
        });
        return (grain, ticket);
    }

    [Test]
    public async Task RunTrackedSweepAsync_relays_the_repair_progress_and_removes_orphans()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("d", Bytes("1"));
        var idx = TagIndex(tree, index);
        await idx.Key("d").AddAsync(["red"]);
        await tree.DeleteAsync("d");
        var (operation, ticket) = await BeginOperationAsync();

        var report = await Coordinator(index).RunTrackedSweepAsync(ticket);
        var record = await operation.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.OrphanRowsRemoved, Is.GreaterThanOrEqualTo(1));
            Assert.That(record!.Phase, Is.EqualTo(LatticeMaintenanceProgress.Repairing));
            Assert.That(record.CompletedUnits, Is.EqualTo(1));
            Assert.That(record.TotalUnits, Is.EqualTo(1));
            Assert.That(record.UnitName, Is.EqualTo(LatticeMaintenanceProgress.Trees));
        });
    }

    [Test]
    public async Task A_cancelled_tracked_sweep_is_abandoned_and_the_coordinator_goes_idle()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var tree = Tree($"items-{sfx}");
        await tree.SetAsync("a", Bytes("1"));
        await TagIndex(tree, index).Key("a").AddAsync(["red"]);
        var (operation, ticket) = await BeginOperationAsync();
        await operation.RequestCancelAsync();

        Assert.That(
            async () => await Coordinator(index).RunTrackedSweepAsync(ticket),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(await Coordinator(index).IsIdleAsync(), Is.True,
            "An abandoned sweep must not leave the coordinator in progress, or no scheduled sweep could start.");
    }
}
