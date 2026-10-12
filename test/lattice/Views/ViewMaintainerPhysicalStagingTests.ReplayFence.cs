using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewMaintainerPhysicalStagingTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Rebind_to_an_empty_physical_log_retires_every_old_replay_fence_component(bool directRebuild)
    {
        var h = await Harness.CreateAsync();
        h.State.State.ReplayFenceWalTreeId = Old;
        h.State.State.ReplayFenceOffsets = new Dictionary<int, long> { [0] = 8 };
        h.State.State.ReplayFenceStagedTransactions = [Transaction];
        h.State.State.ReplayFenceSupersession = new Dictionary<string, HybridLogicalClock>
        {
            ["new-only"] = Clock(99),
        };
        await h.Grain.DrainAsync();
        Assert.That(h.State.State.ReplayFenceWalTreeId, Is.EqualTo(Old),
            "The old head is still unpassed before the physical rebind.");

        h.Alias = New;
        if (directRebuild)
        {
            await h.Grain.RebuildAsync();
        }
        else
        {
            await h.Grain.NotifySourceIdentityChangedAsync(New);
            await h.Grain.DrainAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(New));
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(1));
            Assert.That(h.State.State.ReplayFenceWalTreeId, Is.Null);
            Assert.That(h.State.State.ReplayFenceOffsets, Is.Null);
            Assert.That(h.State.State.ReplayFenceStagedTransactions, Is.Null);
            Assert.That(h.State.State.ReplayFenceSupersession, Is.Null);
        });
    }
}
