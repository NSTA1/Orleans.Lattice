using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewMaintainerMaintenanceCancellationTests
{
    [Test]
    public async Task EnsureBound_source_swap_heal_advances_in_bounded_slices_before_swapping()
    {
        var h = Create(bound: Retired, sourceKeyCount: 5);
        h.Grain.HealSliceKeyBudget = 2;

        // Three projection slices (2 + 2 + 1 keys); the first also begins and scans,
        // the last one swaps.
        const int stepsBeforeSwap = 2;
        for (var step = 0; step < stepsBeforeSwap; step++)
        {
            Assert.That(await h.Grain.EnsureBoundForTestingAsync(h.Registration), Is.True,
                "a heal in progress must short-circuit the drain");
            Assert.Multiple(() =>
            {
                Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(0),
                    $"step {step} must not publish the generation before the final slice");
                Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(Retired));
            });
        }

        Assert.That(await h.Grain.EnsureBoundForTestingAsync(h.Registration), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1));
            Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(First));
        });
        await h.Reader.Received(1).GetHeadOffsetAsync(First, 0, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnsureBound_heal_restarts_after_a_decommission_cancellation_invalidates_it()
    {
        var h = Create(bound: Retired, sourceKeyCount: 5);
        h.Grain.HealSliceKeyBudget = 2;
        await h.Grain.EnsureBoundForTestingAsync(h.Registration);
        await h.Grain.EnsureBoundForTestingAsync(h.Registration);
        await h.Reader.Received(1).GetHeadOffsetAsync(First, 0, Arg.Any<CancellationToken>());

        await h.Grain.CancelMaintenanceForDecommissionAsync();
        await h.Grain.EnsureBoundForTestingAsync(h.Registration);

        await h.Reader.Received(2).GetHeadOffsetAsync(First, 0, Arg.Any<CancellationToken>());
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(0),
            "the restarted heal must begin again rather than resume the invalidated one");
    }
}
