using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Tests.Operations;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The tracked WAL move (#4124): <see cref="ILatticeAdminTrackedGrain.ExecuteWalMoveTrackedAsync"/>
/// relays the copy, verification and flip progress to the coordinated operation it
/// names, and a cancellation observed before the flip leaves the source live.
/// </summary>
public sealed partial class LatticeAdminGrainWalMoveTests
{
    private const string OperationKey = "default|move-op";

    private static RecordingOperationGrain WireOperation(Harness harness)
    {
        var operation = new RecordingOperationGrain();
        harness.Factory.GetGrain<ILatticeOperationGrain>(OperationKey, null).Returns(operation);
        harness.Registry.UpdateWalPlacementAsync(TreeId, Arg.Any<long>(), Arg.Any<int>(), Arg.Any<string>())
            .Returns(ci => Task.FromResult(WalPlacementPin.Create().WithPartition((int)ci[2], (string)ci[3], (long)ci[1] + 1)));
        return operation;
    }

    private static ILatticeAdminTrackedGrain Tracked(Harness h) => h.Grain;

    private static LatticeOperationTicket Ticket => new() { OperationKey = OperationKey };

    [Test]
    public async Task ExecuteWalMoveTrackedAsync_reports_entries_copied_then_verification_and_flip()
    {
        var harness = CreateHarness();
        harness.Source.Seed(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        harness.QuiesceScript.Add(() => Quiesced(10));
        var operation = WireOperation(harness);

        var receipt = await Tracked(harness).ExecuteWalMoveTrackedAsync(
            TreeId, 0, SecondaryKey, new WalMoveOptions { CopyPageSize = 4 }, Ticket);

        var copying = operation.Reports.Where(r => r.Phase == LatticeMaintenanceProgress.Copying).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(copying.Select(r => r.CompletedUnits), Is.EqualTo(new long[] { 0, 4, 8, 10 }));
            Assert.That(copying.Select(r => r.TotalUnits), Is.All.EqualTo(10));
            Assert.That(copying[0].UnitName, Is.EqualTo(LatticeMaintenanceProgress.Entries));
            Assert.That(operation.Reports.Select(r => r.Phase).Distinct(), Is.EqualTo(new[]
            {
                LatticeMaintenanceProgress.Copying, LatticeMaintenanceProgress.Verifying, LatticeMaintenanceProgress.Flipping,
            }));
        });
    }

    [Test]
    public async Task A_cancel_signalled_at_the_flip_stops_the_move_and_releases_the_source()
    {
        var harness = CreateHarness();
        harness.Source.Seed(1, 2, 3);
        harness.QuiesceScript.Add(() => Quiesced(3));
        var operation = WireOperation(harness);
        operation.StopOnPhase = LatticeMaintenanceProgress.Flipping;

        Assert.That(
            async () => await Tracked(harness).ExecuteWalMoveTrackedAsync(TreeId, 0, SecondaryKey, null, Ticket),
            Throws.InstanceOf<OperationCanceledException>());

        await harness.Registry.DidNotReceiveWithAnyArgs().UpdateWalPlacementAsync(default!, default, default(int), default!);
        Assert.Multiple(() =>
        {
            Assert.That(harness.DeactivateCalls, Is.EqualTo(1), "The fenced source is released at once.");
            Assert.That(operation.Reports[^1].Phase, Is.EqualTo(LatticeMaintenanceProgress.Flipping));
        });
    }

    [Test]
    public void A_cancel_signalled_mid_copy_banks_the_entries_already_copied()
    {
        var harness = CreateHarness();
        harness.Source.Seed(1, 2, 3, 4, 5, 6, 7, 8);
        harness.QuiesceScript.Add(() => Quiesced(8));
        var operation = WireOperation(harness);
        operation.StopOnPhase = LatticeMaintenanceProgress.Copying;

        Assert.That(
            async () => await Tracked(harness).ExecuteWalMoveTrackedAsync(
                TreeId, 0, SecondaryKey, new WalMoveOptions { CopyPageSize = 2 }, Ticket),
            Throws.InstanceOf<OperationCanceledException>());

        Assert.Multiple(() =>
        {
            Assert.That(harness.DeactivateCalls, Is.EqualTo(1));
            Assert.That(harness.Target.AppendedOffsets, Has.Count.LessThan(8), "The copy stopped part-way.");
            Assert.That(operation.Reports[^1].Phase, Is.EqualTo(LatticeMaintenanceProgress.Copying));
        });
    }

    [Test]
    public void ExecuteWalMoveTrackedAsync_rejects_a_null_ticket()
    {
        var harness = CreateHarness();
        Assert.That(
            async () => await Tracked(harness).ExecuteWalMoveTrackedAsync(TreeId, 0, SecondaryKey, null, null!),
            Throws.ArgumentNullException);
    }
}
