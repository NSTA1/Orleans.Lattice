using System.Text;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.HostContract;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Faults;

/// <summary>
/// The WAL fault seams (A12, #5382) produce the state they claim: a held WAL target keeps a
/// move waiting before its flip until it is stopped, leaving the partition where it was; and
/// each kind of planted materialiser pin is read by the WAL reclamation probe as the state it
/// models, and is gone once released.
/// </summary>
[TestFixture]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
public sealed class WalSeamTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromMinutes(2);

    private static JourneyEstate Estate => FaultEstate.Current;

    private static LatticeTreeAdminApiGrpcClient Admin() => LatticeTreeAdminApiGrpcClient.Create(Estate.Invoker(), JourneyEstate.Serialization);

    private static LatticeDataApiGrpcClient Data() => LatticeDataApiGrpcClient.Create(Estate.Invoker(), JourneyEstate.Serialization);

    /// <summary>A move to a held target waits before its flip, stops when asked, and leaves the partition on its source.</summary>
    [Test]
    public async Task A_move_to_a_held_target_waits_and_stops_before_its_flip()
    {
        const string tree = "seam-wal-move";
        var admin = Admin();
        for (var i = 0; i < 20; i++)
        {
            await Data().SetAsync(new DataSetRequest { TreeId = tree, Key = $"key-{i:D3}", Value = Encoding.UTF8.GetBytes($"value-{i:D3}") });
        }

        var before = await PartitionKeyAsync(admin, tree);
        await Estate.HoldWalTargetAsync(tree);
        try
        {
            var move = await admin.StartWalMoveAsync(tree, 0, JourneyHostEnvironment.SecondaryWalProviderKey);
            var copying = await WaitForAsync(admin, move.OperationId, status => status.State == LatticeOperationState.Running && status.Phase.Length > 0);
            await Task.Delay(TimeSpan.FromSeconds(5));
            var waiting = await admin.GetTreeAdminOperationStatusAsync(move.OperationId);
            Assert.That(waiting!.IsTerminal, Is.False, $"the move waits on its held target (phase {waiting.Phase}, first seen in {copying.Phase})");

            await admin.CancelTreeAdminOperationAsync(move.OperationId);
            var stopped = await WaitForAsync(admin, move.OperationId, status => status.IsTerminal);
            Assert.That(stopped.State, Is.EqualTo(LatticeOperationState.Cancelled), stopped.FailureReason);
        }
        finally
        {
            await Estate.ReleaseWalTargetAsync(tree);
        }

        Assert.That(await PartitionKeyAsync(admin, tree), Is.EqualTo(before), "the partition stays on its source");
        await Data().SetAsync(new DataSetRequest { TreeId = tree, Key = "after", Value = [1] });
        Assert.That((await Data().GetAsync(new DataGetRequest { TreeId = tree, Key = "after" })).Found, Is.True, "the source serves writes again");
    }

    /// <summary>Each planted pin reads as the reclamation state it models, and is gone once released.</summary>
    [Test]
    public async Task Each_planted_pin_reads_as_its_reclamation_state()
    {
        const string tree = "seam-wal-pins";
        var admin = Admin();
        await admin.CreateTreeAsync(tree, shardCount: 2);

        var stranded = await Estate.PlantWalPinAsync(tree, JourneyWalPin.Stranded);
        var report = await admin.GetWalReclamationAsync(tree);
        Assert.Multiple(() =>
        {
            Assert.That(report.FloorHolder?.ConsumerId, Is.EqualTo(stranded));
            Assert.That(report.IsWedged, Is.True, "a stranded pin blocks reclamation");
        });
        await Estate.ReleaseWalPinAsync(tree, stranded);

        var unchecked_ = await Estate.PlantWalPinAsync(tree, JourneyWalPin.Unchecked);
        report = await admin.GetWalReclamationAsync(tree);
        Assert.Multiple(() =>
        {
            Assert.That(report.FloorHolder?.ConsumerId, Is.EqualTo(unchecked_));
            Assert.That(report.FloorHolder?.State, Is.EqualTo(TreeWalFloorHolderState.NeverCheckpointed));
            Assert.That(report.FloorHolder?.PinOffset, Is.EqualTo(-1));
            Assert.That(report.IsWedged, Is.False, "a pin with no offset waits for a checkpoint");
        });
        await Estate.ReleaseWalPinAsync(tree, unchecked_);

        var unidentified = await Estate.PlantWalPinAsync(tree, JourneyWalPin.Unidentified);
        report = await admin.GetWalReclamationAsync(tree);
        Assert.Multiple(() =>
        {
            Assert.That(report.FloorHolder?.ConsumerId, Is.EqualTo(unidentified));
            Assert.That(report.FloorHolder?.State, Is.EqualTo(TreeWalFloorHolderState.Unreadable), "an unidentified consumer's state cannot be read");
        });
        await Estate.ReleaseWalPinAsync(tree, unidentified);

        report = await admin.GetWalReclamationAsync(tree);
        Assert.That(report.FloorHolder?.ConsumerId, Is.Not.AnyOf(stranded, unchecked_, unidentified), "every planted pin is gone");
    }

    private static async Task<LatticeOperationStatus> WaitForAsync(LatticeTreeAdminApiGrpcClient admin, string operationId, Func<LatticeOperationStatus, bool> condition)
    {
        var deadline = DateTime.UtcNow + Patience;
        LatticeOperationStatus? last = null;
        while (DateTime.UtcNow < deadline)
        {
            last = await admin.GetTreeAdminOperationStatusAsync(operationId);
            if (last is not null && condition(last))
            {
                return last;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }

        throw new TimeoutException($"Operation {operationId} did not reach the expected state in {Patience}: last {last?.State} {last?.Phase} {last?.FailureReason}.");
    }

    private static async Task<string> PartitionKeyAsync(LatticeTreeAdminApiGrpcClient admin, string tree)
    {
        var placement = await admin.GetWalPlacementAsync(tree);
        return placement.Partitions.IsDefaultOrEmpty || !placement.Partitions.Any(partition => partition.Partition == 0)
            ? placement.DefaultProviderKey
            : placement.Partitions.First(partition => partition.Partition == 0).ProviderKey;
    }
}
