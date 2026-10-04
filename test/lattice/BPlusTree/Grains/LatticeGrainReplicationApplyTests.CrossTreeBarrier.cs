using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the cross-tree receiver barrier rows of
/// <c>spec/atomic-commit/RefinementCrossCluster.md</c> (issue #4436):
/// <c>ReceiverRegister(tr)</c>, <c>ReceiverNotify(tr)</c> and
/// <c>ReceiverFinalize(tr)</c>. They drive the receiver's apply seam for a
/// replicated cross-tree saga over two trees and observe each tree's registry
/// between the two trees' terminals: the first tree must have delegated the
/// saga's status to the barrier (its registry carries one cross-tree delegation)
/// and must still hide its key, and once the second tree's terminal completes
/// the barrier both trees must be visible with their delegations dropped.
/// </summary>
public partial class LatticeGrainReplicationApplyTests
{
    private const string BarrierOrigin = "site-x";

    [Test]
    public async Task A_cross_tree_terminal_delegates_its_tree_to_the_barrier_until_every_tree_arrives()
    {
        var treeA = "rapply-xtree-a-" + Guid.NewGuid().ToString("N")[..8];
        var treeB = "rapply-xtree-b-" + Guid.NewGuid().ToString("N")[..8];
        var operationId = "op-" + Guid.NewGuid().ToString("N");
        var txid = Guid.NewGuid();
        var hlc = Hlc(70_000, 0);
        var waitSet = new[] { treeA, treeB };

        var applyA = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(treeA);
        var applyB = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(treeB);
        var latticeA = _fixture.Cluster.Client.GetGrain<ILattice>(treeA);
        var latticeB = _fixture.Cluster.Client.GetGrain<ILattice>(treeB);
        var registryA = TxRegistryRouting.GetRegistry(_fixture.Cluster.Client, treeA, txid);
        var registryB = TxRegistryRouting.GetRegistry(_fixture.Cluster.Client, treeB, txid);

        await applyA.ApplyPreparedSetAsync(
            "k", new byte[] { 1 }, hlc, BarrierOrigin, sourceVectorClock: null,
            expiresAtTicks: 0, transactionId: txid, atomicBatchSize: 1, atomicBatchIndex: 0);
        await applyB.ApplyPreparedSetAsync(
            "k", new byte[] { 2 }, hlc, BarrierOrigin, sourceVectorClock: null,
            expiresAtTicks: 0, transactionId: txid, atomicBatchSize: 1, atomicBatchIndex: 0);

        await applyA.ApplyTxTerminalAsync(
            txid, committed: true, shardIndex: 0,
            terminalHlc: hlc with { WallClockTicks = hlc.WallClockTicks + 1 },
            originClusterId: BarrierOrigin, atomicShardCount: 1,
            crossTreeOperationId: operationId, crossTreeWaitSet: waitSet);

        var midA = await registryA.ObserveCrossTreeInFlightAsync();
        var midStatusA = await registryA.GetStatusAsync(txid);
        var midValueA = await latticeA.GetAsync("k");
        var midValueB = await latticeB.GetAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(midA.InFlightCount, Is.EqualTo(1),
                "Tree A's tally is complete, so its registry must delegate the saga's status to the "
                + "receiver barrier while the barrier waits for tree B.");
            Assert.That(midStatusA, Is.EqualTo(TxStatus.InFlight),
                "The barrier has not decided, so the delegated status must read InFlight.");
            Assert.That(midValueA, Is.Null,
                "Tree A must stay invisible until every tree in the wait set has arrived.");
            Assert.That(midValueB, Is.Null,
                "Tree B has not received its terminal.");
        });

        await applyB.ApplyTxTerminalAsync(
            txid, committed: true, shardIndex: 0,
            terminalHlc: hlc with { WallClockTicks = hlc.WallClockTicks + 2 },
            originClusterId: BarrierOrigin, atomicShardCount: 1,
            crossTreeOperationId: operationId, crossTreeWaitSet: waitSet);

        // The recorded decisions are read FIRST and without a dial: a status
        // read or an in-flight observation resolves a surviving delegation
        // against the barrier and caches the verdict itself, which would hide a
        // finalise that never marked the registry.
        var recordedA = await registryA.GetRecordedStatusAsync(txid);
        var recordedB = await registryB.GetRecordedStatusAsync(txid);
        var finalA = await registryA.ObserveCrossTreeInFlightAsync();
        var finalB = await registryB.ObserveCrossTreeInFlightAsync();
        var valueA = await latticeA.GetAsync("k");
        var valueB = await latticeB.GetAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(valueA, Is.EqualTo(new byte[] { 1 }));
            Assert.That(valueB, Is.EqualTo(new byte[] { 2 }));
            Assert.That(recordedA, Is.EqualTo(TxStatus.Committed),
                "Finalising tree A must record the barrier's verdict in its own registry.");
            Assert.That(recordedB, Is.EqualTo(TxStatus.Committed),
                "Finalising tree B must record the barrier's verdict in its own registry.");
            Assert.That(finalA.InFlightCount, Is.Zero, "Tree A's local decision must drop its delegation.");
            Assert.That(finalB.InFlightCount, Is.Zero, "Tree B's local decision must drop its delegation.");
        });
    }
}
