using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the cross-tree terminal routing in
/// <see cref="ReplicationApplier"/>: a terminal carrying a
/// <see cref="WalRecord.CrossTreeOperationId"/> threads the operation id and a
/// receiver-scoped wait set (the participant trees replicated on this receiver)
/// to <see cref="IReplicationApplyGrain.ApplyTxTerminalAsync"/>, so the receiver
/// barrier flips every replicated participating tree together. Participant trees
/// not replicated here are excluded from the wait set, so partial-replication
/// cross-tree batches stay valid.
/// </summary>
public partial class ReplicationApplierTests
{
    private const string SiblingTree = "tree-b";

    private static (ReplicationApplier Applier, IReplicationApplyGrain Apply) CreateCrossTreeApplier(
        IReadOnlyDictionary<string, LatticeMergeMode>? replicatedTrees)
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>()).Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions { ClusterId = LocalCluster, ReplicatedTrees = replicatedTrees };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var applier = new ReplicationApplier(factory, monitor);
        return (applier, apply);
    }

    private static WalRecord CrossTreeTerminalEntry(
        Guid transactionId,
        int shardIndex,
        string operationId,
        IReadOnlyList<string> participants) => new()
        {
            TreeId = Tree,
            Op = MutationKind.TxCommit,
            Key = shardIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = RemoteCluster,
            TransactionId = transactionId,
            ShardIndex = shardIndex,
            CrossTreeOperationId = operationId,
            CrossTreeParticipants = participants,
        };

    [Test]
    public async Task ApplyAsync_cross_tree_terminal_includes_replicated_participants_in_wait_set()
    {
        var (applier, apply) = CreateCrossTreeApplier(new Dictionary<string, LatticeMergeMode>
        {
            [Tree] = LatticeMergeMode.LwwRegister,
            [SiblingTree] = LatticeMergeMode.LwwRegister,
        });
        var txid = Guid.NewGuid();

        await applier.ApplyAsync(CrossTreeTerminalEntry(txid, 3, "op-1", new[] { Tree, SiblingTree }));

        await apply.Received(1).ApplyTxTerminalAsync(
            txid, true, 3, Arg.Any<HybridLogicalClock>(), RemoteCluster, Arg.Any<int>(),
            "op-1",
            Arg.Is<IReadOnlyList<string>>(ws => ws.Contains(Tree) && ws.Contains(SiblingTree) && ws.Count == 2),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_cross_tree_terminal_excludes_unreplicated_participants_from_wait_set()
    {
        // SiblingTree is a participant but NOT replicated on this receiver, so it
        // must be excluded - the barrier completes on the present subset.
        var (applier, apply) = CreateCrossTreeApplier(new Dictionary<string, LatticeMergeMode>
        {
            [Tree] = LatticeMergeMode.LwwRegister,
        });
        var txid = Guid.NewGuid();

        await applier.ApplyAsync(CrossTreeTerminalEntry(txid, 3, "op-2", new[] { Tree, SiblingTree }));

        await apply.Received(1).ApplyTxTerminalAsync(
            txid, true, 3, Arg.Any<HybridLogicalClock>(), RemoteCluster, Arg.Any<int>(),
            "op-2",
            Arg.Is<IReadOnlyList<string>>(ws => ws.Count == 1 && ws.Contains(Tree)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_non_cross_tree_terminal_passes_null_operation_id()
    {
        var (applier, apply) = CreateCrossTreeApplier(replicatedTrees: null);
        var txid = Guid.NewGuid();

        var entry = new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.TxCommit,
            Key = "3",
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = RemoteCluster,
            TransactionId = txid,
            ShardIndex = 3,
        };

        await applier.ApplyAsync(entry);

        await apply.Received(1).ApplyTxTerminalAsync(
            txid, true, 3, Arg.Any<HybridLogicalClock>(), RemoteCluster, Arg.Any<int>(),
            null,
            null,
            Arg.Any<CancellationToken>());
    }
}

