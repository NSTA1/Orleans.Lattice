using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationShipperGrainTests
{
    [Test]
    public async Task Cross_tree_terminal_with_no_registry_membership_ships_unstamped()
    {
        // A sub-saga parked before replication was enabled records no
        // cross-tree membership (#5073), exactly like one from before
        // memberships existed (#4683). Its terminal must ship as it is, not
        // fail the tick.
        var factory = Substitute.For<IGrainFactory>();
        var (grain, state, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipBatchSize = 128,
            ShipCursorWriteInterval = 1,
            WireVersionNegotiationEnabled = false,
        }, grainFactory: factory);
        var txid = StampTransactionId(0);
        var registry = Substitute.For<ITxRegistryGrain>();
        factory.GetGrain<ITxRegistryGrain>(TxRegistryRouting.ShardKey(Tree, txid)).Returns(registry);
        registry.GetCrossTreeMembershipsAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(new Dictionary<Guid, CrossTreeMembership>());
        feed.Append(MakeTerminalEntry(MutationKind.TxCommit, 0, ticks: 1, transactionId: txid) with
        {
            CrossTreeOperationId = "operation-before-replication",
        });

        await grain.PumpForTestingAsync(CancellationToken.None);

        var batch = transport.ReceivedCalls()
            .Where(call => call.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .Select(call => (ReplicationBatch)call.GetArguments()[0]!)
            .Single(batch => batch.EncodedEnvelope is not null);
        var record = batch.EncodedEnvelope!.Value.EncodedEntries.ToArray()
            .Select(segment => feed.DecodeShippedEntry(segment.AsSpan()))
            .Single();
        Assert.Multiple(() =>
        {
            Assert.That(record.TransactionId, Is.EqualTo(txid));
            Assert.That(record.CrossTreeOperationId, Is.EqualTo("operation-before-replication"));
            Assert.That(record.CrossTreeDecisionStamps, Is.Null);
            Assert.That(record.CrossTreeDecisionSequences, Is.Null);
            Assert.That(state.State.ConsecutiveFailures, Is.Zero);
        });
        await registry.Received(1).GetCrossTreeMembershipsAsync(Arg.Any<IReadOnlyList<Guid>>());
    }
}
