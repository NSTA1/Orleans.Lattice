using System.Collections.Immutable;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationShipperGrainTests
{
    [TestCase(1, false)]
    [TestCase(4, false)]
    [TestCase(40, false)]
    [TestCase(4, true)]
    public async Task Stamp_lookup_waves_settle_before_any_terminal_is_sent(int count, bool fail)
    {
        var factory = Substitute.For<IGrainFactory>();
        var (grain, state, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipBatchSize = 128,
            ShipCursorWriteInterval = 1,
            WireVersionNegotiationEnabled = false,
        }, grainFactory: factory);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var width = Math.Min(count, BoundedFanOut.DefaultWidth);
        var entered = 0;
        var active = 0;
        var peak = 0;
        var registries = new List<ITxRegistryGrain>();
        for (var slot = 0; slot < count; slot++)
        {
            var index = slot;
            var txid = StampTransactionId(slot);
            var registry = Substitute.For<ITxRegistryGrain>();
            registries.Add(registry);
            factory.GetGrain<ITxRegistryGrain>(TxRegistryRouting.ShardKey(Tree, txid)).Returns(registry);
            registry.GetCrossTreeMembershipsAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(async call =>
            {
                Assert.That(call.Arg<IReadOnlyList<Guid>>(), Is.EqualTo(new[] { txid }));
                active++;
                peak = Math.Max(peak, active);
                if (++entered == width) arrived.TrySetResult();
                try
                {
                    await release.Task;
                    if (fail && index == 0) throw new InvalidOperationException("stamp unavailable");
                    return new Dictionary<Guid, CrossTreeMembership>
                    {
                        [txid] = new()
                        {
                            OperationId = $"operation-{index}",
                            Participants = [Tree],
                            DecisionStamps = ImmutableDictionary<string, long>.Empty.Add(Tree, index + 1),
                        },
                    };
                }
                finally { active--; }
            });
            feed.Append(MakeTerminalEntry(MutationKind.TxAbort, 0, ticks: slot + 1, transactionId: txid) with
            {
                CrossTreeOperationId = $"operation-{slot}",
            });
        }

        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var operation = Task.Factory.StartNew(() => grain.PumpForTestingAsync(CancellationToken.None),
            CancellationToken.None, TaskCreationOptions.None, turn).Unwrap();
        try
        {
            await arrived.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.That(entered, Is.EqualTo(width));
            Assert.That(peak, Is.EqualTo(width), "Serial lookup mutation must fail this assertion.");
            await transport.DidNotReceiveWithAnyArgs().SendAsync(default!, default);
            Assert.That(operation.IsCompleted, Is.False);
        }
        finally
        {
            release.TrySetResult();
            await operation.WaitAsync(TimeSpan.FromSeconds(10));
        }

        Assert.That(active, Is.Zero);
        Assert.That(peak, Is.EqualTo(width));
        if (fail)
        {
            await transport.DidNotReceiveWithAnyArgs().SendAsync(default!, default);
            Assert.That(state.State.PartitionCursors.Values, Has.All.EqualTo(0L));
            Assert.That(state.State.ConsecutiveFailures, Is.GreaterThan(0));
        }
        else
        {
            Assert.That(entered, Is.EqualTo(count));
            Assert.That(LastShippedEntryCount(transport), Is.EqualTo(count));
            var batch = transport.ReceivedCalls()
                .Where(call => call.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
                .Select(call => (ReplicationBatch)call.GetArguments()[0]!)
                .Single(batch => batch.EncodedEnvelope is not null);
            var records = batch.EncodedEnvelope!.Value.EncodedEntries.ToArray()
                .Select(segment => feed.DecodeShippedEntry(segment.AsSpan())).ToArray();
            Assert.That(records.Select(record => record.CrossTreeDecisionStamps![Tree]),
                Is.EqualTo(Enumerable.Range(1, count).Select(value => (long)value)));
        }
        foreach (var registry in registries)
            await registry.Received(1).GetCrossTreeMembershipsAsync(Arg.Any<IReadOnlyList<Guid>>());
    }

    private static Guid StampTransactionId(int shard)
    {
        var bytes = new byte[16];
        bytes[7] = 0x80;
        bytes[8] = 0x80;
        bytes[15] = (byte)shard;
        return new Guid(bytes);
    }
}
