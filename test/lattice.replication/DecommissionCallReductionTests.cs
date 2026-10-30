using NSubstitute;
using System.Collections.Immutable;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public sealed class DecommissionCallReductionTests
{
    private const string Tree = "decommission-call-budget";
    private const string Peer = "removed-peer";

    [TestCase(false)]
    [TestCase(true)]
    public async Task Decommission_marks_once_before_any_tree_work_and_reports_previous_state(bool previous)
    {
        var h = Create([]);
        var persisted = new TaskCompletionSource<bool>();
        h.Markers.MarkDecommissionedAndGetPreviousAsync(Peer, Arg.Any<DateTimeOffset>()).Returns(persisted.Task);

        var call = h.Subject.DecommissionPeerAsync(Peer);
        Assert.That(call.IsCompleted, Is.False);
        await h.Registry.DidNotReceive().GetAllTreeIdsAsync();
        persisted.SetResult(previous);
        var outcome = await call;

        Assert.That(outcome.AlreadyDecommissioned, Is.EqualTo(previous));
        await h.Markers.Received(1).MarkDecommissionedAndGetPreviousAsync(Peer, Arg.Any<DateTimeOffset>());
        await h.Markers.DidNotReceive().IsDecommissionedAsync(Arg.Any<string>());
        await h.Markers.DidNotReceive().MarkDecommissionedAsync(Arg.Any<string>(), Arg.Any<DateTimeOffset>());
    }

    [Test]
    public void Decommission_marker_failure_prevents_tree_work()
    {
        var h = Create([]);
        var failure = new IOException("marker failed");
        h.Markers.MarkDecommissionedAndGetPreviousAsync(Peer, Arg.Any<DateTimeOffset>())
            .Returns(Task.FromException<bool>(failure));
        Assert.That(async () => await h.Subject.DecommissionPeerAsync(Peer), Throws.Exception.SameAs(failure));
        h.Registry.DidNotReceive().GetAllTreeIdsAsync();
    }

    [Test]
    public async Task Decommission_batches_by_stamped_registry_and_bounds_every_payload()
    {
        var ids = Enumerable.Range(0, 257).Select(_ => Guid.NewGuid())
            .Concat(Enumerable.Range(0, 260).Select(_ => StampedId(200)))
            .Concat([StampedId(3), StampedId(3)]).ToArray();
        var h = Create(ids);
        var windows = new List<Guid[]>();
        foreach (var group in ids.GroupBy(id => TxRegistryRouting.ShardKey(Tree, id)))
        {
            var registry = h.Factory.GetGrain<ITxRegistryGrain>(group.Key);
            var own = group.ToHashSet();
            registry.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(call =>
            {
                var window = call.ArgAt<IReadOnlyList<Guid>>(0).ToArray();
                Assert.That(window.All(own.Contains), Is.True, "ids must reach their encoded registry");
                Assert.That(window.Length, Is.InRange(1, ITxRegistryGrain.MaxMaintenanceProbeBatchSize));
                windows.Add(window);
                return Task.FromResult(window.ToDictionary(id => id, _ => TxStatus.Committed));
            });
        }

        await h.Subject.DecommissionPeerAsync(Peer);

        Assert.That(windows.SelectMany(w => w), Is.EquivalentTo(ids));
        Assert.That(windows.Count, Is.EqualTo(7));
        await h.Registry.Received(2).GetEntryAsync(Tree);
        await h.Shard.Received(2).GetLeftmostLeafIdAsync();
        foreach (var txid in ids)
        {
            await h.Shard.Received(1).AppendTxTerminalAsync(txid, true, null, Arg.Any<CancellationToken>());
            await TxRegistryRouting.GetRegistry(h.Factory, Tree, txid).DidNotReceive().GetStatusAsync(Arg.Any<Guid>());
        }
    }

    [Test]
    public async Task Decommission_keeps_nonterminal_statuses_on_the_existing_recorded_decision_path()
    {
        var ids = Enumerable.Range(0, 4).Select(_ => Guid.NewGuid()).ToArray();
        var h = Create(ids);
        var statuses = new[] { TxStatus.Committed, TxStatus.Aborted, TxStatus.InFlight, TxStatus.Indeterminate };
        var registry = TxRegistryRouting.GetRegistry(h.Factory, Tree, ids[0]);
        registry.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(Task.FromResult(ids.Zip(statuses).ToDictionary(p => p.First, p => p.Second)));
        registry.GetRecordedStatusAsync(ids[2]).Returns(TxStatus.Committed);
        registry.GetRecordedStatusAsync(ids[3]).Returns(TxStatus.InFlight);

        await h.Subject.DecommissionPeerAsync(Peer);

        for (var i = 0; i < ids.Length; i++)
            await h.Shard.Received(1).AppendTxTerminalAsync(ids[i], i is 0 or 2, null, Arg.Any<CancellationToken>());
        await registry.DidNotReceive().GetRecordedStatusAsync(ids[0]);
        await registry.DidNotReceive().GetRecordedStatusAsync(ids[1]);
        await registry.Received(1).GetRecordedStatusAsync(ids[2]);
        await registry.Received(1).GetRecordedStatusAsync(ids[3]);
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Decommission_failed_or_incomplete_status_batch_never_starts_terminal_delivery(bool incomplete)
    {
        var ids = Enumerable.Range(0, 129).Select(_ => Guid.NewGuid()).ToArray();
        var h = Create(ids);
        var registry = TxRegistryRouting.GetRegistry(h.Factory, Tree, ids[0]);
        var calls = 0;
        registry.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(call =>
        {
            if (++calls == 1)
                return Task.FromResult(call.ArgAt<IReadOnlyList<Guid>>(0).ToDictionary(id => id, _ => TxStatus.Committed));
            return incomplete
                ? Task.FromResult(new Dictionary<Guid, TxStatus>())
                : Task.FromException<Dictionary<Guid, TxStatus>>(new IOException("registry failed"));
        });

        Assert.That(async () => await h.Subject.DecommissionPeerAsync(Peer),
            incomplete ? Throws.InvalidOperationException : Throws.TypeOf<IOException>());
        Assert.That(calls, Is.EqualTo(2));
        h.Shard.DidNotReceive().AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), null, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Decommission_cancellation_between_windows_prevents_remaining_reads_and_terminals()
    {
        var ids = Enumerable.Range(0, 129).Select(_ => Guid.NewGuid()).ToArray();
        var h = Create(ids);
        using var cancellation = new CancellationTokenSource();
        var registry = TxRegistryRouting.GetRegistry(h.Factory, Tree, ids[0]);
        registry.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(call =>
        {
            cancellation.Cancel();
            return Task.FromResult(call.ArgAt<IReadOnlyList<Guid>>(0).ToDictionary(id => id, _ => TxStatus.Committed));
        });

        Assert.That(async () => await h.Subject.DecommissionPeerAsync(Peer, cancellation.Token),
            Throws.InstanceOf<OperationCanceledException>());
        registry.Received(1).GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>());
        h.Shard.DidNotReceive().AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), null, Arg.Any<CancellationToken>());
    }

    private static Guid StampedId(byte owner)
    {
        var bytes = TxRegistryRouting.MintTransactionId(256).ToByteArray();
        bytes[^1] = owner;
        return new Guid(bytes);
    }

    private static Harness Create(Guid[] pending)
    {
        var factory = Substitute.For<IGrainFactory>();
        var topology = Substitute.For<IReplicationTopology>();
        topology.CurrentPeers.Returns(Array.Empty<string>());
        var markers = Substitute.For<IReplicationDecommissionedPeerRegistryGrain>();
        factory.GetGrain<IReplicationDecommissionedPeerRegistryGrain>(IReplicationDecommissionedPeerRegistryGrain.SingletonKey)
            .Returns(markers);
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>([Tree]));
        registry.GetEntryAsync(Tree).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            ShardMap = ShardMap.CreateDefault(16, 1),
        }));
        factory.GetGrain<ICrossTreePeerEnrolmentGrain>(Tree).Returns(Substitute.For<ICrossTreePeerEnrolmentGrain>());
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(Tree).Returns(Substitute.For<ILatticeBootstrapCoordinatorGrain>());
        var index = Substitute.For<ICrossTreeBarrierIndexGrain>();
        index.GetAsync().Returns(Task.FromResult(ImmutableArray<string>.Empty));
        factory.GetGrain<ICrossTreeBarrierIndexGrain>(Tree).Returns(index);
        var shard = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{Tree}/0").Returns(shard);
        var leafId = GrainId.Create("leaf", "decommission-pending");
        shard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafId));
        var leaf = Substitute.For<IBPlusLeafGrain>();
        factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);
        leaf.GetPendingMutationsForSlotsAndNextSiblingAsync(Arg.Any<int[]>(), 16).Returns(Task.FromResult(
            new LeafPendingMutationsPage
            {
                Mutations = pending.Select(id => new PendingMutationSnapshot
                    { TransactionId = id, Key = id.ToString(), OriginClusterId = Peer }).ToList(),
            }));
        var registries = new Dictionary<string, ITxRegistryGrain>();
        factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(call =>
        {
            var key = call.ArgAt<string>(0);
            if (!registries.TryGetValue(key, out var value))
                registries[key] = value = Substitute.For<ITxRegistryGrain>();
            return value;
        });
        return new Harness(new LatticeReplicationPeerDecommissioner(factory, topology), factory, registry, markers, shard);
    }

    private sealed record Harness(
        LatticeReplicationPeerDecommissioner Subject,
        IGrainFactory Factory,
        ILatticeRegistry Registry,
        IReplicationDecommissionedPeerRegistryGrain Markers,
        IShardRootGrain Shard);
}
