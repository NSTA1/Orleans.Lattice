using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4455: the online snapshot a resize runs must
/// carry the prepared (not yet terminal) saga buckets its source shards hold,
/// not only their live entries. A saga that prepared a key before the snapshot
/// switched mirroring on would otherwise reach the resized copy with only the
/// part of its batch prepared afterwards, so once the alias flips the batch is
/// torn there and the earlier key reverts to its pre-saga value (the
/// shard-ownership spec's OwnerMonotonicSnapshotSkipsBuckets).
/// </summary>
public partial class TreeSnapshotGrainTests
{
    private const string PreparedLogicalTreeId = "logical-tree";

    private sealed class PreparedSweepHarness
    {
        public required TreeSnapshotGrain Grain { get; init; }
        public required IShardRootGrain Destination0 { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required List<string> Order { get; init; }
        public required List<(bool Prepared, Guid TxId)> ReplayContexts { get; init; }
    }

    /// <summary>A key the default two-shard map routes to physical shard 0.</summary>
    private static string KeyOnShardZero()
    {
        var map = ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, ShardCount);
        for (var i = 0; ; i++)
        {
            var key = $"prepared-{i}";
            if (map.Resolve(key) == 0) return key;
        }
    }

    private static PendingMutationSnapshot PreparedSet(Guid txid, string key) => new()
    {
        TransactionId = txid,
        Key = key,
        Value = [7, 7, 7],
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        IsTombstone = false,
        ExpiresAtTicks = 0,
        OriginClusterId = null,
        VectorClock = null,
    };

    private static ITxRegistryGrain StubDecisions(IGrainFactory grainFactory, string treeId, TxStatus status)
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(status));
        registry.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(ci => Task.FromResult(((IReadOnlyList<Guid>)ci[0]).ToDictionary(id => id, _ => status)));
        grainFactory.GetGrain<ITxRegistryGrain>(
                Arg.Is<string>(k => TxRegistryRouting.TreeIdFromKey(k) == treeId), Arg.Any<string?>())
            .Returns(registry);
        return registry;
    }

    private static PreparedSweepHarness CreatePreparedSweepHarness(
        PendingMutationSnapshot snapshot, TxStatus logicalDecision, TxStatus? sourceIdDecision = null)
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var order = new List<string>();
        var replays = new List<(bool, Guid)>();

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.ShadowBegin;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.OperationId = "op-prepared";
        state.State.Mode = SnapshotMode.Online;
        state.State.LogicalTreeId = PreparedLogicalTreeId;

        var leafId = GrainId.Create("leaf", "prepared-source-leaf");
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>()).Returns(ci =>
        {
            order.Add("sweep");
            var slots = (int[])ci[0];
            var slot = ShardMap.GetVirtualSlot(snapshot.Key, (int)ci[1]);
            return Task.FromResult(slots.Contains(slot) ? new List<PendingMutationSnapshot> { snapshot } : []);
        });
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        grainFactory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);

        IShardRootGrain? destination0 = null;
        for (var i = 0; i < ShardCount; i++)
        {
            var index = i;
            var source = Substitute.For<IShardRootGrain>();
            source.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafId));
            source.BeginShadowForwardAsync(DestTreeId, "op-prepared", PreparedLogicalTreeId).Returns(_ =>
            {
                order.Add($"mirror-{index}");
                return Task.CompletedTask;
            });
            grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}").Returns(source);

            var destination = Substitute.For<IShardRootGrain>();
            destination.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(_ =>
            {
                replays.Add((LatticePreparedContext.Current, LatticeTransactionContext.Current));
                return Task.CompletedTask;
            });
            grainFactory.GetGrain<IShardRootGrain>($"{DestTreeId}/{i}").Returns(destination);
            if (i == 0) destination0 = destination;
        }

        StubDecisions(grainFactory, PreparedLogicalTreeId, logicalDecision);
        if (sourceIdDecision is { } other) StubDecisions(grainFactory, SourceTreeId, other);

        return new PreparedSweepHarness
        {
            Grain = grain,
            Destination0 = destination0!,
            Leaf = leaf,
            Order = order,
            ReplayContexts = replays,
        };
    }

    [Test]
    public async Task Online_shadow_begin_carries_an_in_flight_prepared_bucket_onto_the_destination_shard()
    {
        var txid = Guid.NewGuid();
        var key = KeyOnShardZero();
        var h = CreatePreparedSweepHarness(PreparedSet(txid, key), TxStatus.InFlight);

        await h.Grain.BeginShadowForwardAllShardsAsync();

        await h.Destination0.Received(1).SetAsync(key, Arg.Is<byte[]>(v => v.SequenceEqual(new byte[] { 7, 7, 7 })));
        Assert.That(h.ReplayContexts, Is.EqualTo(new[] { (true, txid) }),
            "the bucket must be replayed as a prepare of the same saga, not as a committed write");
        await h.Destination0.Received(1).MarkSagaShadowAsync(txid, Arg.Is<string[]>(k => k.SequenceEqual(new[] { key })));
    }

    [Test]
    public async Task Online_shadow_begin_applies_the_terminal_of_a_saga_decided_before_the_sweep()
    {
        var txid = Guid.NewGuid();
        var key = KeyOnShardZero();
        var h = CreatePreparedSweepHarness(PreparedSet(txid, key), TxStatus.Committed);

        await h.Grain.BeginShadowForwardAllShardsAsync();

        await h.Destination0.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
        await h.Destination0.Received(1).AppendTxTerminalAsync(
            txid, true, Arg.Is<IReadOnlyDictionary<string, byte[]>>(d => d != null && d[key].SequenceEqual(new byte[] { 7, 7, 7 })));
    }

    [Test]
    public async Task Online_shadow_begin_reads_the_saga_decision_under_the_logical_tree()
    {
        // A resize snapshots the physical copy, but sagas record their
        // decisions under the logical tree (issue #4368's lesson).
        var txid = Guid.NewGuid();
        var key = KeyOnShardZero();
        var h = CreatePreparedSweepHarness(PreparedSet(txid, key), TxStatus.Committed, sourceIdDecision: TxStatus.InFlight);

        await h.Grain.BeginShadowForwardAllShardsAsync();

        await h.Destination0.Received(1).AppendTxTerminalAsync(txid, true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
        await h.Destination0.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public async Task Online_shadow_begin_sweeps_only_once_every_source_shard_mirrors()
    {
        // A prepare after the sweep read a leaf must reach the destination
        // through the mirror, so the mirror has to be on first.
        var h = CreatePreparedSweepHarness(PreparedSet(Guid.NewGuid(), KeyOnShardZero()), TxStatus.InFlight);

        await h.Grain.BeginShadowForwardAllShardsAsync();

        var firstSweep = h.Order.IndexOf("sweep");
        Assert.That(firstSweep, Is.GreaterThan(h.Order.LastIndexOf("mirror-0")).And.GreaterThan(h.Order.LastIndexOf("mirror-1")),
            "order: " + string.Join(", ", h.Order));
    }

    [Test]
    public async Task Online_shadow_begin_asks_each_shard_only_for_the_slots_its_routing_sends_there()
    {
        var h = CreatePreparedSweepHarness(PreparedSet(Guid.NewGuid(), KeyOnShardZero()), TxStatus.InFlight);
        var map = ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, ShardCount);

        await h.Grain.BeginShadowForwardAllShardsAsync();

        var requested = h.Leaf.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.GetPendingMutationsForSlotsAsync))
            .Select(c => (int[])c.GetArguments()[0]!)
            .ToList();
        Assert.That(requested, Has.Count.EqualTo(ShardCount));
        foreach (var slots in requested)
        {
            var owners = slots.Select(s => map.Slots[s]).Distinct().ToList();
            Assert.That(owners, Has.Count.EqualTo(1), "a shard's sweep must not ask for another shard's slots");
        }
        await h.Destination0.DidNotReceive().AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
    }
}
