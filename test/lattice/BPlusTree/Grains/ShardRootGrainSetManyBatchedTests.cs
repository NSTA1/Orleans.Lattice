using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the contract that <see cref="ShardRootGrain.SetManyAsync"/>
/// routes the local apply through <see cref="IBPlusLeafGrain.SetManyAsync"/>
/// per target leaf, not through per-key <see cref="IBPlusLeafGrain.SetAsync"/>.
/// This is the foreground bulk-write seam that has to reach the batched
/// commit-log path the leaf already exposes; the regression that motivated
/// this fixture was a foreground bulk write that silently degraded to N
/// per-key WAL grain hops, suppressing the throughput win the leaf-side
/// batched seam was supposed to deliver against an Azure Table WAL.
/// </summary>
public class ShardRootGrainSetManyBatchedTests
{
    private const string TreeId = "tree";
    private const int ShardIndex = 0;

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
    }

    private static Harness CreateHarness(Action<IBPlusLeafGrain>? configureLeaf = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{ShardIndex}"));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = GrainId.Create("leaf", "root-leaf");
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(null));
        leaf.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(Task.FromResult<SplitResult?>(null));
        leaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        configureLeaf?.Invoke(leaf);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());

        return new Harness { Grain = grain, Leaf = leaf };
    }

    [Test]
    public async Task SetManyAsync_flat_tree_dispatches_one_batched_leaf_call_for_whole_batch()
    {
        // The regression: ShardRootGrain.SetManyAsync used to iterate the
        // bucket and call IBPlusLeafGrain.SetAsync(key, value) per entry.
        // For a 16-key batch landing on one leaf and one WAL partition
        // that paid 16 WAL grain hops instead of one. The fix routes
        // through IBPlusLeafGrain.SetManyAsync, which exercises the
        // batched commit-log seam. This test pins the call shape: exactly
        // one SetManyAsync receiving every entry, zero per-key SetAsync.
        var h = CreateHarness();

        var entries = new List<KeyValuePair<string, byte[]>>();
        for (var i = 0; i < 16; i++)
        {
            entries.Add(new KeyValuePair<string, byte[]>($"k{i:D2}", [(byte)i]));
        }

        await h.Grain.SetManyAsync(entries);

        await h.Leaf.Received(1).SetManyAsync(Arg.Is<List<KeyValuePair<string, byte[]>>>(s => s.Count == 16));
        await h.Leaf.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetManyAsync_empty_input_short_circuits_without_calling_the_leaf()
    {
        var h = CreateHarness();

        await h.Grain.SetManyAsync([]);

        await h.Leaf.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
        await h.Leaf.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetManyAsync_preserves_input_order_and_payloads_on_the_batched_call()
    {
        // The batched seam carries the input list as-is. Out-of-order
        // delivery would corrupt the leaf's per-entry HLC stamping
        // (HLCs are minted in iteration order inside CommitSetManyAsync).
        List<KeyValuePair<string, byte[]>>? captured = null;
        var h = CreateHarness(configureLeaf: l => l.SetManyAsync(Arg.Do<List<KeyValuePair<string, byte[]>>>(s => captured = s)));

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("zeta",  [9]),
            new("alpha", [1]),
            new("mu",    [5]),
        };
        await h.Grain.SetManyAsync(entries);

        Assert.That(captured, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(captured!.Count, Is.EqualTo(3));
            Assert.That(captured[0].Key, Is.EqualTo("zeta"));
            Assert.That(captured[1].Key, Is.EqualTo("alpha"));
            Assert.That(captured[2].Key, Is.EqualTo("mu"));
            Assert.That(captured[0].Value, Is.EqualTo(new byte[] { 9 }));
            Assert.That(captured[1].Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That(captured[2].Value, Is.EqualTo(new byte[] { 5 }));
        });
    }
}
