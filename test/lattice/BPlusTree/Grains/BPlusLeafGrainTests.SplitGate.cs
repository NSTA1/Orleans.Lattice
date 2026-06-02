using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using System.Reflection;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the non-blocking acquire of <c>_splitGate</c> in
/// <c>SplitIfNeededUnderGateAsync</c>. When a concurrent turn already
/// owns an in-flight split, an overflowing write must skip the split
/// (return no <see cref="SplitResult"/>) and complete promptly rather
/// than convoying on the gate for the in-flight migration's duration.
/// The overflowing write's data is already durable before the split
/// predicate runs, so the over-full leaf is transient and correct.
/// </summary>
public sealed class BPlusLeafGrainSplitGateTests
{
    private static SemaphoreSlim SplitGateOf(BPlusLeafGrain grain)
    {
        var field = typeof(BPlusLeafGrain).GetField(
            "_splitGate",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(field, Is.Not.Null, "_splitGate field not found - was it renamed?");
        return (SemaphoreSlim)field!.GetValue(grain)!;
    }

    private static BPlusLeafGrain CreateGrain(
        FakePersistentState<LeafNodeState> state,
        int maxLeafKeys)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "split-gate-leaf"));
        var grainFactory = Substitute.For<IGrainFactory>();
        var resolver = TestOptionsResolver.Create(
            maxLeafKeys: maxLeafKeys,
            shardCount: 1,
            factory: grainFactory);
        return new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            resolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
    }

    [Test]
    public async Task Overflowing_write_skips_split_when_gate_is_held()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, maxLeafKeys: 3);

        // Simulate an in-flight split owning the gate on another turn.
        var gate = SplitGateOf(grain);
        Assert.That(gate.Wait(0), Is.True, "gate should start free");

        try
        {
            // Fill to threshold, then overflow. The overflowing write
            // would normally trigger a split, but the gate is held, so
            // SplitIfNeededUnderGateAsync must skip and return null.
            await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
            await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
            await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
            var result = await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

            // No split bubbled up while the gate was contended.
            Assert.That(result, Is.Null, "split must be skipped while the gate is held");

            // The write is still durable: the over-full leaf holds the key.
            Assert.That(grain.EntriesForTest.ContainsKey("d"), Is.True);
            Assert.That(grain.EntriesForTest.Count, Is.EqualTo(4));
        }
        finally
        {
            gate.Release();
        }
    }

    [Test]
    public async Task Overflowing_write_returns_promptly_when_gate_is_held()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, maxLeafKeys: 3);

        var gate = SplitGateOf(grain);
        Assert.That(gate.Wait(0), Is.True);

        try
        {
            await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
            await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
            await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

            // The overflowing write must not convoy on the held gate.
            var overflow = grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));
            var completed = await Task.WhenAny(overflow, Task.Delay(TimeSpan.FromSeconds(5)));

            Assert.That(completed, Is.SameAs(overflow),
                "overflowing write must not block on the held split gate");
            await overflow;
        }
        finally
        {
            gate.Release();
        }
    }

    [Test]
    public async Task Split_proceeds_normally_when_gate_is_free()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, Orleans.Lattice.Primitives.LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        sibling.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        sibling.SetNextSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);
        sibling.SetPrevSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);
        sibling.SetShardIndexAsync(Arg.Any<int>()).Returns(Task.CompletedTask);
        sibling.SetKeyRangeAsync(Arg.Any<string?>(), Arg.Any<string?>()).Returns(Task.CompletedTask);
        sibling.SetCheckpointOffsetHintAsync(Arg.Any<long>()).Returns(Task.CompletedTask);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "split-gate-leaf"));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(sibling);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(sibling);
        var resolver = TestOptionsResolver.Create(
            maxLeafKeys: 3,
            shardCount: 1,
            factory: grainFactory);
        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            resolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        // Gate is free: the overflowing write owns the split and bubbles
        // a SplitResult up to the caller.
        var result = await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        Assert.That(result, Is.Not.Null, "split should proceed when the gate is free");
    }
}
