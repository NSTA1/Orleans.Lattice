using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the batched split fast-path RPCs:
/// <c>InitializeSiblingAsync</c> (one round-trip seeding of every
/// birth-time metadata slot on a freshly created split sibling) and
/// <c>SetCheckpointOffsetHintsAsync</c> (one round-trip applying a batch
/// of per-partition projection-checkpoint hints). Both replace per-slot /
/// per-partition fan-outs of individual gated setter RPCs in
/// <c>CompleteSplitAsync</c>.
/// </summary>
public sealed class BPlusLeafGrainSiblingInitTests
{
    private static BPlusLeafGrain CreateGrain(FakePersistentState<LeafNodeState> state)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "sibling-init-leaf"));
        var grainFactory = Substitute.For<IGrainFactory>();
        var resolver = TestOptionsResolver.Create(
            maxLeafKeys: 16,
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
    public async Task InitializeSibling_seeds_every_slot_and_persists_once()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var next = GrainId.Create("leaf", "next");
        var prev = GrainId.Create("leaf", "prev");

        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "tree-1",
            ShardIndex = 4,
            LowKeyInclusive = "m",
            HighKeyExclusive = "z",
            NextSibling = next,
            PrevSibling = prev,
        });

        Assert.That(state.State.TreeId, Is.EqualTo("tree-1"));
        Assert.That(state.State.ShardIndex, Is.EqualTo(4));
        Assert.That(state.State.LowKeyInclusive, Is.EqualTo("m"));
        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("z"));
        Assert.That(state.State.NextSibling, Is.EqualTo(next));
        Assert.That(state.State.PrevSibling, Is.EqualTo(prev));

        // One write covers the whole batch, not five.
        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task InitializeSibling_is_idempotent_on_write_once_slots()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "tree-1",
            ShardIndex = 4,
            LowKeyInclusive = "m",
            HighKeyExclusive = "z",
            NextSibling = null,
            PrevSibling = null,
        });
        var writesAfterFirst = state.WriteCount;

        // A re-call with different write-once values must not overwrite
        // the seeded slots and must not pay a second persist.
        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "tree-2",
            ShardIndex = 9,
            LowKeyInclusive = "a",
            HighKeyExclusive = "b",
            NextSibling = null,
            PrevSibling = null,
        });

        Assert.That(state.State.TreeId, Is.EqualTo("tree-1"));
        Assert.That(state.State.ShardIndex, Is.EqualTo(4));
        Assert.That(state.State.LowKeyInclusive, Is.EqualTo("m"));
        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("z"));
        Assert.That(state.WriteCount, Is.EqualTo(writesAfterFirst),
            "no-op re-call must not pay a second persist");
    }

    [Test]
    public async Task InitializeSibling_leaves_shard_index_unseeded_when_null()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "tree-1",
            ShardIndex = null,
            LowKeyInclusive = "m",
            HighKeyExclusive = "z",
            NextSibling = null,
            PrevSibling = null,
        });

        Assert.That(state.State.ShardIndex, Is.Null);
        Assert.That(state.State.TreeId, Is.EqualTo("tree-1"));
    }

    [Test]
    public async Task SetCheckpointOffsetHints_skips_non_positive_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Partition 0 head is 0 (skipped); a single-element array keeps the
        // grain in its default single-partition checkpoint shape.
        await grain.SetCheckpointOffsetHintsAsync([0]);

        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(0));
    }

    [Test]
    public async Task SetCheckpointOffsetHints_applies_partition_zero_hint()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetCheckpointOffsetHintsAsync([42]);

        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(42));
    }

    [Test]
    public void SetCheckpointOffsetHints_throws_on_null()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        Assert.That(
            async () => await grain.SetCheckpointOffsetHintsAsync(null!),
            Throws.ArgumentNullException);
    }
}
