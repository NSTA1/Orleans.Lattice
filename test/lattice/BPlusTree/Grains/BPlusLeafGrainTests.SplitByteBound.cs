using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Reflection;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the <see cref="LatticeOptions.MaxLeafBytes"/> byte bound on leaf
/// size (issue #2481).
/// <para>
/// A key-count bound alone cannot keep a leaf snapshottable. A tree whose
/// values are large reaches a multi-hundred-megabyte leaf while still holding
/// fewer keys than the count bound, so it never splits; capturing its snapshot
/// then fails with <see cref="OutOfMemoryException"/>, which leaves the leaf
/// without durable snapshot coverage and pins its tree's WAL trim floor at
/// zero. These fixtures cover both halves of the remedy: the write-path
/// predicate that stops a leaf growing oversized, and the capture-path repair
/// that divides a leaf which is oversized ALREADY, with no operator action.
/// </para>
/// </summary>
public sealed class BPlusLeafGrainSplitByteBoundTests
{
    private static BPlusLeafGrain CreateGrain(
        FakePersistentState<LeafNodeState> state,
        long maxLeafBytes,
        int maxLeafKeys = 128)
    {
        // A split hands entries to a real sibling reference, so the stub has to
        // carry a grain context; NSubstitute's bare auto-stub fails GetGrainId.
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(Task.CompletedTask);
        sibling.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>()).Returns(Task.CompletedTask);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "byte-bound-leaf"));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(sibling);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(sibling);
        var resolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { MaxLeafBytes = maxLeafBytes },
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

    private static Task<bool> InvokeByteOverflowRepair(
        BPlusLeafGrain grain,
        int maxLeafKeys,
        long maxLeafBytes)
    {
        var method = typeof(BPlusLeafGrain).GetMethod(
            "TrySplitForByteOverflowAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(
            method,
            Is.Not.Null,
            "TrySplitForByteOverflowAsync not found - was it renamed? It is the capture-path "
            + "self-repair entry point that makes an already-oversized deployment recover.");
        return (Task<bool>)method!.Invoke(grain, [maxLeafKeys, maxLeafBytes])!;
    }

    private static byte[] Payload(int size) => Encoding.UTF8.GetBytes(new string('x', size));

    [Test]
    public async Task Leaf_splits_when_state_bytes_exceed_MaxLeafBytes_under_the_key_count_bound()
    {
        var state = new FakePersistentState<LeafNodeState>();
        // The key-count bound is left wide open at its default, so a split here
        // can only have been triggered by the byte bound.
        var grain = CreateGrain(state, maxLeafBytes: 64);

        var first = await grain.SetAsync("a", Payload(40));
        Assert.That(first, Is.Null, "41 bytes is under the bound - no split expected yet");

        var second = await grain.SetAsync("b", Payload(40));

        Assert.That(
            second,
            Is.Not.Null,
            "a leaf holding 82 bytes against a 64-byte bound must split even though its key "
            + "count (2) is far below MaxLeafKeys (128)");
    }

    [Test]
    public async Task Leaf_does_not_split_on_bytes_when_MaxLeafBytes_is_zero()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, maxLeafBytes: 0);

        await grain.SetAsync("a", Payload(40));
        var result = await grain.SetAsync("b", Payload(40));

        Assert.That(
            result,
            Is.Null,
            "MaxLeafBytes = 0 disables the byte bound, restoring pure key-count splitting");
    }

    [Test]
    public async Task Leaf_holding_one_oversized_entry_does_not_split()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, maxLeafBytes: 64);

        // One entry, far over the bound. A split pivots on the median key, so a
        // single-entry leaf has no median: splitting would migrate every entry
        // to the sibling and leave an empty donor, and the predicate would then
        // hold on the sibling forever, allocating a fresh leaf grain each pass.
        var result = await grain.SetAsync("a", Payload(4096));

        Assert.That(
            result,
            Is.Null,
            "a single-entry leaf is irreducible by splitting and must be left intact, "
            + "otherwise the byte predicate never terminates");
        Assert.That(grain.EntriesForTest.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task Capture_path_repair_divides_a_leaf_that_is_already_oversized()
    {
        var state = new FakePersistentState<LeafNodeState>();
        // Seeded with the bound disabled, so the leaf is allowed to grow
        // oversized exactly as a deployment running a pre-fix build did. The
        // write path therefore cannot have split it.
        var grain = CreateGrain(state, maxLeafBytes: 0);

        for (var i = 0; i < 8; i++)
        {
            var seeded = await grain.SetAsync($"k{i}", Payload(40));
            Assert.That(seeded, Is.Null, "seeding must not split while the bound is disabled");
        }

        Assert.That(grain.EntriesForTest.Count, Is.EqualTo(8));

        // Now the repair runs with the bound armed, standing in for an upgraded
        // process reactivating a leaf that is already over the bound and has
        // taken no write since. This is the self-healing path: no operator
        // action, no forced re-write, no re-index.
        var repaired = await InvokeByteOverflowRepair(grain, maxLeafKeys: 128, maxLeafBytes: 64);

        Assert.That(
            repaired,
            Is.True,
            "the capture-path repair must divide an already-oversized quiescent leaf, "
            + "which no write-path predicate would ever reach");
        Assert.That(
            grain.EntriesForTest.Count,
            Is.LessThan(8),
            "the donor must be strictly smaller after the repair");
    }

    [Test]
    public async Task Capture_path_repair_terminates_on_an_irreducible_leaf()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, maxLeafBytes: 0);

        await grain.SetAsync("a", Payload(4096));
        Assert.That(grain.EntriesForTest.Count, Is.EqualTo(1));

        // The leaf is over the bound and cannot be divided. The repair must
        // return without splitting rather than spinning: were it to split, the
        // donor would be emptied, the predicate would still hold on the
        // sibling, and the pass would allocate leaf grains until the ceiling.
        var repaired = await InvokeByteOverflowRepair(grain, maxLeafKeys: 128, maxLeafBytes: 64);

        Assert.That(
            repaired,
            Is.False,
            "an irreducible leaf must be reported, not split");
        Assert.That(grain.EntriesForTest.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task Capture_path_repair_is_a_no_op_for_a_leaf_under_the_bound()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, maxLeafBytes: 0);

        await grain.SetAsync("a", Payload(8));
        await grain.SetAsync("b", Payload(8));

        var repaired = await InvokeByteOverflowRepair(grain, maxLeafKeys: 128, maxLeafBytes: 4096);

        Assert.That(repaired, Is.False, "a leaf under the bound must not be split");
        Assert.That(grain.EntriesForTest.Count, Is.EqualTo(2));
    }
}
