using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Focused unit tests for the leaf-side compaction trigger: when
/// either <see cref="LatticeOptions.MinTombstoneRatioForCompaction"/>
/// or <see cref="LatticeOptions.MaxLeafEntriesBeforeForcedCompaction"/>
/// is non-zero, a mutation that pushes the leaf past the configured
/// threshold dispatches a single
/// <see cref="ITombstoneCompactionGrain.RequestCompactionAsync"/> call
/// scoped to the leaf's shard. Both knobs at their defaults must
/// produce zero dispatches so the v3.4.0 reminder-only deployment
/// remains the no-op baseline.
/// </summary>
[TestFixture]
public class BPlusLeafGrainCompactionTriggerTests
{
    private static (BPlusLeafGrain grain, ITombstoneCompactionGrain compactor) CreateGrainWithCompactor(
        LatticeOptions options,
        FakePersistentState<LeafNodeState>? state = null,
        string treeId = "trig-tree",
        int shardIndex = 0)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "trig-leaf"));

        state ??= new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId;
        state.State.ShardIndex = shardIndex;

        var grainFactory = Substitute.For<IGrainFactory>();
        var compactor = Substitute.For<ITombstoneCompactionGrain>();
        compactor.RequestCompactionAsync(Arg.Any<int>(), Arg.Any<string>())
            .Returns(Task.FromResult(true));
        grainFactory.GetGrain<ITombstoneCompactionGrain>(Arg.Any<string>())
            .Returns(compactor);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: options,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        return (grain, compactor);
    }

    [Test]
    public async Task Defaults_emit_no_trigger()
    {
        var (grain, compactor) = CreateGrainWithCompactor(new LatticeOptions());

        // Seed a 1:1 tombstone:live ratio - well above any reasonable
        // threshold - to prove that with both knobs at their defaults
        // the dispatch is genuinely never invoked.
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("v"));
        await grain.DeleteAsync("a");
        await grain.DeleteAsync("b");

        await compactor.DidNotReceive().RequestCompactionAsync(Arg.Any<int>(), Arg.Any<string>());
    }

    [Test]
    public async Task Ratio_threshold_crossed_dispatches_ratio_trigger()
    {
        var options = new LatticeOptions { MinTombstoneRatioForCompaction = 0.50 };
        var (grain, compactor) = CreateGrainWithCompactor(options, shardIndex: 7);

        // 1 live + 2 tombstones = 0.66 ratio, crossing the 0.50 threshold.
        await grain.SetAsync("live", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("v"));
        await grain.DeleteAsync("a");
        await grain.DeleteAsync("b");

        // Allow the fire-and-forget continuation a tick to schedule.
        await Task.Yield();

        await compactor.Received().RequestCompactionAsync(7, "ratio");
    }

    [Test]
    public async Task Below_ratio_threshold_does_not_dispatch()
    {
        var options = new LatticeOptions { MinTombstoneRatioForCompaction = 0.90 };
        var (grain, compactor) = CreateGrainWithCompactor(options);

        // 4 live + 1 tombstone = 0.20 ratio, well under 0.90.
        for (var i = 0; i < 5; i++)
            await grain.SetAsync($"k{i}", Encoding.UTF8.GetBytes("v"));
        await grain.DeleteAsync("k0");

        await compactor.DidNotReceive().RequestCompactionAsync(Arg.Any<int>(), "ratio");
    }

    [Test]
    public async Task Size_threshold_crossed_dispatches_size_trigger()
    {
        var options = new LatticeOptions { MaxLeafEntriesBeforeForcedCompaction = 3 };
        var (grain, compactor) = CreateGrainWithCompactor(options, shardIndex: 2);

        // 4 entries (live + tombstone) crosses the threshold of 3,
        // and at least one tombstone exists (the predicate requires both).
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("v"));
        await grain.DeleteAsync("a");

        await Task.Yield();

        await compactor.Received().RequestCompactionAsync(2, "size");
    }

    [Test]
    public async Task Size_threshold_with_no_tombstones_does_not_dispatch()
    {
        var options = new LatticeOptions { MaxLeafEntriesBeforeForcedCompaction = 2 };
        var (grain, compactor) = CreateGrainWithCompactor(options);

        // Total > threshold but tombstoneCount == 0; predicate guards
        // against requesting compaction on a leaf that has nothing to reap.
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("v"));

        await compactor.DidNotReceive().RequestCompactionAsync(Arg.Any<int>(), Arg.Any<string>());
    }

    [Test]
    public async Task Both_thresholds_crossed_in_single_evaluation_prefers_ratio()
    {
        var options = new LatticeOptions
        {
            MinTombstoneRatioForCompaction = 0.50,
            MaxLeafEntriesBeforeForcedCompaction = 2,
        };
        var (grain, compactor) = CreateGrainWithCompactor(options);

        // Build to the precise commit where both thresholds first cross
        // simultaneously: 1 live + 2 tombstones (ratio 0.66 >= 0.50,
        // total 3 > 2). The first commit that crosses ratio is the
        // final DeleteAsync; on every prior commit, ratio has not yet
        // crossed but size already has, so size fires first. The
        // contract is that within one evaluation ratio is checked
        // first - asserted by isolating a fresh leaf and reaching the
        // joint-cross commit directly.
        await grain.SetAsync("live", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("v"));
        await grain.DeleteAsync("a"); // size fires here (ratio 0.33 < 0.50)
        await grain.DeleteAsync("b"); // ratio fires here

        await Task.Yield();

        // At least one ratio dispatch must have landed (the final
        // commit's evaluation prefers ratio over size).
        await compactor.Received().RequestCompactionAsync(Arg.Any<int>(), "ratio");
    }
}
