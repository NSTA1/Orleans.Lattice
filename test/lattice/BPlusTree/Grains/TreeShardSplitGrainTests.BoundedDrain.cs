using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue 1973: the split's background
/// <see cref="ShardSplitPhase.Drain"/> sweep is work-bounded and resumes from a
/// persisted <b>key</b> cursor, while the two authoritative sweeps that run once
/// the source is frozen stay deliberately atomic.
/// <para>
/// The properties pinned here are the ones the phase machine depends on: a
/// bounded pass visits at most its budget, does not advance the phase, records
/// where to resume, and a resumed sequence forwards every moved-slot entry
/// exactly once. The Swap and Complete sweeps are asserted to sweep the whole
/// chain in one turn regardless of the per-pass budget, because a partial sweep
/// there would flip routing onto a target that is not yet equal to the source.
/// </para>
/// </summary>
[TestFixture]
public class TreeShardSplitGrainBoundedDrainTests
{
    private const string TreeId = "split-bounded-tree";

    private static string Key(int index) => $"k{index:D4}";

    private sealed record Harness(
        TreeShardSplitGrain Grain,
        FakePersistentState<TreeShardSplitState> State,
        IGrainFactory Factory,
        IShardRootGrain SourceShard,
        IShardRootGrain TargetShard,
        GrainId[] LeafIds,
        List<string> MergedKeys);

    /// <summary>
    /// Builds a split coordinator that is already mid-drain over a source shard
    /// with <paramref name="leafCount"/> leaves, each holding one moved-slot
    /// entry keyed by its position. Leaf <c>i</c> owns <c>[k{i}, k{i+1})</c> and
    /// the source resolves a resume key onto the leaf that owns it.
    /// </summary>
    private static Harness CreateDrainingSplit(int leafCount, int leavesPerPass)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("split", $"{TreeId}/0"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { BackgroundDrainLeavesPerPass = leavesPerPass };
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(TreeId).Returns(TreeId);
        var map = ShardMap.CreateDefault(16, 2);
        registry.GetShardMapAsync(TreeId).Returns(map);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 2 }));
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, options);

        var sourceShard = Substitute.For<IShardRootGrain>();
        var targetShard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(ci =>
        {
            var key = (string)ci[0];
            var idx = int.Parse(key[(key.LastIndexOf('/') + 1)..]);
            return idx == 0 ? sourceShard : targetShard;
        });

        var mergedKeys = new List<string>();
        targetShard.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>())
            .Returns(ci =>
            {
                mergedKeys.AddRange(((Dictionary<string, LwwValue<byte[]>>)ci[0]).Keys);
                return Task.CompletedTask;
            });

        var leafIds = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
            leafIds[i] = GrainId.Create("leaf", $"src-leaf-{i}");

        if (leafCount == 0)
        {
            sourceShard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
            sourceShard.GetLeafIdForKeyAsync(Arg.Any<string?>()).Returns(Task.FromResult<GrainId?>(null));
        }
        else
        {
            sourceShard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafIds[0]));
            sourceShard.GetLeafIdForKeyAsync(null).Returns(Task.FromResult<GrainId?>(leafIds[0]));

            var wall = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
            for (var i = 0; i < leafCount; i++)
            {
                var index = i;
                var leaf = Substitute.For<IBPlusLeafGrain>();
                grainFactory.GetGrain<IBPlusLeafGrain>(leafIds[i]).Returns(leaf);
                leaf.GetDeltaSinceForSlotsAsync(Arg.Any<VersionVector>(), Arg.Any<int[]>(), Arg.Any<int>())
                    .Returns(_ => Task.FromResult(new StateDelta
                    {
                        Entries = new Dictionary<string, LwwValue<byte[]>>
                        {
                            [$"entry-{index}"] = LwwValue<byte[]>.Create(
                                [(byte)index],
                                new HybridLogicalClock { WallClockTicks = wall, Counter = index }),
                        },
                        Version = new VersionVector(),
                    }));
                leaf.GetNextSiblingAsync().Returns(Task.FromResult(
                    index + 1 < leafCount ? (GrainId?)leafIds[index + 1] : null));
                leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
                {
                    LowKeyInclusive = Key(index),
                    HighKeyExclusive = index + 1 < leafCount ? Key(index + 1) : null,
                }));
                sourceShard.GetLeafIdForKeyAsync(Key(index)).Returns(Task.FromResult<GrainId?>(leafIds[index]));
            }
        }

        var state = new FakePersistentState<TreeShardSplitState>
        {
            State = new TreeShardSplitState
            {
                InProgress = true,
                Phase = ShardSplitPhase.Drain,
                OperationId = "op-1",
                SourceShardIndex = 0,
                TargetShardIndex = 1,
                MovedSlots = [1, 3, 5, 7],
                OriginalShardMap = map,
            },
        };

        var grain = new TreeShardSplitGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, optionsResolver,
            new LoggerFactory().CreateLogger<TreeShardSplitGrain>(), state);

        return new Harness(grain, state, grainFactory, sourceShard, targetShard, leafIds, mergedKeys);
    }

    [Test]
    public async Task Drain_visits_at_most_the_configured_leaves_per_pass()
    {
        var h = CreateDrainingSplit(leafCount: 5, leavesPerPass: 2);

        var complete = await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(complete, Is.False, "a bounded pass must yield rather than sweep the whole source");
            Assert.That(h.MergedKeys, Is.EqualTo(new[] { "entry-0", "entry-1" }));
            Assert.That(h.State.State.Phase, Is.EqualTo(ShardSplitPhase.Drain),
                "the split must stay in Drain until the historical sweep is exhausted");
            Assert.That(h.State.State.DrainCursorKey, Is.EqualTo(Key(2)),
                "a yielded pass must persist where to resume");
        });
    }

    [Test]
    public async Task Drain_resumes_from_the_persisted_key_cursor_and_forwards_every_entry_once()
    {
        var h = CreateDrainingSplit(leafCount: 5, leavesPerPass: 2);

        bool complete;
        var passes = 0;
        do
        {
            complete = await h.Grain.DrainAsync();
            passes++;
        }
        while (!complete && passes < 10);

        Assert.Multiple(() =>
        {
            Assert.That(complete, Is.True);
            Assert.That(passes, Is.EqualTo(3), "five leaves at two per pass is 2 + 2 + 1");
            Assert.That(h.MergedKeys, Is.EqualTo(new[]
            {
                "entry-0", "entry-1", "entry-2", "entry-3", "entry-4",
            }), "every moved-slot entry must be forwarded exactly once across the resumed passes");
            Assert.That(h.State.State.Phase, Is.EqualTo(ShardSplitPhase.Swap));
            Assert.That(h.State.State.DrainCursorKey, Is.Null,
                "the cursor must clear once the sweep completes, so Swap starts a fresh sweep");
        });
    }

    [Test]
    public async Task Drain_completes_immediately_for_a_source_with_no_leaves()
    {
        var h = CreateDrainingSplit(leafCount: 0, leavesPerPass: 2);

        var complete = await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(complete, Is.True);
            Assert.That(h.State.State.Phase, Is.EqualTo(ShardSplitPhase.Swap));
            Assert.That(h.State.State.DrainCursorKey, Is.Null);
        });
    }

    /// <summary>
    /// The authoritative sweep inside Swap runs against a source whose moved
    /// slots can no longer change, and is what makes the target provably equal
    /// to the source before routing flips onto it. It must therefore sweep the
    /// whole chain in one turn no matter how small the per-pass budget is.
    /// </summary>
    [Test]
    public async Task Swap_final_drain_is_not_bounded_by_the_per_pass_budget()
    {
        var h = CreateDrainingSplit(leafCount: 5, leavesPerPass: 1);
        h.State.State.Phase = ShardSplitPhase.Swap;

        await h.Grain.SwapAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.MergedKeys, Is.EqualTo(new[]
            {
                "entry-0", "entry-1", "entry-2", "entry-3", "entry-4",
            }), "the pre-flip sweep must read the whole source in one turn");
            Assert.That(h.State.State.Phase, Is.EqualTo(ShardSplitPhase.Reject));
        });
    }

    /// <summary>
    /// Same reasoning for the post-reject sweep, which captures deletes that
    /// landed during the freeze window before the source's split record clears.
    /// </summary>
    [Test]
    public async Task Finalise_final_drain_is_not_bounded_by_the_per_pass_budget()
    {
        var h = CreateDrainingSplit(leafCount: 5, leavesPerPass: 1);
        h.State.State.Phase = ShardSplitPhase.Complete;

        await h.Grain.FinaliseAsync();

        Assert.That(h.MergedKeys, Is.EqualTo(new[]
        {
            "entry-0", "entry-1", "entry-2", "entry-3", "entry-4",
        }), "the post-reject sweep must read the whole source in one turn");
    }

    /// <summary>
    /// The definition-of-done case for this grain: the chain is structurally
    /// changed between two passes. The leaf the cursor points into splits, and
    /// the resumed sweep must still forward every remaining entry rather than
    /// truncate at the old chain's shape.
    /// </summary>
    [Test]
    public async Task Drain_resumed_after_the_cursor_leaf_splits_forwards_both_halves()
    {
        var h = CreateDrainingSplit(leafCount: 3, leavesPerPass: 1);

        var first = await h.Grain.DrainAsync();
        Assert.That(first, Is.False);
        Assert.That(h.State.State.DrainCursorKey, Is.EqualTo(Key(1)));

        // Leaf 1 splits: its right half is grafted in ahead of leaf 2 and holds
        // an entry the first pass never saw.
        var rightHalf = GrainId.Create("leaf", "src-leaf-1-right");
        var rightHalfLeaf = Substitute.For<IBPlusLeafGrain>();

        h.Factory.GetGrain<IBPlusLeafGrain>(rightHalf).Returns(rightHalfLeaf);
        rightHalfLeaf.GetDeltaSinceForSlotsAsync(Arg.Any<VersionVector>(), Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(_ => Task.FromResult(new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>
                {
                    ["entry-1-right"] = LwwValue<byte[]>.Create([9], default),
                },
                Version = new VersionVector(),
            }));
        rightHalfLeaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(h.LeafIds[2]));
        rightHalfLeaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = "k0001m",
            HighKeyExclusive = Key(2),
        }));

        var leftHalf = h.Factory.GetGrain<IBPlusLeafGrain>(h.LeafIds[1]);
        leftHalf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(rightHalf));
        leftHalf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = Key(1),
            HighKeyExclusive = "k0001m",
        }));

        // The shard now routes the new separator onto the right half, which is
        // what lets a pass that parks there resume onto real data instead of
        // resolving nothing and concluding the sweep is finished.
        h.SourceShard.GetLeafIdForKeyAsync("k0001m").Returns(Task.FromResult<GrainId?>(rightHalf));

        bool complete;
        var passes = 0;
        do
        {
            complete = await h.Grain.DrainAsync();
            passes++;
        }
        while (!complete && passes < 10);

        Assert.Multiple(() =>
        {
            Assert.That(complete, Is.True);
            Assert.That(h.MergedKeys, Does.Contain("entry-1-right"),
                "the half grafted in between two passes must not be skipped");
            Assert.That(h.MergedKeys, Does.Contain("entry-2"),
                "the sweep must continue past the split rather than truncate");
        });
    }
}
