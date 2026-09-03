using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 1956: a shard's range delete walked its whole
/// leaf chain inside one non-reentrant call, so a wide range delete held the
/// shard - and every other request queued behind it - for the duration.
/// <para>
/// Bounding it weakens no documented guarantee. <c>ILattice.DeleteRangeAsync</c>
/// already fans out to every physical shard in parallel with no cross-shard
/// atomicity, and its documented visibility is per-key only, so the whole-shard
/// atomicity of the unbounded walk was an implementation artifact rather than a
/// contract. What must survive is the end state: every key in the range is
/// tombstoned once the caller has driven the walk to completion.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainRangeDeleteWorkBoundTests
{
    private const string TreeId = "range-delete-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required Func<int> LeafDeleteCalls { get; init; }
        public required Func<List<string>> RemainingKeys { get; init; }
    }

    /// <summary>
    /// Builds a forward leaf chain where leaf <c>i</c> owns keys
    /// <c>k{i:D3}-*</c> and declares its high bound as the next leaf's first
    /// key, so a bounded walk has a real resume point to return.
    /// </summary>
    private static Harness CreateChain(int leafCount, int keysPerLeaf, int maxLeavesPerBatch)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var ids = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
            ids[i] = GrainId.Create("leaf", $"leaf{i}");
        state.State.RootNodeId = ids[0];
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var live = new Dictionary<int, List<string>>();
        var deleteCalls = 0;

        for (var i = 0; i < leafCount; i++)
        {
            var keys = new List<string>();
            for (var j = 0; j < keysPerLeaf; j++)
                keys.Add($"k{i:D3}-{j:D3}");
            live[i] = keys;
        }

        for (var i = 0; i < leafCount; i++)
        {
            var index = i;
            var leaf = Substitute.For<IBPlusLeafGrain>();

            leaf.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<LatticePredicateNode?>())
                .Returns(call =>
                {
                    Interlocked.Increment(ref deleteCalls);
                    var lo = call.ArgAt<string>(0);
                    var hi = call.ArgAt<string>(1);
                    var mine = live[index];
                    var hit = mine.Where(k =>
                        string.CompareOrdinal(k, lo) >= 0 && string.CompareOrdinal(k, hi) < 0).ToList();
                    foreach (var k in hit) mine.Remove(k);
                    // PastRange once this leaf holds a key at/after the upper
                    // bound, mirroring the real leaf's termination signal.
                    var past = mine.Any(k => string.CompareOrdinal(k, hi) >= 0);
                    return Task.FromResult(new RangeDeleteResult
                    {
                        Deleted = hit.Count,
                        PastRange = past,
                        MatchedKeys = null,
                    });
                });

            // High bound is the next leaf's first key, so a bounded batch can
            // hand back a real resume position.
            var high = index + 1 < leafCount ? $"k{index + 1:D3}-000" : null;
            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = $"k{index:D3}-000",
                HighKeyExclusive = high,
            }));

            var next = index + 1 < leafCount ? (GrainId?)ids[index + 1] : null;
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));
            factory.GetGrain<IBPlusLeafGrain>(ids[index]).Returns(leaf);
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = maxLeavesPerBatch,
                MaxScanPageDuration = TimeSpan.Zero,
            },
            shardCount: 1,
            factory: factory);

        return new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            LeafDeleteCalls = () => Volatile.Read(ref deleteCalls),
            RemainingKeys = () => live.OrderBy(kv => kv.Key).SelectMany(kv => kv.Value).ToList(),
        };
    }

    [Test]
    public async Task A_bounded_batch_stops_after_its_leaf_budget_and_reports_a_resume_key()
    {
        var h = CreateChain(leafCount: 40, keysPerLeaf: 2, maxLeavesPerBatch: 4);

        var page = await h.Grain.DeleteRangeBoundedAsync("k000-000", "k999-999");

        Assert.Multiple(() =>
        {
            Assert.That(h.LeafDeleteCalls(), Is.LessThanOrEqualTo(5),
                "the walk must stop once the leaf budget is spent, instead of " +
                "holding the non-reentrant shard for the whole chain");
            Assert.That(page.ResumeFromInclusive, Is.Not.Null,
                "an incomplete shard walk must hand back a resume key");
        });
    }

    [Test]
    public async Task The_resume_key_strictly_advances_so_the_caller_cannot_spin()
    {
        var h = CreateChain(leafCount: 40, keysPerLeaf: 2, maxLeavesPerBatch: 3);

        var first = await h.Grain.DeleteRangeBoundedAsync("k000-000", "k999-999");
        Assert.That(first.ResumeFromInclusive, Is.Not.Null);
        var second = await h.Grain.DeleteRangeBoundedAsync(first.ResumeFromInclusive!, "k999-999");

        Assert.That(
            string.CompareOrdinal(second.ResumeFromInclusive ?? "k999-999", first.ResumeFromInclusive!),
            Is.GreaterThan(0),
            "each batch must advance past the last, or the driving loop never terminates");
    }

    /// <summary>
    /// The guarantee that must survive bounding: driving the bounded walk to
    /// completion tombstones every key in the range, exactly as the unbounded
    /// walk did.
    /// </summary>
    [Test]
    public async Task Draining_the_bounded_walk_deletes_every_key_in_the_range()
    {
        var h = CreateChain(leafCount: 30, keysPerLeaf: 3, maxLeavesPerBatch: 2);

        var total = 0;
        var from = "k000-000";
        var guard = 0;
        while (guard++ < 500)
        {
            var page = await h.Grain.DeleteRangeBoundedAsync(from, "k999-999");
            total += page.Deleted;
            if (page.ResumeFromInclusive is not { } next) break;
            from = next;
        }

        Assert.Multiple(() =>
        {
            Assert.That(guard, Is.LessThan(500), "the drive loop must terminate");
            Assert.That(total, Is.EqualTo(90), "every key in the range is tombstoned");
            Assert.That(h.RemainingKeys(), Is.Empty);
        });
    }

    [Test]
    public async Task A_partial_range_leaves_keys_outside_it_untouched()
    {
        var h = CreateChain(leafCount: 20, keysPerLeaf: 2, maxLeavesPerBatch: 3);

        var from = "k005-000";
        var guard = 0;
        while (guard++ < 500)
        {
            var page = await h.Grain.DeleteRangeBoundedAsync(from, "k010-000");
            if (page.ResumeFromInclusive is not { } next) break;
            from = next;
        }

        var remaining = h.RemainingKeys();
        Assert.Multiple(() =>
        {
            Assert.That(remaining, Has.None.Matches<string>(k =>
                string.CompareOrdinal(k, "k005-000") >= 0 && string.CompareOrdinal(k, "k010-000") < 0),
                "no key inside the range may survive");
            Assert.That(remaining, Has.Some.EqualTo("k000-000"), "keys below the range survive");
            Assert.That(remaining, Has.Some.EqualTo("k019-001"), "keys above the range survive");
        });
    }

    /// <summary>
    /// The legacy unbounded entrypoint is retained for a caller from an older
    /// build; it must still delete the whole range in one call.
    /// </summary>
    [Test]
    public async Task The_legacy_unbounded_entrypoint_still_deletes_the_whole_range_in_one_call()
    {
        var h = CreateChain(leafCount: 25, keysPerLeaf: 2, maxLeavesPerBatch: 3);

        var deleted = await h.Grain.DeleteRangeAsync("k000-000", "k999-999");

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.EqualTo(50));
            Assert.That(h.RemainingKeys(), Is.Empty);
        });
    }
}
