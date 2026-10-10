using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Round-trip accounting for the atomic value-plus-tags commit under a flag
/// membership mode, and for the covered-marker self-heal.
/// </summary>
/// <remarks>
/// Under a flag mode every membership row of an atomic commit is minted
/// against that row's own current state, so the states are read before the
/// saga is staged. They used to be read one <see cref="ILattice.GetAsync"/> at
/// a time - two serial index-tree round trips per tag - and are now read with
/// one <see cref="ILattice.GetManyAsync"/>. The self-heal used to write one
/// marker per awaited call; under LwwRegister it is now one batched write and
/// under a flag mode a bounded concurrent wave.
/// </remarks>
public partial class LatticeTagIndexRoundTripBatchingTests
{
    /// <summary>
    /// Builds a flag-mode coordinator context whose cross-tree saga is a
    /// substitute that records the batches it was asked to commit.
    /// </summary>
    private static (CountingTree tree, LatticeTagIndexContext ctx, List<List<LatticeTreeBatch>> commits) CreateAtomicFlagMode(
        LatticeMergeMode mode)
    {
        var tree = new CountingTree();
        var commits = new List<List<LatticeTreeBatch>>();
        var saga = Substitute.For<ILatticeCrossTreeTxGrain>();
        saga.CommitAsync(Arg.Any<List<LatticeTreeBatch>>())
            .Returns(ci =>
            {
                lock (commits)
                {
                    commits.Add(ci.Arg<List<LatticeTreeBatch>>());
                }
                return Task.FromResult(CrossTreeAtomicWriteOutcome.Committed);
            });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(tree.Lattice);
        grainFactory.GetGrain<ILatticeCrossTreeTxGrain>(Arg.Any<string>()).Returns(saga);
        return (tree, LatticeTagIndexContext.CreateForCoordinator(
            grainFactory, IndexName, mode, replicaId: "r1"), commits);
    }

    private static string TagMajorRow(string tag, string treeId, string key) =>
        string.Concat(tag, "\0", treeId, "\0", key);

    private static string KeyMajorRow(string treeId, string key, string tag) =>
        string.Concat("\0k\0", treeId, "\0", key, "\0", tag);

    [TestCase(LatticeMergeMode.OrFlag)]
    [TestCase(LatticeMergeMode.RwFlag)]
    public async Task Atomic_flag_mode_commit_reads_every_row_state_in_one_batched_read(LatticeMergeMode mode)
    {
        var (tree, ctx, _) = CreateAtomicFlagMode(mode);

        // Warm the covered-tree marker so the measured commit's hint check is a
        // cache hit and every read counted below belongs to the minting step.
        await ctx.CommitValueWithTagsAsync(TreeId, "warm", [9], ["warm"], TagConsistency.Atomic, CancellationToken.None);
        tree.GetAsyncCalls = 0;
        tree.GetManyAsyncCalls = 0;
        tree.GetManyWidths.Clear();

        string[] tags = ["red", "green", "blue", "amber", "violet"];
        await ctx.CommitValueWithTagsAsync(TreeId, "k1", [1, 2, 3], tags, TagConsistency.Atomic, CancellationToken.None);

        // Was ten serial GetAsync round trips (two rows per tag) before the
        // saga could be staged.
        Assert.Multiple(() =>
        {
            Assert.That(tree.GetAsyncCalls, Is.Zero);
            Assert.That(tree.GetManyAsyncCalls, Is.EqualTo(1));
            Assert.That(tree.GetManyWidths, Is.EqualTo(new[] { tags.Length * 2 }));
        });
    }

    [Test]
    public async Task Atomic_flag_mode_commit_mints_each_row_against_its_own_state_in_staging_order()
    {
        var (tree, ctx, commits) = CreateAtomicFlagMode(LatticeMergeMode.OrFlag);

        // One row already carries this replica's dot at counter 3, so its
        // freshly minted enable must take counter 4; every other row is absent
        // and mints counter 1. A batched read that mismatched states to rows
        // would mint the wrong counter on one of them.
        var seeded = new OrFlag();
        seeded.Enable("r1", 3);
        tree.Data[TagMajorRow("green", TreeId, "k1")] = JsonLatticeSerializer<OrFlag>.Default.Serialize(seeded);

        string[] tags = ["red", "green"];
        await ctx.CommitValueWithTagsAsync(TreeId, "k1", [1], tags, TagConsistency.Atomic, CancellationToken.None);

        Assert.That(commits, Has.Count.EqualTo(1));
        var indexSlice = commits[0].Single(b => b.TreeId != TreeId);
        string[] expectedKeys =
        [
            TagMajorRow("red", TreeId, "k1"),
            KeyMajorRow(TreeId, "k1", "red"),
            TagMajorRow("green", TreeId, "k1"),
            KeyMajorRow(TreeId, "k1", "green"),
        ];
        long[] expectedCounters = [1, 1, 4, 1];

        Assert.That(indexSlice.Entries.Select(e => e.Key), Is.EqualTo(expectedKeys));
        Assert.That(indexSlice.EntryDeltas, Is.Not.Null);
        for (var i = 0; i < expectedKeys.Length; i++)
        {
            var delta = JsonLatticeSerializer<OrFlagDelta>.Default.Deserialize(indexSlice.EntryDeltas![i]!);
            var state = JsonLatticeSerializer<OrFlag>.Default.Deserialize(indexSlice.Entries[i].Value);
            Assert.Multiple(() =>
            {
                Assert.That(delta.Enables.Single().ReplicaId, Is.EqualTo("r1"), expectedKeys[i]);
                Assert.That(delta.Enables.Single().Counter, Is.EqualTo(expectedCounters[i]), expectedKeys[i]);
                Assert.That(state.IsEnabled, Is.True, expectedKeys[i]);
                Assert.That(state.Enables.Max(d => d.Counter), Is.EqualTo(expectedCounters[i]), expectedKeys[i]);
            });
        }
    }

    [Test]
    public void Atomic_flag_mode_commit_rejects_an_invalid_tag_before_reading_any_row()
    {
        var (tree, ctx, commits) = CreateAtomicFlagMode(LatticeMergeMode.OrFlag);

        Assert.ThrowsAsync<ArgumentException>(() => ctx.CommitValueWithTagsAsync(
            TreeId, "k1", [1], ["red", "bad\0tag"], TagConsistency.Atomic, CancellationToken.None));
        Assert.Multiple(() =>
        {
            Assert.That(tree.GetAsyncCalls + tree.GetManyAsyncCalls, Is.Zero);
            Assert.That(commits, Is.Empty);
        });
    }

    // ── Covered-marker self-heal ──

    private static void SeedMembershipWithoutMarkers(CountingTree tree, params string[] treeIds)
    {
        foreach (var treeId in treeIds)
        {
            tree.Data[TagMajorRow("red", treeId, "k1")] = [1];
            tree.Data[KeyMajorRow(treeId, "k1", "red")] = [1];
        }
    }

    [Test]
    public async Task Covered_marker_self_heal_writes_every_marker_in_one_batched_call()
    {
        var tree = new CountingTree();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(tree.Lattice);
        var ctx = LatticeTagIndexContext.CreateForCoordinator(grainFactory, IndexName);
        SeedMembershipWithoutMarkers(tree, "t-a", "t-b", "t-c", "t-d");

        var covered = await ctx.GetCoveredTreesAsync(CancellationToken.None);

        // Was four serial SetAsync calls, one per discovered tree.
        Assert.Multiple(() =>
        {
            Assert.That(covered, Is.EqualTo(new[] { "t-a", "t-b", "t-c", "t-d" }));
            Assert.That(tree.SetAsyncCalls, Is.Zero);
            Assert.That(tree.SetManyAsyncCalls, Is.EqualTo(1));
            Assert.That(tree.SetManyWidths, Is.EqualTo(new[] { 4 }));
            Assert.That(
                tree.Data.Keys.Where(k => k.StartsWith("\0covered\0", StringComparison.Ordinal)),
                Is.EqualTo(new[] { "\0covered\0t-a", "\0covered\0t-b", "\0covered\0t-c", "\0covered\0t-d" }));
        });

        // The markers now exist, so a second read is served from them and
        // writes nothing.
        var again = await ctx.GetCoveredTreesAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(again, Is.EqualTo(covered));
            Assert.That(tree.SetManyAsyncCalls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Covered_marker_self_heal_with_no_membership_writes_nothing()
    {
        var tree = new CountingTree();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(tree.Lattice);
        var ctx = LatticeTagIndexContext.CreateForCoordinator(grainFactory, IndexName);

        var covered = await ctx.GetCoveredTreesAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(covered, Is.Empty);
            Assert.That(tree.TotalWriteCalls, Is.Zero);
        });
    }

    [Test]
    public async Task Flag_mode_covered_marker_self_heal_issues_its_marker_writes_concurrently()
    {
        var (tree, ctx) = CreateFlagMode();
        string[] trees = ["t-a", "t-b", "t-c", "t-d"];
        SeedMembershipWithoutMarkers(tree, trees);

        // The gate does not release until every marker enable is in flight, so
        // the serial loop it replaced cannot complete: its first enable would
        // wait forever on a fourth that is never issued.
        var expected = trees.Length;
        var arrived = 0;
        var peak = 0;
        var inFlight = 0;
        var allInFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        tree.ApplyCrdtDeltaGate = () =>
        {
            var now = Interlocked.Increment(ref inFlight);
            int seen;
            while (now > (seen = Volatile.Read(ref peak)) &&
                   Interlocked.CompareExchange(ref peak, now, seen) != seen)
            {
            }

            if (Interlocked.Increment(ref arrived) == expected)
            {
                allInFlight.TrySetResult();
            }

            return WaitThenLeave();
        };

        async Task WaitThenLeave()
        {
            await allInFlight.Task.ConfigureAwait(false);
            Interlocked.Decrement(ref inFlight);
        }

        var heal = ctx.GetCoveredTreesAsync(CancellationToken.None);
        var finished = await Task.WhenAny(heal, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.That(finished, Is.SameAs(heal), "the marker writes were not issued concurrently");
        var covered = await heal;

        Assert.Multiple(() =>
        {
            Assert.That(covered, Is.EqualTo(trees));
            Assert.That(peak, Is.EqualTo(expected));
            Assert.That(tree.CrdtDeltaKeys.Order(StringComparer.Ordinal), Is.EqualTo(trees.Select(t => "\0covered\0" + t)));
            Assert.That(tree.TotalWriteCalls, Is.Zero);
        });
    }
}
