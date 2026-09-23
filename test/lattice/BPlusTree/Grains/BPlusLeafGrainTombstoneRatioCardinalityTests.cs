using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// <c>orleans.lattice.leaf.tombstone.ratio</c> must hold a number of series that
/// does not depend on how many leaves the store holds (issue #2518).
/// <para>
/// The histogram was tagged with the leaf grain's identity, so every leaf that
/// ever ran a compaction pass minted its own series. On a live deployment that
/// one family reached thousands of series and exhausted the collector's
/// per-family budget, after which every newly seen leaf was refused silently.
/// The bound is now one series per tree: the leaf identity is not a tag, and
/// every leaf of a tree records into the same series.
/// </para>
/// <para>
/// The fixture drives many distinct leaves across two trees through the public
/// compaction seam and counts the distinct tag sets actually recorded. Against
/// the per-leaf tag it observes one series per leaf; against the fix, one per
/// tree. Measurements are scoped to this fixture's own randomly named trees, so
/// a concurrently running fixture recording on the same process-wide instrument
/// cannot land in the capture.
/// </para>
/// </summary>
public sealed class BPlusLeafGrainTombstoneRatioCardinalityTests
{
    /// <summary>
    /// Leaves per tree. Well past one, so a per-leaf tag would be unmistakable:
    /// the pre-fix code records one series per leaf, i.e. this many per tree.
    /// </summary>
    private const int LeavesPerTree = 32;

    private const int RowCount = 14;

    /// <summary>Every seventh row is a tombstone.</summary>
    private static bool IsTombstoneRow(int i) => i % 7 == 6;

    /// <summary>
    /// Long enough that no tombstone is old enough to reap, so the pass samples
    /// the ratio and reaps nothing.
    /// </summary>
    private static readonly TimeSpan ReapNothing = TimeSpan.FromDays(365);

    private static LeafSnapshotRow[] Rows()
    {
        var rows = new LeafSnapshotRow[RowCount];
        for (var i = 0; i < RowCount; i++)
        {
            rows[i] = new LeafSnapshotRow(
                $"k{i:D4}",
                new LwwValue<byte[]>
                {
                    Value = IsTombstoneRow(i) ? null : new byte[16],
                    IsTombstone = IsTombstoneRow(i),
                    Timestamp = new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i },
                });
        }

        return rows;
    }

    private static async Task<BPlusLeafGrain> RehydratedLeafAsync(string treeId)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = 25L,
                EncodedRows = LeafSnapshotCodec.Encode(Rows()),
                SnapshotOffsetsByPartition = [25L],
            }));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        // A distinct grain id per leaf: this is the identity the pre-fix code
        // stamped onto every sample as a tag.
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId;
        state.State.ShardIndex = 0;

        var resolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
                MaxLeafBytes = 64L * 1024,
                LeafPartialHydrationEnabled = true,
                LeafHydrationResidentBytes = 8L * 1024,
            },
            maxLeafKeys: 1_000_000,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            resolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        Assert.That(
            await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None),
            Is.True,
            "precondition: the leaf must come online holding the corpus, or its ratio is never sampled");

        return grain;
    }

    /// <summary>
    /// Drives <see cref="LeavesPerTree"/> distinct leaves in each of the given
    /// trees through one compaction pass apiece, and returns every tag set the
    /// ratio histogram recorded for those trees, rendered as a sorted
    /// <c>key=value</c> signature, one entry per measurement.
    /// </summary>
    private static async Task<List<(string Tree, string Signature, string[] Keys)>> RecordAcrossLeavesAsync(
        params string[] treeIds)
    {
        var mine = new HashSet<string>(treeIds, StringComparer.Ordinal);
        var recorded = new List<(string Tree, string Signature, string[] Keys)>();

        var grains = new List<BPlusLeafGrain>();
        foreach (var treeId in treeIds)
        {
            for (var i = 0; i < LeavesPerTree; i++)
            {
                grains.Add(await RehydratedLeafAsync(treeId));
            }
        }

        using (MeterListening.StartForInstrument(
            LatticeMetrics.LeafTombstoneRatio,
            l => l.SetMeasurementEventCallback<double>((_, _, tags, _) =>
            {
                string? tree = null;
                var pairs = new List<string>(tags.Length);
                var keys = new string[tags.Length];
                for (var t = 0; t < tags.Length; t++)
                {
                    keys[t] = tags[t].Key;
                    pairs.Add($"{tags[t].Key}={tags[t].Value}");
                    if (tags[t].Key == LatticeMetrics.TagTree) tree = tags[t].Value as string;
                }

                if (tree is null || !mine.Contains(tree)) return;
                pairs.Sort(StringComparer.Ordinal);
                Array.Sort(keys, StringComparer.Ordinal);
                lock (recorded) recorded.Add((tree, string.Join(",", pairs), keys));
            })))
        {
            foreach (var grain in grains)
            {
                var reaped = await grain.CompactTombstonesAsync(ReapNothing);
                Assert.That(reaped, Is.Zero, "the grace period must hold every tombstone back");
            }
        }

        return recorded;
    }

    [Test]
    public async Task Tombstone_ratio_series_count_is_bounded_by_tree_count_not_leaf_count()
    {
        var treeA = $"tree-ratio-a-{Guid.NewGuid():N}";
        var treeB = $"tree-ratio-b-{Guid.NewGuid():N}";

        var recorded = await RecordAcrossLeavesAsync(treeA, treeB);

        var seriesPerTree = recorded
            .GroupBy(r => r.Tree, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Select(r => r.Signature).Distinct(StringComparer.Ordinal).Count());

        Assert.Multiple(() =>
        {
            // Non-vacuity: every leaf must actually have sampled, otherwise one
            // series per tree is satisfied by a sampler that barely ran.
            Assert.That(
                recorded, Has.Count.EqualTo(2 * LeavesPerTree),
                "every leaf's compaction pass must record exactly one ratio sample");
            Assert.That(
                seriesPerTree.Keys, Is.EquivalentTo(new[] { treeA, treeB }),
                "both trees must have recorded");
            Assert.That(
                seriesPerTree[treeA], Is.EqualTo(1),
                $"{LeavesPerTree} leaves of one tree must share one series, not mint one each");
            Assert.That(
                seriesPerTree[treeB], Is.EqualTo(1),
                $"{LeavesPerTree} leaves of one tree must share one series, not mint one each");
            Assert.That(
                recorded.Select(r => r.Signature).Distinct(StringComparer.Ordinal).Count(), Is.EqualTo(2),
                "the family must hold one series per tree across the whole population of leaves");
        });
    }

    [Test]
    public async Task Tombstone_ratio_is_tagged_by_tree_and_tenant_only()
    {
        var tree = $"tree-ratio-tags-{Guid.NewGuid():N}";

        var recorded = await RecordAcrossLeavesAsync(tree);

        Assert.That(recorded, Has.Count.EqualTo(LeavesPerTree), "every leaf must record one sample");

        var expectedKeys = new[] { LatticeMetrics.TagTree, LatticeTenantLabel.TagTenant };
        Array.Sort(expectedKeys, StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeMetrics.TagLeaf, Is.EqualTo("leaf"), "the retained tag key must not change value");
            foreach (var (_, _, keys) in recorded)
            {
                Assert.That(keys, Is.EqualTo(expectedKeys), "the tag set must be exactly tree and tenant");
                Assert.That(
                    keys, Does.Not.Contain(LatticeMetrics.TagLeaf),
                    "a per-leaf tag makes the family's cardinality grow with the store");
            }
        });
    }
}
