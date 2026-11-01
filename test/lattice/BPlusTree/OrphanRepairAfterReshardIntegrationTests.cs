using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The orphaned-leaf repair unsplices what its audit judged repairable on a
/// tree whose history has sealed its leaves (#5550).
/// <para>
/// A reshard grow splits each source shard of a tree's physical copy and seals
/// every one of its leaves with the slots it gave away, and the logical tree is
/// then aliased onto that copy.
/// An interrupted leaf split then leaves an orphan behind one of those sealed
/// leaves. The audit, a dry run, reports it repairable. Before the fix the
/// repair's unsplice asked the sealed predecessor to unlink it, and the
/// predecessor declined for its seal even though the unsplice widens nothing,
/// so the pass unspliced nothing and the re-audit found the orphan again.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class OrphanRepairAfterReshardIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
            await _fixture.DisposeAsync();
    }

    private IGrainFactory Grains => _fixture.Cluster.Client;

    [Test]
    public async Task An_orphan_behind_a_resharded_aliased_trees_sealed_leaf_is_audited_repairable_and_repaired()
    {
        var treeId = $"orphan-reshard-{Guid.NewGuid():N}";
        var physicalId = $"{treeId}-copy";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var physical = await _fixture.CreateTreeAsync(physicalId);
        var expected = new Dictionary<string, string>(StringComparer.Ordinal);

        await WriteAsync(physical, expected, "pre", 120);

        // The reshard: a grow splits each source shard and seals its leaves.
        var before = await physical.GetRoutingAsync(forceRefresh: true);
        var sourceShards = before.Map.GetPhysicalShardIndices().ToHashSet();
        await physical.ReshardAsync(sourceShards.Count * 2);
        await TestPoll.UntilAsync(() => physical.IsReshardCompleteAsync(), "the reshard to complete",
            timeout: TimeSpan.FromMinutes(3));

        // The alias: the logical tree now routes to the resharded copy, as a
        // restored tree routes to its restored physical copy.
        await Grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).SetAliasAsync(treeId, physicalId);
        var routing = await tree.GetRoutingAsync(forceRefresh: true);
        Assert.Multiple(() =>
        {
            Assert.That(routing.PhysicalTreeId, Is.EqualTo(physicalId), "precondition: the tree is aliased");
            Assert.That(routing.Map.GetPhysicalShardIndices().Count(), Is.EqualTo(sourceShards.Count * 2),
                "precondition: the aliased copy was grown");
        });

        await WriteAsync(tree, expected, "post", 400);

        Assert.That(await CollectFindingsAsync(tree, repair: false), Is.Empty,
            "precondition: the tree has no orphan before one is injected");

        var orphanId = await InjectOrphanBehindSealedLeafAsync(routing, sourceShards);

        var audit = await CollectFindingsAsync(tree, repair: false);
        Assert.That(audit.Select(f => (f.LeafId, f.Disposition)),
            Is.EqualTo(new[] { (orphanId.ToString(), OrphanedLeafDisposition.Repairable) }),
            "the audit finds the injected leaf and judges it repairable");

        var repair = await CollectFindingsAsync(tree, repair: true);
        Assert.That(repair.Select(f => (f.LeafId, f.Disposition)),
            Is.EqualTo(new[] { (orphanId.ToString(), OrphanedLeafDisposition.Repaired) }),
            "the repair unsplices the leaf its audit judged repairable (#5550)");

        Assert.That(await CollectFindingsAsync(tree, repair: false), Is.Empty,
            "the re-audit no longer finds the orphan");

        foreach (var (key, value) in expected)
        {
            var read = await tree.GetAsync(key);
            Assert.That(read is null ? null : Encoding.UTF8.GetString(read), Is.EqualTo(value),
                $"no row was lost by the repair: {key}");
        }
    }

    private static async Task WriteAsync(ILattice tree, Dictionary<string, string> expected, string prefix, int count)
    {
        var batch = new List<KeyValuePair<string, byte[]>>(count);
        for (var i = 0; i < count; i++)
        {
            var key = $"{prefix}-{i:D4}";
            var value = $"value-{key}";
            expected[key] = value;
            batch.Add(new KeyValuePair<string, byte[]>(key, Encoding.UTF8.GetBytes(value)));
        }

        await tree.SetManyAsync(batch);
    }

    private static async Task<List<OrphanedLeafFinding>> CollectFindingsAsync(ILattice tree, bool repair)
    {
        var findings = new List<OrphanedLeafFinding>();
        string? resume = null;
        do
        {
            var report = repair
                ? await tree.RepairOrphanedLeavesAsync(resume)
                : await tree.InspectOrphanedLeavesAsync(resume);
            Assert.That(report.Gaps, Is.Empty, "every region of the tree was judged");
            findings.AddRange(report.Findings);
            resume = report.ResumeFrom;
        }
        while (resume is not null);

        return findings;
    }

    /// <summary>
    /// Leaves the persisted state of a leaf split interrupted before its parent
    /// adopted the new sibling, behind the second leaf of a source shard: the
    /// sibling declares its donor's bounds, holds a shadow copy of the donor's
    /// readable rows, and is linked into the chain from both sides.
    /// </summary>
    private async Task<GrainId> InjectOrphanBehindSealedLeafAsync(RoutingInfo routing, HashSet<int> sourceShards)
    {
        foreach (var shardIndex in routing.Map.GetPhysicalShardIndices().Where(sourceShards.Contains).Order())
        {
            var shard = Grains.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{shardIndex}");
            if (await shard.GetLeftmostLeafIdAsync() is not { } headId) continue;

            var head = await Grains.GetGrain<IBPlusLeafGrain>(headId).GetKeyRangeAndNextSiblingAsync();
            if (head.NextSibling is not { } donorId) continue;

            var donor = Grains.GetGrain<IBPlusLeafGrain>(donorId);
            var link = await donor.GetKeyRangeAndNextSiblingAsync();
            if (link.KeyRange.LowKeyInclusive is null) continue;

            // Only rows a reader can see through the donor: a sealed leaf may
            // still hold rows for the slots it gave away, and an orphan copy of
            // one of those would be refused as unverifiable rather than
            // exercise the unsplice.
            var shadow = new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
            foreach (var row in await donor.GetLiveRawEntriesAsync())
            {
                if (await donor.GetAsync(row.Key) is not null)
                    shadow[row.Key] = row.ToLwwValue();
            }

            if (shadow.Count == 0) continue;

            var orphanId = Grains.GetGrain<IBPlusLeafGrain>(Guid.NewGuid()).GetGrainId();
            var orphan = Grains.GetGrain<IBPlusLeafGrain>(orphanId);
            using (LatticeNewLeafIntentContext.BeginScope(orphanId))
            {
                await orphan.InitializeSiblingAsync(new SiblingInitialization
                {
                    TreeId = routing.PhysicalTreeId,
                    ShardIndex = shardIndex,
                    LowKeyInclusive = link.KeyRange.LowKeyInclusive,
                    HighKeyExclusive = link.KeyRange.HighKeyExclusive,
                    PrevSibling = donorId,
                    NextSibling = link.NextSibling,
                });
                await orphan.MergeEntriesAsync(shadow);
            }

            await donor.SetNextSiblingAsync(orphanId);
            if (link.NextSibling is { } next)
                await Grains.GetGrain<IBPlusLeafGrain>(next).SetPrevSiblingAsync(orphanId);

            return orphanId;
        }

        Assert.Fail("precondition: some source shard has a second leaf with readable rows to shadow");
        return default;
    }
}
