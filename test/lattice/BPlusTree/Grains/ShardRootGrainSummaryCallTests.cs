using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class ShardRootGrainSummaryCallTests
{
    [TestCase("count")]
    [TestCase("moved-count")]
    [TestCase("any")]
    [TestCase("shallow")]
    [TestCase("deep")]
    [TestCase("footprint")]
    public async Task Walk_uses_one_summary_call_per_leaf_without_separate_metadata(string operation)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", "summary/0"));
        var factory = Substitute.For<IGrainFactory>();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        var leafIds = ids.Select(id => GrainId.Create("leaf", id.ToString("N"))).ToArray();
        var rootKey = Guid.NewGuid();
        var rootId = GrainId.Create("internal", rootKey.ToString("N"));
        var root = Substitute.For<IBPlusInternalGrain>();
        root.GetLeftmostChildAsync().Returns(leafIds[0]);
        root.GetLeftmostChildWithMetadataAsync().Returns((leafIds[0], true));
        root.GetRoutingTableAsync().Returns(new RoutingTableSnapshot
        {
            SeparatorKeys = [null, "m"],
            ChildIds = leafIds,
            ChildrenAreLeaves = true,
        });
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);
        factory.GetGrain<IBPlusInternalGrain>(rootKey).Returns(root);
        var leaves = new IBPlusLeafGrain[2];
        for (var i = 0; i < leaves.Length; i++)
        {
            var leaf = leaves[i] = Substitute.For<IBPlusLeafGrain>();
            var link = new LeafChainLink
            {
                NextSibling = i == 0 ? leafIds[1] : null,
                KeyRange = new LeafKeyRange
                {
                    LowKeyInclusive = i == 0 ? null : "m",
                    HighKeyExclusive = i == 0 ? "m" : null,
                },
            };
            leaf.CountAndNextSiblingAsync(null, null).Returns(new LeafCountPage
            {
                Count = operation == "any" ? 0 : 3,
                Link = link,
            });
            leaf.GetStatsAndNextSiblingAsync().Returns(new LeafStatsPage
            {
                Stats = new LeafStats { LiveKeys = 3, Tombstones = 1, StateBytes = 20 },
                Link = link,
            });
            factory.GetGrain<IBPlusLeafGrain>(leafIds[i]).Returns(leaf);
            factory.GetGrain<IBPlusLeafGrain>(ids[i]).Returns(leaf);
            var snapshot = Substitute.For<ILeafSnapshotStorageGrain>();
            snapshot.GetSnapshotByteSizeAsync(Arg.Any<CancellationToken>()).Returns(10L);
            factory.GetGrain<ILeafSnapshotStorageGrain>(ids[i]).Returns(snapshot);
        }
        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;
        var grain = new ShardRootGrain(context, state, factory,
            TestOptionsResolver.Create(baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = 10,
                MaxScanPageDuration = TimeSpan.Zero,
            }, factory: factory),
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());

        switch (operation)
        {
            case "count":
                Assert.That((await grain.CountBoundedAsync(null, null)).Count, Is.EqualTo(6));
                break;
            case "moved-count":
                Assert.That((await grain.CountWithMovedAwayBoundedAsync(null)).Count, Is.EqualTo(6));
                break;
            case "any":
                Assert.That((await grain.AnyBoundedAsync(null)).Found, Is.False);
                break;
            case "shallow":
            case "deep":
                var report = (await grain.GetDiagnosticsBoundedAsync(operation == "deep", null)).Report;
                Assert.That(report.LiveKeys, Is.EqualTo(6));
                Assert.That(report.Tombstones, Is.EqualTo(operation == "deep" ? 2 : 0));
                break;
            case "footprint":
                var usage = (await grain.RefreshLeafByteFootprintsBoundedAsync(
                    null, default, CancellationToken.None)).Usage;
                Assert.That(usage.LeafStateBytes, Is.EqualTo(40));
                Assert.That(usage.SnapshotBytes, Is.EqualTo(20));
                Assert.That(usage.LiveKeys, Is.EqualTo(6));
                break;
            default:
                Assert.Fail("Unknown operation.");
                break;
        }

        foreach (var leaf in leaves)
        {
            if (operation is "deep" or "footprint")
                await leaf.Received(1).GetStatsAndNextSiblingAsync();
            else
                await leaf.Received(1).CountAndNextSiblingAsync(null, null);
            await leaf.DidNotReceive().GetKeyRangeAndNextSiblingAsync();
            await leaf.DidNotReceive().GetNextSiblingAsync();
            await leaf.DidNotReceive().GetStatsAsync();
            await leaf.DidNotReceive().CountAsync(Arg.Any<string?>(), Arg.Any<string?>());
            await leaf.DidNotReceive().CountAsync();
        }
    }
}
