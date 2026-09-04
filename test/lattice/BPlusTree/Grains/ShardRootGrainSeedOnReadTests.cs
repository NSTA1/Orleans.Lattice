using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the actual behaviour of <see cref="ShardRootGrain"/>'s read-side
/// entry points against a shard that has never been written to.
///
/// <para>
/// Each of these methods awaits <c>PrepareForOperationAsync</c> first, which
/// either sync-completes on the <c>RootNodeId</c>-not-null fast path or runs
/// <c>EnsureRootAsync</c> - which seeds a deterministic root leaf, persists
/// it, and otherwise throws. So a never-written shard is provably rooted by
/// the time the body runs, and each method reports the seeded root leaf
/// rather than an "empty shard" placeholder. Four <c>RootNodeId is null</c>
/// guards used to sit after those awaits documenting the impossible empty
/// case; they were unreachable from the commit that introduced them and were
/// removed in issue #1996. These tests exist so the removed assumption cannot
/// be reintroduced silently: every assertion below would have to change to
/// re-add a guard.
/// </para>
///
/// <para>
/// The root leaf substitute reports a sentinel <c>77</c> everywhere, so a
/// resurrected guard shows up immediately as a zero / null result rather than
/// as a subtle behavioural difference.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainSeedOnReadTests
{
    private const string TreeId = "seed-on-read-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>Sentinel the substituted root leaf reports; a revived empty-shard guard would report 0 instead.</summary>
    private const long LeafSentinel = 77;

    [Test]
    public async Task GetShardProjectionDigestAsync_seeds_the_root_and_reads_the_seeded_leaf()
    {
        var (grain, state, leaf) = CreateGrain();
        Assert.That(state.State.RootNodeId, Is.Null, "precondition: a never-written shard has no root.");

        var digest = await grain.GetShardProjectionDigestAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(digest.EntryCount, Is.EqualTo(LeafSentinel),
                "GetShardProjectionDigestAsync must read the root leaf that "
                + "PrepareForOperationAsync seeded, not return a bare empty digest.");
            Assert.That(state.State.RootNodeId, Is.Not.Null,
                "PrepareForOperationAsync must have materialised a root, so no "
                + "post-await empty-shard guard can be reachable.");
        });

        await leaf.Received(1).GetProjectionDigestAsync();
    }

    [Test]
    public async Task GetShardProjectionDigestForRangeAsync_seeds_the_root_and_reads_the_seeded_leaf()
    {
        var (grain, state, leaf) = CreateGrain();

        var digest = await grain.GetShardProjectionDigestForRangeAsync(null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(digest.EntryCount, Is.EqualTo(LeafSentinel),
                "GetShardProjectionDigestForRangeAsync must fold the seeded root "
                + "leaf's in-range snapshot, not return a bare empty digest.");
            Assert.That(state.State.RootNodeId, Is.Not.Null,
                "PrepareForOperationAsync must have materialised a root.");
        });

        await leaf.Received(1).GetProjectionDigestForRangeAsync(null, null);
    }

    [Test]
    public async Task GetTopologySnapshotAsync_seeds_the_root_and_describes_the_seeded_leaf()
    {
        var (grain, state, leaf) = CreateGrain();

        var topology = await grain.GetTopologySnapshotAsync(2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(topology.LiveCount, Is.EqualTo(LeafSentinel),
                "GetTopologySnapshotAsync must describe the seeded root leaf.");
            Assert.That(topology.IsLeaf, Is.True, "A freshly-seeded root is a leaf.");
            Assert.That(topology.ShardIndex, Is.EqualTo(0),
                "The shard root stamps its own shard index onto the returned node.");
            Assert.That(state.State.RootNodeId, Is.Not.Null,
                "PrepareForOperationAsync must have materialised a root, so the "
                + "method can never observe an empty shard and never returns null.");
        });

        await leaf.Received(1).GetTopologyNodeAsync();
    }

    [Test]
    public async Task WarmUpAsync_seeds_the_root_and_pings_the_seeded_leaf()
    {
        var (grain, state, leaf) = CreateGrain();

        await grain.WarmUpAsync();

        Assert.That(state.State.RootNodeId, Is.Not.Null,
            "WarmUpAsync must materialise a root through PrepareForOperationAsync.");

        // The ping is the whole point of warm-up: an early return on a
        // "still empty" shard would leave the root leaf cold.
        await leaf.Received(1).CountAsync();
    }

    private static (ShardRootGrain Grain, FakePersistentState<ShardRootState> State, IBPlusLeafGrain Leaf) CreateGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();

        var factory = Substitute.For<IGrainFactory>();

        var registry = Substitute.For<ILatticeRegistry>();
        registry.RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>())
            .Returns(Task.CompletedTask);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var leafGrainContext = Substitute.For<IGrainContext>();
        var leafGrainId = GrainId.Create("leaf", "seed-on-read-root-leaf");
        leafGrainContext.GrainId.Returns(leafGrainId);

        var leaf = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)leaf).GrainContext.Returns(leafGrainContext);
        leaf.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        leaf.SetShardIndexAsync(Arg.Any<int>()).Returns(Task.CompletedTask);
        leaf.CountAsync().Returns(Task.FromResult((int)LeafSentinel));
        leaf.GetProjectionDigestAsync().Returns(Task.FromResult(new LeafProjectionDigest
        {
            Hash = new byte[16],
            EntryCount = LeafSentinel,
            CheckpointOffset = 0,
            Version = LeafProjectionDigest.CurrentVersion,
        }));
        leaf.GetProjectionDigestForRangeAsync(Arg.Any<string?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(new ChildDigestSnapshot
            {
                Hash = new byte[16],
                EntryCount = LeafSentinel,
                CheckpointOffset = 0,
            }));
        leaf.GetTopologyNodeAsync().Returns(Task.FromResult(new ShardTopologyNode
        {
            NodeId = leafGrainId.ToString(),
            IsLeaf = true,
            SubtreeDepth = 1,
            EntryCount = LeafSentinel,
            LiveCount = LeafSentinel,
        }));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(leaf);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            factory: factory);

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return (grain, state, leaf);
    }
}
