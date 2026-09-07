using System.Text;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests that lock in the cache-side half of the same-silo
/// revision-cookie optimisation. The companion
/// <see cref="BPlusLeafGrainTests"/> partial covers the registry-side
/// invariants (publish-once <see cref="System.Runtime.CompilerServices.StrongBox{T}"/>,
/// monotonic advancement, deactivation pruning, allocation-free
/// steady-state bumps); these tests cover the matching reader contract
/// on <see cref="LeafCacheGrain"/>:
/// <list type="bullet">
///   <item>RPC is elided iff the published cookie has not advanced
///   since the last successful refresh.</item>
///   <item>RPC is taken when the cookie has advanced.</item>
///   <item>RPC is taken when the cookie is absent (cross-silo
///   primary), so multi-silo deployments retain their existing
///   refresh semantics by construction.</item>
///   <item>RPC is taken after a primary re-activation resets the
///   cookie sequence, so a quiescent re-activation cannot trick the
///   cache into believing nothing has changed.</item>
///   <item>RPC count is bounded by cookie advancement, not by read
///   count, so a tight read loop with no intervening writes does not
///   amplify cross-grain dispatch.</item>
/// </list>
/// A future change that drops the cookie skip-path or relocates the
/// cookie snapshot (for example, snapshotting the cookie before the
/// RPC instead of after, which would silently re-RPC every read) will
/// fail the call-count assertions below.
/// </summary>
public partial class LeafCacheGrainTests
{
    /// <summary>
    /// Builds a <see cref="LeafCacheGrain"/> whose primary leaf id is
    /// uniquely scoped to the calling test, alongside a real
    /// <see cref="BPlusLeafGrain"/> whose <see cref="GrainId"/>
    /// matches the cache's parsed primary id. Writes on the real leaf
    /// populate the process-wide revision registry; the cache reads
    /// the cookie via the static accessor and reaches the cross-grain
    /// path through a separate mocked <see cref="IBPlusLeafGrain"/>
    /// returned by the cache's <see cref="IGrainFactory"/>. The split
    /// (real leaf for registry, mock leaf for RPCs) lets the tests
    /// assert <see cref="NSubstitute.Received"/> counts on
    /// <see cref="IBPlusLeafGrain.GetDeltaSinceAsync"/> while the
    /// cookie is advanced by genuine state mutations.
    /// </summary>
    private static (LeafCacheGrain cache, BPlusLeafGrain registryPopulator, IBPlusLeafGrain mockPrimary, GrainId leafId) CreateCacheWithRegistryPopulator(
        string testName,
        LatticeOptions? options = null)
    {
        var unique = $"{testName}-{Guid.NewGuid():N}";
        var leafId = GrainId.Create("leaf", unique);

        // Real leaf: publishes the same-silo cookie via writes. Its
        // GrainId must match the cache's PrimaryLeafId, which the
        // cache parses from its own grain key.
        var registryPopulator = BPlusLeafGrainTests.CreateLeafGrainForCrossFixtureUse(replicaId: unique);

        // Mock primary: returned by the cache's grain factory, so the
        // cache's cross-grain calls land on a substitute we can assert
        // on. Default behaviour returns an empty delta and a tree id;
        // tests can override via .Returns(...) before exercising the
        // cache.
        var mockPrimary = Substitute.For<IBPlusLeafGrain>();
        mockPrimary.GetTreeIdAsync().Returns("test-tree");
        mockPrimary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());

        var cacheContext = Substitute.For<IGrainContext>();
        cacheContext.GrainId.Returns(GrainId.Create("cache", leafId.ToString()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(mockPrimary);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        // CacheTtl must be zero so we exercise the cookie fast-path
        // (and not the TTL fast-path) on every read.
        optionsMonitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions { CacheTtl = TimeSpan.Zero });

        var resolver = CreateResolver(grainFactory, optionsMonitor);
        var cache = new LeafCacheGrain(cacheContext, grainFactory, optionsMonitor, resolver, TestOriginClusterIdResolver.Default());
        return (cache, registryPopulator, mockPrimary, leafId);
    }

    [Test]
    public async Task RefreshAsync_skips_RPC_when_revision_cookie_unchanged()
    {
        var (cache, registryPopulator, mockPrimary, _) =
            CreateCacheWithRegistryPopulator(nameof(RefreshAsync_skips_RPC_when_revision_cookie_unchanged));

        // Advance the cookie once via a real write. The cache's
        // first read must take the cross-grain path because
        // _lastSeenPrimaryRevision is still 0 (the gate condition
        // requires > 0 to enable the skip).
        await registryPopulator.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await cache.GetAsync("any");

        // Second read with no intervening write: the cookie is
        // unchanged, the gate is satisfied, RPC must be elided.
        await cache.GetAsync("any");

        await mockPrimary.Received(1).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());
    }

    [Test]
    public async Task RefreshAsync_calls_RPC_when_revision_cookie_advances()
    {
        var (cache, registryPopulator, mockPrimary, _) =
            CreateCacheWithRegistryPopulator(nameof(RefreshAsync_calls_RPC_when_revision_cookie_advances));

        await registryPopulator.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await cache.GetAsync("any");                                  // RPC #1: cookie 0 -> snapshot
        await registryPopulator.SetAsync("k2", Encoding.UTF8.GetBytes("v2")); // cookie advances
        await cache.GetAsync("any");                                  // RPC #2: cookie advanced

        await mockPrimary.Received(2).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());
    }

    [Test]
    public async Task RefreshAsync_calls_RPC_when_cookie_absent_simulating_cross_silo_primary()
    {
        // Cross-silo simulation: build the cache without ever
        // populating the registry for its primary id. Every read
        // must reach the cross-grain path because TryGetLeafRevision
        // returns false on absence, the post-RPC snapshot stores 0,
        // and the gate condition stays disabled. This is the
        // multi-silo correctness guarantee made by the optimisation.
        var unique = $"crosssilo-{Guid.NewGuid():N}";
        var leafId = GrainId.Create("leaf", unique);

        var mockPrimary = Substitute.For<IBPlusLeafGrain>();
        mockPrimary.GetTreeIdAsync().Returns("test-tree");
        mockPrimary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());

        var cacheContext = Substitute.For<IGrainContext>();
        cacheContext.GrainId.Returns(GrainId.Create("cache", leafId.ToString()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(mockPrimary);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { CacheTtl = TimeSpan.Zero });

        var resolver = CreateResolver(grainFactory, optionsMonitor);
        var cache = new LeafCacheGrain(cacheContext, grainFactory, optionsMonitor, resolver, TestOriginClusterIdResolver.Default());

        // Pre-condition: registry has no entry for this leaf id.
        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out _), Is.False,
            "precondition: registry must be empty for the cross-silo simulation");

        await cache.GetAsync("any");
        await cache.GetAsync("any");
        await cache.GetAsync("any");

        await mockPrimary.Received(3).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());
    }

    [Test]
    public async Task RefreshAsync_calls_RPC_after_primary_reactivation_resets_cookie()
    {
        // The dangling-cookie shape: a cache observed cookie N from
        // an activation that has now deactivated. The matching
        // registry entry was pruned by OnDeactivateAsync; a
        // re-activation publishes a fresh StrongBox from a higher seed
        // and the first bump moves it past that seed. The cache, holding
        // _lastSeenPrimaryRevision from the prior activation, sees a
        // different value; the equality check fails and the refresh is
        // correctly forced.
        //
        // Classified as a GUARD, not a discriminator: the two activations
        // here perform different numbers of writes, so the cookies differ
        // even under the pre-#2151 per-activation counter. The equal-value
        // collision is covered by
        // RefreshAsync_calls_RPC_when_reactivated_primary_reaches_the_same_bump_count.
        var unique = $"reactivate-{Guid.NewGuid():N}";
        var leafId = GrainId.Create("leaf", unique);

        var first = BPlusLeafGrainTests.CreateLeafGrainForCrossFixtureUse(replicaId: unique);
        var mockPrimary = Substitute.For<IBPlusLeafGrain>();
        mockPrimary.GetTreeIdAsync().Returns("test-tree");
        mockPrimary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());

        var cacheContext = Substitute.For<IGrainContext>();
        cacheContext.GrainId.Returns(GrainId.Create("cache", leafId.ToString()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(mockPrimary);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { CacheTtl = TimeSpan.Zero });

        var resolver = CreateResolver(grainFactory, optionsMonitor);
        var cache = new LeafCacheGrain(cacheContext, grainFactory, optionsMonitor, resolver, TestOriginClusterIdResolver.Default());

        // First activation: many writes -> cookie advances; cache
        // observes it via one RPC.
        for (int i = 0; i < 5; i++)
        {
            await first.SetAsync($"k{i}", Encoding.UTF8.GetBytes("v"));
        }
        await cache.GetAsync("any");                  // RPC #1: snapshots cookie = 5
        await cache.GetAsync("any");                  // skip: cookie unchanged
        await mockPrimary.Received(1).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());

        // Deactivate the first activation and verify the registry
        // entry was pruned so the next bump runs against a fresh box.
        await ((IGrainBase)first).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);
        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out _), Is.False,
            "deactivation must prune the registry entry");

        // Second activation of the same GrainId: one write -> cookie
        // = 1. Cache holds _lastSeenPrimaryRevision = 5 (or whatever
        // the first activation advanced to); 1 != prior value, so the
        // RPC must be taken.
        var second = BPlusLeafGrainTests.CreateLeafGrainForCrossFixtureUse(replicaId: unique);
        await second.SetAsync("kfirst", Encoding.UTF8.GetBytes("v"));

        await cache.GetAsync("any");                  // RPC #2: cookie diverges
        await mockPrimary.Received(2).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());
    }

    [Test]
    public async Task RefreshAsync_calls_RPC_when_reactivated_primary_reaches_the_same_bump_count()
    {
        // ABA discriminator (issue #2151). This is the collision the
        // sibling test above cannot see: it drives the FIRST activation
        // through five writes and the second through one, so the two
        // cookies differ (1 != 5) and the cache refreshes for the wrong
        // reason - it passes with the defect fully present, which makes it
        // a guard rather than a discriminator.
        //
        // Here both activations perform EXACTLY ONE write. Under a
        // per-activation counter seeded at 0 both publish the value 1, the
        // cache's equality check succeeds against a DIFFERENT activation's
        // state, and it returns early on "provably fresh" - serving its
        // stale snapshot with no TTL involved and therefore no bound on how
        // long it does so. Seeding each activation from a process-wide
        // ticket makes the values disjoint, so the refresh is forced.
        //
        // CacheTtl is zero here (the product default): the ABA travels the
        // revision branch, which is reached before the TTL gate, so this
        // defect needs no TTL configuration to bite.
        var unique = $"aba-{Guid.NewGuid():N}";
        var leafId = GrainId.Create("leaf", unique);

        var mockPrimary = Substitute.For<IBPlusLeafGrain>();
        mockPrimary.GetTreeIdAsync().Returns("test-tree");
        mockPrimary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());

        var cacheContext = Substitute.For<IGrainContext>();
        cacheContext.GrainId.Returns(GrainId.Create("cache", leafId.ToString()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(mockPrimary);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { CacheTtl = TimeSpan.Zero });

        var resolver = CreateResolver(grainFactory, optionsMonitor);
        var cache = new LeafCacheGrain(cacheContext, grainFactory, optionsMonitor, resolver, TestOriginClusterIdResolver.Default());

        // Activation A: exactly one state advance.
        var first = BPlusLeafGrainTests.CreateLeafGrainForCrossFixtureUse(replicaId: unique);
        await first.SetAsync("k", Encoding.UTF8.GetBytes("v1"));
        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out var cookieUnderFirst), Is.True);

        await cache.GetAsync("any");   // RPC #1: stamps the activation-A cookie.
        await cache.GetAsync("any");   // skip: cookie unchanged.
        await mockPrimary.Received(1).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());

        await ((IGrainBase)first).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        // Activation B: exactly one state advance - the same bump count as
        // activation A, which is precisely the shape that collides.
        var second = BPlusLeafGrainTests.CreateLeafGrainForCrossFixtureUse(replicaId: unique);
        await second.SetAsync("k", Encoding.UTF8.GetBytes("v2"));
        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out var cookieUnderSecond), Is.True);
        Assert.That(cookieUnderSecond, Is.Not.EqualTo(cookieUnderFirst),
            "precondition: activation B must not republish activation A's cookie value");

        await cache.GetAsync("any");
        await mockPrimary.Received(2).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());
    }

    [Test]
    public async Task RefreshAsync_calls_RPC_after_primary_reactivates_without_writing_under_a_non_zero_CacheTtl()
    {
        // DISCRIMINATOR for the replay-path half of issue #2151.
        //
        // Read the TTL setting first, because it is the whole reason this
        // test can see anything. The cache's revision branch is only taken
        // when the registry holds an entry; with no entry it falls to the
        // TTL gate. That gate is guarded by `ttl > TimeSpan.Zero`, and
        // LatticeOptions.CacheTtl defaults to Zero - so at the default the
        // gate is UNREACHABLE, the cache refreshes on every read anyway, and
        // an assertion written here passes whether or not the defect is
        // present. Setting a non-zero TTL explicitly is what makes this a
        // test rather than a tautology.
        //
        // The shape: a cache observes a real cookie from activation A, the
        // leaf deactivates and re-activates WITHOUT any write (the projection
        // rebuild path - rebuild deactivates, the next activation replays),
        // and the cache reads again inside the TTL window. If activation
        // publishes a cookie the cache sees a value different from its stamp,
        // takes the revision branch, and refreshes. If it does not, the
        // registry is empty, the cache falls to the TTL gate, and it keeps
        // serving its snapshot for the rest of the window.
        var unique = $"replaypublish-{Guid.NewGuid():N}";
        var leafId = GrainId.Create("leaf", unique);

        var mockPrimary = Substitute.For<IBPlusLeafGrain>();
        mockPrimary.GetTreeIdAsync().Returns("test-tree");
        mockPrimary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());

        var cacheContext = Substitute.For<IGrainContext>();
        cacheContext.GrainId.Returns(GrainId.Create("cache", leafId.ToString()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(mockPrimary);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>())
            .Returns(new LatticeOptions { CacheTtl = TimeSpan.FromMinutes(5) });

        var resolver = CreateResolver(grainFactory, optionsMonitor);
        var cache = new LeafCacheGrain(cacheContext, grainFactory, optionsMonitor, resolver, TestOriginClusterIdResolver.Default());

        var first = BPlusLeafGrainTests.CreateLeafGrainForCrossFixtureUse(replicaId: unique);
        await first.SetAsync("k", Encoding.UTF8.GetBytes("v1"));

        await cache.GetAsync("any");   // RPC #1: stamps the activation-A cookie.
        await cache.GetAsync("any");   // skip: cookie unchanged.
        await mockPrimary.Received(1).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());

        await ((IGrainBase)first).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);
        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out _), Is.False,
            "precondition: deactivation prunes the registry entry");

        // Re-activate with no writes at all. Everything the leaf does here
        // runs on the activation path.
        var second = BPlusLeafGrainTests.CreateLeafGrainForCrossFixtureUse(replicaId: unique);
        await ((IGrainBase)second).OnActivateAsync(CancellationToken.None);

        // If this fails at Received(1): the primary re-activated and rebuilt
        // its projection, but the cache stayed on its snapshot. With no
        // cookie published on the activation path the registry is empty, the
        // revision branch is not taken, and the TTL gate returns early for
        // the rest of the window (issue #2151 defect 1).
        await cache.GetAsync("any");
        await mockPrimary.Received(2).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());
    }

    [Test]
    public async Task RefreshAsync_calls_RPC_only_once_per_cookie_advance_across_many_reads()
    {
        // Amortisation invariant: the optimisation's value
        // proposition is that a tight read loop following a single
        // write produces a bounded number of cross-grain calls
        // independent of read count. Concretely: 1 write + 100 reads
        // == 1 RPC. This pins the gate's hot-path semantics; if a
        // future change moves the snapshot of _lastSeenPrimaryRevision
        // before the RPC instead of after (which would re-RPC every
        // read because the cached cookie never reaches the current
        // value), this assertion fails with a 100x call-count
        // explosion.
        var (cache, registryPopulator, mockPrimary, _) =
            CreateCacheWithRegistryPopulator(nameof(RefreshAsync_calls_RPC_only_once_per_cookie_advance_across_many_reads));

        await registryPopulator.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        for (int i = 0; i < 100; i++)
        {
            await cache.GetAsync("any");
        }

        await mockPrimary.Received(1).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());
    }
}
