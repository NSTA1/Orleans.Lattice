using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Region;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpRegionCatalog"/>: the topology-agnostic
/// catalog that projects the region router's snapshot and lazily enriches the
/// current region's cluster id from the state facade when the router did not know
/// it (the in-silo case). Proves the fast path (router already knows every cluster
/// id) returns the snapshot verbatim, the enrichment path fills the current
/// region's empty cluster id, and enrichment is best-effort when the state facade
/// is absent or faults.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpRegionCatalogTests
{
    private static LatticeApiMcpRegionRouter Router(string currentClusterId)
    {
        var current = new LatticeApiMcpRegionDefinition
        {
            RegionId = "current",
            ClusterId = currentClusterId,
            IsCurrent = true,
            Groups = new Dictionary<LatticeApiMcpGroup, string?> { [LatticeApiMcpGroup.State] = null },
        };

        return new LatticeApiMcpRegionRouter("current", new[] { current });
    }

    private static IServiceProvider ServicesWith(ILatticeStateQuery? stateQuery)
    {
        var services = new ServiceCollection();
        if (stateQuery is not null)
        {
            services.AddSingleton(stateQuery);
        }

        return services.BuildServiceProvider();
    }

    [Test]
    public async Task Known_cluster_id_returns_the_snapshot_verbatim_without_a_state_call()
    {
        var stateQuery = Substitute.For<ILatticeStateQuery>();
        var catalog = new LatticeApiMcpRegionCatalog(Router("cluster-a"), ServicesWith(stateQuery));

        var regions = await catalog.ListRegionsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(regions[0].ClusterId, Is.EqualTo("cluster-a"));
            _ = stateQuery.DidNotReceive().GetClusterInfoAsync(Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task Empty_cluster_id_is_enriched_from_the_state_facade()
    {
        var stateQuery = Substitute.For<ILatticeStateQuery>();
        stateQuery.GetClusterInfoAsync(Arg.Any<CancellationToken>())
            .Returns(new ClusterInfo { ClusterId = "resolved-cluster", ServiceId = "svc" });

        var catalog = new LatticeApiMcpRegionCatalog(Router(string.Empty), ServicesWith(stateQuery));

        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions[0].ClusterId, Is.EqualTo("resolved-cluster"));
    }

    [Test]
    public async Task Enrichment_is_best_effort_when_no_state_facade_is_registered()
    {
        var catalog = new LatticeApiMcpRegionCatalog(Router(string.Empty), ServicesWith(stateQuery: null));

        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions[0].ClusterId, Is.Empty,
            "Without a state facade the cluster id stays empty; the region set is still authoritative.");
    }

    [Test]
    public async Task Enrichment_is_best_effort_when_the_state_facade_faults()
    {
        var stateQuery = Substitute.For<ILatticeStateQuery>();
        stateQuery.GetClusterInfoAsync(Arg.Any<CancellationToken>())
            .Returns<Task<ClusterInfo>>(_ => throw new InvalidOperationException("state down"));

        var catalog = new LatticeApiMcpRegionCatalog(Router(string.Empty), ServicesWith(stateQuery));

        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions[0].ClusterId, Is.Empty);
    }

    [Test]
    public void Null_router_throws()
        => Assert.That(
            () => new LatticeApiMcpRegionCatalog(null!, ServicesWith(stateQuery: null)),
            Throws.ArgumentNullException);

    [Test]
    public void Null_services_throws()
        => Assert.That(
            () => new LatticeApiMcpRegionCatalog(Router("c"), null!),
            Throws.ArgumentNullException);

    private static LatticeApiMcpRegionRouter RouterWithPeer(string peerRegionId)
    {
        var current = new LatticeApiMcpRegionDefinition
        {
            RegionId = "us",
            ClusterId = "cluster-us",
            IsCurrent = true,
            Groups = new Dictionary<LatticeApiMcpGroup, string?> { [LatticeApiMcpGroup.State] = null },
        };
        var peer = new LatticeApiMcpRegionDefinition
        {
            RegionId = peerRegionId,
            ClusterId = $"cluster-{peerRegionId}",
            IsCurrent = false,
            Groups = new Dictionary<LatticeApiMcpGroup, string?>
            {
                [LatticeApiMcpGroup.State] = $"https://{peerRegionId}-state:5001",
            },
        };

        return new LatticeApiMcpRegionRouter("us", new[] { current, peer });
    }

    private static IServiceProvider ServicesWith(RegionIdentityVerdict peerVerdict)
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeApiMcpRegionIdentityVerifier>(new FakeVerifier(peerVerdict));
        return services.BuildServiceProvider();
    }

    [Test]
    public async Task Verified_peer_is_listed()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            RouterWithPeer("eu"), ServicesWith(RegionIdentityVerdict.Verified));

        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us", "eu" }));
    }

    [Test]
    public async Task Skipped_peer_is_listed()
    {
        // The verifier cannot assert against the peer (fail-open), so it stays routable.
        var catalog = new LatticeApiMcpRegionCatalog(
            RouterWithPeer("eu"), ServicesWith(RegionIdentityVerdict.Skipped));

        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us", "eu" }));
    }

    [Test]
    public async Task Mismatched_peer_is_omitted()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            RouterWithPeer("eu"), ServicesWith(RegionIdentityVerdict.Mismatch));

        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us" }),
            "Fail-closed discovery: a peer that resolves to the wrong cluster is not advertised.");
    }

    [Test]
    public async Task Unreachable_peer_is_omitted()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            RouterWithPeer("eu"), ServicesWith(RegionIdentityVerdict.Unreachable));

        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us" }));
    }

    private sealed class FakeVerifier(RegionIdentityVerdict peerVerdict) : ILatticeApiMcpRegionIdentityVerifier
    {
        public ValueTask<RegionIdentityVerdict> VerifyAsync(
            string regionId, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(peerVerdict);
    }
}
