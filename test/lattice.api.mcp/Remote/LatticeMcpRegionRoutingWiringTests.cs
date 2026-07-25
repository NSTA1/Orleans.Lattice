using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Region;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the region wiring both MCP topologies register: the in-silo base
/// binding's single current region, and the remote-host binding's multi-region
/// router and catalog built from the configured default region plus each peer.
/// Proves fail-closed discovery (a group with no configured endpoint is not
/// routable in a region), that the router and catalog derive from the same
/// configuration, and that the default single-region configuration is unchanged.
/// </summary>
[TestFixture]
public sealed class LatticeMcpRegionRoutingWiringTests
{
    private static readonly FakeCallInvoker Idle = new(_ => throw new InvalidOperationException());

    private static LatticeApiMcpRemoteEndpoint Endpoint(string address)
        => new() { Endpoint = address, CallInvoker = Idle };

    private static ServiceProvider MultiRegionProvider()
        => new ServiceCollection()
            .AddLatticeMcpRemote(o =>
            {
                o.RegionId = "us";
                o.ClusterId = "cluster-us";
                o.State = Endpoint("https://us-state:5001");
                o.Data = Endpoint("https://us-data:5002");
                o.Regions.Add(new LatticeApiMcpRemoteRegionOptions
                {
                    RegionId = "eu",
                    ClusterId = "cluster-eu",
                    State = Endpoint("https://eu-state:5001"),
                });
            })
            .BuildServiceProvider();

    [Test]
    public void In_silo_binding_registers_a_single_current_region_router()
    {
        using var provider = new ServiceCollection().AddLatticeMcp().BuildServiceProvider();

        var router = provider.GetRequiredService<ILatticeApiMcpRegionRouter>();

        Assert.Multiple(() =>
        {
            Assert.That(router.DefaultRegionId, Is.EqualTo("current"));
            Assert.That(router.Snapshot(), Has.Count.EqualTo(1));
            Assert.That(router.Snapshot()[0].IsCurrent, Is.True);
        });
    }

    [Test]
    public void In_silo_binding_registers_a_region_catalog()
    {
        using var provider = new ServiceCollection().AddLatticeMcp().BuildServiceProvider();

        Assert.That(provider.GetService<ILatticeRegionCatalog>(), Is.Not.Null);
    }

    [Test]
    public void Remote_binding_default_region_id_comes_from_options()
    {
        using var provider = MultiRegionProvider();

        Assert.That(
            provider.GetRequiredService<ILatticeApiMcpRegionRouter>().DefaultRegionId,
            Is.EqualTo("us"));
    }

    [Test]
    public void Remote_binding_lists_the_default_region_and_each_peer_current_first()
    {
        using var provider = MultiRegionProvider();

        var snapshot = provider.GetRequiredService<ILatticeApiMcpRegionRouter>().Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot[0].RegionId, Is.EqualTo("us"));
            Assert.That(snapshot[0].IsCurrent, Is.True);
            Assert.That(snapshot[0].ClusterId, Is.EqualTo("cluster-us"));
            Assert.That(snapshot[1].RegionId, Is.EqualTo("eu"));
            Assert.That(snapshot[1].IsCurrent, Is.False);
            Assert.That(snapshot[1].ClusterId, Is.EqualTo("cluster-eu"));
        });
    }

    [Test]
    public void Peer_region_is_routable_only_for_the_groups_it_configures()
    {
        using var provider = MultiRegionProvider();
        var router = provider.GetRequiredService<ILatticeApiMcpRegionRouter>();

        Assert.Multiple(() =>
        {
            // The EU peer configures State only.
            Assert.That(router.Resolve("eu", LatticeApiMcpGroup.State).IsRouted, Is.True);
            Assert.That(router.Resolve("eu", LatticeApiMcpGroup.Data).IsRouted, Is.False,
                "Fail-closed: a group with no endpoint in the region must not be routable there.");
        });
    }

    [Test]
    public void Unknown_region_is_rejected_by_the_wired_router()
    {
        using var provider = MultiRegionProvider();

        Assert.That(
            provider.GetRequiredService<ILatticeApiMcpRegionRouter>()
                .Resolve("mars", LatticeApiMcpGroup.State).IsRouted,
            Is.False);
    }

    [Test]
    public async Task Remote_catalog_lists_the_configured_regions_with_per_group_reachability()
    {
        using var provider = MultiRegionProvider();

        var regions = await provider.GetRequiredService<ILatticeRegionCatalog>().ListRegionsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us", "eu" }));

            var euState = regions[1].Groups.Single(g => g.Group == "state");
            var euData = regions[1].Groups.Single(g => g.Group == "data");
            Assert.That(euState.Available, Is.True);
            Assert.That(euState.Endpoint, Is.EqualTo("https://eu-state:5001"));
            Assert.That(euData.Available, Is.False);
        });
    }

    [Test]
    public void Single_region_configuration_is_unchanged()
    {
        using var provider = new ServiceCollection()
            .AddLatticeMcpRemote(o => o.State = Endpoint("https://state:5001"))
            .BuildServiceProvider();

        var router = provider.GetRequiredService<ILatticeApiMcpRegionRouter>();

        Assert.Multiple(() =>
        {
            Assert.That(router.DefaultRegionId, Is.EqualTo(LatticeApiMcpRemoteOptions.DefaultRegionId));
            Assert.That(router.Snapshot(), Has.Count.EqualTo(1),
                "With no configured peers only the current region is routable, exactly as before.");
        });
    }

    [Test]
    public void Facade_over_a_routing_invoker_resolves_without_building_a_channel()
    {
        // The provided CallInvoker means no GrpcChannel is built, proving the
        // routing invoker is assembled per group from the configured endpoints.
        using var provider = MultiRegionProvider();

        Assert.That(provider.GetService<Orleans.Lattice.Api.State.ILatticeStateQuery>(), Is.Not.Null);
    }
}
