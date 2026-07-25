namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpRegionRouter"/>: the frozen, allocation-lean
/// resolver both region discovery and per-call routing derive from. Proves the
/// default-region path (omitted and explicit), fail-closed rejection of an unknown
/// region and of a region that does not serve the requested group, a valid peer
/// route, and the discovery snapshot's per-group reachability.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpRegionRouterTests
{
    private static LatticeApiMcpRegionDefinition Region(
        string id,
        bool current,
        string clusterId,
        params LatticeApiMcpGroup[] groups)
    {
        var map = new Dictionary<LatticeApiMcpGroup, string?>();
        foreach (var group in groups)
        {
            map[group] = current ? null : $"https://{id}-{group}".ToLowerInvariant();
        }

        return new LatticeApiMcpRegionDefinition
        {
            RegionId = id,
            ClusterId = clusterId,
            IsCurrent = current,
            Groups = map,
        };
    }

    private static LatticeApiMcpRegionRouter TwoRegionRouter()
        => new(
            "current",
            new[]
            {
                Region("current", current: true, "cluster-a", LatticeApiMcpGroup.State, LatticeApiMcpGroup.Data),
                Region("peer", current: false, "cluster-b", LatticeApiMcpGroup.State),
            });

    [Test]
    public void Omitted_region_routes_to_the_default_region()
    {
        var route = TwoRegionRouter().Resolve(null, LatticeApiMcpGroup.Data);

        Assert.Multiple(() =>
        {
            Assert.That(route.IsRouted, Is.True);
            Assert.That(route.IsDefault, Is.True);
            Assert.That(route.ServedRegionId, Is.EqualTo("current"));
        });
    }

    [Test]
    public void Whitespace_region_routes_to_the_default_region()
        => Assert.That(TwoRegionRouter().Resolve("   ", LatticeApiMcpGroup.Data).IsDefault, Is.True);

    [Test]
    public void Explicit_default_region_routes_to_the_default_without_a_group_check()
    {
        // The default region serves State+Data; targeting it explicitly for a group
        // it does not enumerate still routes (the tool would not be advertised here
        // otherwise), so this must not be rejected.
        var route = TwoRegionRouter().Resolve("current", LatticeApiMcpGroup.Backup);

        Assert.Multiple(() =>
        {
            Assert.That(route.IsRouted, Is.True);
            Assert.That(route.IsDefault, Is.True);
        });
    }

    [Test]
    public void Valid_peer_region_serving_the_group_routes_there()
    {
        var route = TwoRegionRouter().Resolve("peer", LatticeApiMcpGroup.State);

        Assert.Multiple(() =>
        {
            Assert.That(route.IsRouted, Is.True);
            Assert.That(route.IsDefault, Is.False);
            Assert.That(route.ServedRegionId, Is.EqualTo("peer"));
        });
    }

    [Test]
    public void Unknown_region_is_rejected_fail_closed()
    {
        var route = TwoRegionRouter().Resolve("mars", LatticeApiMcpGroup.State);

        Assert.Multiple(() =>
        {
            Assert.That(route.IsRouted, Is.False);
            Assert.That(route.Fault, Does.Contain("Unknown region 'mars'"));
            Assert.That(route.Fault, Does.Contain("lattice_list_regions"));
        });
    }

    [Test]
    public void Peer_region_not_serving_the_group_is_rejected_fail_closed()
    {
        // The peer serves only State; Data must be refused there.
        var route = TwoRegionRouter().Resolve("peer", LatticeApiMcpGroup.Data);

        Assert.Multiple(() =>
        {
            Assert.That(route.IsRouted, Is.False);
            Assert.That(route.Fault, Does.Contain("Region 'peer'"));
            Assert.That(route.Fault, Does.Contain("data"));
        });
    }

    [Test]
    public void Snapshot_lists_the_regions_current_first_with_per_group_reachability()
    {
        var snapshot = TwoRegionRouter().Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot, Has.Count.EqualTo(2));
            Assert.That(snapshot[0].RegionId, Is.EqualTo("current"));
            Assert.That(snapshot[0].IsCurrent, Is.True);
            Assert.That(snapshot[1].RegionId, Is.EqualTo("peer"));
            Assert.That(snapshot[1].IsCurrent, Is.False);

            // The peer serves State but not Data.
            var peerState = snapshot[1].Groups.Single(g => g.Group == "state");
            var peerData = snapshot[1].Groups.Single(g => g.Group == "data");
            Assert.That(peerState.Available, Is.True);
            Assert.That(peerState.Endpoint, Is.EqualTo("https://peer-state"));
            Assert.That(peerData.Available, Is.False);
            Assert.That(peerData.Endpoint, Is.Null);
        });
    }

    [Test]
    public void Snapshot_reports_every_group_slot_for_each_region()
    {
        var snapshot = TwoRegionRouter().Snapshot();

        Assert.That(
            snapshot[0].Groups.Select(g => g.Group),
            Is.EquivalentTo(new[] { "state", "data", "backup", "auth", "telemetry", "replication" }),
            "Every facade group must have a reachability slot so a caller can read a complete picture.");
    }

    [Test]
    public void DefaultRegionId_is_exposed()
        => Assert.That(TwoRegionRouter().DefaultRegionId, Is.EqualTo("current"));

    [Test]
    public void Null_default_region_id_throws()
        => Assert.That(
            () => new LatticeApiMcpRegionRouter(null!, Array.Empty<LatticeApiMcpRegionDefinition>()),
            Throws.ArgumentException.Or.InstanceOf<ArgumentNullException>());
}
