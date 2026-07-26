using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpRegionIdentityVerifier"/>: the opt-in
/// probe that proves a peer region's advertised endpoint actually reaches the
/// cluster it claims (defeating an anycast/Front-Door endpoint that would silently
/// serve a cross-region call from the wrong region). Proves the match/mismatch
/// verdicts, the fail-open "cannot assert" skips (current region, no advertised
/// cluster id, no state facade, unknown region), memoisation to a single probe,
/// and re-probing after an unreachable attempt.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpRegionIdentityVerifierTests
{
    private static LatticeApiMcpRegionRouter RouterWithPeer(
        string peerRegionId, string peerClusterId, bool peerServesState = true)
    {
        var current = new LatticeApiMcpRegionDefinition
        {
            RegionId = "us",
            ClusterId = "cluster-us",
            IsCurrent = true,
            Groups = new Dictionary<LatticeApiMcpGroup, string?> { [LatticeApiMcpGroup.State] = null },
        };

        var peerGroups = new Dictionary<LatticeApiMcpGroup, string?>();
        if (peerServesState)
        {
            peerGroups[LatticeApiMcpGroup.State] = $"https://{peerRegionId}-state:5001";
        }
        else
        {
            peerGroups[LatticeApiMcpGroup.Data] = $"https://{peerRegionId}-data:5002";
        }

        var peer = new LatticeApiMcpRegionDefinition
        {
            RegionId = peerRegionId,
            ClusterId = peerClusterId,
            IsCurrent = false,
            Groups = peerGroups,
        };

        return new LatticeApiMcpRegionRouter("us", new[] { current, peer });
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

    private static ILatticeStateQuery StateReturning(string clusterId)
    {
        var stateQuery = Substitute.For<ILatticeStateQuery>();
        stateQuery.GetClusterInfoAsync(Arg.Any<CancellationToken>())
            .Returns(new ClusterInfo { ClusterId = clusterId, ServiceId = "svc" });
        return stateQuery;
    }

    [Test]
    public async Task Peer_whose_cluster_matches_the_advertised_id_is_verified()
    {
        var stateQuery = StateReturning("cluster-eu");
        var verifier = new LatticeApiMcpRegionIdentityVerifier(
            RouterWithPeer("eu", "cluster-eu"), ServicesWith(stateQuery));

        Assert.That(await verifier.VerifyAsync("eu"), Is.EqualTo(RegionIdentityVerdict.Verified));
    }

    [Test]
    public async Task Peer_whose_cluster_differs_from_the_advertised_id_is_a_mismatch()
    {
        // The endpoint answered as a different cluster - exactly the anycast/Front-Door
        // "served from the wrong region" trap this verifier exists to catch.
        var stateQuery = StateReturning("cluster-us");
        var verifier = new LatticeApiMcpRegionIdentityVerifier(
            RouterWithPeer("eu", "cluster-eu"), ServicesWith(stateQuery));

        Assert.That(await verifier.VerifyAsync("eu"), Is.EqualTo(RegionIdentityVerdict.Mismatch));
    }

    [Test]
    public async Task Probe_failure_is_unreachable()
    {
        var stateQuery = Substitute.For<ILatticeStateQuery>();
        stateQuery.GetClusterInfoAsync(Arg.Any<CancellationToken>())
            .Returns<Task<ClusterInfo>>(_ => throw new InvalidOperationException("state down"));
        var verifier = new LatticeApiMcpRegionIdentityVerifier(
            RouterWithPeer("eu", "cluster-eu"), ServicesWith(stateQuery));

        Assert.That(await verifier.VerifyAsync("eu"), Is.EqualTo(RegionIdentityVerdict.Unreachable));
    }

    [Test]
    public async Task Current_region_is_skipped_without_probing()
    {
        var stateQuery = StateReturning("cluster-eu");
        var verifier = new LatticeApiMcpRegionIdentityVerifier(
            RouterWithPeer("eu", "cluster-eu"), ServicesWith(stateQuery));

        var verdict = await verifier.VerifyAsync("us");

        Assert.Multiple(() =>
        {
            Assert.That(verdict, Is.EqualTo(RegionIdentityVerdict.Skipped));
            _ = stateQuery.DidNotReceive().GetClusterInfoAsync(Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task Unknown_region_is_skipped()
    {
        var verifier = new LatticeApiMcpRegionIdentityVerifier(
            RouterWithPeer("eu", "cluster-eu"), ServicesWith(StateReturning("cluster-eu")));

        Assert.That(await verifier.VerifyAsync("mars"), Is.EqualTo(RegionIdentityVerdict.Skipped));
    }

    [Test]
    public async Task Peer_without_an_advertised_cluster_id_is_skipped_without_probing()
    {
        var stateQuery = StateReturning("cluster-eu");
        var verifier = new LatticeApiMcpRegionIdentityVerifier(
            RouterWithPeer("eu", peerClusterId: string.Empty), ServicesWith(stateQuery));

        var verdict = await verifier.VerifyAsync("eu");

        Assert.Multiple(() =>
        {
            Assert.That(verdict, Is.EqualTo(RegionIdentityVerdict.Skipped));
            _ = stateQuery.DidNotReceive().GetClusterInfoAsync(Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task Peer_without_a_state_facade_is_skipped_without_probing()
    {
        var stateQuery = StateReturning("cluster-eu");
        var verifier = new LatticeApiMcpRegionIdentityVerifier(
            RouterWithPeer("eu", "cluster-eu", peerServesState: false), ServicesWith(stateQuery));

        var verdict = await verifier.VerifyAsync("eu");

        Assert.Multiple(() =>
        {
            Assert.That(verdict, Is.EqualTo(RegionIdentityVerdict.Skipped));
            _ = stateQuery.DidNotReceive().GetClusterInfoAsync(Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task Missing_state_query_is_skipped()
    {
        var verifier = new LatticeApiMcpRegionIdentityVerifier(
            RouterWithPeer("eu", "cluster-eu"), ServicesWith(stateQuery: null));

        Assert.That(await verifier.VerifyAsync("eu"), Is.EqualTo(RegionIdentityVerdict.Skipped));
    }

    [Test]
    public async Task A_stable_verdict_is_memoised_to_a_single_probe()
    {
        var stateQuery = StateReturning("cluster-eu");
        var verifier = new LatticeApiMcpRegionIdentityVerifier(
            RouterWithPeer("eu", "cluster-eu"), ServicesWith(stateQuery));

        var first = await verifier.VerifyAsync("eu");
        var second = await verifier.VerifyAsync("eu");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(RegionIdentityVerdict.Verified));
            Assert.That(second, Is.EqualTo(RegionIdentityVerdict.Verified));
            _ = stateQuery.Received(1).GetClusterInfoAsync(Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task An_unreachable_verdict_is_evicted_and_reprobed()
    {
        var stateQuery = Substitute.For<ILatticeStateQuery>();
        stateQuery.GetClusterInfoAsync(Arg.Any<CancellationToken>())
            .Returns<Task<ClusterInfo>>(
                _ => throw new InvalidOperationException("state down"),
                _ => Task.FromResult(new ClusterInfo { ClusterId = "cluster-eu", ServiceId = "svc" }));
        var verifier = new LatticeApiMcpRegionIdentityVerifier(
            RouterWithPeer("eu", "cluster-eu"), ServicesWith(stateQuery));

        var first = await verifier.VerifyAsync("eu");
        var second = await verifier.VerifyAsync("eu");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(RegionIdentityVerdict.Unreachable));
            Assert.That(second, Is.EqualTo(RegionIdentityVerdict.Verified),
                "An unreachable attempt is transient and must be re-probed, not cached.");
            _ = stateQuery.Received(2).GetClusterInfoAsync(Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public void Null_router_throws()
        => Assert.That(
            () => new LatticeApiMcpRegionIdentityVerifier(null!, ServicesWith(stateQuery: null)),
            Throws.ArgumentNullException);

    [Test]
    public void Null_services_throws()
        => Assert.That(
            () => new LatticeApiMcpRegionIdentityVerifier(RouterWithPeer("eu", "cluster-eu"), null!),
            Throws.ArgumentNullException);
}
