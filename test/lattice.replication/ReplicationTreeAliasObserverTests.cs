using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for <see cref="ReplicationTreeAliasObserver"/> (issue #1665):
/// the replication-side hook that fans a core tree-registry physical-identity
/// swap out to the per-peer shipper grains as an event-driven rebind. Verifies
/// the per-peer fan-out, the empty-peer fast path, best-effort isolation when
/// one peer's notify throws, and cancellation propagation.
/// </summary>
[TestFixture]
public class ReplicationTreeAliasObserverTests
{
    private static TreeAliasChange Change(
        string tree = "alpha",
        string oldPhysical = "alpha",
        string newPhysical = "alpha-v2") => new()
    {
        TreeId = tree,
        OldPhysicalTreeId = oldPhysical,
        NewPhysicalTreeId = newPhysical,
    };

    private static ReplicationTreeAliasObserver Create(
        IGrainFactory factory,
        IReadOnlyCollection<string> peers)
    {
        var topology = Substitute.For<IReplicationTopology>();
        topology.CurrentPeers.Returns(peers);
        return new ReplicationTreeAliasObserver(
            factory, topology, NullLogger<ReplicationTreeAliasObserver>.Instance);
    }

    [Test]
    public async Task OnTreeAliasChanged_notifies_each_peer_shipper_with_new_physical_id()
    {
        var factory = Substitute.For<IGrainFactory>();
        var sb = Substitute.For<IReplicationShipperGrain>();
        var sc = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-b").Returns(sb);
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-c").Returns(sc);
        var observer = Create(factory, ["site-b", "site-c"]);

        await observer.OnTreeAliasChangedAsync(Change(newPhysical: "alpha-v2"), CancellationToken.None);

        await sb.Received(1).NotifySourceIdentityChangedAsync("alpha-v2", Arg.Any<CancellationToken>());
        await sc.Received(1).NotifySourceIdentityChangedAsync("alpha-v2", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnTreeAliasChanged_is_noop_when_no_peers_configured()
    {
        var factory = Substitute.For<IGrainFactory>();
        var observer = Create(factory, []);

        await observer.OnTreeAliasChangedAsync(Change(), CancellationToken.None);

        factory.DidNotReceive().GetGrain<IReplicationShipperGrain>(Arg.Any<string>());
    }

    [Test]
    public async Task OnTreeAliasChanged_continues_to_other_peers_when_one_shipper_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        var bad = Substitute.For<IReplicationShipperGrain>();
        bad.NotifySourceIdentityChangedAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("shipper down")));
        var good = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-b").Returns(bad);
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-c").Returns(good);

        var logger = Substitute.For<ILogger<ReplicationTreeAliasObserver>>();
        var topology = Substitute.For<IReplicationTopology>();
        topology.CurrentPeers.Returns(["site-b", "site-c"]);
        var observer = new ReplicationTreeAliasObserver(factory, topology, logger);

        await observer.OnTreeAliasChangedAsync(Change(newPhysical: "alpha-v2"), CancellationToken.None);

        // The failing peer must not starve the healthy one.
        await good.Received(1).NotifySourceIdentityChangedAsync("alpha-v2", Arg.Any<CancellationToken>());
        logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            Arg.Is<Exception?>(e => e is InvalidOperationException),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public void OnTreeAliasChanged_propagates_cancellation()
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationShipperGrain>());
        var observer = Create(factory, ["site-b"]);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            () => observer.OnTreeAliasChangedAsync(Change(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var factory = Substitute.For<IGrainFactory>();
        var topology = Substitute.For<IReplicationTopology>();
        var logger = NullLogger<ReplicationTreeAliasObserver>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(() => new ReplicationTreeAliasObserver(null!, topology, logger), Throws.ArgumentNullException);
            Assert.That(() => new ReplicationTreeAliasObserver(factory, null!, logger), Throws.ArgumentNullException);
            Assert.That(() => new ReplicationTreeAliasObserver(factory, topology, null!), Throws.ArgumentNullException);
        });
    }
}
