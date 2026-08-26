using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit coverage for <see cref="ViewTreeAliasObserver"/> (issue #1665): the
/// view-side hook that fans a core tree-registry physical-identity swap out to
/// every materialised-view maintainer sourcing from the affected logical tree as
/// an event-driven rebind. Verifies the source-tree fan-out (including the
/// many-views-one-source case), that non-matching views are skipped, that the
/// empty-catalog and no-match cases issue no notify, best-effort isolation when
/// one maintainer's notify throws, and cancellation propagation.
/// </summary>
[TestFixture]
public class ViewTreeAliasObserverTests
{
    private static TreeAliasChange Change(
        string tree = "sys-auth-policy",
        string oldPhysical = "sys-auth-policy",
        string newPhysical = "sys-auth-policy-v2") => new()
    {
        TreeId = tree,
        OldPhysicalTreeId = oldPhysical,
        NewPhysicalTreeId = newPhysical,
    };

    private static ViewRegistration Reg(string viewName, string sourceTreeId) =>
        new(viewName, sourceTreeId, Substitute.For<ILatticeViewProjection>());

    private static ViewTreeAliasObserver Create(
        IGrainFactory factory,
        params ViewRegistration[] registrations)
    {
        var catalog = Substitute.For<IViewCatalog>();
        catalog.All().Returns(registrations);
        return new ViewTreeAliasObserver(
            factory, catalog, NullLogger<ViewTreeAliasObserver>.Instance);
    }

    [Test]
    public async Task OnTreeAliasChanged_notifies_every_view_sourcing_the_changed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var groups = Substitute.For<IViewMaintainerGrain>();
        var edges = Substitute.For<IViewMaintainerGrain>();
        factory.GetGrain<IViewMaintainerGrain>("sys-auth-policy-history").Returns(groups);
        factory.GetGrain<IViewMaintainerGrain>("sys-auth-policy-audit").Returns(edges);

        // Two distinct views share one source tree - both must be rebound.
        var observer = Create(
            factory,
            Reg("sys-auth-policy-history", "sys-auth-policy"),
            Reg("sys-auth-policy-audit", "sys-auth-policy"));

        await observer.OnTreeAliasChangedAsync(Change(newPhysical: "sys-auth-policy-v2"), CancellationToken.None);

        await groups.Received(1).NotifySourceIdentityChangedAsync("sys-auth-policy-v2", Arg.Any<CancellationToken>());
        await edges.Received(1).NotifySourceIdentityChangedAsync("sys-auth-policy-v2", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnTreeAliasChanged_skips_views_sourcing_a_different_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var match = Substitute.For<IViewMaintainerGrain>();
        var other = Substitute.For<IViewMaintainerGrain>();
        factory.GetGrain<IViewMaintainerGrain>("policy-view").Returns(match);
        factory.GetGrain<IViewMaintainerGrain>("orders-view").Returns(other);

        var observer = Create(
            factory,
            Reg("policy-view", "sys-auth-policy"),
            Reg("orders-view", "orders"));

        await observer.OnTreeAliasChangedAsync(Change(tree: "sys-auth-policy", newPhysical: "sys-auth-policy-v2"), CancellationToken.None);

        await match.Received(1).NotifySourceIdentityChangedAsync("sys-auth-policy-v2", Arg.Any<CancellationToken>());
        await other.DidNotReceive().NotifySourceIdentityChangedAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnTreeAliasChanged_is_noop_when_catalog_is_empty()
    {
        var factory = Substitute.For<IGrainFactory>();
        var observer = Create(factory);

        await observer.OnTreeAliasChangedAsync(Change(), CancellationToken.None);

        factory.DidNotReceive().GetGrain<IViewMaintainerGrain>(Arg.Any<string>());
    }

    [Test]
    public async Task OnTreeAliasChanged_is_noop_when_no_view_sources_the_changed_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var observer = Create(factory, Reg("orders-view", "orders"));

        await observer.OnTreeAliasChangedAsync(Change(tree: "sys-auth-policy"), CancellationToken.None);

        factory.DidNotReceive().GetGrain<IViewMaintainerGrain>(Arg.Any<string>());
    }

    [Test]
    public async Task OnTreeAliasChanged_continues_to_other_views_when_one_maintainer_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        var bad = Substitute.For<IViewMaintainerGrain>();
        bad.NotifySourceIdentityChangedAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("maintainer down")));
        var good = Substitute.For<IViewMaintainerGrain>();
        factory.GetGrain<IViewMaintainerGrain>("bad-view").Returns(bad);
        factory.GetGrain<IViewMaintainerGrain>("good-view").Returns(good);

        var logger = Substitute.For<ILogger<ViewTreeAliasObserver>>();
        var catalog = Substitute.For<IViewCatalog>();
        catalog.All().Returns([Reg("bad-view", "sys-auth-policy"), Reg("good-view", "sys-auth-policy")]);
        var observer = new ViewTreeAliasObserver(factory, catalog, logger);

        await observer.OnTreeAliasChangedAsync(Change(newPhysical: "sys-auth-policy-v2"), CancellationToken.None);

        // The failing view must not starve the healthy one.
        await good.Received(1).NotifySourceIdentityChangedAsync("sys-auth-policy-v2", Arg.Any<CancellationToken>());
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
        factory.GetGrain<IViewMaintainerGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IViewMaintainerGrain>());
        var observer = Create(factory, Reg("policy-view", "sys-auth-policy"));
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
        var catalog = Substitute.For<IViewCatalog>();
        var logger = NullLogger<ViewTreeAliasObserver>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(() => new ViewTreeAliasObserver(null!, catalog, logger), Throws.ArgumentNullException);
            Assert.That(() => new ViewTreeAliasObserver(factory, null!, logger), Throws.ArgumentNullException);
            Assert.That(() => new ViewTreeAliasObserver(factory, catalog, null!), Throws.ArgumentNullException);
        });
    }
}
