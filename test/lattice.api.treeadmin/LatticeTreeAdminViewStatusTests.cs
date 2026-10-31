using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the view status read on <see cref="LatticeTreeAdmin"/>: the live
/// apply lag and active generation are captured with one fused
/// <c>IViewMaintainerGrain.GetStatusAsync</c> call rather than separate lag and
/// active-tree reads. Driven purely with substitutes - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminViewStatusTests
{
    private const string View = "orders-by-customer";
    private const string Source = "orders";

    private sealed class AllowGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Allow());
    }

    private static (LatticeTreeAdmin Facade, IViewMaintainerGrain Maintainer) Create(ViewMaintainerStatus status)
    {
        var factory = Substitute.For<IGrainFactory>();
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.GetStatusAsync(Arg.Any<CancellationToken>()).Returns(status);
        factory.GetGrain<IViewMaintainerGrain>(View).Returns(maintainer);

        var projection = Substitute.For<ILatticeViewProjection>();
        projection.ProjectionVersion.Returns("provider-v1");
        var catalog = Substitute.For<IViewCatalog>();
        catalog.TryGet(View).Returns(new ViewRegistration(View, Source, projection, ProjectionProviderKey: "provider-a"));

        var facade = new LatticeTreeAdmin(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new AllowGate()),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver(),
            viewCatalog: catalog,
            viewFactory: Substitute.For<ILatticeViewFactory>());
        return (facade, maintainer);
    }

    [Test]
    public async Task GetViewStatusAsync_projects_lag_and_active_generation_from_one_maintainer_call()
    {
        var (facade, maintainer) = Create(new ViewMaintainerStatus(7, "view-orders-by-customer#g2"));

        var status = await facade.GetViewStatusAsync(View);

        Assert.Multiple(() =>
        {
            Assert.That(status.ViewName, Is.EqualTo(View));
            Assert.That(status.SourceTreeId, Is.EqualTo(Source));
            Assert.That(status.ApplyLag, Is.EqualTo(7));
            Assert.That(status.ActiveTreeId, Is.EqualTo("view-orders-by-customer#g2"));
        });
        await maintainer.Received(1).GetStatusAsync(Arg.Any<CancellationToken>());
        await maintainer.DidNotReceive().GetLagAsync(Arg.Any<CancellationToken>());
        await maintainer.DidNotReceive().GetActiveTreeIdAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetViewStatusAsync_reports_an_empty_active_tree_id_when_the_maintainer_has_none()
    {
        var (facade, _) = Create(new ViewMaintainerStatus(0, null!));

        var status = await facade.GetViewStatusAsync(View);

        Assert.That(status.ActiveTreeId, Is.Empty);
    }
}
