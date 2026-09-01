using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Tests.Tenancy;
using Orleans.Lattice.Explorer.UI.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// The former free-text tenant controls are now a placement shim: they hold no
/// state and no behaviour, and render the real tenant scope control.
/// </summary>
/// <remarks>
/// A pure component test over stub seams - no cluster, host or channel - so it
/// carries no slow category.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantControlsBunitTests : BunitContext
{
    private readonly ExplorerTenantContext _context = new();

    private void Configure(bool isOperator)
    {
        JSInterop.Mode = JSRuntimeMode.Loose;
        _context.ActiveTenant = new ExplorerTenantId(SampleTenant.TenantId);

        var gate = new StubOperatorGate(isOperator);
        var view = new ExplorerTenantView(_context, gate);
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(true);

        Services.AddSingleton(session);
        Services.AddSingleton<IExplorerTenantContext>(_context);
        Services.AddSingleton<IExplorerTenantView>(view);
        Services.AddSingleton<IExplorerTenantScopeNotices>(new ExplorerTenantScopeNotices());
        Services.AddSingleton<IExplorerAccessibleTenantSource>(
            new FakeAccessibleTenantSource(SampleTenant.TenantId));
        Services.AddSingleton<IExplorerTenantSwitcher>(new ExplorerTenantSwitcher(view, _context, gate));
    }

    [Test]
    public void The_shim_renders_the_tenant_scope_control()
    {
        Configure(isOperator: false);

        var cut = Render<TenantControls>();

        Assert.That(cut.FindComponents<TenantScope>(), Is.Not.Empty);
    }

    [Test]
    public void The_shim_no_longer_offers_a_free_text_tenant_box()
    {
        // The defect this issue closes: a box demanding a tenant id from memory.
        Configure(isOperator: true);

        var cut = Render<TenantControls>();

        Assert.That(cut.FindAll("input[type=text]"), Is.Empty);
    }

    [Test]
    public void The_shim_forwards_its_placement_class()
    {
        Configure(isOperator: false);

        var cut = Render<TenantControls>(parameters => parameters.Add(p => p.Class, "lx-placed"));

        Assert.That(cut.Find("div").GetAttribute("class"), Does.Contain("lx-placed"));
    }
}
