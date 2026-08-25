using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// The mandatory isolation matrix for the Explorer's fail-closed tenant view.
/// Every row is asserted directly against <see cref="ExplorerTenantView"/> with a
/// deterministic operator gate and tenant context - no cluster, no timing, no
/// ordering, no wall-clock, and no GC dependence.
/// </summary>
[TestFixture]
public class ExplorerTenantViewTests
{
    private static readonly ExplorerTenantId Acme = new("acme");

    // A mixed catalog spanning two tenants, the default tenant's legacy trees,
    // and a platform-internal tree owned by no tenant.
    private static readonly string[] Catalog =
    [
        "t/acme/orders",
        "t/acme/customers",
        "t/globex/orders",
        "legacy-orders",
        "_lattice_registry",
    ];

    private static ExplorerTenantView CreateView(
        ExplorerTenantId? activeTenant,
        ExplorerTenantVisibility requested,
        bool isOperator)
    {
        var context = new ExplorerTenantContext
        {
            ActiveTenant = activeTenant,
            RequestedVisibility = requested,
        };
        return new ExplorerTenantView(context, new StubOperatorGate(isOperator));
    }

    private static ValueTask<IReadOnlyList<string>> ScopeAsync(ExplorerTenantView view) =>
        view.ScopeAsync<string>(Catalog, static id => id);

    // --- Constructor guards ---

    [Test]
    public void Ctor_nullContext_throws()
    {
        Assert.That(
            () => new ExplorerTenantView(null!, new StubOperatorGate(false)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_nullGate_throws()
    {
        Assert.That(
            () => new ExplorerTenantView(new ExplorerTenantContext(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void IsActive_isTrue()
    {
        Assert.That(CreateView(Acme, ExplorerTenantVisibility.ActiveTenant, false).IsActive, Is.True);
    }

    [Test]
    public void ActiveTenant_reflectsContext()
    {
        Assert.That(
            CreateView(Acme, ExplorerTenantVisibility.ActiveTenant, false).ActiveTenant,
            Is.EqualTo(Acme));
    }

    // --- Isolation matrix: ScopeAsync ---

    [Test]
    public async Task ScopeAsync_tenant_seesOwnTreesOnly()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.ActiveTenant, isOperator: false);

        var scoped = await ScopeAsync(view);

        Assert.That(scoped, Is.EqualTo(new[] { "t/acme/orders", "t/acme/customers" }));
    }

    [Test]
    public async Task ScopeAsync_tenant_doesNotSeeOtherTenantTrees()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.ActiveTenant, isOperator: false);

        var scoped = await ScopeAsync(view);

        Assert.That(scoped, Does.Not.Contain("t/globex/orders"));
        Assert.That(scoped, Does.Not.Contain("legacy-orders"));
        Assert.That(scoped, Does.Not.Contain("_lattice_registry"));
    }

    [Test]
    public async Task ScopeAsync_defaultTenant_seesLegacyBareTrees()
    {
        var view = CreateView(ExplorerTenantId.Default, ExplorerTenantVisibility.ActiveTenant, isOperator: false);

        var scoped = await ScopeAsync(view);

        Assert.That(scoped, Is.EqualTo(new[] { "legacy-orders" }));
    }

    [Test]
    public async Task ScopeAsync_operatorWithoutRequestedAllTenants_seesActiveTenantOnly()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.ActiveTenant, isOperator: true);

        var scoped = await ScopeAsync(view);

        Assert.That(scoped, Is.EqualTo(new[] { "t/acme/orders", "t/acme/customers" }));
    }

    [Test]
    public async Task ScopeAsync_operatorRequestingAllTenants_seesEveryTree()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.AllTenants, isOperator: true);

        var scoped = await ScopeAsync(view);

        Assert.That(scoped, Is.SameAs(Catalog));
    }

    [Test]
    public async Task ScopeAsync_nonOperatorRequestingAllTenants_failsClosedToActiveTenant()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.AllTenants, isOperator: false);

        var scoped = await ScopeAsync(view);

        Assert.That(scoped, Is.EqualTo(new[] { "t/acme/orders", "t/acme/customers" }));
    }

    [Test]
    public async Task ScopeAsync_anonymousNoActiveTenant_seesNothing()
    {
        var view = CreateView(activeTenant: null, ExplorerTenantVisibility.ActiveTenant, isOperator: false);

        var scoped = await ScopeAsync(view);

        Assert.That(scoped, Is.Empty);
    }

    [Test]
    public async Task ScopeAsync_anonymousRequestingAllTenants_failsClosedToNothing()
    {
        var view = CreateView(activeTenant: null, ExplorerTenantVisibility.AllTenants, isOperator: false);

        var scoped = await ScopeAsync(view);

        Assert.That(scoped, Is.Empty);
    }

    [Test]
    public async Task ScopeAsync_operatorAllTenantsNoActiveTenant_seesEveryTree()
    {
        var view = CreateView(activeTenant: null, ExplorerTenantVisibility.AllTenants, isOperator: true);

        var scoped = await ScopeAsync(view);

        Assert.That(scoped, Is.SameAs(Catalog));
    }

    [Test]
    public async Task ScopeAsync_noMatches_returnsEmptyWithoutAllocatingList()
    {
        // Active tenant with no owned trees in the page -> empty result.
        var view = CreateView(new ExplorerTenantId("nobody"), ExplorerTenantVisibility.ActiveTenant, isOperator: false);

        var scoped = await ScopeAsync(view);

        Assert.That(scoped, Is.Empty);
    }

    [Test]
    public void ScopeAsync_nullItems_throws()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.ActiveTenant, false);

        Assert.That(
            async () => await view.ScopeAsync<string>(null!, static id => id),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ScopeAsync_nullSelector_throws()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.ActiveTenant, false);

        Assert.That(
            async () => await view.ScopeAsync<string>(Catalog, null!),
            Throws.ArgumentNullException);
    }

    // --- Isolation matrix: ResolveEffectiveVisibilityAsync ---

    [Test]
    public async Task ResolveEffectiveVisibility_activeTenantRequest_isActiveTenant()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.ActiveTenant, isOperator: true);

        Assert.That(
            await view.ResolveEffectiveVisibilityAsync(),
            Is.EqualTo(ExplorerTenantVisibility.ActiveTenant));
    }

    [Test]
    public async Task ResolveEffectiveVisibility_operatorRequestingAllTenants_isAllTenants()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.AllTenants, isOperator: true);

        Assert.That(
            await view.ResolveEffectiveVisibilityAsync(),
            Is.EqualTo(ExplorerTenantVisibility.AllTenants));
    }

    [Test]
    public async Task ResolveEffectiveVisibility_nonOperatorRequestingAllTenants_failsClosedToActiveTenant()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.AllTenants, isOperator: false);

        Assert.That(
            await view.ResolveEffectiveVisibilityAsync(),
            Is.EqualTo(ExplorerTenantVisibility.ActiveTenant));
    }

    // --- Isolation matrix: IsVisible ---

    [Test]
    public void IsVisible_allTenants_everyTreeVisible()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.ActiveTenant, false);

        Assert.That(view.IsVisible(ExplorerTenantVisibility.AllTenants, "t/globex/orders"), Is.True);
    }

    [Test]
    public void IsVisible_activeTenant_onlyOwnedTreeVisible()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.ActiveTenant, false);

        Assert.That(view.IsVisible(ExplorerTenantVisibility.ActiveTenant, "t/acme/orders"), Is.True);
        Assert.That(view.IsVisible(ExplorerTenantVisibility.ActiveTenant, "t/globex/orders"), Is.False);
    }

    [Test]
    public void IsVisible_activeTenantNoActiveTenant_nothingVisible()
    {
        var view = CreateView(activeTenant: null, ExplorerTenantVisibility.ActiveTenant, false);

        Assert.That(view.IsVisible(ExplorerTenantVisibility.ActiveTenant, "t/acme/orders"), Is.False);
    }

    [Test]
    public void IsVisible_nullTreeId_throws()
    {
        var view = CreateView(Acme, ExplorerTenantVisibility.ActiveTenant, false);

        Assert.That(
            () => view.IsVisible(ExplorerTenantVisibility.ActiveTenant, null!),
            Throws.ArgumentNullException);
    }
}
