using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Coverage for the identity-to-tenant resolver. Every row is asserted directly
/// against <see cref="DefaultExplorerTenantIdentityResolver"/> with a deterministic
/// tenant view, a substituted auth session, and a real per-circuit context - no
/// cluster, no timing, no ordering, no wall-clock, and no GC dependence.
/// </summary>
[TestFixture]
public class DefaultExplorerTenantIdentityResolverTests
{
    private static IExplorerAuthSession Session(bool authenticated)
    {
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(authenticated);
        return session;
    }

    private static IExplorerTenantView ActiveView(ExplorerTenantContext context) =>
        new ExplorerTenantView(context, new StubOperatorGate(isOperator: false));

    // --- Constructor guards ---

    [Test]
    public void Ctor_nullView_throws()
    {
        Assert.That(
            () => new DefaultExplorerTenantIdentityResolver(null!, Session(true), new ExplorerTenantContext()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_nullSession_throws()
    {
        var context = new ExplorerTenantContext();
        Assert.That(
            () => new DefaultExplorerTenantIdentityResolver(ActiveView(context), null!, context),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_nullContext_throws()
    {
        Assert.That(
            () => new DefaultExplorerTenantIdentityResolver(NullExplorerTenantView.Instance, Session(true), null!),
            Throws.ArgumentNullException);
    }

    // --- Inactive view: byte-for-byte-unchanged invariant ---

    [Test]
    public async Task ResolveAsync_inactiveView_authenticated_leavesActiveTenantNull()
    {
        var context = new ExplorerTenantContext();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            NullExplorerTenantView.Instance, Session(authenticated: true), context);

        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.Null);
    }

    [Test]
    public async Task ResolveAsync_inactiveView_neverTouchesContext()
    {
        // A pre-set tenant on the inactive path must be left exactly as-is.
        var context = new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId("acme") };
        var resolver = new DefaultExplorerTenantIdentityResolver(
            NullExplorerTenantView.Instance, Session(authenticated: false), context);

        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId("acme")));
    }

    // --- Active view: fail-closed identity mapping ---

    [Test]
    public async Task ResolveAsync_activeView_authenticated_mapsToDefaultTenant()
    {
        var context = new ExplorerTenantContext();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context), Session(authenticated: true), context);

        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(ExplorerTenantId.Default));
    }

    [Test]
    public async Task ResolveAsync_activeView_anonymous_establishesNoTenant()
    {
        var context = new ExplorerTenantContext();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context), Session(authenticated: false), context);

        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.Null);
    }

    [Test]
    public async Task ResolveAsync_activeView_signOut_clearsPreviouslyResolvedTenant()
    {
        var context = new ExplorerTenantContext { ActiveTenant = ExplorerTenantId.Default };
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context), Session(authenticated: false), context);

        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.Null);
    }

    [Test]
    public async Task ResolveAsync_activeView_authenticated_isIdempotent()
    {
        var context = new ExplorerTenantContext();
        var resolver = new DefaultExplorerTenantIdentityResolver(
            ActiveView(context), Session(authenticated: true), context);

        await resolver.ResolveAsync();
        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(ExplorerTenantId.Default));
    }
}
