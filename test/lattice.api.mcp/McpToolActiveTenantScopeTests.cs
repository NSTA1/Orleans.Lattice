using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="McpToolActiveTenantScope"/>, the per-invocation
/// active-tenant-stamping helper the discovery core's tool wrapper uses. Proves
/// the helper lifts the bridged tenant onto the ambient
/// <see cref="LatticeActiveTenantContext"/> for the call, leaves the ambient
/// context untouched fail-closed for a request that asserts no tenant, is a no-op
/// when no HTTP context is available, and restores the prior ambient value on
/// dispose. All deterministic - fakes, no cluster.
/// </summary>
[TestFixture]
public sealed class McpToolActiveTenantScopeTests
{
    private static IServiceProvider Provider(bool withHttpContext, ILatticeApiMcpActiveTenantBridge? bridge)
    {
        var services = new ServiceCollection();
        if (withHttpContext)
        {
            services.AddSingleton<IHttpContextAccessor>(
                new HttpContextAccessor { HttpContext = new DefaultHttpContext() });
        }

        if (bridge is not null)
        {
            services.AddSingleton(bridge);
        }

        return services.BuildServiceProvider();
    }

    [Test]
    public void Stamp_lifts_the_bridged_tenant_onto_the_ambient_context()
    {
        var tenant = TenantId.Parse("acme");
        var services = Provider(withHttpContext: true, new FakeBridge(tenant));

        using (McpToolActiveTenantScope.Stamp(services))
        {
            Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(tenant),
                "The bridged tenant must be the ambient active tenant for the duration of the scope.");
        }

        Assert.That(LatticeActiveTenantContext.Current, Is.Null,
            "Dispose must restore the prior (empty) ambient active tenant.");
    }

    [Test]
    public void Stamp_leaves_the_ambient_tenant_untouched_when_no_tenant_asserted()
    {
        var services = Provider(withHttpContext: true, new FakeBridge(null));

        using (LatticeActiveTenantContext.With(TenantId.Parse("outer")))
        {
            using (McpToolActiveTenantScope.Stamp(services))
            {
                Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(TenantId.Parse("outer")),
                    "A null bridge result is the cold path: it must leave the ambient active tenant untouched.");
            }

            Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(TenantId.Parse("outer")),
                "Dispose of a cold-path no-op scope must not disturb the ambient active tenant.");
        }
    }

    [Test]
    public void Stamp_is_a_no_op_when_no_http_context_is_available()
    {
        var services = Provider(withHttpContext: false, new FakeBridge(TenantId.Parse("unused")));

        using (LatticeActiveTenantContext.With(TenantId.Parse("outer")))
        using (McpToolActiveTenantScope.Stamp(services))
        {
            Assert.That(LatticeActiveTenantContext.Current, Is.EqualTo(TenantId.Parse("outer")),
                "With no HTTP context the helper is a no-op and must not disturb the ambient active tenant.");
        }
    }

    [Test]
    public void Stamp_rejects_null_services()
    {
        Assert.That(() => McpToolActiveTenantScope.Stamp(null!), Throws.ArgumentNullException);
    }

    private sealed class FakeBridge(TenantId? tenant) : ILatticeApiMcpActiveTenantBridge
    {
        public TenantId? Resolve(HttpContext context) => tenant;
    }
}
