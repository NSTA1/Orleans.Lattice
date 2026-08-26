using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit coverage for <see cref="HttpContextLatticeApiMcpActiveTenantBridge"/>, the
/// header-based active-tenant bridge that lifts an inbound MCP request header into
/// a <see cref="TenantId"/>. Proves the happy-path parse, whitespace trimming, the
/// custom-header and disable-via-empty-name knobs, that the header is read
/// independently of session authentication, and the fail-closed null results for
/// an absent, blank, or syntactically invalid tenant header.
/// </summary>
[TestFixture]
public sealed class HttpContextLatticeApiMcpActiveTenantBridgeTests
{
    private static HttpContextLatticeApiMcpActiveTenantBridge CreateBridge(
        LatticeApiMcpOptions? options = null) =>
        new(Options.Create(options ?? new LatticeApiMcpOptions()));

    private static DefaultHttpContext ContextWith(params (string Key, string Value)[] headers)
    {
        var context = new DefaultHttpContext();
        foreach (var (key, value) in headers)
        {
            context.Request.Headers[key] = value;
        }

        return context;
    }

    [Test]
    public void Resolve_parses_a_valid_tenant_id()
    {
        var bridge = CreateBridge();

        var tenant = bridge.Resolve(ContextWith(("lattice-active-tenant", "acme")));

        Assert.That(tenant, Is.EqualTo(TenantId.Parse("acme")));
    }

    [Test]
    public void Resolve_trims_surrounding_whitespace()
    {
        var bridge = CreateBridge();

        var tenant = bridge.Resolve(ContextWith(("lattice-active-tenant", "  acme  ")));

        Assert.That(tenant, Is.EqualTo(TenantId.Parse("acme")));
    }

    [Test]
    public void Resolve_returns_null_when_header_absent()
    {
        var bridge = CreateBridge();

        var tenant = bridge.Resolve(ContextWith(("x-other", "ignored")));

        Assert.That(tenant, Is.Null);
    }

    [Test]
    public void Resolve_returns_null_when_header_blank()
    {
        var bridge = CreateBridge();

        var tenant = bridge.Resolve(ContextWith(("lattice-active-tenant", "   ")));

        Assert.That(tenant, Is.Null);
    }

    [Test]
    public void Resolve_returns_null_on_a_syntactically_invalid_tenant()
    {
        var bridge = CreateBridge();

        Assert.Multiple(() =>
        {
            Assert.That(bridge.Resolve(ContextWith(("lattice-active-tenant", "ACME"))), Is.Null);
            Assert.That(bridge.Resolve(ContextWith(("lattice-active-tenant", "-acme"))), Is.Null);
            Assert.That(bridge.Resolve(ContextWith(("lattice-active-tenant", "a/b"))), Is.Null);
        });
    }

    [Test]
    public void Resolve_accepts_the_reserved_default_tenant_id()
    {
        var bridge = CreateBridge();

        var tenant = bridge.Resolve(ContextWith(("lattice-active-tenant", TenantId.DefaultId)));

        Assert.That(tenant, Is.EqualTo(TenantId.Default));
    }

    [Test]
    public void Resolve_honours_a_custom_header_name()
    {
        var bridge = CreateBridge(new LatticeApiMcpOptions
        {
            ActiveTenantHeaderName = "x-tenant",
        });

        var tenant = bridge.Resolve(ContextWith(("x-tenant", "acme")));

        Assert.That(tenant, Is.EqualTo(TenantId.Parse("acme")));
    }

    [Test]
    public void Resolve_returns_null_when_header_name_disabled()
    {
        var bridge = CreateBridge(new LatticeApiMcpOptions
        {
            ActiveTenantHeaderName = string.Empty,
        });

        var tenant = bridge.Resolve(ContextWith(("lattice-active-tenant", "acme")));

        Assert.That(tenant, Is.Null);
    }

    [Test]
    public void Resolve_throws_on_null_context()
    {
        var bridge = CreateBridge();

        Assert.Throws<ArgumentNullException>(() => bridge.Resolve(null!));
    }
}
