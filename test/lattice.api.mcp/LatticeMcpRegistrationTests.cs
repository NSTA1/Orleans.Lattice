using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.AspNetCore;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Registration-front-door tests for
/// <see cref="LatticeMcpServiceCollectionExtensions.AddLatticeMcp"/>. Proves the
/// binding wires the fail-closed default-deny authorizer, the credential bridge,
/// the HTTP context accessor, and the MCP server; that it is idempotent; that a
/// host-supplied authorizer or bridge registered first is preserved (TryAdd
/// semantics); that options bind; and that the stateless toggle flows onto the
/// transport options.
/// </summary>
[TestFixture]
public sealed class LatticeMcpRegistrationTests
{
    private static ServiceCollection SeedServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        return services;
    }

    [Test]
    public void AddLatticeMcp_registers_the_default_deny_authorizer()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var authorizer = provider.GetRequiredService<ILatticeApiMcpAuthorizer>();
        Assert.That(authorizer, Is.TypeOf<DenyAllMcpAuthorizer>());
    }

    [Test]
    public void AddLatticeMcp_preserves_a_host_supplied_authorizer()
    {
        var services = SeedServices();
        services.AddSingleton<ILatticeApiMcpAuthorizer, AllowAllMcpAuthorizer>();

        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var authorizer = provider.GetRequiredService<ILatticeApiMcpAuthorizer>();
        Assert.That(authorizer, Is.TypeOf<AllowAllMcpAuthorizer>(),
            "TryAdd must not overwrite a permissive authorizer the host opted into first.");
    }

    [Test]
    public void AddLatticeMcp_registers_the_http_context_bridge_by_default()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var bridge = provider.GetRequiredService<ILatticeApiMcpCredentialBridge>();
        Assert.That(bridge, Is.TypeOf<HttpContextLatticeApiMcpCredentialBridge>());
    }

    [Test]
    public void AddLatticeMcp_preserves_a_host_supplied_bridge()
    {
        var services = SeedServices();
        services.AddSingleton<ILatticeApiMcpCredentialBridge, StubBridge>();

        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var bridge = provider.GetRequiredService<ILatticeApiMcpCredentialBridge>();
        Assert.That(bridge, Is.TypeOf<StubBridge>(),
            "TryAdd must not overwrite a credential bridge the host registered first.");
    }

    [Test]
    public void AddLatticeMcp_registers_the_http_context_active_tenant_bridge_by_default()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var bridge = provider.GetRequiredService<ILatticeApiMcpActiveTenantBridge>();
        Assert.That(bridge, Is.TypeOf<HttpContextLatticeApiMcpActiveTenantBridge>());
    }

    [Test]
    public void AddLatticeMcp_preserves_a_host_supplied_active_tenant_bridge()
    {
        var services = SeedServices();
        services.AddSingleton<ILatticeApiMcpActiveTenantBridge, StubActiveTenantBridge>();

        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var bridge = provider.GetRequiredService<ILatticeApiMcpActiveTenantBridge>();
        Assert.That(bridge, Is.TypeOf<StubActiveTenantBridge>(),
            "TryAdd must not overwrite an active-tenant bridge the host registered first.");
    }

    [Test]
    public void AddLatticeMcp_registers_the_http_context_accessor()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<IHttpContextAccessor>(), Is.Not.Null);
    }

    [Test]
    public void AddLatticeMcp_binds_options()
    {
        var services = SeedServices();
        services.AddLatticeMcp(o =>
        {
            o.RequireAuthorization = false;
            o.TransportPattern = "/mcp";
            o.Stateless = true;
            o.CredentialHeaderName = "x-cred";
            o.CredentialScheme = "custom";
            o.ActiveTenantHeaderName = "x-tenant";
            o.EnableStateTools = true;
            o.EnableDataTools = true;
            o.EnableBackupTools = true;
            o.EnableAuthTools = true;
        });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.False);
            Assert.That(options.TransportPattern, Is.EqualTo("/mcp"));
            Assert.That(options.Stateless, Is.True);
            Assert.That(options.CredentialHeaderName, Is.EqualTo("x-cred"));
            Assert.That(options.CredentialScheme, Is.EqualTo("custom"));
            Assert.That(options.ActiveTenantHeaderName, Is.EqualTo("x-tenant"));
            Assert.That(options.EnableStateTools, Is.True);
            Assert.That(options.EnableDataTools, Is.True);
            Assert.That(options.EnableBackupTools, Is.True);
            Assert.That(options.EnableAuthTools, Is.True);
        });
    }

    [Test]
    public void AddLatticeMcp_defaults_require_authorization_to_true()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        Assert.That(options.RequireAuthorization, Is.True,
            "The MCP surface must default to fail-closed enforcement.");
    }

    [Test]
    public void AddLatticeMcp_defaults_all_facade_flags_to_false()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        Assert.Multiple(() =>
        {
            Assert.That(options.EnableStateTools, Is.False);
            Assert.That(options.EnableDataTools, Is.False);
            Assert.That(options.EnableBackupTools, Is.False);
            Assert.That(options.EnableAuthTools, Is.False);
        });
    }

    [Test]
    public void AddLatticeMcp_flows_the_stateless_toggle_onto_the_transport_options()
    {
        var services = SeedServices();
        services.AddLatticeMcp(o => o.Stateless = true);

        using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<IOptions<HttpServerTransportOptions>>().Value;
        Assert.That(transport.Stateless, Is.True);
    }

    [Test]
    public void AddLatticeMcp_leaves_the_transport_stateful_by_default()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<IOptions<HttpServerTransportOptions>>().Value;
        Assert.That(transport.Stateless, Is.False);
    }

    [Test]
    public void AddLatticeMcp_registers_the_mcp_server()
    {
        var services = SeedServices();
        var before = services.Count(d => d.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService));

        services.AddLatticeMcp();

        var after = services.Count(d => d.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService));
        Assert.That(after, Is.GreaterThan(before),
            "AddMcpServer must register the MCP server's hosted service.");
    }

    [Test]
    public void AddLatticeMcp_is_idempotent_for_the_authorizer()
    {
        var services = SeedServices();
        services.AddLatticeMcp();
        services.AddLatticeMcp();

        var registrations = services.Count(d => d.ServiceType == typeof(ILatticeApiMcpAuthorizer));
        Assert.That(registrations, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeMcp_throws_on_null_services()
    {
        Assert.Throws<ArgumentNullException>(
            () => LatticeMcpServiceCollectionExtensions.AddLatticeMcp(null!));
    }

    private sealed class StubBridge : ILatticeApiMcpCredentialBridge
    {
        public LatticeCredential? Resolve(HttpContext context) => null;
    }

    private sealed class StubActiveTenantBridge : ILatticeApiMcpActiveTenantBridge
    {
        public TenantId? Resolve(HttpContext context) => null;
    }
}
