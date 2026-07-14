using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="McpToolCredentialScope"/>, the per-invocation
/// credential-stamping helper the discovery core's tool wrapper uses. Proves the
/// helper lifts the bridged credential onto the ambient
/// <see cref="LatticeCredentialContext"/> for the call, clears it fail-closed for
/// an unauthenticated request, leaves the ambient context untouched when no HTTP
/// context is available, and restores the prior ambient value on dispose. All
/// deterministic - fakes, no cluster.
/// </summary>
[TestFixture]
public sealed class McpToolCredentialScopeTests
{
    private const string Header = "x-agent";

    private static IServiceProvider Provider(bool withHttpContext, ILatticeApiMcpCredentialBridge? bridge)
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
    public void Stamp_lifts_the_bridged_credential_onto_the_ambient_context()
    {
        var credential = new LatticeCredential("agent", scheme: "demo", principalId: "agent");
        var services = Provider(withHttpContext: true, new FakeBridge(credential));

        using (McpToolCredentialScope.Stamp(services))
        {
            Assert.That(LatticeCredentialContext.Current, Is.EqualTo(credential),
                "The bridged credential must be the ambient credential for the duration of the scope.");
        }

        Assert.That(LatticeCredentialContext.Current, Is.Null, "Dispose must restore the prior (empty) ambient credential.");
    }

    [Test]
    public void Stamp_clears_the_ambient_credential_for_an_unauthenticated_request()
    {
        var services = Provider(withHttpContext: true, new FakeBridge(null));

        using (LatticeCredentialContext.Use("outer"))
        {
            using (McpToolCredentialScope.Stamp(services))
            {
                Assert.That(LatticeCredentialContext.Current, Is.Null,
                    "A null bridge result must clear the ambient credential so the facade denies the caller as anonymous.");
            }

            Assert.That(LatticeCredentialContext.Current?.Token, Is.EqualTo("outer"),
                "Dispose must restore the prior ambient credential.");
        }
    }

    [Test]
    public void Stamp_leaves_the_ambient_context_untouched_when_no_http_context_is_available()
    {
        var services = Provider(withHttpContext: false, new FakeBridge(new LatticeCredential("unused")));

        using (LatticeCredentialContext.Use("outer"))
        using (McpToolCredentialScope.Stamp(services))
        {
            Assert.That(LatticeCredentialContext.Current?.Token, Is.EqualTo("outer"),
                "With no HTTP context the helper is a no-op and must not disturb the ambient credential.");
        }
    }

    [Test]
    public void Stamp_rejects_null_services()
    {
        Assert.That(() => McpToolCredentialScope.Stamp(null!), Throws.ArgumentNullException);
    }

    private sealed class FakeBridge(LatticeCredential? credential) : ILatticeApiMcpCredentialBridge
    {
        public LatticeCredential? Resolve(HttpContext context) => credential;
    }
}
