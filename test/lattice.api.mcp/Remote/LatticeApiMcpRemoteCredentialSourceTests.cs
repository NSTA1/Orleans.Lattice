using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpRemoteCredentialSource"/>, which selects
/// the credential to forward on each outbound gRPC call. Proves the documented
/// first-match resolution order: a trusted system-origin introspection forwards
/// the configured administrator credential; otherwise the ambient stamped
/// credential; otherwise the caller credential the bridge resolves from the HTTP
/// request; otherwise anonymous. Deterministic - no gRPC, no network.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpRemoteCredentialSourceTests
{
    private static LatticeApiMcpRemoteCredentialSource Source(
        LatticeCredential? bridgeCredential = null,
        Action<LatticeApiMcpRemoteOptions>? configure = null,
        bool withHttpContext = true)
        => new(
            new StubHttpContextAccessor(withHttpContext ? new DefaultHttpContext() : null),
            new StubBridge(bridgeCredential),
            RemoteTestSupport.OptionsMonitor(configure ?? (_ => { })));

    [Test]
    public void Constructor_null_accessor_throws()
        => Assert.That(
            () => new LatticeApiMcpRemoteCredentialSource(null!, new StubBridge(null), RemoteTestSupport.OptionsMonitor(_ => { })),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_bridge_throws()
        => Assert.That(
            () => new LatticeApiMcpRemoteCredentialSource(new StubHttpContextAccessor(null), null!, RemoteTestSupport.OptionsMonitor(_ => { })),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_options_throws()
        => Assert.That(
            () => new LatticeApiMcpRemoteCredentialSource(new StubHttpContextAccessor(null), new StubBridge(null), null!),
            Throws.ArgumentNullException);

    [Test]
    public void System_origin_with_administrator_credential_forwards_administrator()
    {
        var admin = new LatticeCredential("admin-token");
        var source = Source(configure: o => o.AdministratorCredential = admin);

        using (LatticeSystemOrigin.Enter())
        {
            Assert.That(source.ResolveOutbound(), Is.EqualTo(admin));
        }
    }

    [Test]
    public void System_origin_without_administrator_falls_through_to_ambient()
    {
        var ambient = new LatticeCredential("caller-token");
        var source = Source();

        using (LatticeSystemOrigin.Enter())
        using (LatticeCredentialContext.With(ambient))
        {
            Assert.That(source.ResolveOutbound(), Is.EqualTo(ambient));
        }
    }

    [Test]
    public void Ambient_credential_is_forwarded_when_not_system_origin()
    {
        var ambient = new LatticeCredential("caller-token");
        var source = Source(bridgeCredential: new LatticeCredential("bridge-token"));

        using (LatticeCredentialContext.With(ambient))
        {
            Assert.That(source.ResolveOutbound(), Is.EqualTo(ambient));
        }
    }

    [Test]
    public void Http_bridge_credential_is_forwarded_when_no_ambient()
    {
        var bridge = new LatticeCredential("bridge-token");
        var source = Source(bridgeCredential: bridge);

        Assert.That(source.ResolveOutbound(), Is.EqualTo(bridge));
    }

    [Test]
    public void No_http_context_and_no_ambient_resolves_null()
    {
        var source = Source(withHttpContext: false);
        Assert.That(source.ResolveOutbound(), Is.Null);
    }

    [Test]
    public void Anonymous_bridge_resolves_null()
    {
        var source = Source(bridgeCredential: null);
        Assert.That(source.ResolveOutbound(), Is.Null);
    }

    private sealed class StubHttpContextAccessor(HttpContext? context) : IHttpContextAccessor
    {
        public HttpContext? HttpContext { get; set; } = context;
    }

    private sealed class StubBridge(LatticeCredential? credential) : ILatticeApiMcpCredentialBridge
    {
        public LatticeCredential? Resolve(HttpContext context) => credential;
    }
}
