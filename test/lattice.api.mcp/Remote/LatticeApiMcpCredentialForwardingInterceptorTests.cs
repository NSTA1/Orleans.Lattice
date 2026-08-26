using Grpc.Core;
using Grpc.Core.Interceptors;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpCredentialForwardingInterceptor"/>, the
/// client-side gRPC interceptor that forwards the resolved caller credential to
/// the remote cluster as a request header on every outbound call. Proves the
/// header is stamped with the configured scheme on unary and server-streaming
/// calls, honours a custom header name / empty scheme, leaves an anonymous or
/// empty-token call unmodified, and validates its dependencies. Deterministic
/// over a <see cref="FakeCallInvoker"/>.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpCredentialForwardingInterceptorTests
{
    private static LatticeApiMcpCredentialForwardingInterceptor Interceptor(
        LatticeCredential? credential,
        Action<LatticeApiMcpRemoteOptions>? configure = null)
        => new(
            new StubCredentialSource(credential),
            RemoteTestSupport.OptionsMonitor(configure ?? (_ => { })));

    private static string? HeaderValue(FakeCallInvoker invoker, string name)
        => invoker.LastHeaders?.GetValue(name);

    [Test]
    public void Constructor_null_source_throws()
        => Assert.That(
            () => new LatticeApiMcpCredentialForwardingInterceptor(null!, RemoteTestSupport.OptionsMonitor(_ => { })),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_options_throws()
        => Assert.That(
            () => new LatticeApiMcpCredentialForwardingInterceptor(new StubCredentialSource(null), null!),
            Throws.ArgumentNullException);

    [Test]
    public async Task Unary_call_stamps_bearer_authorization_header()
    {
        var invoker = new FakeCallInvoker(_ => new TreeCatalogPage());
        var intercepted = invoker.Intercept(Interceptor(new LatticeCredential("tok-123")));

        await LatticeStateApiGrpcClient.Create(intercepted, RemoteTestSupport.Serializer)
            .ListTreesAsync(new CatalogRequest());

        Assert.That(HeaderValue(invoker, "authorization"), Is.EqualTo("Bearer tok-123"));
    }

    [Test]
    public async Task Server_streaming_call_stamps_header()
    {
        var invoker = new FakeCallInvoker(
            _ => throw new InvalidOperationException(),
            _ => Array.Empty<StateChangeNotification>());
        var intercepted = invoker.Intercept(Interceptor(new LatticeCredential("tok-abc")));

        await foreach (var _ in LatticeStateApiGrpcClient.Create(intercepted, RemoteTestSupport.Serializer)
            .ObserveChangesAsync(new StateObserveRequest { TreeId = "t" }))
        {
            // Drain (empty) - the header is stamped when the call starts.
        }

        Assert.That(HeaderValue(invoker, "authorization"), Is.EqualTo("Bearer tok-abc"));
    }

    [Test]
    public async Task Custom_header_name_and_scheme_are_honoured()
    {
        var invoker = new FakeCallInvoker(_ => new TreeCatalogPage());
        var interceptor = Interceptor(
            new LatticeCredential("tok"),
            o =>
            {
                o.CredentialHeaderName = "x-lattice-auth";
                o.CredentialScheme = "Token";
            });

        await LatticeStateApiGrpcClient.Create(invoker.Intercept(interceptor), RemoteTestSupport.Serializer)
            .ListTreesAsync(new CatalogRequest());

        Assert.That(HeaderValue(invoker, "x-lattice-auth"), Is.EqualTo("Token tok"));
    }

    [Test]
    public async Task Empty_scheme_sends_bare_token()
    {
        var invoker = new FakeCallInvoker(_ => new TreeCatalogPage());
        var interceptor = Interceptor(new LatticeCredential("tok"), o => o.CredentialScheme = string.Empty);

        await LatticeStateApiGrpcClient.Create(invoker.Intercept(interceptor), RemoteTestSupport.Serializer)
            .ListTreesAsync(new CatalogRequest());

        Assert.That(HeaderValue(invoker, "authorization"), Is.EqualTo("tok"));
    }

    [Test]
    public async Task Anonymous_call_adds_no_header()
    {
        var invoker = new FakeCallInvoker(_ => new TreeCatalogPage());
        var intercepted = invoker.Intercept(Interceptor(credential: null));

        await LatticeStateApiGrpcClient.Create(intercepted, RemoteTestSupport.Serializer)
            .ListTreesAsync(new CatalogRequest());

        Assert.That(invoker.LastHeaders, Is.Null.Or.Empty);
    }

    [Test]
    public async Task Empty_token_adds_no_header()
    {
        var invoker = new FakeCallInvoker(_ => new TreeCatalogPage());
        var intercepted = invoker.Intercept(Interceptor(new LatticeCredential(string.Empty)));

        await LatticeStateApiGrpcClient.Create(intercepted, RemoteTestSupport.Serializer)
            .ListTreesAsync(new CatalogRequest());

        Assert.That(invoker.LastHeaders, Is.Null.Or.Empty);
    }

    [Test]
    public async Task Active_tenant_header_is_forwarded_when_ambient_tenant_is_set()
    {
        var invoker = new FakeCallInvoker(_ => new TreeCatalogPage());
        var intercepted = invoker.Intercept(Interceptor(new LatticeCredential("tok-123")));

        using (LatticeActiveTenantContext.With(TenantId.Parse("acme")))
        {
            await LatticeStateApiGrpcClient.Create(intercepted, RemoteTestSupport.Serializer)
                .ListTreesAsync(new CatalogRequest());
        }

        Assert.That(HeaderValue(invoker, "lattice-active-tenant"), Is.EqualTo("acme"));
    }

    [Test]
    public async Task Active_tenant_header_is_forwarded_for_an_anonymous_call()
    {
        // The tenant assertion is independent of caller identity: even with no
        // credential, a stamped active tenant is forwarded so the remote bridge
        // sees it (the tenancy add-on then re-validates it against membership).
        var invoker = new FakeCallInvoker(_ => new TreeCatalogPage());
        var intercepted = invoker.Intercept(Interceptor(credential: null));

        using (LatticeActiveTenantContext.With(TenantId.Parse("acme")))
        {
            await LatticeStateApiGrpcClient.Create(intercepted, RemoteTestSupport.Serializer)
                .ListTreesAsync(new CatalogRequest());
        }

        Assert.That(HeaderValue(invoker, "lattice-active-tenant"), Is.EqualTo("acme"));
    }

    [Test]
    public async Task Active_tenant_header_honours_a_custom_header_name()
    {
        var invoker = new FakeCallInvoker(_ => new TreeCatalogPage());
        var interceptor = Interceptor(
            new LatticeCredential("tok"),
            o => o.ActiveTenantHeaderName = "x-tenant");

        using (LatticeActiveTenantContext.With(TenantId.Parse("acme")))
        {
            await LatticeStateApiGrpcClient.Create(invoker.Intercept(interceptor), RemoteTestSupport.Serializer)
                .ListTreesAsync(new CatalogRequest());
        }

        Assert.That(HeaderValue(invoker, "x-tenant"), Is.EqualTo("acme"));
    }

    [Test]
    public async Task No_active_tenant_header_when_none_asserted()
    {
        var invoker = new FakeCallInvoker(_ => new TreeCatalogPage());
        var intercepted = invoker.Intercept(Interceptor(new LatticeCredential("tok-123")));

        // No LatticeActiveTenantContext scope: the cold path must add no tenant header.
        await LatticeStateApiGrpcClient.Create(intercepted, RemoteTestSupport.Serializer)
            .ListTreesAsync(new CatalogRequest());

        Assert.That(HeaderValue(invoker, "lattice-active-tenant"), Is.Null);
    }

    [Test]
    public async Task No_active_tenant_header_when_header_name_disabled()
    {
        var invoker = new FakeCallInvoker(_ => new TreeCatalogPage());
        var interceptor = Interceptor(
            new LatticeCredential("tok"),
            o => o.ActiveTenantHeaderName = string.Empty);

        using (LatticeActiveTenantContext.With(TenantId.Parse("acme")))
        {
            await LatticeStateApiGrpcClient.Create(invoker.Intercept(interceptor), RemoteTestSupport.Serializer)
                .ListTreesAsync(new CatalogRequest());
        }

        Assert.That(HeaderValue(invoker, "lattice-active-tenant"), Is.Null);
    }

    private sealed class StubCredentialSource(LatticeCredential? credential) : ILatticeApiMcpRemoteCredentialSource
    {
        public LatticeCredential? ResolveOutbound() => credential;
    }
}
