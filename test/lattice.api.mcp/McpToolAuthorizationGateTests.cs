using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="McpToolAuthorizationGate"/>, the shared coarse
/// authorization decision the discovery core (<c>tools/list</c>) and the
/// credential-stamping wrapper (<c>tools/call</c>) consult. Proves it delegates
/// to the registered <see cref="ILatticeApiMcpAuthorizer"/>, carries the request
/// context and the target tool name into the decision, fails closed when no
/// request context or no authorizer is available, resolves the ambient HTTP
/// context on the accessor overload, and propagates cancellation. All
/// deterministic - fakes, no cluster.
/// </summary>
[TestFixture]
public sealed class McpToolAuthorizationGateTests
{
    private static IServiceProvider ProviderWith(ILatticeApiMcpAuthorizer? authorizer, HttpContext? ambientHttpContext = null)
    {
        var services = new ServiceCollection();
        if (authorizer is not null)
        {
            services.AddSingleton(authorizer);
        }

        if (ambientHttpContext is not null)
        {
            services.AddSingleton<IHttpContextAccessor>(
                new HttpContextAccessor { HttpContext = ambientHttpContext });
        }

        return services.BuildServiceProvider();
    }

    [Test]
    public async Task Allows_when_the_authorizer_permits_the_tool()
    {
        var services = ProviderWith(new AllowAllMcpAuthorizer());

        var allowed = await McpToolAuthorizationGate.IsAuthorizedAsync(
            services, new DefaultHttpContext(), "lattice_data_get", CancellationToken.None);

        Assert.That(allowed, Is.True);
    }

    [Test]
    public async Task Denies_when_the_authorizer_rejects_the_tool()
    {
        var services = ProviderWith(new DenyAllMcpAuthorizer());

        var allowed = await McpToolAuthorizationGate.IsAuthorizedAsync(
            services, new DefaultHttpContext(), "lattice_data_get", CancellationToken.None);

        Assert.That(allowed, Is.False, "The default-deny authorizer must reject the tool.");
    }

    [Test]
    public async Task Carries_the_http_context_and_tool_name_into_the_decision()
    {
        LatticeApiMcpAuthorizationContext? observed = null;
        var services = ProviderWith(new FakeAuthorizer(context =>
        {
            observed = context;
            return true;
        }));
        var call = new DefaultHttpContext();

        await McpToolAuthorizationGate.IsAuthorizedAsync(services, call, "lattice_data_set", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.Not.Null);
            Assert.That(observed!.Value.Call, Is.SameAs(call));
            Assert.That(observed!.Value.ToolName, Is.EqualTo("lattice_data_set"));
        });
    }

    [Test]
    public async Task Fails_closed_when_no_http_context_is_available()
    {
        // A permissive authorizer is registered, but without a request context
        // the caller cannot be described, so the gate must still deny.
        var services = ProviderWith(new AllowAllMcpAuthorizer());

        var allowed = await McpToolAuthorizationGate.IsAuthorizedAsync(
            services, httpContext: null, "lattice_data_get", CancellationToken.None);

        Assert.That(allowed, Is.False);
    }

    [Test]
    public async Task Fails_closed_when_no_authorizer_is_registered()
    {
        var services = ProviderWith(authorizer: null);

        var allowed = await McpToolAuthorizationGate.IsAuthorizedAsync(
            services, new DefaultHttpContext(), "lattice_data_get", CancellationToken.None);

        Assert.That(allowed, Is.False);
    }

    [Test]
    public async Task Accessor_overload_resolves_the_ambient_http_context()
    {
        var call = new DefaultHttpContext();
        var services = ProviderWith(new FakeAuthorizer(context => ReferenceEquals(context.Call, call)), ambientHttpContext: call);

        var allowed = await McpToolAuthorizationGate.IsAuthorizedAsync(
            services, "lattice_data_get", CancellationToken.None);

        Assert.That(allowed, Is.True,
            "The accessor overload must resolve the ambient HTTP context and pass it to the authorizer.");
    }

    [Test]
    public async Task Accessor_overload_fails_closed_when_no_ambient_http_context()
    {
        var services = ProviderWith(new AllowAllMcpAuthorizer());

        var allowed = await McpToolAuthorizationGate.IsAuthorizedAsync(
            services, "lattice_data_get", CancellationToken.None);

        Assert.That(allowed, Is.False, "With no ambient HTTP context the gate must fail closed.");
    }

    [Test]
    public void Rejects_null_services()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => McpToolAuthorizationGate.IsAuthorizedAsync(null!, "t", CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                () => McpToolAuthorizationGate.IsAuthorizedAsync(null!, new DefaultHttpContext(), "t", CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Propagates_cancellation_from_the_authorizer()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var services = ProviderWith(new FakeAuthorizer(_ => true));

        Assert.That(
            async () => await McpToolAuthorizationGate.IsAuthorizedAsync(
                services, new DefaultHttpContext(), "lattice_data_get", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    private sealed class FakeAuthorizer(Func<LatticeApiMcpAuthorizationContext, bool> decide)
        : ILatticeApiMcpAuthorizer
    {
        public Task<bool> IsAuthorizedAsync(
            LatticeApiMcpAuthorizationContext authorizationContext,
            CancellationToken cancellationToken)
            => cancellationToken.IsCancellationRequested
                ? Task.FromCanceled<bool>(cancellationToken)
                : Task.FromResult(decide(authorizationContext));
    }
}
