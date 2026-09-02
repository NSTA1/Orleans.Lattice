using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the strict-binding guard <see cref="CredentialStampingTool"/>
/// applies to every facade-backed tool call: an argument the wrapped tool does not
/// declare in its input schema is rejected with an <see cref="McpException"/>
/// naming the offender, rather than being silently discarded (issue #1941).
/// </summary>
/// <remarks>
/// <para>
/// The decorator is the single seam every facade tool funnels through, so the
/// guard is asserted once here against a representative wrapped tool
/// (<c>lattice_data_get</c>, which declares <c>treeId</c> and <c>key</c>). The
/// rejection runs after the coarse authorization gate but before credential
/// stamping and region routing, so these tests only need a permissive authorizer
/// and an ambient HTTP context - the throw never reaches the facade.
/// </para>
/// <para>
/// The decorator-added <c>region</c> selector is part of the wrapped tool's
/// advertised schema, so it must be treated as a known argument even though the
/// inner tool never declared it; a dedicated test pins that.
/// </para>
/// </remarks>
[TestFixture]
public sealed class CredentialStampingToolArgumentValidationTests
{
    private static McpServerTool WrappedDataGet()
    {
        var inner = new DataToolGroup(enableWrites: true)
            .Tools.Single(t => t.ProtocolTool.Name == "lattice_data_get");
        return new CredentialStampingTool(inner, LatticeApiMcpGroup.Data);
    }

    private static ServiceProvider AuthorizedServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeApiMcpAuthorizer>(new AllowAllMcpAuthorizer());
        services.AddSingleton<IHttpContextAccessor>(
            new HttpContextAccessor { HttpContext = new DefaultHttpContext() });
        services.AddSingleton<ILatticeDataApi>(new FakeDataApi());
        return services.BuildServiceProvider();
    }

    [Test]
    public async Task Rejects_an_unknown_argument_and_names_the_offender()
    {
        await using var services = AuthorizedServices();

        var ex = Assert.ThrowsAsync<McpException>(async () =>
            await McpToolInvocation.CallAsync(
                WrappedDataGet(),
                services,
                McpToolInvocation.Args(("treeId", "orders"), ("keyy", "k"))));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("'keyy'"),
                "The rejection must name the offending argument so the caller can spot the typo.");
            Assert.That(ex.Message, Does.Contain("does not accept"),
                "The rejection must state that the argument is not accepted, not silently ignore it.");
        });
    }

    [Test]
    public async Task Names_every_unknown_argument_when_more_than_one_is_supplied()
    {
        await using var services = AuthorizedServices();

        var ex = Assert.ThrowsAsync<McpException>(async () =>
            await McpToolInvocation.CallAsync(
                WrappedDataGet(),
                services,
                McpToolInvocation.Args(("treeId", "orders"), ("keyy", "k"), ("limit", 10))));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("'keyy'"));
            Assert.That(ex.Message, Does.Contain("'limit'"));
        });
    }

    [Test]
    public async Task Treats_the_decorator_added_region_as_a_known_argument()
    {
        await using var services = AuthorizedServices();

        // The guard runs before region routing, so a call carrying region plus a
        // genuinely unknown argument must name only the unknown one, proving region
        // is in the accepted set even though the inner tool never declared it.
        var ex = Assert.ThrowsAsync<McpException>(async () =>
            await McpToolInvocation.CallAsync(
                WrappedDataGet(),
                services,
                McpToolInvocation.Args(
                    ("treeId", "orders"), ("key", "k"), ("region", "east"), ("bogus", "x"))));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("'bogus'"), "The genuinely unknown argument must be named.");
            Assert.That(ex.Message, Does.Contain("the argument(s): 'bogus'."),
                "region must not appear among the offenders: it is a declared (decorator-added) argument, "
                + "so the sole rejected argument is 'bogus'.");
        });
    }

    [Test]
    public async Task Accepts_a_call_whose_arguments_are_all_declared()
    {
        await using var services = AuthorizedServices();

        var result = await McpToolInvocation.CallAsync(
            WrappedDataGet(),
            services,
            McpToolInvocation.Args(("treeId", "orders"), ("key", "k")));

        Assert.That(result.IsError, Is.Not.True,
            "A call whose arguments are all declared by the tool must pass the guard and reach the facade.");
    }
}
