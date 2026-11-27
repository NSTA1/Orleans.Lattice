using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="AuthorizedMetaTool"/>, the invocation half of the
/// lock-step gate for a discovery meta-tool such as <c>lattice_list_regions</c>.
/// Advertisement is not a capability: a session's tool collection is built once,
/// so the gate must run again on every <c>tools/call</c> against the authorizer's
/// current verdict and the call's own request context.
/// </summary>
[TestFixture]
public sealed class AuthorizedMetaToolTests
{
    private const string ToolName = "lattice_list_regions";

    private static AuthorizedMetaTool Wrap(Action onInvoke)
        => new(McpServerTool.Create(
            () =>
            {
                onInvoke();
                return "regions";
            },
            new McpServerToolCreateOptions { Name = ToolName }));

    private static ServiceProvider Services(ILatticeApiMcpAuthorizer authorizer, HttpContext? httpContext)
        => new ServiceCollection()
            .AddSingleton(authorizer)
            .AddSingleton<IHttpContextAccessor>(new HttpContextAccessor { HttpContext = httpContext })
            .BuildServiceProvider();

    [Test]
    public async Task A_permitted_call_reaches_the_wrapped_tool()
    {
        var invoked = 0;
        var tool = Wrap(() => invoked++);
        await using var services = Services(new AllowAllMcpAuthorizer(), new DefaultHttpContext());

        var result = await McpToolInvocation.CallAsync(tool, services);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.Not.True);
            Assert.That(invoked, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_call_the_authorizer_now_refuses_is_denied_without_reaching_the_tool()
    {
        var invoked = 0;
        var tool = Wrap(() => invoked++);
        await using var services = Services(new DenyAllMcpAuthorizer(), new DefaultHttpContext());

        var ex = Assert.ThrowsAsync<McpException>(async () => await McpToolInvocation.CallAsync(tool, services));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("not authorized"));
            Assert.That(invoked, Is.Zero, "A refused call must never reach the meta-tool's payload.");
        });
    }

    [Test]
    public async Task A_call_with_no_request_context_is_denied()
    {
        var invoked = 0;
        var tool = Wrap(() => invoked++);
        await using var services = Services(new AllowAllMcpAuthorizer(), httpContext: null);

        Assert.ThrowsAsync<McpException>(async () => await McpToolInvocation.CallAsync(tool, services));
        Assert.That(invoked, Is.Zero);
    }
}
