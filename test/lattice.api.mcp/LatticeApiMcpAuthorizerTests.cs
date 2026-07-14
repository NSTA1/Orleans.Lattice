using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit coverage for the coarse MCP authorization seam
/// (<see cref="ILatticeApiMcpAuthorizer"/>) and its two built-in
/// implementations. Proves the shipped default fails closed
/// (<see cref="DenyAllMcpAuthorizer"/> rejects every request), the opt-in
/// <see cref="AllowAllMcpAuthorizer"/> permits every request, and the
/// authorization context validates and carries its inputs.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpAuthorizerTests
{
    private static LatticeApiMcpAuthorizationContext Context(string? toolName = null)
        => new(new DefaultHttpContext(), toolName);

    [Test]
    public async Task DenyAllMcpAuthorizer_rejects_every_request()
    {
        var authorizer = new DenyAllMcpAuthorizer();

        var allowed = await authorizer.IsAuthorizedAsync(Context("lattice_get"), CancellationToken.None);

        Assert.That(allowed, Is.False,
            "The shipped default must fail closed until a host opts in.");
    }

    [Test]
    public async Task AllowAllMcpAuthorizer_permits_every_request()
    {
        var authorizer = new AllowAllMcpAuthorizer();

        var allowed = await authorizer.IsAuthorizedAsync(Context("lattice_set"), CancellationToken.None);

        Assert.That(allowed, Is.True);
    }

    [Test]
    public void AuthorizationContext_carries_its_inputs()
    {
        var call = new DefaultHttpContext();
        var context = new LatticeApiMcpAuthorizationContext(call, "lattice_capabilities");

        Assert.Multiple(() =>
        {
            Assert.That(context.Call, Is.SameAs(call));
            Assert.That(context.ToolName, Is.EqualTo("lattice_capabilities"));
        });
    }

    [Test]
    public void AuthorizationContext_defaults_tool_name_to_null()
    {
        var context = new LatticeApiMcpAuthorizationContext(new DefaultHttpContext());

        Assert.That(context.ToolName, Is.Null);
    }

    [Test]
    public void AuthorizationContext_throws_on_null_call()
    {
        Assert.Throws<ArgumentNullException>(
            () => new LatticeApiMcpAuthorizationContext(null!));
    }
}
