using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

[TestFixture]
public sealed class TreeAdminOwnershipDenialTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task SetAlias_ownership_denial_surfaces_through_wrapped_tool(bool remote)
    {
        var denial = new LatticeTreeOwnershipDeniedException("different owner");
        Exception fault = remote
            ? new RpcException(new Status(StatusCode.PermissionDenied, denial.Message))
            : denial;
        var facade = Substitute.For<ILatticeTreeAdmin>();
        facade.SetTreeAliasAsync("logical", "physical", Arg.Any<CancellationToken>())
            .ThrowsAsync(fault);
        var collection = new ServiceCollection();
        collection.AddSingleton(facade);
        collection.AddSingleton<ILatticeApiMcpAuthorizer>(new AllowAllMcpAuthorizer());
        collection.AddSingleton<IHttpContextAccessor>(
            new HttpContextAccessor { HttpContext = new DefaultHttpContext() });
        await using var services = collection.BuildServiceProvider();
        var group = new TreeAdminToolGroup(services, Options.Create(
            new LatticeApiMcpOptions { EnableTreeAdminLifecycleTools = true }));
        var inner = group.Tools.Single(t => t.ProtocolTool.Name == "lattice_treeadmin_tree_set_alias");
        var tool = new CredentialStampingTool(inner, LatticeApiMcpGroup.TreeAdmin);

        var error = Assert.ThrowsAsync<McpException>(async () =>
            await McpToolInvocation.CallAsync(tool, services,
                McpToolInvocation.Args(("treeId", "logical"), ("physicalTreeId", "physical"))));

        Assert.That(error!.Message, Does.Contain("different owner"));
        Assert.That(error.Message, Does.Contain(remote
            ? nameof(StatusCode.PermissionDenied) : nameof(LatticeTreeOwnershipDeniedException)));
        await facade.Received(1).SetTreeAliasAsync("logical", "physical", Arg.Any<CancellationToken>());
    }
}
