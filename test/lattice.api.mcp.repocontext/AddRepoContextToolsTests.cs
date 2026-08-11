using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Registration tests for
/// <see cref="LatticeMcpRepoContextServiceCollectionExtensions.AddRepoContextTools"/>.
/// Proves the opt-in registers exactly one repository-context tool group serving
/// the repository-context group, is idempotent, and validates its arguments.
/// </summary>
[TestFixture]
public sealed class AddRepoContextToolsTests
{
    // The tool-group service interface is internal to the MCP package; obtain its
    // Type via the accessible RepoContextToolGroup rather than naming it.
    private static readonly Type ToolGroupInterface = typeof(RepoContextToolGroup)
        .GetInterfaces()
        .Single(i => i.Name == "ILatticeApiMcpToolGroup");

    [Test]
    public void AddRepoContextTools_registers_a_single_repo_context_tool_group()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        using var provider = services.BuildServiceProvider();
        var groups = provider.GetServices(ToolGroupInterface).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(groups, Has.Exactly(1).InstanceOf<RepoContextToolGroup>());
            Assert.That(((RepoContextToolGroup)groups.Single()!).Group,
                Is.EqualTo(LatticeApiMcpGroup.RepoContext));
        });
    }

    [Test]
    public void AddRepoContextTools_is_idempotent_for_the_tool_group()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();
        services.AddRepoContextTools();

        var registrations = services.Count(d => d.ServiceType == ToolGroupInterface);
        Assert.That(registrations, Is.EqualTo(1));
    }

    [Test]
    public void AddRepoContextTools_returns_the_same_collection_for_chaining()
    {
        var services = new ServiceCollection();
        Assert.That(services.AddRepoContextTools(), Is.SameAs(services));
    }

    [Test]
    public void AddRepoContextTools_rejects_a_null_service_collection()
        => Assert.Throws<ArgumentNullException>(
            () => LatticeMcpRepoContextServiceCollectionExtensions.AddRepoContextTools(null!));
}
