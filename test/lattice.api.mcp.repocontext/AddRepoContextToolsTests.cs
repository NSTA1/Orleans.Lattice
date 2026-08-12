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

    [Test]
    public void AddRepoContextTools_registers_the_bootstrap_coordinator_and_real_vector_ingestor()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        var ingestor = services.SingleOrDefault(d => d.ServiceType == typeof(IRepoContextVectorIngestor));

        Assert.Multiple(() =>
        {
            Assert.That(
                services.Any(d => d.ServiceType == typeof(RepoContextBootstrapService)), Is.True);
            Assert.That(ingestor, Is.Not.Null);
            // The seam is wired to the embed-and-store ingestor via a factory, so
            // the deferred no-op is no longer the registered implementation.
            Assert.That(ingestor!.ImplementationType, Is.Not.EqualTo(typeof(NoOpRepoContextVectorIngestor)));
            Assert.That(
                services.Any(d => d.ServiceType == typeof(RepoContextVectorWriter)), Is.True);
            Assert.That(
                services.Any(d => d.ServiceType == typeof(IRepoContextSemanticIndex)), Is.True);
            Assert.That(
                services.Any(d => d.ServiceType == typeof(RepoContextSearchService)), Is.True);
        });
    }

    [Test]
    public void AddRepoContextTools_does_not_offer_the_write_tools_by_default()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        using var provider = services.BuildServiceProvider();
        var group = (RepoContextToolGroup)provider.GetServices(ToolGroupInterface).Single()!;

        Assert.That(
            group.Tools.Select(t => t.ProtocolTool.Name),
            Is.EquivalentTo(new[]
            {
                "repocontext_health", "repocontext_recall", "repocontext_scan", "repocontext_list_topics",
                "repocontext_search",
            }));
    }

    [Test]
    public void AddRepoContextTools_offers_the_write_tools_when_writes_are_enabled()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools(enableWrites: true);

        using var provider = services.BuildServiceProvider();
        var group = (RepoContextToolGroup)provider.GetServices(ToolGroupInterface).Single()!;

        Assert.That(
            group.Tools.Select(t => t.ProtocolTool.Name),
            Is.EquivalentTo(new[]
            {
                "repocontext_health", "repocontext_recall", "repocontext_scan", "repocontext_list_topics",
                "repocontext_search",
                "repocontext_bootstrap", "repocontext_remember", "repocontext_update", "repocontext_forget",
            }));
    }

    [Test]
    public void AddRepoContextTools_workspace_mode_offers_the_dynamic_repo_tools()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools(enableWrites: true, workspaceMode: true, workspaceRoot: "/workspace");

        using var provider = services.BuildServiceProvider();
        var group = (RepoContextToolGroup)provider.GetServices(ToolGroupInterface).Single()!;

        Assert.That(
            group.Tools.Select(t => t.ProtocolTool.Name),
            Is.EquivalentTo(new[]
            {
                "repocontext_health", "repocontext_recall", "repocontext_scan", "repocontext_list_topics",
                "repocontext_search", "repocontext_list_repos",
                "repocontext_add_repo", "repocontext_remove_repo",
                "repocontext_remember", "repocontext_update", "repocontext_forget",
            }));
    }

    [Test]
    public void AddRepoContextTools_registers_an_enforcing_guard_when_a_workspace_root_is_supplied()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools(enableWrites: true, workspaceMode: true, workspaceRoot: "/workspace");

        using var provider = services.BuildServiceProvider();
        var guard = provider.GetRequiredService<RepoContextWorkspaceGuard>();

        Assert.That(guard.IsEnforcing, Is.True);
    }

    [Test]
    public void AddRepoContextTools_registers_a_disabled_guard_by_default()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        using var provider = services.BuildServiceProvider();
        var guard = provider.GetRequiredService<RepoContextWorkspaceGuard>();

        Assert.That(guard.IsEnforcing, Is.False);
    }
}
