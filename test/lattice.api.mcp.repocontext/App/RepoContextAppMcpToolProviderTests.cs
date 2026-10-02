namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// The provider adapts exactly the group's app-surface tools, under their app-local names,
/// whatever the host configuration, and never contributes a tool the configuration withholds.
/// </summary>
[TestFixture]
public sealed class RepoContextAppMcpToolProviderTests
{
    [Test]
    public void Provider_serves_the_repo_context_slug()
        => Assert.That(new RepoContextAppMcpToolProvider(new RepoContextToolGroup()).Slug.Value, Is.EqualTo("repo-context"));

    [TestCase(false, false, false)]
    [TestCase(false, false, true)]
    [TestCase(true, false, true)]
    [TestCase(true, true, true)]
    [TestCase(true, true, false)]
    public void Provider_contributes_exactly_the_app_tool_names_under_any_configuration(bool writes, bool workspace, bool guarded)
    {
        var provider = new RepoContextAppMcpToolProvider(new RepoContextToolGroup(writes, workspace, guarded));

        Assert.That(provider.Tools.Select(t => t.ProtocolTool.Name), Is.EquivalentTo(RepoContextAppManifest.AppToolNames));
    }

    [Test]
    public void Provider_never_contributes_a_mutating_or_path_taking_tool()
    {
        var provider = new RepoContextAppMcpToolProvider(new RepoContextToolGroup(enableWrites: true, workspaceMode: true));
        var names = provider.Tools.Select(t => t.ProtocolTool.Name).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                names,
                Is.Not.Empty,
                "Has.None passes on an empty population, so a provider that contributed no tools "
                + "at all would be indistinguishable here from one that correctly withheld the "
                + "mutating and path-taking ones.");
            Assert.That(names, Has.None.AnyOf("remember", "update", "forget", "claim", "renew_claim", "release_claim", "add_repo", "remove_repo", "reset_index", "bootstrap", "changed", "list_repos"));
        });
    }

    [Test]
    public void Provider_adapts_the_groups_own_tool_instances()
    {
        var group = new RepoContextToolGroup();
        var provider = new RepoContextAppMcpToolProvider(group);

        var search = provider.Tools.Single(t => t.ProtocolTool.Name == "search");
        var groupSearch = group.Tools.Single(t => t.ProtocolTool.Name == "repocontext_search");

        Assert.Multiple(() =>
        {
            Assert.That(search, Is.InstanceOf<RepoContextAppToolAlias>());
            Assert.That(search.ProtocolTool.InputSchema.GetRawText(), Is.EqualTo(groupSearch.ProtocolTool.InputSchema.GetRawText()));
            Assert.That(search.ProtocolTool.Description, Is.EqualTo(groupSearch.ProtocolTool.Description));
        });
    }

    [Test]
    public void Provider_returns_the_same_prebuilt_list_on_every_read()
    {
        var provider = new RepoContextAppMcpToolProvider(new RepoContextToolGroup());

        Assert.That(provider.Tools, Is.SameAs(provider.Tools));
    }

    [Test]
    public void Provider_rejects_a_null_group()
        => Assert.Throws<ArgumentNullException>(() => _ = new RepoContextAppMcpToolProvider(null!));
}
