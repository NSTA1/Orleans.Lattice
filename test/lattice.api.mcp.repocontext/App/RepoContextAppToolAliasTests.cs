using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// The app-local alias over a group tool changes only the advertised name and delegates
/// everything else to the inner tool.
/// </summary>
[TestFixture]
public sealed class RepoContextAppToolAliasTests
{
    private static McpServerTool GroupTool(string name)
        => new RepoContextToolGroup().Tools.Single(t => t.ProtocolTool.Name == name);

    [Test]
    public void Alias_advertises_the_local_name_and_preserves_the_rest_of_the_definition()
    {
        var inner = GroupTool("repocontext_search");

        var alias = new RepoContextAppToolAlias(inner, "search");

        Assert.Multiple(() =>
        {
            Assert.That(alias.ProtocolTool.Name, Is.EqualTo("search"));
            Assert.That(alias.ProtocolTool.Title, Is.EqualTo(inner.ProtocolTool.Title));
            Assert.That(alias.ProtocolTool.Description, Is.EqualTo(inner.ProtocolTool.Description));
            Assert.That(alias.ProtocolTool.InputSchema.GetRawText(), Is.EqualTo(inner.ProtocolTool.InputSchema.GetRawText()));
            Assert.That(alias.ProtocolTool.OutputSchema?.GetRawText(), Is.EqualTo(inner.ProtocolTool.OutputSchema?.GetRawText()));
            Assert.That(alias.ProtocolTool.Annotations, Is.SameAs(inner.ProtocolTool.Annotations));
            Assert.That(inner.ProtocolTool.Name, Is.EqualTo("repocontext_search"), "the inner tool is not renamed");
        });
    }

    [Test]
    public async Task Alias_invocation_delegates_to_the_inner_tool()
    {
        var host = new RepoContextAppTestHost(services => services.AddRepoContextTools());
        var alias = new RepoContextAppToolAlias(GroupTool("repocontext_stats"), "stats");

        var result = await host.InvokeAsync(alias);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.Not.True);
            Assert.That(result.StructuredContent.ToString(), Does.Contain("calls"));
        });
    }

    [Test]
    public void Alias_rejects_a_null_inner_tool()
        => Assert.Throws<ArgumentNullException>(() => _ = new RepoContextAppToolAlias(null!, "search"));

    [Test]
    public void Alias_rejects_a_null_local_name()
        => Assert.Throws<ArgumentNullException>(() => _ = new RepoContextAppToolAlias(GroupTool("repocontext_search"), null!));
}
