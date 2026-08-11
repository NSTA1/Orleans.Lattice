using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="RepoContextToolGroup"/>: it serves the repository-context
/// group, contributes exactly the read-only <c>repocontext_health</c> probe built
/// once, and annotates it read-only and non-destructive.
/// </summary>
[TestFixture]
public sealed class RepoContextToolGroupTests
{
    [Test]
    public void Group_is_repo_context()
        => Assert.That(new RepoContextToolGroup().Group, Is.EqualTo(LatticeApiMcpGroup.RepoContext));

    [Test]
    public void Tools_is_a_stable_non_null_instance()
    {
        var group = new RepoContextToolGroup();
        Assert.That(group.Tools, Is.Not.Null.And.SameAs(group.Tools));
    }

    [Test]
    public void Contributes_exactly_the_health_tool()
    {
        var names = new RepoContextToolGroup().Tools.Select(t => t.ProtocolTool.Name).ToArray();
        Assert.That(names, Is.EquivalentTo(new[] { "repocontext_health" }));
    }

    [Test]
    public void The_health_tool_is_annotated_read_only_and_non_destructive()
    {
        var tool = new RepoContextToolGroup().Tools.Single();
        var annotations = tool.ProtocolTool.Annotations;

        Assert.Multiple(() =>
        {
            Assert.That(annotations, Is.Not.Null);
            Assert.That(annotations!.ReadOnlyHint, Is.True);
            Assert.That(annotations!.DestructiveHint, Is.False);
        });
    }

    [Test]
    public void The_health_tool_carries_a_title_and_description()
    {
        var tool = new RepoContextToolGroup().Tools.Single();

        Assert.Multiple(() =>
        {
            Assert.That(tool.ProtocolTool.Title, Is.Not.Null.And.Not.Empty);
            Assert.That(tool.ProtocolTool.Description, Is.Not.Null.And.Not.Empty);
        });
    }
}
