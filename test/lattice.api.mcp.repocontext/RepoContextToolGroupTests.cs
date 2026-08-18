using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="RepoContextToolGroup"/>: it serves the repository-context
/// group, contributes the read-only <c>repocontext_health</c> probe and the
/// read-only capture tools (<c>repocontext_recall</c>, <c>_scan</c>,
/// <c>_list_topics</c>) by default, adds the mutating tools
/// (<c>repocontext_bootstrap</c>, <c>_remember</c>, <c>_update</c>, <c>_forget</c>)
/// only when writes are opted in, and annotates each tool correctly.
/// </summary>
[TestFixture]
public sealed class RepoContextToolGroupTests
{
    private static readonly string[] ReadToolNames =
        [
            "repocontext_health", "repocontext_recall", "repocontext_scan",
            "repocontext_list_topics", "repocontext_search", "repocontext_index_status",
            "repocontext_neighbors", "repocontext_outline", "repocontext_changed",
            "repocontext_related",
        ];

    private static readonly string[] WriteToolNames =
        ["repocontext_bootstrap", "repocontext_remember", "repocontext_update", "repocontext_forget"];

    private static readonly string[] WorkspaceReadToolNames =
        [
            "repocontext_health", "repocontext_recall", "repocontext_scan",
            "repocontext_list_topics", "repocontext_search", "repocontext_index_status",
            "repocontext_neighbors", "repocontext_outline", "repocontext_changed",
            "repocontext_related", "repocontext_list_repos",
        ];

    private static readonly string[] WorkspaceWriteToolNames =
        ["repocontext_add_repo", "repocontext_remove_repo", "repocontext_remember", "repocontext_update", "repocontext_forget"];

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
    public void Contributes_exactly_the_read_tools_by_default()
    {
        var names = new RepoContextToolGroup().Tools.Select(t => t.ProtocolTool.Name).ToArray();
        Assert.That(names, Is.EquivalentTo(ReadToolNames));
    }

    [Test]
    public void Contributes_the_write_tools_only_when_writes_are_enabled()
    {
        var names = new RepoContextToolGroup(enableWrites: true)
            .Tools.Select(t => t.ProtocolTool.Name).ToArray();
        Assert.That(names, Is.EquivalentTo(ReadToolNames.Concat(WriteToolNames)));
    }

    [Test]
    public void Workspace_mode_adds_list_repos_to_the_read_surface()
    {
        var names = new RepoContextToolGroup(enableWrites: false, workspaceMode: true)
            .Tools.Select(t => t.ProtocolTool.Name).ToArray();
        Assert.That(names, Is.EquivalentTo(WorkspaceReadToolNames));
    }

    [Test]
    public void Workspace_mode_replaces_bootstrap_with_add_and_remove_repo()
    {
        var names = new RepoContextToolGroup(enableWrites: true, workspaceMode: true)
            .Tools.Select(t => t.ProtocolTool.Name).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(names, Is.EquivalentTo(WorkspaceReadToolNames.Concat(WorkspaceWriteToolNames)));
            Assert.That(names, Does.Not.Contain("repocontext_bootstrap"));
        });
    }

    [TestCase("repocontext_list_repos")]
    public void Workspace_read_tools_are_annotated_read_only_and_non_destructive(string toolName)
    {
        var tool = new RepoContextToolGroup(enableWrites: true, workspaceMode: true)
            .Tools.Single(t => t.ProtocolTool.Name == toolName);
        var annotations = tool.ProtocolTool.Annotations;

        Assert.Multiple(() =>
        {
            Assert.That(annotations, Is.Not.Null);
            Assert.That(annotations!.ReadOnlyHint, Is.True);
            Assert.That(annotations!.DestructiveHint, Is.False);
        });
    }

    [TestCase("repocontext_add_repo")]
    [TestCase("repocontext_remove_repo")]
    public void Workspace_write_tools_are_annotated_mutating_and_destructive(string toolName)
    {
        var tool = new RepoContextToolGroup(enableWrites: true, workspaceMode: true)
            .Tools.Single(t => t.ProtocolTool.Name == toolName);
        var annotations = tool.ProtocolTool.Annotations;

        Assert.Multiple(() =>
        {
            Assert.That(annotations, Is.Not.Null);
            Assert.That(annotations!.ReadOnlyHint, Is.False);
            Assert.That(annotations!.DestructiveHint, Is.True);
        });
    }

    [TestCaseSource(nameof(ReadToolNames))]
    public void Read_tools_are_annotated_read_only_and_non_destructive(string toolName)
    {
        var tool = new RepoContextToolGroup(enableWrites: true)
            .Tools.Single(t => t.ProtocolTool.Name == toolName);
        var annotations = tool.ProtocolTool.Annotations;

        Assert.Multiple(() =>
        {
            Assert.That(annotations, Is.Not.Null);
            Assert.That(annotations!.ReadOnlyHint, Is.True);
            Assert.That(annotations!.DestructiveHint, Is.False);
        });
    }

    [TestCaseSource(nameof(WriteToolNames))]
    public void Write_tools_are_annotated_mutating_and_destructive(string toolName)
    {
        var tool = new RepoContextToolGroup(enableWrites: true)
            .Tools.Single(t => t.ProtocolTool.Name == toolName);
        var annotations = tool.ProtocolTool.Annotations;

        Assert.Multiple(() =>
        {
            Assert.That(annotations, Is.Not.Null);
            Assert.That(annotations!.ReadOnlyHint, Is.False);
            Assert.That(annotations!.DestructiveHint, Is.True);
        });
    }

    [Test]
    public void Every_tool_carries_a_title_and_description()
    {
        var tools = new RepoContextToolGroup(enableWrites: true).Tools;

        Assert.Multiple(() =>
        {
            foreach (var tool in tools)
            {
                Assert.That(tool.ProtocolTool.Title, Is.Not.Null.And.Not.Empty, tool.ProtocolTool.Name);
                Assert.That(tool.ProtocolTool.Description, Is.Not.Null.And.Not.Empty, tool.ProtocolTool.Name);
            }
        });
    }

    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    [TestCase(true, true)]
    public void Every_tool_is_wrapped_in_the_invocation_logger(bool enableWrites, bool workspaceMode)
    {
        var tools = new RepoContextToolGroup(enableWrites, workspaceMode).Tools;

        Assert.That(tools, Is.Not.Empty);
        Assert.Multiple(() =>
        {
            foreach (var tool in tools)
            {
                Assert.That(
                    tool,
                    Is.InstanceOf<RepoContextToolInvocationLogger>(),
                    tool.ProtocolTool.Name);
            }
        });
    }
}
