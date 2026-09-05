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
            "repocontext_related", "repocontext_context", "repocontext_stats",
        ];

    private static readonly string[] WriteToolNames =
        ["repocontext_bootstrap", "repocontext_remember", "repocontext_update", "repocontext_forget"];

    private static readonly string[] WorkspaceReadToolNames =
        [
            "repocontext_health", "repocontext_recall", "repocontext_scan",
            "repocontext_list_topics", "repocontext_search", "repocontext_index_status",
            "repocontext_neighbors", "repocontext_outline", "repocontext_changed",
            "repocontext_related", "repocontext_context", "repocontext_stats", "repocontext_list_repos",
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

    // The tests below cover the per-tool operation minimum that keeps this group's
    // mutating tools out of a read-only caller's advertised surface. The group's
    // coarse capability mask spans the whole data plane and a group is admitted
    // when any single Allow rule intersects it, so a caller holding only
    // LatticeOperation.Read is legitimately admitted to the group - it has read
    // tools to use - but must not be offered the tools that write to, delete from,
    // or drive an ingest into the store.

    private static readonly string[] MutatingToolNames =
        [
            "repocontext_remember", "repocontext_update", "repocontext_forget",
            "repocontext_add_repo", "repocontext_remove_repo", "repocontext_bootstrap",
        ];

    private static readonly string[] NonMutatingToolNames =
        [
            "repocontext_health", "repocontext_recall", "repocontext_scan",
            "repocontext_search", "repocontext_context", "repocontext_outline",
            "repocontext_related", "repocontext_list_repos",
        ];

    [Test]
    public void RequiredOperationsFor_a_mutating_tool_is_not_satisfied_by_a_read_grant()
    {
        var group = new RepoContextToolGroup(enableWrites: true, workspaceMode: true);

        Assert.Multiple(() =>
        {
            foreach (var name in MutatingToolNames)
            {
                var required = group.RequiredOperationsFor(name);
                Assert.That(
                    required & LatticeOperation.Read,
                    Is.EqualTo(LatticeOperation.None),
                    $"'{name}' must not be reachable on a bare read grant.");
                Assert.That(
                    required & LatticeOperation.RangeRead,
                    Is.EqualTo(LatticeOperation.None),
                    $"'{name}' must not be reachable on a bare range-read grant.");
                Assert.That(
                    required & LatticeOperation.Write,
                    Is.Not.EqualTo(LatticeOperation.None),
                    $"'{name}' must be reachable on a write grant.");
            }
        });
    }

    [Test]
    public void RequiredOperationsFor_a_read_tool_returns_the_group_mask()
    {
        var group = new RepoContextToolGroup(enableWrites: true, workspaceMode: true);
        var groupMask = LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.RepoContext);

        Assert.Multiple(() =>
        {
            foreach (var name in NonMutatingToolNames)
            {
                Assert.That(
                    group.RequiredOperationsFor(name),
                    Is.EqualTo(groupMask),
                    $"'{name}' should keep the group's coarse mask.");
                Assert.That(
                    group.RequiredOperationsFor(name) & LatticeOperation.Read,
                    Is.Not.EqualTo(LatticeOperation.None),
                    $"'{name}' must stay reachable on a read grant.");
            }
        });
    }

    /// <summary>
    /// The refinement is name-based, so an unrecognised name must fall back to the
    /// group mask rather than being withheld: a future read tool added to the group
    /// stays advertised without needing to be listed here.
    /// </summary>
    [Test]
    public void RequiredOperationsFor_an_unknown_tool_name_returns_the_group_mask()
    {
        var group = new RepoContextToolGroup();

        Assert.That(
            group.RequiredOperationsFor("repocontext_some_future_read_tool"),
            Is.EqualTo(LatticeApiMcpGroupCapabilityMap.RequiredOperations(LatticeApiMcpGroup.RepoContext)));
    }

    /// <summary>
    /// Every mutating name the refinement lists must actually be a tool the group
    /// can contribute, otherwise the list has drifted from the surface it guards
    /// and a renamed tool would silently lose its protection.
    /// </summary>
    [Test]
    public void Every_listed_mutating_name_is_a_real_tool_of_this_group()
    {
        var singleRepo = new RepoContextToolGroup(enableWrites: true)
            .Tools.Select(t => t.ProtocolTool.Name);
        var workspace = new RepoContextToolGroup(enableWrites: true, workspaceMode: true, workspaceGuarded: true)
            .Tools.Select(t => t.ProtocolTool.Name);
        var contributable = singleRepo.Concat(workspace).ToHashSet(StringComparer.Ordinal);

        Assert.That(MutatingToolNames, Is.SubsetOf(contributable));
    }
}
