using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="StateToolGroup"/> - the read-only state tool module.
/// Proves the module serves the state group, contributes the full read-only tool
/// set built once, that every tool is annotated read-only and non-destructive,
/// and that the injected <c>ILatticeStateQuery</c> collaborator is excluded from
/// every tool's input schema (so it is resolved from DI at invocation rather than
/// solicited from the caller). Deterministic; no cluster, no MCP transport.
/// </summary>
[TestFixture]
public sealed class StateToolGroupTests
{
    private static readonly string[] ExpectedToolNames =
    {
        "lattice_state_get_cluster_info",
        "lattice_state_list_trees",
        "lattice_state_list_views",
        "lattice_state_list_tag_indexes",
        "lattice_state_list_tag_values",
        "lattice_state_list_covered_trees",
        "lattice_state_list_index_tags",
        "lattice_state_scan_tag_members",
        "lattice_state_get_tree_summary",
        "lattice_state_get_shard_summaries",
        "lattice_state_get_physical_shard_count",
        "lattice_state_get_tree_structure",
        "lattice_state_scan_entries",
        "lattice_state_get_entry",
        "lattice_state_get_entry_history",
        "lattice_state_cancel_scan",
    };

    private static StateToolGroup CreateGroup()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<ILatticeStateQuery>());
        return new StateToolGroup(services.BuildServiceProvider());
    }

    private static HashSet<string> InputSchemaPropertyNames(McpServerTool tool)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (tool.ProtocolTool.InputSchema.ValueKind == JsonValueKind.Object
            && tool.ProtocolTool.InputSchema.TryGetProperty("properties", out var props)
            && props.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in props.EnumerateObject())
            {
                names.Add(property.Name);
            }
        }

        return names;
    }

    [Test]
    public void Group_serves_the_state_facade()
    {
        Assert.That(CreateGroup().Group, Is.EqualTo(LatticeApiMcpGroup.State));
    }

    [Test]
    public void Contributes_exactly_the_expected_read_only_tool_set()
    {
        var group = CreateGroup();

        var names = group.Tools.Select(t => t.ProtocolTool.Name).ToArray();

        Assert.That(names, Is.EquivalentTo(ExpectedToolNames));
    }

    [Test]
    public void Every_tool_is_annotated_read_only_and_non_destructive()
    {
        var group = CreateGroup();

        Assert.Multiple(() =>
        {
            foreach (var tool in group.Tools)
            {
                var annotations = tool.ProtocolTool.Annotations;
                Assert.That(annotations, Is.Not.Null, $"{tool.ProtocolTool.Name} must carry annotations.");
                Assert.That(annotations!.ReadOnlyHint, Is.True, $"{tool.ProtocolTool.Name} must be read-only.");
                Assert.That(annotations!.DestructiveHint, Is.False, $"{tool.ProtocolTool.Name} must be non-destructive.");
            }
        });
    }

    [Test]
    public void The_injected_state_query_is_excluded_from_every_tool_input_schema()
    {
        var group = CreateGroup();

        Assert.Multiple(() =>
        {
            foreach (var tool in group.Tools)
            {
                var names = InputSchemaPropertyNames(tool);
                Assert.That(names, Does.Not.Contain("query"),
                    $"{tool.ProtocolTool.Name} must resolve ILatticeStateQuery from DI, not solicit it from the caller.");
                Assert.That(names, Does.Not.Contain("cancellationToken"),
                    $"{tool.ProtocolTool.Name} must not expose the cancellation token as an argument.");
            }
        });
    }

    [Test]
    public void The_cluster_info_tool_takes_no_arguments()
    {
        var group = CreateGroup();
        var clusterInfo = group.Tools.Single(t => t.ProtocolTool.Name == "lattice_state_get_cluster_info");

        Assert.That(InputSchemaPropertyNames(clusterInfo), Is.Empty,
            "get_cluster_info binds only the DI query and the cancellation token, so its schema is empty.");
    }

    [Test]
    public void The_list_trees_tool_surfaces_paging_and_the_system_tree_flag()
    {
        var group = CreateGroup();
        var listTrees = group.Tools.Single(t => t.ProtocolTool.Name == "lattice_state_list_trees");

        var names = InputSchemaPropertyNames(listTrees);

        Assert.Multiple(() =>
        {
            Assert.That(names, Has.Count.EqualTo(3));
            Assert.That(names, Does.Contain("pageSize"));
            Assert.That(names, Does.Contain("pageToken"));
            Assert.That(names, Does.Contain("includeSystemTrees"));
        });
    }

    [Test]
    public void The_scan_entries_tool_surfaces_the_continuation_token_and_preview_budget()
    {
        var group = CreateGroup();
        var scanEntries = group.Tools.Single(t => t.ProtocolTool.Name == "lattice_state_scan_entries");

        var names = InputSchemaPropertyNames(scanEntries);

        Assert.Multiple(() =>
        {
            Assert.That(names, Does.Contain("continuationToken"));
            Assert.That(names, Does.Contain("valuePreviewBudget"));
            Assert.That(names, Does.Contain("pageSize"));
        });
    }

    [Test]
    public void Tools_are_built_once_and_returned_as_a_stable_list()
    {
        var group = CreateGroup();

        Assert.That(group.Tools, Is.SameAs(group.Tools),
            "The tool list is materialised once in the constructor.");
    }

    [Test]
    public void Constructor_rejects_a_null_service_provider()
    {
        Assert.Throws<ArgumentNullException>(() => new StateToolGroup(null!));
    }
}
