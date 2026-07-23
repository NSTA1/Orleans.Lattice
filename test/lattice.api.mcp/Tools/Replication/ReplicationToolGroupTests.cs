using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="ReplicationToolGroup"/> and its
/// <c>AddReplicationTools</c> registration: proves the group belongs to the
/// replication facade group, that replication control gates the mutating tools
/// (inspect-only by default, all three tools when opted in), that every tool
/// carries the correct read-only / destructive annotation, and that the
/// registration wires the group and the option flags. All deterministic - no
/// cluster, no ordering assumptions.
/// </summary>
[TestFixture]
public sealed class ReplicationToolGroupTests
{
    private static readonly string[] InspectToolNames =
    {
        "lattice_replication_get_config",
    };

    private static readonly string[] ControlToolNames =
    {
        "lattice_replication_enable",
        "lattice_replication_disable",
    };

    private static ReplicationToolGroup CreateGroup(bool enableControl)
        => new(Options.Create(new LatticeApiMcpOptions { EnableReplicationControlTools = enableControl }));

    private static HashSet<string> ToolNames(ReplicationToolGroup group)
        => group.Tools.Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);

    private static McpServerTool Tool(ReplicationToolGroup group, string name)
        => group.Tools.Single(t => t.ProtocolTool.Name == name);

    [Test]
    public void Group_is_the_replication_facade_group()
    {
        Assert.That(CreateGroup(enableControl: false).Group, Is.EqualTo(LatticeApiMcpGroup.Replication));
    }

    [Test]
    public void Control_disabled_offers_only_the_inspect_tool()
    {
        var group = CreateGroup(enableControl: false);

        Assert.That(ToolNames(group), Is.EquivalentTo(InspectToolNames),
            "With replication control disabled the group must expose only the read-only inspect tool.");
    }

    [Test]
    public void Control_enabled_offers_inspect_and_control_tools()
    {
        var group = CreateGroup(enableControl: true);

        Assert.That(ToolNames(group), Is.EquivalentTo(InspectToolNames.Concat(ControlToolNames)));
    }

    [Test]
    public void Inspect_tool_is_annotated_read_only_and_non_destructive()
    {
        var group = CreateGroup(enableControl: true);

        Assert.Multiple(() =>
        {
            foreach (var name in InspectToolNames)
            {
                var annotations = Tool(group, name).ProtocolTool.Annotations;
                Assert.That(annotations?.ReadOnlyHint, Is.True, $"{name} must be read-only.");
                Assert.That(annotations?.DestructiveHint, Is.False, $"{name} must be non-destructive.");
            }
        });
    }

    [Test]
    public void Control_tools_are_annotated_destructive_and_not_read_only()
    {
        var group = CreateGroup(enableControl: true);

        Assert.Multiple(() =>
        {
            foreach (var name in ControlToolNames)
            {
                var annotations = Tool(group, name).ProtocolTool.Annotations;
                Assert.That(annotations?.DestructiveHint, Is.True, $"{name} must be destructive.");
                Assert.That(annotations?.ReadOnlyHint, Is.False, $"{name} must not be read-only.");
            }
        });
    }

    [Test]
    public void Constructor_rejects_null_options()
    {
        Assert.That(() => new ReplicationToolGroup(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddReplicationTools_rejects_null_services()
    {
        Assert.That(
            () => LatticeMcpReplicationServiceCollectionExtensions.AddReplicationTools(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddReplicationTools_inspect_only_registers_a_group_with_only_the_inspect_tool()
    {
        var provider = new ServiceCollection().AddReplicationTools().BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        var group = (ReplicationToolGroup)provider.GetRequiredService<ILatticeApiMcpToolGroup>();

        Assert.Multiple(() =>
        {
            Assert.That(options.EnableReplicationTools, Is.True);
            Assert.That(options.EnableReplicationControlTools, Is.False);
            Assert.That(ToolNames(group), Is.EquivalentTo(InspectToolNames));
        });
    }

    [Test]
    public void AddReplicationTools_with_control_registers_a_group_with_all_tools()
    {
        var provider = new ServiceCollection().AddReplicationTools(enableControl: true).BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        var group = (ReplicationToolGroup)provider.GetRequiredService<ILatticeApiMcpToolGroup>();

        Assert.Multiple(() =>
        {
            Assert.That(options.EnableReplicationTools, Is.True);
            Assert.That(options.EnableReplicationControlTools, Is.True);
            Assert.That(ToolNames(group), Is.EquivalentTo(InspectToolNames.Concat(ControlToolNames)));
        });
    }

    [Test]
    public void AddReplicationTools_called_twice_registers_exactly_one_group()
    {
        var provider = new ServiceCollection()
            .AddReplicationTools()
            .AddReplicationTools()
            .BuildServiceProvider();

        var groups = provider.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<ReplicationToolGroup>()
            .ToArray();

        Assert.That(groups, Has.Length.EqualTo(1));
    }
}
