using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TreeAdminToolGroup"/>, the tree-administration tool
/// module, and its <c>AddTreeAdminTools</c> registration. Proves the group serves
/// <see cref="LatticeApiMcpGroup.TreeAdmin"/>, exposes only the read-only
/// schema-inspection tools until schema control is opted in, then adds the mutating
/// schema-management tools; that read tools carry the read-only annotation and write
/// tools the destructive annotation; that the injected
/// <see cref="ILatticeSchemaControl"/> facade parameter is bound from services
/// rather than exposed in a tool's input schema; and that the registration wires
/// exactly one group and sets the option flag. Deterministic - no cluster, no
/// invocation.
/// </summary>
[TestFixture]
public sealed class TreeAdminToolGroupTests
{
    private static readonly string[] InspectionToolNames =
    {
        "lattice_treeadmin_schema_get_policy",
        "lattice_treeadmin_schema_list_dead_letters",
        "lattice_treeadmin_schema_count_dead_letters",
        "lattice_treeadmin_schema_get_version_config",
        "lattice_treeadmin_schema_get_remediation_status",
        "lattice_treeadmin_schema_scan_compliance",
        "lattice_treeadmin_schema_probe_capabilities",
    };

    private static readonly string[] ManagementToolNames =
    {
        "lattice_treeadmin_schema_set_policy",
        "lattice_treeadmin_schema_clear_policy",
        "lattice_treeadmin_schema_set_version_config",
        "lattice_treeadmin_schema_clear_version_config",
        "lattice_treeadmin_schema_advance_target_version",
        "lattice_treeadmin_schema_advance_and_migrate",
        "lattice_treeadmin_schema_migrate_to_target",
        "lattice_treeadmin_schema_remediate",
    };

    private static TreeAdminToolGroup CreateGroup(bool enableSchemaControl)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<ILatticeSchemaControl>());
        var provider = services.BuildServiceProvider();
        var options = Options.Create(
            new LatticeApiMcpOptions { EnableTreeAdminSchemaControlTools = enableSchemaControl });
        return new TreeAdminToolGroup(provider, options);
    }

    private static IReadOnlyList<string> Names(TreeAdminToolGroup group)
        => group.Tools.Select(t => t.ProtocolTool.Name).ToList();

    [Test]
    public void Group_serves_the_tree_admin_facade_group()
    {
        Assert.That(CreateGroup(enableSchemaControl: false).Group, Is.EqualTo(LatticeApiMcpGroup.TreeAdmin));
    }

    [Test]
    public void Inspection_only_when_schema_control_is_disabled()
    {
        var group = CreateGroup(enableSchemaControl: false);

        Assert.Multiple(() =>
        {
            Assert.That(Names(group), Is.EquivalentTo(InspectionToolNames));
            foreach (var management in ManagementToolNames)
            {
                Assert.That(Names(group), Does.Not.Contain(management),
                    "The mutating schema-management tools must be hidden until schema control is opted in.");
            }
        });
    }

    [Test]
    public void Management_tools_appear_when_schema_control_is_enabled()
    {
        var group = CreateGroup(enableSchemaControl: true);

        Assert.That(Names(group), Is.EquivalentTo(InspectionToolNames.Concat(ManagementToolNames)));
    }

    [Test]
    public void Inspection_tools_are_annotated_read_only()
    {
        var group = CreateGroup(enableSchemaControl: true);

        Assert.Multiple(() =>
        {
            foreach (var name in InspectionToolNames)
            {
                var annotations = ServerTool(group, name).ProtocolTool.Annotations;
                Assert.That(annotations?.ReadOnlyHint, Is.True, $"{name} must be read-only.");
                Assert.That(annotations?.DestructiveHint, Is.False, $"{name} must not be destructive.");
            }
        });
    }

    [Test]
    public void Management_tools_are_annotated_destructive()
    {
        var group = CreateGroup(enableSchemaControl: true);

        Assert.Multiple(() =>
        {
            foreach (var name in ManagementToolNames)
            {
                var annotations = ServerTool(group, name).ProtocolTool.Annotations;
                Assert.That(annotations?.DestructiveHint, Is.True, $"{name} must be destructive.");
                Assert.That(annotations?.ReadOnlyHint, Is.False, $"{name} must not be read-only.");
            }
        });
    }

    [Test]
    public void The_facade_parameter_is_bound_from_services_not_the_input_schema()
    {
        var group = CreateGroup(enableSchemaControl: true);

        Assert.Multiple(() =>
        {
            foreach (var tool in group.Tools)
            {
                Assert.That(SchemaHasProperty(tool, "schema"), Is.False,
                    $"{tool.ProtocolTool.Name} must resolve its facade from services, not expose it as an argument.");
                Assert.That(SchemaHasProperty(tool, "cancellationToken"), Is.False,
                    $"{tool.ProtocolTool.Name} must not expose the cancellation token as an argument.");
            }
        });
    }

    [Test]
    public void Business_arguments_stay_in_the_input_schema()
    {
        var group = CreateGroup(enableSchemaControl: true);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaHasProperty(ServerTool(group, "lattice_treeadmin_schema_get_policy"), "treeId"), Is.True);
            Assert.That(SchemaHasProperty(ServerTool(group, "lattice_treeadmin_schema_set_policy"), "policy"), Is.True);
            Assert.That(SchemaHasProperty(ServerTool(group, "lattice_treeadmin_schema_set_version_config"), "schemaId"), Is.True);
            Assert.That(SchemaHasProperty(ServerTool(group, "lattice_treeadmin_schema_set_version_config"), "targetVersion"), Is.True);
            Assert.That(SchemaHasProperty(ServerTool(group, "lattice_treeadmin_schema_advance_target_version"), "newTargetVersion"), Is.True);
            Assert.That(SchemaHasProperty(ServerTool(group, "lattice_treeadmin_schema_remediate"), "transform"), Is.True);
            Assert.That(SchemaHasProperty(ServerTool(group, "lattice_treeadmin_schema_remediate"), "targetPolicy"), Is.True);
        });
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        var options = Options.Create(new LatticeApiMcpOptions());

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new TreeAdminToolGroup(null!, options));
            Assert.Throws<ArgumentNullException>(() => new TreeAdminToolGroup(provider, null!));
        });
    }

    [Test]
    public void AddTreeAdminTools_rejects_null_services()
    {
        Assert.That(
            () => LatticeMcpTreeAdminServiceCollectionExtensions.AddTreeAdminTools(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddTreeAdminTools_registers_exactly_one_discoverable_group_inspect_only_by_default()
    {
        var provider = new ServiceCollection()
            .AddSingleton(Substitute.For<ILatticeSchemaControl>())
            .AddTreeAdminTools()
            .BuildServiceProvider();

        var group = provider.GetServices<ILatticeApiMcpToolGroup>().OfType<TreeAdminToolGroup>().Single();

        Assert.Multiple(() =>
        {
            Assert.That(group.Group, Is.EqualTo(LatticeApiMcpGroup.TreeAdmin));
            Assert.That(group.Tools.Select(t => t.ProtocolTool.Name), Is.EquivalentTo(InspectionToolNames));
        });
    }

    [Test]
    public void AddTreeAdminTools_with_schema_control_adds_the_management_tools()
    {
        var provider = new ServiceCollection()
            .AddSingleton(Substitute.For<ILatticeSchemaControl>())
            .AddTreeAdminTools(enableSchemaControl: true)
            .BuildServiceProvider();

        var group = provider.GetServices<ILatticeApiMcpToolGroup>().OfType<TreeAdminToolGroup>().Single();

        Assert.That(
            group.Tools.Select(t => t.ProtocolTool.Name),
            Is.EquivalentTo(InspectionToolNames.Concat(ManagementToolNames)));
    }

    [Test]
    public void AddTreeAdminTools_called_twice_registers_exactly_one_group()
    {
        var provider = new ServiceCollection()
            .AddSingleton(Substitute.For<ILatticeSchemaControl>())
            .AddTreeAdminTools()
            .AddTreeAdminTools()
            .BuildServiceProvider();

        var groups = provider.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<TreeAdminToolGroup>()
            .ToArray();

        Assert.That(groups, Has.Length.EqualTo(1));
    }

    private static McpServerTool ServerTool(TreeAdminToolGroup group, string name)
        => group.Tools.Single(t => t.ProtocolTool.Name == name);

    private static bool SchemaHasProperty(McpServerTool tool, string propertyName)
    {
        var schema = tool.ProtocolTool.InputSchema;
        return schema.ValueKind == JsonValueKind.Object
            && schema.TryGetProperty("properties", out var properties)
            && properties.TryGetProperty(propertyName, out _);
    }
}
