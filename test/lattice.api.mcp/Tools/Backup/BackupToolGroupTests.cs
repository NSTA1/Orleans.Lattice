using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="BackupToolGroup"/> and its
/// <c>AddBackupTools</c> registration: proves the group belongs to the backup
/// facade group, that backup control gates the mutating tools (inspect-only by
/// default, all ten tools when opted in), that every tool carries the correct
/// read-only / destructive annotation, and that the registration wires the group
/// and the option flags. All deterministic - no cluster, no ordering assumptions.
/// </summary>
[TestFixture]
public sealed class BackupToolGroupTests
{
    private static readonly string[] InspectToolNames =
    {
        "lattice_backup_list",
        "lattice_backup_describe",
        "lattice_backup_inventory",
        "lattice_backup_scope_status",
        "lattice_backup_export_artifact",
    };

    private static readonly string[] ControlToolNames =
    {
        "lattice_backup_create",
        "lattice_backup_create_incremental",
        "lattice_backup_restore",
        "lattice_backup_revert_restore",
        "lattice_backup_delete",
    };

    private static BackupToolGroup CreateGroup(bool enableControl)
        => new(Options.Create(new LatticeApiMcpOptions { EnableBackupControlTools = enableControl }));

    private static HashSet<string> ToolNames(BackupToolGroup group)
        => group.Tools.Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);

    private static McpServerTool Tool(BackupToolGroup group, string name)
        => group.Tools.Single(t => t.ProtocolTool.Name == name);

    [Test]
    public void Group_is_the_backup_facade_group()
    {
        Assert.That(CreateGroup(enableControl: false).Group, Is.EqualTo(LatticeApiMcpGroup.Backup));
    }

    [Test]
    public void Control_disabled_offers_only_the_inspect_tools()
    {
        var group = CreateGroup(enableControl: false);

        Assert.That(ToolNames(group), Is.EquivalentTo(InspectToolNames),
            "With backup control disabled the group must expose only the read-only inspect tools.");
    }

    [Test]
    public void Control_enabled_offers_inspect_and_control_tools()
    {
        var group = CreateGroup(enableControl: true);

        Assert.That(ToolNames(group), Is.EquivalentTo(InspectToolNames.Concat(ControlToolNames)));
    }

    [Test]
    public void Inspect_tools_are_annotated_read_only_and_non_destructive()
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
        Assert.That(() => new BackupToolGroup(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddBackupTools_rejects_null_services()
    {
        Assert.That(
            () => LatticeMcpBackupServiceCollectionExtensions.AddBackupTools(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddBackupTools_inspect_only_registers_a_group_with_only_inspect_tools()
    {
        var provider = new ServiceCollection().AddBackupTools().BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        var group = (BackupToolGroup)provider.GetRequiredService<ILatticeApiMcpToolGroup>();

        Assert.Multiple(() =>
        {
            Assert.That(options.EnableBackupTools, Is.True);
            Assert.That(options.EnableBackupControlTools, Is.False);
            Assert.That(ToolNames(group), Is.EquivalentTo(InspectToolNames));
        });
    }

    [Test]
    public void AddBackupTools_with_control_registers_a_group_with_all_tools()
    {
        var provider = new ServiceCollection().AddBackupTools(enableControl: true).BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        var group = (BackupToolGroup)provider.GetRequiredService<ILatticeApiMcpToolGroup>();

        Assert.Multiple(() =>
        {
            Assert.That(options.EnableBackupTools, Is.True);
            Assert.That(options.EnableBackupControlTools, Is.True);
            Assert.That(ToolNames(group), Is.EquivalentTo(InspectToolNames.Concat(ControlToolNames)));
        });
    }

    [Test]
    public void AddBackupTools_called_twice_registers_exactly_one_group()
    {
        var provider = new ServiceCollection()
            .AddBackupTools()
            .AddBackupTools()
            .BuildServiceProvider();

        var groups = provider.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<BackupToolGroup>()
            .ToArray();

        Assert.That(groups, Has.Length.EqualTo(1));
    }
}
