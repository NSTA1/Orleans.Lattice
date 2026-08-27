using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TenantAdminToolGroup"/> and its
/// <c>AddTenantAdminTools</c> registration: proves the group belongs to the
/// tenant-admin facade group, that the tenant lifecycle is all-mutating so the
/// group is empty until control is opted in and then exposes exactly the four
/// lifecycle tools, that every tool carries the destructive / non-read-only
/// annotation, and that the registration wires the group and the option flags.
/// This is the enable-gating surface of a fail-closed security control, so the
/// disabled-by-default behaviour is asserted directly. All deterministic - no
/// cluster, no ordering assumptions.
/// </summary>
[TestFixture]
public sealed class TenantAdminToolGroupTests
{
    private static readonly string[] ControlToolNames =
    {
        "lattice_tenant_create",
        "lattice_tenant_suspend",
        "lattice_tenant_resume",
        "lattice_tenant_delete",
        "lattice_tenant_set_quotas",
    };

    private static TenantAdminToolGroup CreateGroup(bool enableControl)
        => new(Options.Create(new LatticeApiMcpOptions { EnableTenantAdminControlTools = enableControl }));

    private static HashSet<string> ToolNames(TenantAdminToolGroup group)
        => group.Tools.Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);

    private static McpServerTool Tool(TenantAdminToolGroup group, string name)
        => group.Tools.Single(t => t.ProtocolTool.Name == name);

    [Test]
    public void Group_is_the_tenant_admin_facade_group()
    {
        Assert.That(CreateGroup(enableControl: false).Group, Is.EqualTo(LatticeApiMcpGroup.TenantAdmin));
    }

    [Test]
    public void Control_disabled_offers_no_tools()
    {
        var group = CreateGroup(enableControl: false);

        Assert.That(group.Tools, Is.Empty,
            "The tenant lifecycle is all-mutating, so with control disabled the group must expose no tools.");
    }

    [Test]
    public void Control_enabled_offers_exactly_the_five_lifecycle_tools()
    {
        var group = CreateGroup(enableControl: true);

        Assert.That(ToolNames(group), Is.EquivalentTo(ControlToolNames));
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
        Assert.That(() => new TenantAdminToolGroup(null!), Throws.ArgumentNullException);
    }

    /// <summary>
    /// Regression: an optional tool parameter declared without an explicit C#
    /// default is emitted into the JSON input schema as <c>required</c>, which
    /// contradicts the tool's own description and makes the documented behaviour
    /// unreachable for a schema-conforming agent. <c>lattice_tenant_create</c>
    /// told the caller to "omit it (or pass an empty list) to seed the calling
    /// subject" while rejecting exactly that, and every one of the six nullable
    /// quota dimensions was mandatory despite each being documented "null for
    /// unbounded".
    /// </summary>
    [TestCase("lattice_tenant_create", "tenantId")]
    [TestCase("lattice_tenant_suspend", "tenantId")]
    [TestCase("lattice_tenant_resume", "tenantId")]
    [TestCase("lattice_tenant_delete", "tenantId")]
    [TestCase("lattice_tenant_set_quotas", "tenantId")]
    public void Control_tool_schemas_require_only_genuinely_mandatory_parameters(
        string toolName, string expectedRequired)
    {
        var group = CreateGroup(enableControl: true);
        var schema = Tool(group, toolName).ProtocolTool.InputSchema;

        var required = schema.TryGetProperty("required", out var node)
            ? node.EnumerateArray().Select(e => e.GetString()!).ToArray()
            : [];

        Assert.That(required, Is.EquivalentTo(new[] { expectedRequired }),
            $"{toolName} must mark only genuinely mandatory parameters required; "
            + $"got [{string.Join(", ", required)}].");
    }

    [Test]
    public void Tenant_create_advertises_the_optional_admin_subjects_parameter()
    {
        var group = CreateGroup(enableControl: true);
        var schema = Tool(group, "lattice_tenant_create").ProtocolTool.InputSchema;

        Assert.That(
            schema.GetProperty("properties").TryGetProperty("adminSubjects", out _),
            Is.True,
            "the parameter must still be offered - just not demanded.");
    }

    [Test]
    public void Set_quotas_advertises_every_quota_dimension_as_optional()
    {
        var group = CreateGroup(enableControl: true);
        var properties = Tool(group, "lattice_tenant_set_quotas")
            .ProtocolTool.InputSchema.GetProperty("properties");

        Assert.Multiple(() =>
        {
            foreach (var dimension in new[]
                     {
                         "maxBytes", "maxKeys", "maxMemoryBytes",
                         "maxTreeCount", "maxOpsPerSecond", "burstPercent",
                     })
            {
                Assert.That(properties.TryGetProperty(dimension, out _), Is.True,
                    $"{dimension} must be offered on the schema.");
            }
        });
    }

    [Test]
    public void AddTenantAdminTools_rejects_null_services()
    {
        Assert.That(
            () => LatticeMcpTenantAdminServiceCollectionExtensions.AddTenantAdminTools(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddTenantAdminTools_without_control_advertises_the_capability_but_registers_no_tools()
    {
        var provider = new ServiceCollection().AddTenantAdminTools().BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        var group = (TenantAdminToolGroup)provider.GetRequiredService<ILatticeApiMcpToolGroup>();

        Assert.Multiple(() =>
        {
            Assert.That(options.EnableTenantAdminTools, Is.True,
                "Registering the group advertises the tenantadmin capability.");
            Assert.That(options.EnableTenantAdminControlTools, Is.False);
            Assert.That(group.Tools, Is.Empty);
        });
    }

    [Test]
    public void AddTenantAdminTools_with_control_registers_a_group_with_all_five_tools()
    {
        var provider = new ServiceCollection().AddTenantAdminTools(enableControl: true).BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        var group = (TenantAdminToolGroup)provider.GetRequiredService<ILatticeApiMcpToolGroup>();

        Assert.Multiple(() =>
        {
            Assert.That(options.EnableTenantAdminTools, Is.True);
            Assert.That(options.EnableTenantAdminControlTools, Is.True);
            Assert.That(ToolNames(group), Is.EquivalentTo(ControlToolNames));
        });
    }

    [Test]
    public void AddTenantAdminTools_called_twice_registers_exactly_one_group()
    {
        var provider = new ServiceCollection()
            .AddTenantAdminTools()
            .AddTenantAdminTools()
            .BuildServiceProvider();

        var groups = provider.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<TenantAdminToolGroup>()
            .ToArray();

        Assert.That(groups, Has.Length.EqualTo(1));
    }
}
