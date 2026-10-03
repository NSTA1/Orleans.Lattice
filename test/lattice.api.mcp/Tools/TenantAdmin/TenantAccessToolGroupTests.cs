using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Fakes;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TenantAccessToolGroup"/> and its registration through
/// <c>AddTenantAdminTools</c>: the tools are absent without the control opt-in,
/// each facade's tools appear only when that facade is registered, the reads are
/// annotated read-only and the writes destructive, the schemas require only the
/// genuinely mandatory arguments, and registering the module leaves
/// <c>lattice_capabilities</c> and the session instructions unchanged. All
/// deterministic - no cluster, no transport.
/// </summary>
[TestFixture]
public sealed class TenantAccessToolGroupTests
{
    private static readonly string[] ReadToolNames =
    [
        "lattice_tenant_group_list",
        "lattice_tenant_group_get",
        "lattice_tenant_group_members",
        "lattice_tenant_member_list",
        "lattice_tenant_rule_list",
        "lattice_tenant_rule_get",
        "lattice_tenant_explain",
        "lattice_tenant_effective_permissions",
        "lattice_tenant_access_posture",
    ];

    private static readonly string[] WriteToolNames =
    [
        "lattice_tenant_group_upsert",
        "lattice_tenant_group_remove",
        "lattice_tenant_group_member_add",
        "lattice_tenant_group_member_remove",
        "lattice_tenant_member_add",
        "lattice_tenant_member_remove",
        "lattice_tenant_rule_put",
        "lattice_tenant_rule_remove",
    ];

    private static ServiceProvider Provider(bool directory, bool policy)
    {
        var services = new ServiceCollection();
        var gate = new FakeTenantAccessGate();
        var policyFake = new FakeTenantPolicyAdmin(gate);
        if (directory)
        {
            services.AddSingleton<ILatticeTenantDirectoryAdmin>(new FakeTenantDirectoryAdmin(gate, policyFake));
        }

        if (policy)
        {
            services.AddSingleton<ILatticeTenantPolicyAdmin>(policyFake);
        }

        return services.BuildServiceProvider();
    }

    private static TenantAccessToolGroup CreateGroup(bool enableControl, bool directory = true, bool policy = true)
        => new(
            Provider(directory, policy),
            Options.Create(new LatticeApiMcpOptions { EnableTenantAdminControlTools = enableControl }));

    private static string[] ToolNames(ILatticeApiMcpToolGroup group)
        => group.Tools.Select(t => t.ProtocolTool.Name).ToArray();

    private static McpServerTool Tool(ILatticeApiMcpToolGroup group, string name)
        => group.Tools.Single(t => t.ProtocolTool.Name == name);

    [Test]
    public void Group_is_the_tenant_admin_facade_group()
    {
        Assert.That(CreateGroup(enableControl: true).Group, Is.EqualTo(LatticeApiMcpGroup.TenantAdmin));
    }

    [Test]
    public void Control_disabled_offers_no_tools_even_with_both_facades_registered()
    {
        Assert.That(CreateGroup(enableControl: false).Tools, Is.Empty,
            "The module shares the tenant-admin group's single opt-in: without it nothing is contributed.");
    }

    [Test]
    public void Control_enabled_without_the_facades_offers_no_tools()
    {
        Assert.That(CreateGroup(enableControl: true, directory: false, policy: false).Tools, Is.Empty,
            "A facade that is not registered contributes no tools, so no tool can bind a missing facade.");
    }

    [Test]
    public void Control_enabled_with_both_facades_offers_every_tool()
    {
        Assert.That(ToolNames(CreateGroup(enableControl: true)), Is.EquivalentTo(ReadToolNames.Concat(WriteToolNames)));
    }

    [Test]
    public void Only_the_directory_facade_contributes_only_the_directory_tools()
    {
        Assert.That(
            ToolNames(CreateGroup(enableControl: true, directory: true, policy: false)),
            Is.EquivalentTo(TenantAccessToolGroup.DirectoryToolNames));
    }

    [Test]
    public void Only_the_policy_facade_contributes_only_the_policy_tools()
    {
        Assert.That(
            ToolNames(CreateGroup(enableControl: true, directory: false, policy: true)),
            Is.EquivalentTo(TenantAccessToolGroup.PolicyToolNames));
    }

    [Test]
    public void Tool_names_follow_the_tenant_tool_convention_and_do_not_collide_with_the_tenant_admin_group()
    {
        var names = ToolNames(CreateGroup(enableControl: true));
        var existing = ToolNames(new TenantAdminToolGroup(
            Options.Create(new LatticeApiMcpOptions { EnableTenantAdminControlTools = true })));

        Assert.Multiple(() =>
        {
            Assert.That(names, Has.All.StartsWith("lattice_tenant_"));
            Assert.That(names.Intersect(existing), Is.Empty);
        });
    }

    [Test]
    public void Reads_are_read_only_and_writes_are_destructive()
    {
        var group = CreateGroup(enableControl: true);

        Assert.Multiple(() =>
        {
            foreach (var name in ReadToolNames)
            {
                var annotations = Tool(group, name).ProtocolTool.Annotations;
                Assert.That(annotations?.ReadOnlyHint, Is.True, $"{name} must be read-only.");
                Assert.That(annotations?.DestructiveHint, Is.False, $"{name} must not be destructive.");
            }

            foreach (var name in WriteToolNames)
            {
                var annotations = Tool(group, name).ProtocolTool.Annotations;
                Assert.That(annotations?.ReadOnlyHint, Is.False, $"{name} must not be read-only.");
                Assert.That(annotations?.DestructiveHint, Is.True, $"{name} must be destructive.");
            }
        });
    }

    [TestCase("lattice_tenant_group_list", "tenantId")]
    [TestCase("lattice_tenant_group_get", "tenantId,name")]
    [TestCase("lattice_tenant_group_members", "tenantId,groupName")]
    [TestCase("lattice_tenant_member_list", "tenantId")]
    [TestCase("lattice_tenant_group_upsert", "tenantId,name")]
    [TestCase("lattice_tenant_group_remove", "tenantId,name")]
    [TestCase("lattice_tenant_group_member_add", "tenantId,groupName,memberId")]
    [TestCase("lattice_tenant_group_member_remove", "tenantId,groupName,memberId")]
    [TestCase("lattice_tenant_member_add", "tenantId,subjectId")]
    [TestCase("lattice_tenant_member_remove", "tenantId,subjectId")]
    [TestCase("lattice_tenant_rule_list", "tenantId")]
    [TestCase("lattice_tenant_rule_get", "tenantId,ruleId")]
    [TestCase("lattice_tenant_rule_put", "tenantId,ruleId,subjectId,scopeKind,operations,effect")]
    [TestCase("lattice_tenant_rule_remove", "tenantId,ruleId")]
    [TestCase("lattice_tenant_explain", "tenantId,subjectId,treeName,operation")]
    [TestCase("lattice_tenant_effective_permissions", "tenantId,subjectId")]
    [TestCase("lattice_tenant_access_posture", "tenantId")]
    public void Schemas_require_only_genuinely_mandatory_parameters(string toolName, string expectedRequired)
    {
        var schema = Tool(CreateGroup(enableControl: true), toolName).ProtocolTool.InputSchema;

        var required = schema.TryGetProperty("required", out var node)
            ? node.EnumerateArray().Select(e => e.GetString()!).ToArray()
            : [];

        Assert.Multiple(() =>
        {
            Assert.That(required, Is.EquivalentTo(expectedRequired.Split(',')));
            Assert.That(schema.GetProperty("properties").TryGetProperty("directory", out _), Is.False,
                "The facade is bound from dependency injection, never from the tool arguments.");
            Assert.That(schema.GetProperty("properties").TryGetProperty("policy", out _), Is.False);
        });
    }

    [Test]
    public void Every_tool_description_is_ascii()
    {
        var group = CreateGroup(enableControl: true);

        Assert.Multiple(() =>
        {
            foreach (var tool in group.Tools)
            {
                var text = tool.ProtocolTool.Description + tool.ProtocolTool.InputSchema.GetRawText();
                Assert.That(text.All(c => c < 128), Is.True, $"{tool.ProtocolTool.Name} must be plain ASCII.");
            }
        });
    }

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        var options = Options.Create(new LatticeApiMcpOptions());
        using var provider = new ServiceCollection().BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantAccessToolGroup(null!, options), Throws.ArgumentNullException);
            Assert.That(() => new TenantAccessToolGroup(provider, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void AddTenantAdminTools_registers_the_module_once_and_contributes_nothing_without_control()
    {
        using var provider = new ServiceCollection()
            .AddSingleton<ILatticeTenantDirectoryAdmin>(new FakeTenantDirectoryAdmin())
            .AddSingleton<ILatticeTenantPolicyAdmin>(new FakeTenantPolicyAdmin())
            .AddTenantAdminTools()
            .AddTenantAdminTools()
            .BuildServiceProvider();

        var groups = provider.GetServices<ILatticeApiMcpToolGroup>().OfType<TenantAccessToolGroup>().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(groups, Has.Length.EqualTo(1));
            Assert.That(groups[0].Tools, Is.Empty);
        });
    }

    [Test]
    public void AddTenantAdminTools_with_control_and_registered_facades_contributes_every_tool()
    {
        using var provider = new ServiceCollection()
            .AddSingleton<ILatticeTenantDirectoryAdmin>(new FakeTenantDirectoryAdmin())
            .AddSingleton<ILatticeTenantPolicyAdmin>(new FakeTenantPolicyAdmin())
            .AddTenantAdminTools(enableControl: true)
            .BuildServiceProvider();

        var group = provider.GetServices<ILatticeApiMcpToolGroup>().OfType<TenantAccessToolGroup>().Single();

        Assert.That(ToolNames(group), Is.EquivalentTo(ReadToolNames.Concat(WriteToolNames)));
    }

    [Test]
    public void Facade_registration_is_detected_without_resolving_the_facade()
    {
        var resolved = 0;
        using var provider = new ServiceCollection()
            .AddSingleton<ILatticeTenantDirectoryAdmin>(_ =>
            {
                resolved++;
                return new FakeTenantDirectoryAdmin();
            })
            .BuildServiceProvider();

        var group = new TenantAccessToolGroup(
            provider, Options.Create(new LatticeApiMcpOptions { EnableTenantAdminControlTools = true }));

        Assert.Multiple(() =>
        {
            Assert.That(ToolNames(group), Is.EquivalentTo(TenantAccessToolGroup.DirectoryToolNames));
            Assert.That(resolved, Is.Zero,
                "Building the tool list must not construct the facade: a facade whose dependencies are "
                + "missing must not break the whole MCP session.");
        });
    }

    [Test]
    public async Task Capabilities_and_instructions_are_unchanged_when_the_module_contributes_nothing()
    {
        var options = Options.Create(new LatticeApiMcpOptions { EnableTenantAdminControlTools = true });
        var existing = new TenantAdminToolGroup(options);
        using var empty = new ServiceCollection().BuildServiceProvider();
        var module = new TenantAccessToolGroup(empty, options);

        var before = await PlanAsync(existing);
        var after = await PlanAsync(existing, module);

        Assert.Multiple(() =>
        {
            Assert.That(module.Tools, Is.Empty);
            Assert.That(Json(after.Capabilities), Is.EqualTo(Json(before.Capabilities)));
            Assert.That(after.Instructions, Is.EqualTo(before.Instructions));
            Assert.That(Names(after.Tools), Is.EquivalentTo(Names(before.Tools)));
        });
    }

    [Test]
    public async Task Capabilities_are_unchanged_and_the_new_tools_listed_when_the_module_contributes()
    {
        var options = Options.Create(new LatticeApiMcpOptions { EnableTenantAdminControlTools = true });
        var existing = new TenantAdminToolGroup(options);
        var module = CreateGroup(enableControl: true);

        var before = await PlanAsync(existing);
        var after = await PlanAsync(existing, module);

        Assert.Multiple(() =>
        {
            Assert.That(Json(after.Capabilities), Is.EqualTo(Json(before.Capabilities)),
                "The module advertises under the already-registered tenant-admin group: no capability changes.");
            Assert.That(after.Instructions, Is.EqualTo(before.Instructions));
            Assert.That(Names(after.Tools), Is.EquivalentTo(
                Names(before.Tools).Concat(ReadToolNames).Concat(WriteToolNames)));
        });
    }

    private static async Task<LatticeApiMcpSessionPlan> PlanAsync(params ILatticeApiMcpToolGroup[] groups)
    {
        var services = new ServiceCollection()
            .AddSingleton<ILatticeApiMcpAuthorizer>(new AllowAllMcpAuthorizer())
            .BuildServiceProvider();
        var configurator = new LatticeApiMcpSessionConfigurator(
            new FixedBridge(new LatticeCredential("operator")),
            new FixedResolver(TestAccessSets.Granting(LatticeApiMcpGroup.TenantAdmin)),
            groups,
            services,
            NullLogger<LatticeApiMcpSessionConfigurator>.Instance);

        var context = new DefaultHttpContext { RequestServices = new ServiceCollection().BuildServiceProvider() };
        return await configurator.BuildSessionPlanAsync(context, CancellationToken.None);
    }

    private static string Json(LatticeApiMcpCapabilities capabilities)
        => JsonSerializer.Serialize(capabilities, LatticeApiMcpToolSerialization.Options);

    private static string[] Names(McpServerPrimitiveCollection<McpServerTool> tools)
        => tools.Select(t => t.ProtocolTool.Name).ToArray();

    private sealed class FixedBridge(LatticeCredential? credential) : ILatticeApiMcpCredentialBridge
    {
        public LatticeCredential? Resolve(HttpContext context) => credential;
    }

    private sealed class FixedResolver(LatticeApiMcpAccessSet access) : ILatticeApiMcpPermissionResolver
    {
        public ValueTask<LatticeApiMcpAccessSet> ResolveAsync(LatticeCredential credential, CancellationToken cancellationToken)
            => new(access);
    }
}
