using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.Tests;

[TestFixture]
public sealed class SchemaToolSupportTests
{
    [TestCase(false, LatticeOperation.Read)]
    [TestCase(true, LatticeOperation.Read)]
    [TestCase(false, LatticeOperation.SchemaAdmin)]
    [TestCase(true, LatticeOperation.SchemaAdmin)]
    public async Task Schema_discovery_reports_installed_features_not_permission_flags(bool versioning, LatticeOperation operations)
    {
        var schema = Substitute.For<ILatticeSchemaControl>();
        schema.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new LatticeSchemaCapabilities
        {
            TreeId = "__schema_capability_probe__",
            CanViewPolicy = true, CanViewVersionConfig = true, CanManagePolicy = true, CanManageVersion = true,
            SupportsEnforcement = true, SupportsCompliance = true, SupportsDeadLetters = true, SupportsRemediation = true,
            SupportsVersioning = versioning, SupportsRuntimeRegistration = versioning,
        });
        using var services = new ServiceCollection().AddSingleton(schema)
            .AddSingleton<ILatticeApiMcpAuthorizer, AllowAllMcpAuthorizer>().BuildServiceProvider();
        var groups = new[] { new TreeAdminToolGroup(services,
            Options.Create(new LatticeApiMcpOptions { EnableTreeAdminSchemaControlTools = true })) };
        var bridge = Substitute.For<ILatticeApiMcpCredentialBridge>();
        bridge.Resolve(Arg.Any<HttpContext>()).Returns(new LatticeCredential("schema-admin"));
        var resolver = new SchemaPermissionResolver(operations);
        var configurator = new LatticeApiMcpSessionConfigurator(
            bridge, resolver,
            groups, services, NullLogger<LatticeApiMcpSessionConfigurator>.Instance);
        var context = new DefaultHttpContext { RequestServices = services };
        var plan = await configurator.BuildSessionPlanAsync(context, CancellationToken.None);
        var names = plan.Tools.Select(t => t.ProtocolTool.Name).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(names.Contains("lattice_treeadmin_schema_get_policy"), Is.EqualTo(operations == LatticeOperation.Read));
            Assert.That(names.Contains("lattice_treeadmin_schema_set_policy"), Is.EqualTo(operations == LatticeOperation.SchemaAdmin));
            Assert.That(names.Contains("lattice_treeadmin_schema_get_version_config"), Is.EqualTo(versioning && operations == LatticeOperation.Read));
            Assert.That(names.Contains("lattice_treeadmin_schema_register"), Is.EqualTo(versioning && operations == LatticeOperation.SchemaAdmin));
            Assert.That(names.Contains("lattice_treeadmin_schema_migration_start"), Is.EqualTo(versioning && operations == LatticeOperation.SchemaAdmin));
            Assert.That(names, Does.Not.Contain("lattice_treeadmin_tree_delete"));
        });
        await schema.Received(1).ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await schema.DidNotReceive().GetVersionConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await schema.DidNotReceive().GetPolicyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Registry_handlers_forward_without_requiring_a_read_after_a_management_write()
    {
        var schema = Substitute.For<ILatticeSchemaControl>();
        var catalog = new LatticeSchemaRuntimeRegistration { Descriptors = [new(8, 1, "v1")] };
        schema.GetRegistryAsync("tree", Arg.Any<CancellationToken>()).Returns(catalog);
        Assert.That(await TreeAdminSchemaToolHandlers.GetRegistryAsync(schema, "tree"), Is.SameAs(catalog));
        Assert.That(await TreeAdminSchemaToolHandlers.RegisterSchemaAsync(schema, "tree", catalog), Is.SameAs(catalog));
        await schema.Received(1).RegisterSchemaAsync("tree", catalog, Arg.Any<CancellationToken>());
        await schema.Received(1).GetRegistryAsync("tree", Arg.Any<CancellationToken>());
    }

    [Test]
    public void Unsupported_and_unknown_schema_tools_are_default_deny()
    {
        var caps = new LatticeSchemaCapabilities { TreeId = "tree" };
        Assert.Multiple(() =>
        {
            Assert.That(SchemaToolSupport.IsSupported("lattice_treeadmin_schema_get_policy", caps), Is.False);
            Assert.That(SchemaToolSupport.IsSupported("lattice_treeadmin_schema_future_method", caps), Is.False);
            Assert.That(SchemaToolSupport.IsSupported("lattice_treeadmin_schema_probe_capabilities", caps), Is.True);
        });
    }

    private sealed class SchemaPermissionResolver(LatticeOperation operations) : ILatticeApiMcpPermissionResolver
    {
        public ValueTask<LatticeApiMcpAccessSet> ResolveAsync(LatticeCredential credential, CancellationToken cancellationToken) =>
            new(LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.TreeAdmin).WithOperations(operations));
    }
}
