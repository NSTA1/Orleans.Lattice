using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="AuthToolGroup"/>, the auth-admin tool module. Proves
/// the group serves <see cref="LatticeApiMcpGroup.Auth"/>, exposes only the
/// read-only introspection tools until administration is opted in, then adds the
/// mutating administration verbs; that read tools carry the read-only annotation
/// and write tools the destructive annotation; and that the injected
/// <see cref="ILatticeAuthAdmin"/> facade parameter is bound from services rather
/// than exposed in a tool's input schema. Deterministic - no cluster, no
/// invocation.
/// </summary>
[TestFixture]
public sealed class AuthToolGroupTests
{
    private static readonly string[] IntrospectionToolNames =
    {
        "auth_explain",
        "auth_effective_permissions",
        "auth_get_user",
        "auth_list_users",
        "auth_get_group",
        "auth_list_groups",
        "auth_list_group_members",
        "auth_list_subject_groups",
        "auth_get_rule",
        "auth_list_rules",
        "auth_list_rules_for_tree",
    };

    private static readonly string[] AdministrationToolNames =
    {
        "auth_upsert_user",
        "auth_remove_user",
        "auth_upsert_group",
        "auth_remove_group",
        "auth_add_member",
        "auth_remove_member",
        "auth_put_rule",
        "auth_remove_rule",
    };

    private static AuthToolGroup CreateGroup(bool enableAdministration)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<ILatticeAuthAdmin>());
        var provider = services.BuildServiceProvider();
        var options = Options.Create(new LatticeApiMcpOptions { EnableAuthAdministration = enableAdministration });
        return new AuthToolGroup(provider, options);
    }

    private static IReadOnlyList<string> Names(AuthToolGroup group)
        => group.Tools.Select(t => t.ProtocolTool.Name).ToList();

    [Test]
    public void Group_serves_the_auth_facade_group()
    {
        Assert.That(CreateGroup(enableAdministration: false).Group, Is.EqualTo(LatticeApiMcpGroup.Auth));
    }

    [Test]
    public void Introspection_only_when_administration_is_disabled()
    {
        var group = CreateGroup(enableAdministration: false);

        Assert.Multiple(() =>
        {
            Assert.That(Names(group), Is.EquivalentTo(IntrospectionToolNames));
            foreach (var admin in AdministrationToolNames)
            {
                Assert.That(Names(group), Does.Not.Contain(admin),
                    "The mutating administration verbs must be hidden until administration is opted in.");
            }
        });
    }

    [Test]
    public void Administration_verbs_appear_when_enabled()
    {
        var group = CreateGroup(enableAdministration: true);

        Assert.That(Names(group), Is.EquivalentTo(IntrospectionToolNames.Concat(AdministrationToolNames)));
    }

    [Test]
    public void Introspection_tools_are_annotated_read_only()
    {
        var group = CreateGroup(enableAdministration: true);

        Assert.Multiple(() =>
        {
            foreach (var name in IntrospectionToolNames)
            {
                var annotations = ServerTool(group, name).ProtocolTool.Annotations;
                Assert.That(annotations?.ReadOnlyHint, Is.True, $"{name} must be read-only.");
                Assert.That(annotations?.DestructiveHint, Is.False, $"{name} must not be destructive.");
            }
        });
    }

    [Test]
    public void Administration_tools_are_annotated_destructive()
    {
        var group = CreateGroup(enableAdministration: true);

        Assert.Multiple(() =>
        {
            foreach (var name in AdministrationToolNames)
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
        var group = CreateGroup(enableAdministration: true);

        Assert.Multiple(() =>
        {
            foreach (var tool in group.Tools)
            {
                Assert.That(SchemaHasProperty(tool, "admin"), Is.False,
                    $"{tool.ProtocolTool.Name} must resolve its facade from services, not expose it as an argument.");
                Assert.That(SchemaHasProperty(tool, "cancellationToken"), Is.False,
                    $"{tool.ProtocolTool.Name} must not expose the cancellation token as an argument.");
            }
        });
    }

    [Test]
    public void Business_arguments_stay_in_the_input_schema()
    {
        var group = CreateGroup(enableAdministration: true);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaHasProperty(ServerTool(group, "auth_explain"), "subjectId"), Is.True);
            Assert.That(SchemaHasProperty(ServerTool(group, "auth_upsert_user"), "userId"), Is.True);
            Assert.That(SchemaHasProperty(ServerTool(group, "auth_put_rule"), "ruleId"), Is.True);
        });
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        var options = Options.Create(new LatticeApiMcpOptions());

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new AuthToolGroup(null!, options));
            Assert.Throws<ArgumentNullException>(() => new AuthToolGroup(provider, null!));
        });
    }

    private static McpServerTool ServerTool(AuthToolGroup group, string name)
        => group.Tools.Single(t => t.ProtocolTool.Name == name);

    private static bool SchemaHasProperty(McpServerTool tool, string propertyName)
    {
        var schema = tool.ProtocolTool.InputSchema;
        return schema.ValueKind == JsonValueKind.Object
            && schema.TryGetProperty("properties", out var properties)
            && properties.TryGetProperty(propertyName, out _);
    }
}
