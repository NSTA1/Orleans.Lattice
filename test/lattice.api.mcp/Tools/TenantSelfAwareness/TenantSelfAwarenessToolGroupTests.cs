using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TenantSelfAwarenessToolGroup"/> and its
/// <c>AddTenantSelfAwarenessTools</c> registration. They prove the tenancy-gated
/// invariant that guards every non-tenancy deployment's MCP surface: when the
/// tenancy-gated <see cref="ILatticeTenantSelfService"/> facade is absent the group
/// contributes <b>no</b> tools, and when it is present the group contributes
/// exactly the three read-only tenant self-awareness tools, all annotated read-only
/// and non-destructive, advertised under the read-only
/// <see cref="LatticeApiMcpGroup.State"/> group so the fixed capability surface is
/// unchanged when tenancy is off. All deterministic - no cluster, no ordering
/// assumptions.
/// </summary>
[TestFixture]
public sealed class TenantSelfAwarenessToolGroupTests
{
    private static readonly string[] SelfAwarenessToolNames =
    {
        "lattice_tenant_current",
        "lattice_tenant_list",
        "lattice_tenant_get",
    };

    private static TenantSelfAwarenessToolGroup GroupWithFacade()
        => new([Substitute.For<ILatticeTenantSelfService>()]);

    private static TenantSelfAwarenessToolGroup GroupWithoutFacade()
        => new(Array.Empty<ILatticeTenantSelfService>());

    private static HashSet<string> ToolNames(TenantSelfAwarenessToolGroup group)
        => group.Tools.Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);

    private static McpServerTool Tool(TenantSelfAwarenessToolGroup group, string name)
        => group.Tools.Single(t => t.ProtocolTool.Name == name);

    [Test]
    public void Group_advertises_under_the_read_only_state_group()
    {
        Assert.That(GroupWithFacade().Group, Is.EqualTo(LatticeApiMcpGroup.State));
    }

    [Test]
    public void Tenancy_disabled_offers_no_tools()
    {
        var group = GroupWithoutFacade();

        Assert.That(group.Tools, Is.Empty,
            "With tenancy disabled the facade is absent, so the group must contribute no tools "
            + "and a non-tenancy deployment's MCP surface stays byte-for-byte unchanged.");
    }

    [Test]
    public void Tenancy_enabled_offers_exactly_the_three_self_awareness_tools()
    {
        var group = GroupWithFacade();

        Assert.That(ToolNames(group), Is.EquivalentTo(SelfAwarenessToolNames));
    }

    [Test]
    public void Self_awareness_tools_are_annotated_read_only_and_not_destructive()
    {
        var group = GroupWithFacade();

        Assert.Multiple(() =>
        {
            foreach (var name in SelfAwarenessToolNames)
            {
                var annotations = Tool(group, name).ProtocolTool.Annotations;
                Assert.That(annotations?.ReadOnlyHint, Is.True, $"{name} must be read-only.");
                Assert.That(annotations?.DestructiveHint, Is.False, $"{name} must not be destructive.");
            }
        });
    }

    [Test]
    public void Constructor_rejects_null_facades()
    {
        Assert.That(() => new TenantSelfAwarenessToolGroup(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddTenantSelfAwarenessTools_rejects_null_services()
    {
        Assert.That(
            () => LatticeMcpTenantSelfAwarenessServiceCollectionExtensions.AddTenantSelfAwarenessTools(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddTenantSelfAwarenessTools_without_facade_registers_a_group_that_offers_no_tools()
    {
        var provider = new ServiceCollection().AddTenantSelfAwarenessTools().BuildServiceProvider();

        var group = (TenantSelfAwarenessToolGroup)provider.GetRequiredService<ILatticeApiMcpToolGroup>();

        Assert.That(group.Tools, Is.Empty,
            "Without the tenancy-gated facade registered, the group contributes no tools.");
    }

    [Test]
    public void AddTenantSelfAwarenessTools_with_facade_registers_a_group_with_all_three_tools()
    {
        var provider = new ServiceCollection()
            .AddSingleton(Substitute.For<ILatticeTenantSelfService>())
            .AddTenantSelfAwarenessTools()
            .BuildServiceProvider();

        var group = (TenantSelfAwarenessToolGroup)provider.GetRequiredService<ILatticeApiMcpToolGroup>();

        Assert.That(ToolNames(group), Is.EquivalentTo(SelfAwarenessToolNames));
    }

    [Test]
    public void AddTenantSelfAwarenessTools_called_twice_registers_exactly_one_group()
    {
        var provider = new ServiceCollection()
            .AddTenantSelfAwarenessTools()
            .AddTenantSelfAwarenessTools()
            .BuildServiceProvider();

        var groups = provider.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<TenantSelfAwarenessToolGroup>()
            .ToArray();

        Assert.That(groups, Has.Length.EqualTo(1));
    }
}
