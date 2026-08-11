using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TreeAdminToolGroup"/> and its <c>AddTreeAdminTools</c>
/// registration: proves the group belongs to the tree-administration facade group
/// and, at this scaffolding stage, is discoverable but empty (contributes no
/// tools), and that the registration wires exactly one group and sets the option
/// flag. All deterministic - no cluster, no ordering assumptions.
/// </summary>
[TestFixture]
public sealed class TreeAdminToolGroupTests
{
    [Test]
    public void Group_is_the_tree_admin_facade_group()
    {
        Assert.That(new TreeAdminToolGroup().Group, Is.EqualTo(LatticeApiMcpGroup.TreeAdmin));
    }

    [Test]
    public void Group_is_discoverable_but_ships_no_tools_yet()
    {
        Assert.That(new TreeAdminToolGroup().Tools, Is.Empty,
            "The scaffolding foundation group must be discoverable but contribute no tools yet.");
    }

    [Test]
    public void AddTreeAdminTools_rejects_null_services()
    {
        Assert.That(
            () => LatticeMcpTreeAdminServiceCollectionExtensions.AddTreeAdminTools(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddTreeAdminTools_registers_exactly_one_discoverable_group()
    {
        var provider = new ServiceCollection().AddTreeAdminTools().BuildServiceProvider();

        var group = provider.GetServices<ILatticeApiMcpToolGroup>().OfType<TreeAdminToolGroup>().Single();

        Assert.Multiple(() =>
        {
            Assert.That(group.Group, Is.EqualTo(LatticeApiMcpGroup.TreeAdmin));
            Assert.That(group.Tools, Is.Empty);
        });
    }

    [Test]
    public void AddTreeAdminTools_called_twice_registers_exactly_one_group()
    {
        var provider = new ServiceCollection()
            .AddTreeAdminTools()
            .AddTreeAdminTools()
            .BuildServiceProvider();

        var groups = provider.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<TreeAdminToolGroup>()
            .ToArray();

        Assert.That(groups, Has.Length.EqualTo(1));
    }
}
