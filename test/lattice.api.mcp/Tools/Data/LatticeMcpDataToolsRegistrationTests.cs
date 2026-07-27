using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Registration tests for
/// <see cref="LatticeMcpDataToolsServiceCollectionExtensions.AddDataTools"/>.
/// Proves the data module is registered as an <see cref="ILatticeApiMcpToolGroup"/>
/// serving the data group, that the write opt-in flows to the contributed tool
/// set (read-only by default, read plus write when opted in), that the group is a
/// singleton, and that a null service collection is rejected.
/// </summary>
[TestFixture]
public sealed class LatticeMcpDataToolsRegistrationTests
{
    private static DataToolGroup ResolveGroup(IServiceProvider provider)
        => provider.GetServices<ILatticeApiMcpToolGroup>().OfType<DataToolGroup>().Single();

    [Test]
    public void AddDataTools_registers_a_data_tool_group()
    {
        var services = new ServiceCollection();
        services.AddDataTools();

        using var provider = services.BuildServiceProvider();
        Assert.That(ResolveGroup(provider).Group, Is.EqualTo(LatticeApiMcpGroup.Data));
    }

    [Test]
    public void AddDataTools_defaults_to_read_only_tools()
    {
        var services = new ServiceCollection();
        services.AddDataTools();

        using var provider = services.BuildServiceProvider();
        var group = ResolveGroup(provider);

        Assert.That(
            group.Tools.Select(t => t.ProtocolTool.Name),
            Is.EquivalentTo(new[]
            {
                "lattice_data_get", "lattice_data_read_range",
                "lattice_data_pncounter_get", "lattice_data_orset_get", "lattice_data_orflag_get",
                "lattice_data_rwflag_get", "lattice_data_version_vector_get", "lattice_data_mvregister_get",
                "lattice_data_sequence_get", "lattice_data_ormap_get",
            }));
    }

    [Test]
    public void AddDataTools_with_writes_enabled_registers_the_write_tools()
    {
        var services = new ServiceCollection();
        services.AddDataTools(enableWrites: true);

        using var provider = services.BuildServiceProvider();
        var group = ResolveGroup(provider);

        Assert.That(
            group.Tools.Select(t => t.ProtocolTool.Name),
            Is.EquivalentTo(new[]
            {
                "lattice_data_get", "lattice_data_read_range", "lattice_data_set", "lattice_data_delete",
                "lattice_data_set_many", "lattice_data_set_many_atomic", "lattice_data_set_many_atomic_cross_tree",
                "lattice_data_pncounter_get", "lattice_data_orset_get", "lattice_data_orflag_get",
                "lattice_data_rwflag_get", "lattice_data_version_vector_get", "lattice_data_mvregister_get",
                "lattice_data_sequence_get", "lattice_data_ormap_get",
                "lattice_data_pncounter", "lattice_data_orset", "lattice_data_orflag", "lattice_data_rwflag",
                "lattice_data_version_vector_tick", "lattice_data_mvregister_set", "lattice_data_sequence",
                "lattice_data_ormap",
            }));
    }

    [Test]
    public void AddDataTools_registers_the_group_as_a_singleton()
    {
        var services = new ServiceCollection();
        services.AddDataTools();

        using var provider = services.BuildServiceProvider();
        var first = ResolveGroup(provider);
        var second = ResolveGroup(provider);

        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddDataTools_returns_the_service_collection_for_chaining()
    {
        var services = new ServiceCollection();

        Assert.That(services.AddDataTools(), Is.SameAs(services));
    }

    [Test]
    public void AddDataTools_rejects_a_null_service_collection()
    {
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddDataTools());
    }
}
