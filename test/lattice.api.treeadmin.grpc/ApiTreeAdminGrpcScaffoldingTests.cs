using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Sanity check for the <c>Orleans.Lattice.Api.TreeAdmin.Grpc</c> package: the
/// assembly loads, the binding exposes the reserved service name, and it owns its
/// own serialization-alias registry (<c>oitg.</c>) that is distinct from the parent
/// control-API facade's registry (<c>oit.</c>), so the wire messages the binding
/// adds never collide with the transport-agnostic facade DTOs.
/// </summary>
[TestFixture]
public sealed class ApiTreeAdminGrpcScaffoldingTests
{
    [Test]
    public void Grpc_binding_owns_a_distinct_alias_namespace_from_the_control_api()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrpcTreeAdminTypeAliases.AliasPrefix, Is.EqualTo("oitg."));
            Assert.That(ApiTreeAdminTypeAliases.AliasPrefix, Is.EqualTo("oit."));
            Assert.That(GrpcTreeAdminTypeAliases.AliasPrefix, Is.Not.EqualTo(ApiTreeAdminTypeAliases.AliasPrefix));
        });
    }

    [Test]
    public void Service_name_is_the_reserved_tree_admin_control_api_name()
    {
        Assert.That(LatticeTreeAdminGrpcMethods.ServiceName, Is.EqualTo("orleans.lattice.api.treeadmin"));
    }
}
