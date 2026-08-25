using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Sanity check for the <c>Orleans.Lattice.Api.TenantAdmin.Grpc</c> package: the
/// assembly loads, the binding exposes the reserved service name, and it owns its
/// own serialization-alias registry (<c>oitng.</c>) that is distinct from the
/// parent control-API facade's registry (<c>oitn.</c>), so the wire messages the
/// binding adds never collide with the transport-agnostic facade DTOs.
/// </summary>
[TestFixture]
public sealed class ApiTenantAdminGrpcScaffoldingTests
{
    [Test]
    public void Grpc_binding_owns_a_distinct_alias_namespace_from_the_control_api()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrpcTenantAdminTypeAliases.AliasPrefix, Is.EqualTo("oitng."));
            Assert.That(ApiTenantAdminTypeAliases.AliasPrefix, Is.EqualTo("oitn."));
            Assert.That(GrpcTenantAdminTypeAliases.AliasPrefix, Is.Not.EqualTo(ApiTenantAdminTypeAliases.AliasPrefix));
        });
    }

    [Test]
    public void Service_name_is_the_reserved_tenant_admin_control_api_name()
    {
        Assert.That(LatticeTenantAdminGrpcMethods.ServiceName, Is.EqualTo("orleans.lattice.api.tenantadmin"));
    }
}
