using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// Sanity check for the <c>Orleans.Lattice.Api.Schema.Grpc</c> package: the
/// assembly loads and the gRPC binding owns its own serialization-alias registry
/// (<c>oisg.</c>) that is distinct from the parent control-API facade's registry
/// (<c>ois.</c>), so the wire messages the binding adds never collide with the
/// transport-agnostic facade DTOs.
/// </summary>
[TestFixture]
public sealed class ApiSchemaGrpcScaffoldingTests
{
    [Test]
    public void Grpc_binding_owns_a_distinct_alias_namespace_from_the_control_api()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrpcSchemaTypeAliases.AliasPrefix, Is.EqualTo("oisg."));
            Assert.That(ApiSchemaTypeAliases.AliasPrefix, Is.EqualTo("ois."));
            Assert.That(GrpcSchemaTypeAliases.AliasPrefix, Is.Not.EqualTo(ApiSchemaTypeAliases.AliasPrefix));
        });
    }

    [Test]
    public void Service_name_is_the_reserved_schema_control_api_name()
    {
        Assert.That(LatticeSchemaGrpcMethods.ServiceName, Is.EqualTo("orleans.lattice.api.schema"));
    }
}
