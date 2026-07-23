using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Sanity check for the <c>Orleans.Lattice.Api.Replication.Grpc</c> package: the
/// assembly loads and the gRPC binding owns its own serialization-alias registry
/// (<c>oirg.</c>) that is distinct from the parent control-API facade's registry
/// (<c>oir.</c>), so the wire messages the binding adds never collide with the
/// transport-agnostic facade DTOs.
/// </summary>
public sealed class ApiReplicationGrpcScaffoldingTests
{
    [Test]
    public void Grpc_binding_owns_a_distinct_alias_namespace_from_the_control_api()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrpcReplicationTypeAliases.AliasPrefix, Is.EqualTo("oirg."));
            Assert.That(ApiReplicationTypeAliases.AliasPrefix, Is.EqualTo("oir."));
            Assert.That(GrpcReplicationTypeAliases.AliasPrefix, Is.Not.EqualTo(ApiReplicationTypeAliases.AliasPrefix));
        });
    }
}
