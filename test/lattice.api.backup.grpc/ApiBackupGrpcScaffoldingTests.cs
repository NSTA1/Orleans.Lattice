using Orleans.Lattice.Api.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Sanity check for the <c>Orleans.Lattice.Api.Backup.Grpc</c> package: the
/// assembly loads and the gRPC binding owns its own serialization-alias registry
/// (<c>oibg.</c>) that is distinct from the parent control-API facade's registry
/// (<c>oib.</c>), so the wire messages the binding adds never collide with the
/// transport-agnostic facade DTOs.
/// </summary>
public sealed class ApiBackupGrpcScaffoldingTests
{
    [Test]
    public void Grpc_binding_owns_a_distinct_alias_namespace_from_the_control_api()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrpcBackupTypeAliases.AliasPrefix, Is.EqualTo("oibg."));
            Assert.That(ApiBackupTypeAliases.AliasPrefix, Is.EqualTo("oib."));
            Assert.That(GrpcBackupTypeAliases.AliasPrefix, Is.Not.EqualTo(ApiBackupTypeAliases.AliasPrefix));
        });
    }
}
