using Orleans.Lattice.Api.Backup.Grpc;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Scaffolding sanity check for the <c>Orleans.Lattice.Api.Backup.Grpc</c>
/// package: the assembly loads and the reserved gRPC serialization-alias prefix
/// is stable, guarding the reservation later backup gRPC releases depend on.
/// </summary>
public sealed class ApiBackupGrpcScaffoldingTests
{
    [Test]
    public void Alias_prefix_is_the_reserved_backup_grpc_namespace()
    {
        Assert.That(ApiBackupGrpcTypeAliases.AliasPrefix, Is.EqualTo("obg."));
    }
}
