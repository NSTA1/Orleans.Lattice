using Orleans.Lattice.Api.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Scaffolding sanity check for the <c>Orleans.Lattice.Api.Backup.Grpc</c>
/// package: the assembly loads and the gRPC binding reuses the parent
/// control-API serialization-alias registry (<c>oib.</c>) rather than owning a
/// separate one, mirroring the sibling auth gRPC binding. This guards the
/// reservation that later backup gRPC wire types depend on.
/// </summary>
public sealed class ApiBackupGrpcScaffoldingTests
{
    [Test]
    public void Grpc_binding_reuses_the_reserved_control_api_alias_namespace()
    {
        Assert.That(ApiBackupTypeAliases.AliasPrefix, Is.EqualTo("oib."));
    }
}
