using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>Binds the storage back-pressure mapping guard (#5723) to this package's gRPC facade services.</summary>
[TestFixture]
public sealed class GrpcSaturationMappingContractTests : GrpcSaturationMappingContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeBackupApiGrpcClient).Assembly;
}