using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>Binds the transient cluster-fault mapping guard (#5742) to this package's gRPC facade services.</summary>
[TestFixture]
public sealed class GrpcTransientFaultMappingContractTests : GrpcTransientFaultMappingContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeTreeAdminApiGrpcClient).Assembly;
}