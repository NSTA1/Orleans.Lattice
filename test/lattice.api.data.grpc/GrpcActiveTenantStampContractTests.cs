using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Binds the shared gRPC active-tenant stamping guard to this package, so a
/// facade service that stops lifting the caller's asserted tenant fails the
/// build rather than silently serving every tenant the shared cluster-global
/// namespace.
/// </summary>
[TestFixture]
public sealed class GrpcActiveTenantStampContractTests : GrpcActiveTenantStampContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeDataApiGrpcService).Assembly;
}