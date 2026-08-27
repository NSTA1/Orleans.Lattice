using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Binds the shared gRPC active-tenant stamping guard to this package, so a
/// facade service that stops lifting the caller's asserted tenant fails the
/// build rather than silently serving every tenant the shared cluster-global
/// namespace.
/// </summary>
/// <remarks>
/// This binding is the one the original sweep missed, and the omission was
/// observable end to end: the self-service surface reports the tenant the caller
/// is acting as, so without the stamp it answered with the reserved default
/// tenant for every caller no matter which tenant they asserted.
/// </remarks>
[TestFixture]
public sealed class GrpcActiveTenantStampContractTests : GrpcActiveTenantStampContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeTenantAdminGrpcService).Assembly;
}
