using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests;

[TestFixture]
public sealed class GrpcActiveTenantStampContractTests : GrpcActiveTenantStampContractTestsBase
{
    protected override Assembly PackageAssembly => typeof(LatticeAppsApiGrpcClient).Assembly;
}
