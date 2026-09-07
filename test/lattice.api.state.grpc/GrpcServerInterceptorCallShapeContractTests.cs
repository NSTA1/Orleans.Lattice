using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Guards the <c>Orleans.Lattice.Api.State.Grpc</c> package against a server-side gRPC interceptor that
/// gates one call shape but leaves another on the pass-through base, which would
/// admit calls of that shape with no authorization decision at all. See
/// <see cref="GrpcServerInterceptorCallShapeContractTestsBase"/> for the rationale.
/// </summary>
[TestFixture]
public sealed class GrpcServerInterceptorCallShapeContractTests
    : GrpcServerInterceptorCallShapeContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeStateApiGrpcAuthInterceptor).Assembly;
}