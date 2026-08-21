using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Unit test for the cancellation path of
/// <see cref="LatticeReplicationApiGrpcAuthInterceptor"/>: when the authorizer
/// observes cancellation during the auth check, the interceptor surfaces a
/// <see cref="StatusCode.Cancelled"/> gRPC fault rather than leaking the raw
/// <see cref="OperationCanceledException"/>. Driven directly, without a gRPC host.
/// </summary>
public sealed class LatticeReplicationApiGrpcAuthInterceptorCancellationTests
{
    private const string ServicePrefix = "/orleans.lattice.api.replication/";

    [Test]
    public void UnaryServerHandler_maps_authorizer_cancellation_to_cancelled()
    {
        var authorizer = Substitute.For<ILatticeReplicationApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeReplicationApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns<Task<bool>>(_ => throw new OperationCanceledException());

        var options = Substitute.For<IOptionsMonitor<LatticeReplicationApiGrpcOptions>>();
        options.CurrentValue.Returns(new LatticeReplicationApiGrpcOptions { RequireAuthorization = true });
        var interceptor = new LatticeReplicationApiGrpcAuthInterceptor(
            authorizer,
            options,
            NullLogger<LatticeReplicationApiGrpcAuthInterceptor>.Instance);

        var continuationRan = false;
        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new ReplicationEnableRequestMessage { TreeId = "orders", Mode = LatticeMergeMode.RwFlag },
            new FakeServerCallContext(ServicePrefix + "EnableReplication"),
            (_, _) =>
            {
                continuationRan = true;
                return Task.FromResult(new ReplicationEnableResponse { TreeId = "orders" });
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
            Assert.That(continuationRan, Is.False);
        });
    }
}
