using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests.PeerStatus;

/// <summary>
/// Proves the peer-status service sits behind the binding's existing
/// authorization posture: the shared interceptor recognises its service name,
/// maps <c>GetPeerStatus</c> to <see cref="LatticeReplicationApiOperation.GetPeerStatus"/>
/// with the tree filter as its target, exempts nothing on it, and denies it under
/// the default-deny authorizer.
/// </summary>
[TestFixture]
public sealed class LatticeReplicationStatusGrpcInterceptorTests
{
    private static readonly string StatusMethod =
        "/" + LatticeReplicationStatusGrpcMethods.ServiceName + "/" + LatticeReplicationStatusGrpcMethods.GetPeerStatusMethodName;

    private static LatticeReplicationApiGrpcAuthInterceptor CreateInterceptor(
        ILatticeReplicationApiAuthorizer authorizer,
        bool requireAuthorization = true)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeReplicationApiGrpcOptions>>();
        options.CurrentValue.Returns(new LatticeReplicationApiGrpcOptions { RequireAuthorization = requireAuthorization });
        return new LatticeReplicationApiGrpcAuthInterceptor(
            authorizer, options, NullLogger<LatticeReplicationApiGrpcAuthInterceptor>.Instance);
    }

    [Test]
    public void DescribeCall_maps_get_peer_status_with_its_tree_filter_as_the_target()
    {
        var (operation, target) = LatticeReplicationApiGrpcAuthInterceptor.DescribeCall(
            StatusMethod, new ReplicationPeerStatusQuery { TreeId = "a/crm/contacts" });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeReplicationApiOperation.GetPeerStatus));
            Assert.That(target, Is.EqualTo("a/crm/contacts"));
        });
    }

    [Test]
    public void DescribeCall_maps_an_unfiltered_get_peer_status_to_a_null_target()
    {
        var (operation, target) = LatticeReplicationApiGrpcAuthInterceptor.DescribeCall(
            StatusMethod, new ReplicationPeerStatusQuery { TreeId = string.Empty });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeReplicationApiOperation.GetPeerStatus));
            Assert.That(target, Is.Null);
        });
    }

    [Test]
    public void DescribeCall_maps_an_unknown_status_method_to_Unknown()
    {
        var (operation, _) = LatticeReplicationApiGrpcAuthInterceptor.DescribeCall(
            "/" + LatticeReplicationStatusGrpcMethods.ServiceName + "/EnableReplication",
            ReplicationPeerStatusQuery.All);

        Assert.That(operation, Is.EqualTo(LatticeReplicationApiOperation.Unknown));
    }

    [Test]
    public void No_status_method_is_exempt_from_authorization()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationApiGrpcAuthInterceptor.IsUnauthenticatedMethod(StatusMethod), Is.False);
            Assert.That(
                LatticeReplicationApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                    "/" + LatticeReplicationStatusGrpcMethods.ServiceName + "/GetAuthScheme"),
                Is.False,
                "the discovery exemption belongs to the control service only");
        });
    }

    [Test]
    public void Default_deny_rejects_get_peer_status()
    {
        var interceptor = CreateInterceptor(new DenyAllReplicationApiAuthorizer());
        var continuationRan = false;

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            ReplicationPeerStatusQuery.All,
            new FakeServerCallContext(StatusMethod),
            (_, _) =>
            {
                continuationRan = true;
                return Task.FromResult(ReplicationPeerStatusPage.Empty("west"));
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(continuationRan, Is.False);
        });
    }

    [Test]
    public async Task An_authorizer_sees_the_get_peer_status_operation_and_target()
    {
        var authorizer = Substitute.For<ILatticeReplicationApiAuthorizer>();
        LatticeReplicationApiAuthorizationContext? seen = null;
        authorizer.IsAuthorizedAsync(Arg.Do<LatticeReplicationApiAuthorizationContext>(c => seen = c), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(true));
        var interceptor = CreateInterceptor(authorizer);

        var page = await interceptor.UnaryServerHandler(
            new ReplicationPeerStatusQuery { TreeId = "orders" },
            new FakeServerCallContext(StatusMethod),
            (_, _) => Task.FromResult(ReplicationPeerStatusPage.Empty("west")));

        Assert.Multiple(() =>
        {
            Assert.That(page.LocalRegionId, Is.EqualTo("west"));
            Assert.That(seen?.Operation, Is.EqualTo(LatticeReplicationApiOperation.GetPeerStatus));
            Assert.That(seen?.TargetId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public async Task Enforcement_on_the_status_service_is_skipped_when_RequireAuthorization_is_false()
    {
        var authorizer = Substitute.For<ILatticeReplicationApiAuthorizer>();
        var interceptor = CreateInterceptor(authorizer, requireAuthorization: false);

        var page = await interceptor.UnaryServerHandler(
            ReplicationPeerStatusQuery.All,
            new FakeServerCallContext(StatusMethod),
            (_, _) => Task.FromResult(ReplicationPeerStatusPage.Empty("west")));

        Assert.That(page, Is.Not.Null);
        await authorizer.DidNotReceiveWithAnyArgs().IsAuthorizedAsync(default, default);
    }

    [Test]
    public void The_new_operation_is_appended_so_existing_values_keep_their_numbers()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)LatticeReplicationApiOperation.EnableReplication, Is.Zero);
            Assert.That((int)LatticeReplicationApiOperation.DisableReplication, Is.EqualTo(1));
            Assert.That((int)LatticeReplicationApiOperation.GetReplicationConfig, Is.EqualTo(2));
            Assert.That((int)LatticeReplicationApiOperation.Unknown, Is.EqualTo(3));
            Assert.That((int)LatticeReplicationApiOperation.GetPeerStatus, Is.EqualTo(4));
            Assert.That((int)LatticeReplicationApiOperation.DecommissionPeer, Is.EqualTo(5));
        });
    }
}
