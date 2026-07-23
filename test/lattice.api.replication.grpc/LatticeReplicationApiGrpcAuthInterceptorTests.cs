using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeReplicationApiGrpcAuthInterceptor"/> and its
/// static call-decoding helpers. Covers the operation / target mapping
/// (<c>DescribeCall</c>), the unauthenticated <c>GetAuthScheme</c> exemption, and
/// the default-deny enforcement path (a denied call fails with
/// <see cref="StatusCode.PermissionDenied"/>) - all without standing up a gRPC
/// host.
/// </summary>
public sealed class LatticeReplicationApiGrpcAuthInterceptorTests
{
    private const string ServicePrefix = "/orleans.lattice.api.replication/";

    private static string FullMethod(string methodName) => ServicePrefix + methodName;

    private static LatticeReplicationApiGrpcAuthInterceptor CreateInterceptor(
        ILatticeReplicationApiAuthorizer authorizer,
        bool requireAuthorization = true)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeReplicationApiGrpcOptions>>();
        options.CurrentValue.Returns(new LatticeReplicationApiGrpcOptions { RequireAuthorization = requireAuthorization });
        return new LatticeReplicationApiGrpcAuthInterceptor(
            authorizer,
            options,
            NullLogger<LatticeReplicationApiGrpcAuthInterceptor>.Instance);
    }

    [Test]
    public void DescribeCall_maps_enable_to_operation_and_target()
    {
        var (operation, target) = LatticeReplicationApiGrpcAuthInterceptor.DescribeCall(
            FullMethod("EnableReplication"),
            new ReplicationEnableRequestMessage { TreeId = "orders", Mode = LatticeMergeMode.RwFlag });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeReplicationApiOperation.EnableReplication));
            Assert.That(target, Is.EqualTo("orders"));
        });
    }

    [Test]
    public void DescribeCall_maps_disable_to_operation_and_target()
    {
        var (operation, target) = LatticeReplicationApiGrpcAuthInterceptor.DescribeCall(
            FullMethod("DisableReplication"),
            new ReplicationDisableRequestMessage { TreeId = "customers" });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeReplicationApiOperation.DisableReplication));
            Assert.That(target, Is.EqualTo("customers"));
        });
    }

    [Test]
    public void DescribeCall_maps_get_config_to_operation_with_null_target()
    {
        var (operation, target) = LatticeReplicationApiGrpcAuthInterceptor.DescribeCall(
            FullMethod("GetReplicationConfig"),
            new ReplicationGetConfigRequest());

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeReplicationApiOperation.GetReplicationConfig));
            Assert.That(target, Is.Null);
        });
    }

    [Test]
    public void DescribeCall_maps_unknown_method_to_Unknown()
    {
        var (operation, target) = LatticeReplicationApiGrpcAuthInterceptor.DescribeCall(
            FullMethod("SomeFutureRpc"),
            new ReplicationGetConfigRequest());

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeReplicationApiOperation.Unknown));
            Assert.That(target, Is.Null);
        });
    }

    [Test]
    public void IsUnauthenticatedMethod_is_true_only_for_get_auth_scheme()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationApiGrpcAuthInterceptor.IsUnauthenticatedMethod(FullMethod("GetAuthScheme")), Is.True);
            Assert.That(LatticeReplicationApiGrpcAuthInterceptor.IsUnauthenticatedMethod(FullMethod("EnableReplication")), Is.False);
            Assert.That(LatticeReplicationApiGrpcAuthInterceptor.IsUnauthenticatedMethod(FullMethod("DisableReplication")), Is.False);
            Assert.That(LatticeReplicationApiGrpcAuthInterceptor.IsUnauthenticatedMethod(FullMethod("GetReplicationConfig")), Is.False);
        });
    }

    [Test]
    public async Task GetAuthScheme_is_exempt_from_authorization()
    {
        var authorizer = Substitute.For<ILatticeReplicationApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeReplicationApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(false));
        var interceptor = CreateInterceptor(authorizer);

        var continuationRan = false;
        var response = await interceptor.UnaryServerHandler(
            new AuthSchemeAdvertisementRequest(),
            new FakeServerCallContext(FullMethod("GetAuthScheme")),
            (_, _) =>
            {
                continuationRan = true;
                return Task.FromResult(new AuthSchemeAdvertisement());
            });

        Assert.Multiple(() =>
        {
            Assert.That(continuationRan, Is.True);
            Assert.That(response, Is.Not.Null);
        });
        await authorizer.DidNotReceiveWithAnyArgs()
            .IsAuthorizedAsync(default, default);
    }

    [Test]
    public void Denied_enable_call_fails_with_permission_denied()
    {
        var authorizer = Substitute.For<ILatticeReplicationApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeReplicationApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(false));
        var interceptor = CreateInterceptor(authorizer);

        var continuationRan = false;
        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new ReplicationEnableRequestMessage { TreeId = "orders", Mode = LatticeMergeMode.RwFlag },
            new FakeServerCallContext(FullMethod("EnableReplication")),
            (_, _) =>
            {
                continuationRan = true;
                return Task.FromResult(new ReplicationEnableResponse { TreeId = "orders" });
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(continuationRan, Is.False);
        });
    }

    [Test]
    public async Task Allowed_enable_call_invokes_the_continuation()
    {
        var authorizer = Substitute.For<ILatticeReplicationApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeReplicationApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(true));
        var interceptor = CreateInterceptor(authorizer);

        var continuationRan = false;
        var response = await interceptor.UnaryServerHandler(
            new ReplicationEnableRequestMessage { TreeId = "orders", Mode = LatticeMergeMode.RwFlag },
            new FakeServerCallContext(FullMethod("EnableReplication")),
            (_, _) =>
            {
                continuationRan = true;
                return Task.FromResult(new ReplicationEnableResponse { TreeId = "orders" });
            });

        Assert.Multiple(() =>
        {
            Assert.That(continuationRan, Is.True);
            Assert.That(response.TreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public async Task Enforcement_is_skipped_when_RequireAuthorization_is_false()
    {
        var authorizer = Substitute.For<ILatticeReplicationApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeReplicationApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(false));
        var interceptor = CreateInterceptor(authorizer, requireAuthorization: false);

        var response = await interceptor.UnaryServerHandler(
            new ReplicationDisableRequestMessage { TreeId = "orders" },
            new FakeServerCallContext(FullMethod("DisableReplication")),
            (_, _) => Task.FromResult(new ReplicationDisableResponse { TreeId = "orders" }));

        Assert.That(response.TreeId, Is.EqualTo("orders"));
        await authorizer.DidNotReceiveWithAnyArgs().IsAuthorizedAsync(default, default);
    }

    [Test]
    public async Task Unrelated_service_method_bypasses_the_interceptor()
    {
        var authorizer = Substitute.For<ILatticeReplicationApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeReplicationApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(false));
        var interceptor = CreateInterceptor(authorizer);

        var continuationRan = false;
        await interceptor.UnaryServerHandler(
            new ReplicationEnableRequestMessage { TreeId = "orders", Mode = LatticeMergeMode.RwFlag },
            new FakeServerCallContext("/some.other.service/DoThing"),
            (_, _) =>
            {
                continuationRan = true;
                return Task.FromResult(new ReplicationEnableResponse { TreeId = "orders" });
            });

        Assert.That(continuationRan, Is.True);
        await authorizer.DidNotReceiveWithAnyArgs().IsAuthorizedAsync(default, default);
    }
}
