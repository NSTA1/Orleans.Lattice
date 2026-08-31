using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Unit coverage for the runtime enforcement path of
/// <see cref="LatticeTreeAdminApiGrpcAuthInterceptor"/>, driven directly with no
/// live server. <c>TreeAdminGrpcInterceptorMappingTests</c> already pins the
/// static operation/target decoding; this fixture covers what actually happens to
/// an inbound call: the enforcement-disabled short circuit, the service-prefix and
/// unauthenticated-method bypasses, the deny rejection, and the
/// authorizer-cancellation mapping.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminApiGrpcAuthInterceptorUnitTests
{
    private static string FullMethod(string methodName) =>
        $"/{LatticeTreeAdminGrpcMethods.ServiceName}/{methodName}";

    private static LatticeTreeAdminApiGrpcAuthInterceptor Create(
        ILatticeTreeAdminApiAuthorizer authorizer,
        bool requireAuthorization = true)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTreeAdminApiGrpcOptions>>();
        monitor.CurrentValue.Returns(new LatticeTreeAdminApiGrpcOptions { RequireAuthorization = requireAuthorization });
        return new LatticeTreeAdminApiGrpcAuthInterceptor(
            authorizer,
            monitor,
            Substitute.For<ILogger<LatticeTreeAdminApiGrpcAuthInterceptor>>());
    }

    [Test]
    public void Constructor_rejects_a_null_authorizer()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTreeAdminApiGrpcOptions>>();

        Assert.Throws<ArgumentNullException>(() => new LatticeTreeAdminApiGrpcAuthInterceptor(
            null!,
            monitor,
            Substitute.For<ILogger<LatticeTreeAdminApiGrpcAuthInterceptor>>()));
    }

    [Test]
    public void Constructor_rejects_a_null_options_monitor()
    {
        Assert.Throws<ArgumentNullException>(() => new LatticeTreeAdminApiGrpcAuthInterceptor(
            new AllowAllTreeAdminApiAuthorizer(),
            null!,
            Substitute.For<ILogger<LatticeTreeAdminApiGrpcAuthInterceptor>>()));
    }

    [Test]
    public void Constructor_rejects_a_null_logger()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTreeAdminApiGrpcOptions>>();

        Assert.Throws<ArgumentNullException>(() => new LatticeTreeAdminApiGrpcAuthInterceptor(
            new AllowAllTreeAdminApiAuthorizer(),
            monitor,
            null!));
    }

    [Test]
    public async Task UnaryServerHandler_when_authorization_disabled_skips_the_authorizer()
    {
        var authorizer = Substitute.For<ILatticeTreeAdminApiAuthorizer>();
        var interceptor = Create(authorizer, requireAuthorization: false);
        var response = new TreeExistenceResult { TreeId = "orders", Exists = true };

        var result = await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTreeAdminApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UnaryServerHandler_non_tree_admin_service_method_bypasses_enforcement()
    {
        var authorizer = Substitute.For<ILatticeTreeAdminApiAuthorizer>();
        var interceptor = Create(authorizer);
        var response = new TreeExistenceResult { TreeId = "orders" };

        var result = await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            new FakeServerCallContext("/some.other.Service/DoThing"),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTreeAdminApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UnaryServerHandler_exempts_the_unauthenticated_auth_scheme_discovery_rpc()
    {
        // A client must be able to learn how to sign in before it holds any
        // credential, so GetAuthScheme runs even under the default-deny authorizer.
        var interceptor = Create(new DenyTreeAdminApiAuthorizer());
        var response = new AuthSchemeAdvertisement();

        var result = await interceptor.UnaryServerHandler(
            new AuthSchemeAdvertisementRequest(),
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.GetAuthSchemeMethodName)),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
    }

    [Test]
    public void UnaryServerHandler_default_deny_authorizer_rejects_with_PermissionDenied()
    {
        var interceptor = Create(new DenyTreeAdminApiAuthorizer());
        var continuationRan = false;

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
            (_, _) =>
            {
                continuationRan = true;
                return Task.FromResult(new TreeExistenceResult { TreeId = "orders" });
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("ILatticeTreeAdminApiAuthorizer"));
            Assert.That(continuationRan, Is.False, "a denied call must never reach the continuation");
        });
    }

    [Test]
    public async Task UnaryServerHandler_permissive_authorizer_invokes_the_continuation()
    {
        var interceptor = Create(new AllowAllTreeAdminApiAuthorizer());
        var response = new TreeExistenceResult { TreeId = "orders", Exists = true };

        var result = await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
    }

    [Test]
    public void UnaryServerHandler_maps_authorizer_cancellation_to_Cancelled()
    {
        var authorizer = Substitute.For<ILatticeTreeAdminApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeTreeAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());
        var interceptor = Create(authorizer);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
            (_, _) => Task.FromResult(new TreeExistenceResult { TreeId = "orders" })));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public async Task UnaryServerHandler_hands_the_authorizer_the_decoded_operation_and_target()
    {
        LatticeTreeAdminApiAuthorizationContext? observed = null;
        var authorizer = Substitute.For<ILatticeTreeAdminApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTreeAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                observed = call.Arg<LatticeTreeAdminApiAuthorizationContext>();
                return Task.FromResult(true);
            });
        var interceptor = Create(authorizer);
        var context = new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CreateTreeMethodName));

        await interceptor.UnaryServerHandler(
            new TreeAdminCreateRequest { TreeId = "orders" },
            context,
            (_, _) => Task.FromResult(new TreeCreationResult { TreeId = "orders" }));

        Assert.That(observed, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(observed!.Value.Operation, Is.EqualTo(LatticeTreeAdminApiOperation.CreateTree));
            Assert.That(observed!.Value.TargetId, Is.EqualTo("orders"));
            Assert.That(observed!.Value.Call, Is.SameAs(context));
        });
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_request()
    {
        var interceptor = Create(new AllowAllTreeAdminApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () => await interceptor.UnaryServerHandler(
            (TreeAdminTreeRequest)null!,
            new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
            (_, _) => Task.FromResult(new TreeExistenceResult { TreeId = "orders" })));
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_context()
    {
        var interceptor = Create(new AllowAllTreeAdminApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () => await interceptor.UnaryServerHandler(
            new TreeAdminTreeRequest { TreeId = "orders" },
            null!,
            (_, _) => Task.FromResult(new TreeExistenceResult { TreeId = "orders" })));
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_continuation()
    {
        var interceptor = Create(new AllowAllTreeAdminApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await interceptor.UnaryServerHandler<TreeAdminTreeRequest, TreeExistenceResult>(
                new TreeAdminTreeRequest { TreeId = "orders" },
                new FakeServerCallContext(FullMethod(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName)),
                null!));
    }
}
