using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Runtime unit coverage for
/// <see cref="LatticeTenantAdminApiGrpcAuthInterceptor"/> - the default-deny
/// transport gate in front of the destructive tenant lifecycle RPCs. The sibling
/// fixture asserts the interceptor's pure decode helpers; this one drives
/// <c>UnaryServerHandler</c> itself so the enforcement path is exercised end to
/// end without standing up a gRPC server: the service-prefix scoping, the
/// unauthenticated and self-service exemptions, the
/// <c>RequireAuthorization=false</c> short circuit, the allow and deny outcomes,
/// and the cancellation translation.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminApiGrpcAuthInterceptorTests
{
    private const string Svc = "/orleans.lattice.api.tenantadmin/";

    private static string Method(string name) => Svc + name;

    private static LatticeTenantAdminApiGrpcAuthInterceptor Interceptor(
        ILatticeTenantAdminApiAuthorizer authorizer,
        bool requireAuthorization = true)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTenantAdminApiGrpcOptions>>();
        monitor.CurrentValue.Returns(new LatticeTenantAdminApiGrpcOptions
        {
            RequireAuthorization = requireAuthorization,
        });
        return new LatticeTenantAdminApiGrpcAuthInterceptor(
            authorizer,
            monitor,
            NullLogger<LatticeTenantAdminApiGrpcAuthInterceptor>.Instance);
    }

    private static UnaryServerMethod<TenantAdminTenantRequest, TenantDeletionResult> Continuation(
        Action? onInvoked = null)
        => (request, context) =>
        {
            onInvoked?.Invoke();
            return Task.FromResult(new TenantDeletionResult { TenantId = request.TenantId, CascadedTreeCount = 0 });
        };

    [Test]
    public void Constructor_rejects_a_null_authorizer()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTenantAdminApiGrpcOptions>>();

        Assert.That(
            () => new LatticeTenantAdminApiGrpcAuthInterceptor(
                null!, monitor, NullLogger<LatticeTenantAdminApiGrpcAuthInterceptor>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_rejects_null_options()
    {
        Assert.That(
            () => new LatticeTenantAdminApiGrpcAuthInterceptor(
                new AllowAllTenantAdminApiAuthorizer(),
                null!,
                NullLogger<LatticeTenantAdminApiGrpcAuthInterceptor>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_rejects_a_null_logger()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTenantAdminApiGrpcOptions>>();

        Assert.That(
            () => new LatticeTenantAdminApiGrpcAuthInterceptor(
                new AllowAllTenantAdminApiAuthorizer(), monitor, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_request()
    {
        var interceptor = Interceptor(new AllowAllTenantAdminApiAuthorizer());

        Assert.That(
            async () => await interceptor.UnaryServerHandler<TenantAdminTenantRequest, TenantDeletionResult>(
                null!,
                new FakeServerCallContext(Method(LatticeTenantAdminGrpcMethods.DeleteTenantMethodName)),
                Continuation()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_context()
    {
        var interceptor = Interceptor(new AllowAllTenantAdminApiAuthorizer());

        Assert.That(
            async () => await interceptor.UnaryServerHandler(
                new TenantAdminTenantRequest { TenantId = "acme" },
                null!,
                Continuation()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_continuation()
    {
        var interceptor = Interceptor(new AllowAllTenantAdminApiAuthorizer());

        Assert.That(
            async () => await interceptor.UnaryServerHandler<TenantAdminTenantRequest, TenantDeletionResult>(
                new TenantAdminTenantRequest { TenantId = "acme" },
                new FakeServerCallContext(Method(LatticeTenantAdminGrpcMethods.DeleteTenantMethodName)),
                null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task An_unrelated_grpc_service_is_never_gated_by_this_interceptor()
    {
        var authorizer = Substitute.For<ILatticeTenantAdminApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTenantAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(false));
        var interceptor = Interceptor(authorizer);
        var invoked = false;

        var result = await interceptor.UnaryServerHandler(
            new TenantAdminTenantRequest { TenantId = "acme" },
            new FakeServerCallContext("/some.other.service/DeleteTenant"),
            Continuation(() => invoked = true));

        Assert.Multiple(() =>
        {
            Assert.That(invoked, Is.True, "a call to an unrelated service must pass straight through");
            Assert.That(result.TenantId, Is.EqualTo("acme"));
        });
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTenantAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task The_unauthenticated_auth_scheme_rpc_bypasses_the_authorizer()
    {
        var authorizer = Substitute.For<ILatticeTenantAdminApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTenantAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(false));
        var interceptor = Interceptor(authorizer);

        var response = await interceptor.UnaryServerHandler(
            new AuthSchemeAdvertisementRequest(),
            new FakeServerCallContext(Method(LatticeTenantAdminGrpcMethods.GetAuthSchemeMethodName)),
            (_, _) => Task.FromResult(new AuthSchemeAdvertisement()));

        Assert.That(response, Is.Not.Null);
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTenantAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>());
    }

    [TestCase(LatticeTenantAdminGrpcMethods.GetCurrentTenantMethodName)]
    [TestCase(LatticeTenantAdminGrpcMethods.ListAccessibleTenantsMethodName)]
    [TestCase(LatticeTenantAdminGrpcMethods.GetTenantMethodName)]
    public async Task The_read_only_self_service_rpcs_bypass_the_default_deny_admin_gate(string methodName)
    {
        var authorizer = Substitute.For<ILatticeTenantAdminApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTenantAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(false));
        var interceptor = Interceptor(authorizer);
        var invoked = false;

        await interceptor.UnaryServerHandler(
            new TenantAdminTenantRequest { TenantId = "acme" },
            new FakeServerCallContext(Method(methodName)),
            Continuation(() => invoked = true));

        Assert.That(invoked, Is.True,
            "self-service reads must reach the facade, which enforces its own fail-closed scoping");
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTenantAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Enforcement_is_skipped_when_the_host_turns_RequireAuthorization_off()
    {
        var authorizer = Substitute.For<ILatticeTenantAdminApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTenantAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(false));
        var interceptor = Interceptor(authorizer, requireAuthorization: false);
        var invoked = false;

        await interceptor.UnaryServerHandler(
            new TenantAdminTenantRequest { TenantId = "acme" },
            new FakeServerCallContext(Method(LatticeTenantAdminGrpcMethods.DeleteTenantMethodName)),
            Continuation(() => invoked = true));

        Assert.That(invoked, Is.True);
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTenantAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_authorized_lifecycle_call_reaches_the_continuation()
    {
        var interceptor = Interceptor(new AllowAllTenantAdminApiAuthorizer());
        var invoked = false;

        var result = await interceptor.UnaryServerHandler(
            new TenantAdminTenantRequest { TenantId = "acme" },
            new FakeServerCallContext(Method(LatticeTenantAdminGrpcMethods.DeleteTenantMethodName)),
            Continuation(() => invoked = true));

        Assert.Multiple(() =>
        {
            Assert.That(invoked, Is.True);
            Assert.That(result.TenantId, Is.EqualTo("acme"));
        });
    }

    [Test]
    public void The_default_deny_authorizer_rejects_a_lifecycle_call_with_permission_denied()
    {
        var interceptor = Interceptor(new DenyTenantAdminApiAuthorizer());
        var invoked = false;

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new TenantAdminTenantRequest { TenantId = "acme" },
            new FakeServerCallContext(Method(LatticeTenantAdminGrpcMethods.DeleteTenantMethodName)),
            Continuation(() => invoked = true)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(invoked, Is.False, "a denied call must never reach the facade");
        });
    }

    [Test]
    public async Task The_authorizer_receives_the_decoded_operation_and_target_tenant()
    {
        LatticeTenantAdminApiAuthorizationContext? seen = null;
        var authorizer = Substitute.For<ILatticeTenantAdminApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTenantAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                seen = call.Arg<LatticeTenantAdminApiAuthorizationContext>();
                return Task.FromResult(true);
            });
        var interceptor = Interceptor(authorizer);

        await interceptor.UnaryServerHandler(
            new TenantAdminTenantRequest { TenantId = "acme" },
            new FakeServerCallContext(Method(LatticeTenantAdminGrpcMethods.SuspendTenantMethodName)),
            Continuation());

        Assert.That(seen, Is.Not.Null);
        var described = seen!.Value;
        Assert.Multiple(() =>
        {
            Assert.That(described.Operation, Is.EqualTo(LatticeTenantAdminApiOperation.SuspendTenant));
            Assert.That(described.TargetId, Is.EqualTo("acme"));
            Assert.That(described.Call.Method,
                Is.EqualTo(Method(LatticeTenantAdminGrpcMethods.SuspendTenantMethodName)));
        });
    }

    [Test]
    public void A_cancelled_authorization_check_surfaces_as_the_cancelled_status()
    {
        var authorizer = Substitute.For<ILatticeTenantAdminApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTenantAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns<Task<bool>>(_ => throw new OperationCanceledException());
        var interceptor = Interceptor(authorizer);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new TenantAdminTenantRequest { TenantId = "acme" },
            new FakeServerCallContext(Method(LatticeTenantAdminGrpcMethods.ResumeTenantMethodName)),
            Continuation()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void An_unrecognised_method_on_the_lattice_service_is_still_gated()
    {
        var interceptor = Interceptor(new DenyTenantAdminApiAuthorizer());

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new TenantAdminTenantRequest { TenantId = "acme" },
            new FakeServerCallContext(Method("SomeFutureRpc")),
            Continuation()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied),
            "an unmapped method must decode to Unknown and stay behind the default-deny gate");
    }

    [Test]
    public async Task The_call_cancellation_token_is_handed_to_the_authorizer()
    {
        using var cts = new CancellationTokenSource();
        CancellationToken seen = default;
        var authorizer = Substitute.For<ILatticeTenantAdminApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTenantAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                seen = call.Arg<CancellationToken>();
                return Task.FromResult(true);
            });
        var interceptor = Interceptor(authorizer);

        await interceptor.UnaryServerHandler(
            new TenantAdminTenantRequest { TenantId = "acme" },
            new FakeServerCallContext(
                Method(LatticeTenantAdminGrpcMethods.DeleteTenantMethodName),
                cancellationToken: cts.Token),
            Continuation());

        Assert.That(seen, Is.EqualTo(cts.Token));
    }
}
