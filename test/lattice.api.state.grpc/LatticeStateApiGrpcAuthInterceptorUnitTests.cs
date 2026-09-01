using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Unit coverage for the runtime enforcement path of
/// <see cref="LatticeStateApiGrpcAuthInterceptor"/>, driven directly with no live
/// server. <c>LatticeStateApiAuthInterceptorDescribeCallTests</c> already pins the
/// static operation/target decoding; this fixture covers what happens to an
/// inbound call on both the unary and server-streaming handlers: the
/// enforcement-disabled short circuit, the service-prefix and
/// unauthenticated-method bypasses, the deny rejection, and the
/// authorizer-cancellation mapping. It also covers the shipped authorizers, the
/// authorization context's carried state, and the default-closed DI registration.
/// </summary>
[TestFixture]
public sealed class LatticeStateApiGrpcAuthInterceptorUnitTests
{
    private static string FullMethod(string methodName) =>
        $"/{LatticeStateGrpcMethods.ServiceName}/{methodName}";

    private static LatticeStateApiGrpcAuthInterceptor Create(
        ILatticeStateApiAuthorizer authorizer,
        bool requireAuthorization = true)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeStateApiGrpcOptions>>();
        monitor.CurrentValue.Returns(new LatticeStateApiGrpcOptions { RequireAuthorization = requireAuthorization });
        return new LatticeStateApiGrpcAuthInterceptor(
            authorizer,
            monitor,
            Substitute.For<ILogger<LatticeStateApiGrpcAuthInterceptor>>());
    }

    private static ClusterInfoRequest Request => new();

    // ----- Construction guards -----

    [Test]
    public void Constructor_rejects_a_null_authorizer()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeStateApiGrpcOptions>>();

        Assert.Throws<ArgumentNullException>(() => new LatticeStateApiGrpcAuthInterceptor(
            null!, monitor, Substitute.For<ILogger<LatticeStateApiGrpcAuthInterceptor>>()));
    }

    [Test]
    public void Constructor_rejects_a_null_options_monitor()
    {
        Assert.Throws<ArgumentNullException>(() => new LatticeStateApiGrpcAuthInterceptor(
            new AllowAllStateApiAuthorizer(), null!, Substitute.For<ILogger<LatticeStateApiGrpcAuthInterceptor>>()));
    }

    [Test]
    public void Constructor_rejects_a_null_logger()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeStateApiGrpcOptions>>();

        Assert.Throws<ArgumentNullException>(() => new LatticeStateApiGrpcAuthInterceptor(
            new AllowAllStateApiAuthorizer(), monitor, null!));
    }

    // ----- Unary handler -----

    [Test]
    public async Task UnaryServerHandler_when_authorization_disabled_skips_the_authorizer()
    {
        var authorizer = Substitute.For<ILatticeStateApiAuthorizer>();
        var interceptor = Create(authorizer, requireAuthorization: false);
        var response = new ClusterInfo { ClusterId = "c1" };

        var result = await interceptor.UnaryServerHandler(
            Request,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetClusterInfoMethodName),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeStateApiAuthorizationContext>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UnaryServerHandler_non_state_api_service_method_bypasses_enforcement()
    {
        var authorizer = Substitute.For<ILatticeStateApiAuthorizer>();
        var interceptor = Create(authorizer);
        var response = new ClusterInfo();

        var result = await interceptor.UnaryServerHandler(
            Request,
            new StateGrpcCallContext("/some.other.Service/DoThing"),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeStateApiAuthorizationContext>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UnaryServerHandler_exempts_the_unauthenticated_auth_scheme_discovery_rpc()
    {
        var interceptor = Create(new DenyAllStateApiAuthorizer());
        var response = new AuthSchemeAdvertisement();

        var result = await interceptor.UnaryServerHandler(
            new AuthSchemeAdvertisementRequest(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetAuthSchemeMethodName),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
    }

    [Test]
    public void UnaryServerHandler_default_deny_authorizer_rejects_with_PermissionDenied()
    {
        var interceptor = Create(new DenyAllStateApiAuthorizer());
        var continuationRan = false;

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            Request,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetClusterInfoMethodName),
            (_, _) =>
            {
                continuationRan = true;
                return Task.FromResult(new ClusterInfo());
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("ILatticeStateApiAuthorizer"));
            Assert.That(continuationRan, Is.False, "a denied call must never reach the continuation");
        });
    }

    [Test]
    public async Task UnaryServerHandler_permissive_authorizer_invokes_the_continuation()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer());
        var response = new ClusterInfo { ClusterId = "c1" };

        var result = await interceptor.UnaryServerHandler(
            Request,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetClusterInfoMethodName),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
    }

    [Test]
    public void UnaryServerHandler_maps_authorizer_cancellation_to_Cancelled()
    {
        var authorizer = Substitute.For<ILatticeStateApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeStateApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());
        var interceptor = Create(authorizer);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            Request,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetClusterInfoMethodName),
            (_, _) => Task.FromResult(new ClusterInfo())));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public async Task UnaryServerHandler_hands_the_authorizer_the_decoded_operation_and_target()
    {
        LatticeStateApiAuthorizationContext? observed = null;
        var authorizer = Substitute.For<ILatticeStateApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeStateApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                observed = call.Arg<LatticeStateApiAuthorizationContext>();
                return Task.FromResult(true);
            });
        var interceptor = Create(authorizer);
        var context = StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetEntryMethodName);

        await interceptor.UnaryServerHandler(
            new EntryGetRequest { TreeId = "orders", Key = "k1" },
            context,
            (_, _) => Task.FromResult(new EntryGetResponse { TreeId = "orders", Key = "k1" }));

        Assert.That(observed, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(observed!.Value.Operation, Is.EqualTo(LatticeStateApiOperation.GetEntry));
            Assert.That(observed!.Value.TargetTreeId, Is.EqualTo("orders"));
            Assert.That(observed!.Value.Call, Is.SameAs(context));
        });
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_request()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () => await interceptor.UnaryServerHandler(
            (ClusterInfoRequest)null!,
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetClusterInfoMethodName),
            (_, _) => Task.FromResult(new ClusterInfo())));
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_context()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () => await interceptor.UnaryServerHandler(
            Request, null!, (_, _) => Task.FromResult(new ClusterInfo())));
    }

    [Test]
    public void UnaryServerHandler_rejects_a_null_continuation()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await interceptor.UnaryServerHandler<ClusterInfoRequest, ClusterInfo>(
                Request,
                StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetClusterInfoMethodName),
                null!));
    }

    // ----- Server-streaming handler -----

    [Test]
    public async Task ServerStreamingServerHandler_non_state_api_service_method_bypasses_enforcement()
    {
        var authorizer = Substitute.For<ILatticeStateApiAuthorizer>();
        var interceptor = Create(authorizer);
        var invoked = false;

        await interceptor.ServerStreamingServerHandler(
            new TreeMetricsRequest(),
            new RecordingServerStreamWriter<TreeMetricsSnapshot>(),
            new StateGrpcCallContext("/some.other.Service/Stream"),
            (_, _, _) =>
            {
                invoked = true;
                return Task.CompletedTask;
            });

        Assert.That(invoked, Is.True);
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeStateApiAuthorizationContext>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ServerStreamingServerHandler_when_authorization_disabled_skips_the_authorizer()
    {
        var authorizer = Substitute.For<ILatticeStateApiAuthorizer>();
        var interceptor = Create(authorizer, requireAuthorization: false);
        var invoked = false;

        await interceptor.ServerStreamingServerHandler(
            new TreeMetricsRequest(),
            new RecordingServerStreamWriter<TreeMetricsSnapshot>(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName),
            (_, _, _) =>
            {
                invoked = true;
                return Task.CompletedTask;
            });

        Assert.That(invoked, Is.True);
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeStateApiAuthorizationContext>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ServerStreamingServerHandler_default_deny_authorizer_rejects_with_PermissionDenied()
    {
        var interceptor = Create(new DenyAllStateApiAuthorizer());
        var continuationRan = false;

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.ServerStreamingServerHandler(
            new TreeMetricsRequest(),
            new RecordingServerStreamWriter<TreeMetricsSnapshot>(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName),
            (_, _, _) =>
            {
                continuationRan = true;
                return Task.CompletedTask;
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(continuationRan, Is.False, "a denied subscription must never reach the continuation");
        });
    }

    [Test]
    public async Task ServerStreamingServerHandler_permissive_authorizer_invokes_the_continuation()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer());
        var invoked = false;

        await interceptor.ServerStreamingServerHandler(
            new TreeMetricsRequest(),
            new RecordingServerStreamWriter<TreeMetricsSnapshot>(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName),
            (_, _, _) =>
            {
                invoked = true;
                return Task.CompletedTask;
            });

        Assert.That(invoked, Is.True);
    }

    [Test]
    public void ServerStreamingServerHandler_rejects_a_null_request()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () => await interceptor.ServerStreamingServerHandler(
            (TreeMetricsRequest)null!,
            new RecordingServerStreamWriter<TreeMetricsSnapshot>(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName),
            (_, _, _) => Task.CompletedTask));
    }

    [Test]
    public void ServerStreamingServerHandler_rejects_a_null_response_stream()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await interceptor.ServerStreamingServerHandler<TreeMetricsRequest, TreeMetricsSnapshot>(
                new TreeMetricsRequest(),
                null!,
                StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName),
                (_, _, _) => Task.CompletedTask));
    }

    [Test]
    public void ServerStreamingServerHandler_rejects_a_null_context()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () => await interceptor.ServerStreamingServerHandler(
            new TreeMetricsRequest(),
            new RecordingServerStreamWriter<TreeMetricsSnapshot>(),
            null!,
            (_, _, _) => Task.CompletedTask));
    }

    [Test]
    public void ServerStreamingServerHandler_rejects_a_null_continuation()
    {
        var interceptor = Create(new AllowAllStateApiAuthorizer());

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await interceptor.ServerStreamingServerHandler<TreeMetricsRequest, TreeMetricsSnapshot>(
                new TreeMetricsRequest(),
                new RecordingServerStreamWriter<TreeMetricsSnapshot>(),
                StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName),
                null!));
    }

    // ----- Shipped authorizers and the authorization context -----

    [Test]
    public async Task DenyAllStateApiAuthorizer_refuses_every_call()
    {
        var context = new LatticeStateApiAuthorizationContext(
            new StateGrpcCallContext("/test/Method"), LatticeStateApiOperation.ListTrees, targetTreeId: null);

        Assert.That(await new DenyAllStateApiAuthorizer().IsAuthorizedAsync(context, CancellationToken.None), Is.False);
    }

    [Test]
    public async Task AllowAllStateApiAuthorizer_permits_every_call()
    {
        var context = new LatticeStateApiAuthorizationContext(
            new StateGrpcCallContext("/test/Method"), LatticeStateApiOperation.ListTrees, targetTreeId: null);

        Assert.That(await new AllowAllStateApiAuthorizer().IsAuthorizedAsync(context, CancellationToken.None), Is.True);
    }

    [Test]
    public void AuthorizationContext_exposes_the_call_operation_and_target()
    {
        var call = new StateGrpcCallContext("/test/Method");

        var context = new LatticeStateApiAuthorizationContext(call, LatticeStateApiOperation.GetEntry, "orders");

        Assert.Multiple(() =>
        {
            Assert.That(context.Call, Is.SameAs(call));
            Assert.That(context.Operation, Is.EqualTo(LatticeStateApiOperation.GetEntry));
            Assert.That(context.TargetTreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public void AuthorizationContext_rejects_a_null_call()
    {
        Assert.Throws<ArgumentNullException>(() => _ = new LatticeStateApiAuthorizationContext(
            null!, LatticeStateApiOperation.ListTrees, targetTreeId: null));
    }

    // ----- Registration -----

    [Test]
    public void AddLatticeStateApiGrpc_without_a_configure_delegate_registers_default_closed_options()
    {
        // The no-delegate overload takes the AddOptions branch; the binding must
        // still resolve fully-defaulted (and therefore fail-closed) options.
        var services = new ServiceCollection();
        services.AddLatticeStateApiGrpc();
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeStateApiGrpcOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAuthorization, Is.True);
            Assert.That(
                provider.GetRequiredService<ILatticeStateApiAuthorizer>(),
                Is.InstanceOf<DenyAllStateApiAuthorizer>());
        });
    }
}
