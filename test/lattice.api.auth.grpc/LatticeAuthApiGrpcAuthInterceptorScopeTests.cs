using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Unit coverage for the two edges of the auth-API interceptor that the
/// per-RPC gating tests do not reach: the service-name scoping decision that
/// keeps the gate off unrelated gRPC services sharing the same ASP.NET Core
/// pipeline, and the cancellation arm around the authorizer call.
///
/// Scoping matters in both directions. Too narrow and an auth-API RPC slips
/// past the meta-authorizer; too broad and the auth binding starts denying a
/// co-hosted, unrelated service. The cancellation arm matters because an
/// authorizer that observes a dropped client would otherwise surface a raw
/// <see cref="OperationCanceledException"/> as an opaque
/// <see cref="StatusCode.Internal"/> (or escape the pipeline entirely) rather
/// than the truthful <see cref="StatusCode.Cancelled"/>.
/// </summary>
[TestFixture]
public sealed class LatticeAuthApiGrpcAuthInterceptorScopeTests
{
    private static LatticeAuthApiGrpcAuthInterceptor CreateInterceptor(
        ILatticeAuthApiAuthorizer authorizer,
        bool requireAuthorization = true)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeAuthApiGrpcOptions>>();
        options.CurrentValue.Returns(new LatticeAuthApiGrpcOptions
        {
            RequireAuthorization = requireAuthorization,
        });
        return new LatticeAuthApiGrpcAuthInterceptor(
            authorizer,
            options,
            NullLogger<LatticeAuthApiGrpcAuthInterceptor>.Instance);
    }

    private static Task<string> Ok(object request, ServerCallContext context) => Task.FromResult("ok");

    [TestCase("/some.other.Service/DoWork")]
    [TestCase("/grpc.health.v1.Health/Check")]
    [TestCase("/Orleans.Lattice.Api.State/Catalog")]
    public async Task A_method_outside_the_auth_api_service_bypasses_the_gate_entirely(string fullMethod)
    {
        // DenyAll would reject anything that actually reached the gate, so
        // reaching the continuation proves the scoping check short-circuited.
        var interceptor = CreateInterceptor(new DenyAllAuthApiAuthorizer());
        var context = new LoopbackServerCallContext(fullMethod);

        var response = await interceptor.UnaryServerHandler<object, string>(new object(), context, Ok);

        Assert.That(response, Is.EqualTo("ok"),
            "The gate is scoped by service-name prefix; an unrelated co-hosted service must be unaffected.");
    }

    [Test]
    public void A_method_on_the_auth_api_service_is_still_gated()
    {
        var interceptor = CreateInterceptor(new DenyAllAuthApiAuthorizer());
        var context = new LoopbackServerCallContext(
            $"/{LatticeAuthApiGrpcMethods.ServiceName}/{LatticeAuthApiGrpcMethods.ListGroupsMethodName}");

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await interceptor.UnaryServerHandler<object, string>(new AuthPageRequest(), context, Ok));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied),
            "The scoping check must not create a hole in the auth-API surface it guards.");
    }

    [Test]
    public void A_prefix_that_only_looks_like_the_auth_api_service_is_not_gated()
    {
        var interceptor = CreateInterceptor(new DenyAllAuthApiAuthorizer());
        // Same leading characters, different service - must not match the prefix.
        var context = new LoopbackServerCallContext($"/{LatticeAuthApiGrpcMethods.ServiceName}Extra/DoWork");

        Assert.DoesNotThrowAsync(async () =>
            await interceptor.UnaryServerHandler<object, string>(new object(), context, Ok));
    }

    [Test]
    public void A_cancelled_authorizer_check_maps_to_Cancelled()
    {
        var authorizer = Substitute.For<ILatticeAuthApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeAuthApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns<Task<bool>>(_ => throw new OperationCanceledException());
        var interceptor = CreateInterceptor(authorizer);
        var context = new LoopbackServerCallContext(
            $"/{LatticeAuthApiGrpcMethods.ServiceName}/{LatticeAuthApiGrpcMethods.ListGroupsMethodName}");

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await interceptor.UnaryServerHandler<object, string>(new AuthPageRequest(), context, Ok));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
            Assert.That(ex.Status.Detail, Does.Contain("cancelled"));
        });
    }

    [Test]
    public async Task RequireAuthorization_false_skips_the_authorizer_without_consulting_it()
    {
        var authorizer = Substitute.For<ILatticeAuthApiAuthorizer>();
        var interceptor = CreateInterceptor(authorizer, requireAuthorization: false);
        var context = new LoopbackServerCallContext(
            $"/{LatticeAuthApiGrpcMethods.ServiceName}/{LatticeAuthApiGrpcMethods.ListGroupsMethodName}");

        var response = await interceptor.UnaryServerHandler<object, string>(new AuthPageRequest(), context, Ok);

        Assert.That(response, Is.EqualTo("ok"));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeAuthApiAuthorizationContext>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void The_authorizer_receives_the_decoded_operation_and_target()
    {
        LatticeAuthApiAuthorizationContext seen = default;
        var authorizer = Substitute.For<ILatticeAuthApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeAuthApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                seen = callInfo.Arg<LatticeAuthApiAuthorizationContext>();
                return Task.FromResult(true);
            });
        var interceptor = CreateInterceptor(authorizer);
        var context = new LoopbackServerCallContext(
            $"/{LatticeAuthApiGrpcMethods.ServiceName}/{LatticeAuthApiGrpcMethods.GetGroupMethodName}");

        Assert.DoesNotThrowAsync(async () => await interceptor.UnaryServerHandler<object, string>(
            new AuthGroupRef { GroupId = "engineering" }, context, Ok));

        Assert.Multiple(() =>
        {
            Assert.That(seen.Operation, Is.EqualTo(LatticeAuthApiOperation.GetGroup));
            Assert.That(seen.TargetId, Is.EqualTo("engineering"));
            Assert.That(seen.Call, Is.SameAs(context),
                "The authorizer must see the real call context so it can read headers, deadline, and peer.");
        });
    }

    [Test]
    public void UnaryServerHandler_throws_on_null_arguments()
    {
        var interceptor = CreateInterceptor(new AllowAllAuthApiAuthorizer());
        var context = new LoopbackServerCallContext("/svc/M");

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await interceptor.UnaryServerHandler<object, string>(null!, context, Ok));
            Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await interceptor.UnaryServerHandler<object, string>(new object(), null!, Ok));
            Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await interceptor.UnaryServerHandler<object, string>(new object(), context, null!));
        });
    }
}
