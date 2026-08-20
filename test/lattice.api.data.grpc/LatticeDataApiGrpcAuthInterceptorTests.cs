using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeDataApiGrpcAuthInterceptor"/> driven
/// directly - no gRPC server. Proves an unrelated (non-data-API) method is passed
/// straight through without an authorization check, that a permitted call reaches
/// the continuation while a denied one is rejected with
/// <see cref="StatusCode.PermissionDenied"/>, that enforcement is skipped when it
/// is turned off, and that a cancellation raised during the authorization check
/// surfaces as <see cref="StatusCode.Cancelled"/>.
/// </summary>
[TestFixture]
public sealed class LatticeDataApiGrpcAuthInterceptorTests
{
    private const string LatticeMethod = "/orleans.lattice.api.data/Set";
    private const string ForeignMethod = "/some.other.service/Ping";

    private static LatticeDataApiGrpcAuthInterceptor Create(
        ILatticeDataApiAuthorizer authorizer,
        bool requireAuthorization = true)
    {
        var options = new StaticOptionsMonitor(new LatticeDataApiGrpcOptions
        {
            RequireAuthorization = requireAuthorization,
        });
        return new LatticeDataApiGrpcAuthInterceptor(
            authorizer,
            options,
            NullLogger<LatticeDataApiGrpcAuthInterceptor>.Instance);
    }

    private static UnaryServerMethod<DataSetRequest, DataSetResponse> Continuation(DataSetResponse response, Action? onCall = null)
        => (_, _) =>
        {
            onCall?.Invoke();
            return Task.FromResult(response);
        };

    private static DataSetRequest Request() => new() { TreeId = "t", Key = "k", Value = [1] };

    [Test]
    public async Task A_non_data_api_method_is_passed_through_without_an_auth_check()
    {
        var authorizer = Substitute.For<ILatticeDataApiAuthorizer>();
        var interceptor = Create(authorizer);
        var expected = new DataSetResponse();

        var result = await interceptor.UnaryServerHandler(
            Request(),
            new StubServerCallContext(ForeignMethod),
            Continuation(expected));

        Assert.That(result, Is.SameAs(expected));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeDataApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_authorized_call_reaches_the_continuation()
    {
        var authorizer = Substitute.For<ILatticeDataApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeDataApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(true);
        var interceptor = Create(authorizer);
        var reached = false;

        await interceptor.UnaryServerHandler(
            Request(),
            new StubServerCallContext(LatticeMethod),
            Continuation(new DataSetResponse(), () => reached = true));

        Assert.That(reached, Is.True);
    }

    [Test]
    public void A_denied_call_is_rejected_with_permission_denied()
    {
        var authorizer = Substitute.For<ILatticeDataApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeDataApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(false);
        var interceptor = Create(authorizer);

        var ex = Assert.ThrowsAsync<RpcException>(() => interceptor.UnaryServerHandler(
            Request(),
            new StubServerCallContext(LatticeMethod),
            Continuation(new DataSetResponse())));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task Enforcement_is_skipped_when_authorization_is_disabled()
    {
        var authorizer = Substitute.For<ILatticeDataApiAuthorizer>();
        var interceptor = Create(authorizer, requireAuthorization: false);

        await interceptor.UnaryServerHandler(
            Request(),
            new StubServerCallContext(LatticeMethod),
            Continuation(new DataSetResponse()));

        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeDataApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_cancelled_authorization_check_maps_to_cancelled()
    {
        var authorizer = Substitute.For<ILatticeDataApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeDataApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns<Task<bool>>(_ => throw new OperationCanceledException());
        var interceptor = Create(authorizer);

        var ex = Assert.ThrowsAsync<RpcException>(() => interceptor.UnaryServerHandler(
            Request(),
            new StubServerCallContext(LatticeMethod),
            Continuation(new DataSetResponse())));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    /// <summary>
    /// A minimal <see cref="IOptionsMonitor{TOptions}"/> that always reports a fixed
    /// value; the interceptor reads only <see cref="IOptionsMonitor{TOptions}.CurrentValue"/>.
    /// </summary>
    private sealed class StaticOptionsMonitor(LatticeDataApiGrpcOptions value)
        : IOptionsMonitor<LatticeDataApiGrpcOptions>
    {
        public LatticeDataApiGrpcOptions CurrentValue { get; } = value;

        public LatticeDataApiGrpcOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<LatticeDataApiGrpcOptions, string?> listener) => null;
    }
}
