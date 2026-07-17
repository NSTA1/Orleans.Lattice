using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Membership;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// An in-memory, cluster-free end-to-end round-trip for the three identity
/// directory / access-model RPCs added to the auth-API binding. A real
/// <see cref="LatticeAuthApiGrpcClient"/> is driven over a loopback
/// <see cref="CallInvoker"/> that marshals every request and response through the
/// real wire <see cref="Marshaller{T}"/> and dispatches into the real
/// <see cref="LatticeAuthApiGrpcService"/>, whose only collaborator is a fake
/// <see cref="ILatticeAuthAdmin"/> facade. This proves client -&gt; gRPC -&gt; facade
/// (and the response back) is coherent for a directory search, a principal
/// resolve (present and absent), and an access-model read, without standing up a
/// silo.
/// </summary>
[TestFixture]
public sealed class DirectoryRpcLoopbackRoundTripTests
{
    private ServiceProvider _services = null!;
    private LatticeAuthApiGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeAuthApiGrpcMethods.FromServiceProvider(_services);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private LatticeAuthApiGrpcClient ClientFor(ILatticeAuthAdmin admin)
    {
        var bridge = Substitute.For<ILatticeAuthApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);

        var service = new LatticeAuthApiGrpcService(
            _methods,
            admin,
            bridge,
            NullLogger<LatticeAuthApiGrpcService>.Instance);

        var invoker = new LoopbackCallInvoker(service);
        return LatticeAuthApiGrpcClient.Create(invoker, _services);
    }

    [Test]
    public async Task SearchDirectoryAsync_round_trips_the_facade_page_over_the_wire()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>())
            .Returns(new DirectorySearchResult
            {
                Principals = new[]
                {
                    new DirectoryPrincipalDescriptor { Id = "alice", DisplayName = "Alice", Kind = DirectoryPrincipalKind.User },
                },
                ContinuationToken = "next",
                Available = true,
            });
        var client = ClientFor(admin);

        var result = await client.SearchDirectoryAsync(new DirectorySearchRequest { Term = "al", PageSize = 10 });

        Assert.Multiple(() =>
        {
            Assert.That(result.Available, Is.True);
            Assert.That(result.Principals, Has.Count.EqualTo(1));
            Assert.That(result.Principals[0].Id, Is.EqualTo("alice"));
            Assert.That(result.ContinuationToken, Is.EqualTo("next"));
        });
    }

    [Test]
    public async Task SearchDirectoryAsync_round_trips_the_unavailable_result()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>())
            .Returns(DirectorySearchResult.Unavailable);
        var client = ClientFor(admin);

        var result = await client.SearchDirectoryAsync(new DirectorySearchRequest { Term = "x" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Available, Is.False);
            Assert.That(result.Principals, Is.Empty);
        });
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_round_trips_a_present_principal()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.ResolveDirectoryPrincipalAsync("g-1", Arg.Any<CancellationToken>())
            .Returns(new DirectoryPrincipalDescriptor { Id = "g-1", DisplayName = "Group One", Kind = DirectoryPrincipalKind.Group });
        var client = ClientFor(admin);

        var result = await client.ResolveDirectoryPrincipalAsync(new AuthPrincipalRef { PrincipalId = "g-1" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Principal, Is.Not.Null);
            Assert.That(result.Principal!.Id, Is.EqualTo("g-1"));
            Assert.That(result.Principal!.Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
        });
    }

    [Test]
    public async Task ResolveDirectoryPrincipalAsync_round_trips_an_absent_principal_as_null()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.ResolveDirectoryPrincipalAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns((DirectoryPrincipalDescriptor?)null);
        var client = ClientFor(admin);

        var result = await client.ResolveDirectoryPrincipalAsync(new AuthPrincipalRef { PrincipalId = "missing" });

        Assert.That(result.Principal, Is.Null);
    }

    [Test]
    public async Task GetAccessModelAsync_round_trips_the_access_model()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.GetAccessModelAsync(Arg.Any<CancellationToken>())
            .Returns(new AccessModelDescriptor
            {
                AuthenticationMode = AccessAuthenticationMode.Claims,
                RulesEnforced = true,
                DirectoryAvailable = true,
                DirectoryProviderId = "entra",
                DirectoryExplanation = "Use the object id.",
            });
        var client = ClientFor(admin);

        var result = await client.GetAccessModelAsync(new AuthAccessModelQuery());

        Assert.Multiple(() =>
        {
            Assert.That(result.AuthenticationMode, Is.EqualTo(AccessAuthenticationMode.Claims));
            Assert.That(result.RulesEnforced, Is.True);
            Assert.That(result.DirectoryAvailable, Is.True);
            Assert.That(result.DirectoryProviderId, Is.EqualTo("entra"));
            Assert.That(result.DirectoryExplanation, Is.EqualTo("Use the object id."));
        });
    }

    /// <summary>
    /// A <see cref="CallInvoker"/> that loops a unary call back into the service:
    /// it marshals the request through the method's real request marshaller,
    /// dispatches into the matching <see cref="LatticeAuthApiGrpcServiceBase"/>
    /// override, then marshals the response through the real response marshaller,
    /// so the round-trip exercises the actual wire contract end to end.
    /// </summary>
    private sealed class LoopbackCallInvoker(LatticeAuthApiGrpcServiceBase service) : CallInvoker
    {
        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        {
            var response = DispatchAsync(method, request, options.CancellationToken);
            return new AsyncUnaryCall<TResponse>(
                response,
                Task.FromResult(new global::Grpc.Core.Metadata()),
                static () => Status.DefaultSuccess,
                static () => new global::Grpc.Core.Metadata(),
                static () => { });
        }

        private async Task<TResponse> DispatchAsync<TRequest, TResponse>(
            Method<TRequest, TResponse> method, TRequest request, CancellationToken cancellationToken)
            where TRequest : class
            where TResponse : class
        {
            var wireRequest = GrpcWireRoundTrip.Through(method.RequestMarshaller, request);
            var context = new LoopbackServerCallContext($"/{method.ServiceName}/{method.Name}", cancellationToken);

            var responseTask = method.Name switch
            {
                LatticeAuthApiGrpcMethods.SearchDirectoryMethodName =>
                    (Task<TResponse>)(object)service.SearchDirectory((DirectorySearchRequest)(object)wireRequest, context),
                LatticeAuthApiGrpcMethods.ResolveDirectoryPrincipalMethodName =>
                    (Task<TResponse>)(object)service.ResolveDirectoryPrincipal((AuthPrincipalRef)(object)wireRequest, context),
                LatticeAuthApiGrpcMethods.GetAccessModelMethodName =>
                    (Task<TResponse>)(object)service.GetAccessModel((AuthAccessModelQuery)(object)wireRequest, context),
                _ => throw new NotSupportedException($"Loopback does not route '{method.Name}'."),
            };

            var response = await responseTask.ConfigureAwait(false);
            return GrpcWireRoundTrip.Through(method.ResponseMarshaller, response);
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new NotSupportedException();

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new NotSupportedException();
    }
}
