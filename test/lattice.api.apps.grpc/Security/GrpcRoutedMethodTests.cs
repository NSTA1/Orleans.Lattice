using Grpc.AspNetCore.Server;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests.Security;

/// <summary>
/// Pins that the binding's authorization interceptor recognises a call by the gRPC
/// method it was routed to, never by the raw request path. ASP.NET Core endpoint
/// routing matches a path case-insensitively, so a re-cased path still reaches the
/// canonical handler; an ordinal test of the raw path let such a call skip
/// authorization entirely.
/// </summary>
[TestFixture]
public sealed class GrpcRoutedMethodTests
{
    private static readonly Marshaller<object> Opaque = Marshallers.Create<object>(_ => [], _ => new object());

    private static readonly Method<object, object> Canonical =
        new(MethodType.Unary, LatticeAppsGrpcMethods.ServiceName, "List", Opaque, Opaque);

    private static string Recased =>
        "/" + Canonical.ServiceName.ToUpperInvariant() + "/" + Canonical.Name.ToUpperInvariant();

    [Test]
    public void Resolve_returns_the_canonical_name_of_the_routed_method()
    {
        var context = new RoutedCallContext(Recased, Canonical, hosted: true);

        Assert.That(GrpcRoutedMethod.Resolve(context), Is.EqualTo(Canonical.FullName));
    }

    [Test]
    public void Resolve_falls_back_to_the_raw_method_for_a_call_no_router_dispatched()
    {
        var context = new RoutedCallContext(Recased, routedTo: null, hosted: false);

        Assert.That(GrpcRoutedMethod.Resolve(context), Is.EqualTo(Recased));
    }

    [Test]
    public void Resolve_falls_back_to_the_raw_method_when_the_endpoint_names_no_grpc_method()
    {
        var context = new RoutedCallContext(Recased, routedTo: null, hosted: true);

        Assert.That(GrpcRoutedMethod.Resolve(context), Is.EqualTo(Recased));
    }

    [Test]
    public void A_recased_path_is_authorized_as_the_method_it_was_routed_to()
    {
        LatticeAppsApiAuthorizationContext? seen = null;
        var authorizer = Substitute.For<ILatticeAppsApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Do<LatticeAppsApiAuthorizationContext>(c => seen = c), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(false));
        var options = Substitute.For<IOptionsMonitor<LatticeAppsApiGrpcOptions>>();
        options.CurrentValue.Returns(new LatticeAppsApiGrpcOptions());
        var interceptor = new LatticeAppsApiGrpcAuthInterceptor(authorizer, options, NullLogger<LatticeAppsApiGrpcAuthInterceptor>.Instance);
        var reached = false;

        var ex = Assert.ThrowsAsync<RpcException>(() => interceptor.UnaryServerHandler<AppsEmptyRequest, object>(
            new AppsEmptyRequest(),
            new RoutedCallContext(Recased, Canonical, hosted: true),
            (_, _) =>
            {
                reached = true;
                return Task.FromResult(new object());
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(reached, Is.False, "A refused call must never reach the service.");
            Assert.That(seen?.Operation, Is.EqualTo(LatticeAppsApiOperation.List), "The authorizer must see the routed operation.");
        });
    }

    /// <summary>
    /// A server call context carrying a raw method path and, when hosted, the
    /// ASP.NET Core request whose routed endpoint names the canonical method, as
    /// Grpc.AspNetCore exposes it through the call's user state.
    /// </summary>
    private sealed class RoutedCallContext : ServerCallContext
    {
        private readonly string _method;

        public RoutedCallContext(string method, IMethod? routedTo, bool hosted)
        {
            _method = method;
            if (hosted)
            {
                var httpContext = new DefaultHttpContext();
                var metadata = routedTo is null
                    ? new EndpointMetadataCollection()
                    : new EndpointMetadataCollection(new GrpcMethodMetadata(typeof(object), routedTo));
                httpContext.SetEndpoint(new Endpoint(_ => Task.CompletedTask, metadata, "routed"));
                UserState["__HttpContext"] = httpContext;
            }
        }

        protected override string MethodCore => _method;

        protected override string HostCore => "localhost";

        protected override string PeerCore => "ipv4:127.0.0.1:0";

        protected override DateTime DeadlineCore => DateTime.MaxValue;

        protected override global::Grpc.Core.Metadata RequestHeadersCore { get; } = new();

        protected override CancellationToken CancellationTokenCore => CancellationToken.None;

        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();

        protected override Status StatusCore { get; set; }

        protected override WriteOptions? WriteOptionsCore { get; set; }

        protected override AuthContext AuthContextCore { get; } =
            new(null, new Dictionary<string, List<AuthProperty>>());

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) =>
            Task.CompletedTask;
    }
}