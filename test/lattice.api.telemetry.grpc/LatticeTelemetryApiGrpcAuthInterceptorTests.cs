using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Coverage for the telemetry authorization interceptor: the fail-closed default,
/// the single unauthenticated exemption, the per-operation decode handed to a
/// host policy, and the deliberate scoping to this binding's own service so a
/// co-hosted gRPC service is unaffected.
/// </summary>
[TestFixture]
public sealed class LatticeTelemetryApiGrpcAuthInterceptorTests
{
    private static LatticeTelemetryApiGrpcAuthInterceptor Interceptor(
        ILatticeTelemetryApiAuthorizer authorizer,
        LatticeTelemetryApiGrpcOptions? options = null)
        => new(
            authorizer,
            new StaticOptionsMonitor(options ?? new LatticeTelemetryApiGrpcOptions()),
            NullLogger<LatticeTelemetryApiGrpcAuthInterceptor>.Instance);

    private static Task<TelemetryQueryResponse> Continuation(TelemetryQueryRequest request, ServerCallContext context)
        => Task.FromResult(new TelemetryQueryResponse
        {
            QueryId = request.QueryId,
            Scope = TelemetryTenantScope.PinnedTo("t", TelemetryTenantVisibility.ActiveTenant),
            Series = [],
        });

    private static Task<TResponse> Invoke<TRequest, TResponse>(
        LatticeTelemetryApiGrpcAuthInterceptor interceptor,
        string methodName,
        TRequest request,
        UnaryServerMethod<TRequest, TResponse> continuation,
        CancellationToken cancellationToken = default)
        where TRequest : class
        where TResponse : class
        => interceptor.UnaryServerHandler(
            request,
            new FakeServerCallContext(methodName, cancellationToken: cancellationToken),
            continuation);

    [Test]
    public void The_default_authorizer_denies_every_call()
    {
        var exception = Assert.ThrowsAsync<RpcException>(() => Invoke(
            Interceptor(new DenyTelemetryApiAuthorizer()),
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName),
            new TelemetryQueryRequest { QueryId = "lattice.ops.rate" },
            Continuation));

        Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void A_denied_call_never_reaches_the_facade()
    {
        var reached = false;

        Assert.ThrowsAsync<RpcException>(() => Invoke(
            Interceptor(new DenyTelemetryApiAuthorizer()),
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetCatalogMethodName),
            new TelemetryCatalogRequest(),
            (_, _) =>
            {
                reached = true;
                return Task.FromResult(TelemetryQueryCatalog.Empty);
            }));

        Assert.That(reached, Is.False);
    }

    [Test]
    public async Task A_permissive_authorizer_lets_the_call_through()
    {
        var response = await Invoke(
            Interceptor(new AllowAllTelemetryApiAuthorizer()),
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName),
            new TelemetryQueryRequest { QueryId = "lattice.ops.rate" },
            Continuation);

        Assert.That(response.QueryId, Is.EqualTo("lattice.ops.rate"));
    }

    [Test]
    public async Task The_unauthenticated_advertisement_rpc_is_exempt_from_the_deny_gate()
    {
        var authorizer = new RecordingAuthorizer(verdict: false);

        var advertisement = await Invoke(
            Interceptor(authorizer),
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetAuthSchemeMethodName),
            new AuthSchemeAdvertisementRequest(),
            (_, _) => Task.FromResult(new AuthSchemeAdvertisement()));

        Assert.Multiple(() =>
        {
            Assert.That(advertisement, Is.Not.Null);
            Assert.That(
                authorizer.CallCount,
                Is.Zero,
                "A client must be able to discover how to sign in before it holds a credential.");
        });
    }

    [Test]
    public async Task A_call_to_an_unrelated_service_is_not_gated()
    {
        var authorizer = new RecordingAuthorizer(verdict: false);

        var response = await Invoke(
            Interceptor(authorizer),
            "/some.other.service/Rpc",
            new TelemetryQueryRequest { QueryId = "lattice.ops.rate" },
            Continuation);

        Assert.Multiple(() =>
        {
            Assert.That(response, Is.Not.Null);
            Assert.That(authorizer.CallCount, Is.Zero);
        });
    }

    [Test]
    public async Task Enforcement_is_skipped_when_the_host_turns_it_off()
    {
        var authorizer = new RecordingAuthorizer(verdict: false);

        var response = await Invoke(
            Interceptor(authorizer, new LatticeTelemetryApiGrpcOptions { RequireAuthorization = false }),
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName),
            new TelemetryQueryRequest { QueryId = "lattice.ops.rate" },
            Continuation);

        Assert.Multiple(() =>
        {
            Assert.That(response, Is.Not.Null);
            Assert.That(authorizer.CallCount, Is.Zero);
        });
    }

    [Test]
    public void RequireAuthorization_defaults_to_enforcing()
        => Assert.That(new LatticeTelemetryApiGrpcOptions().RequireAuthorization, Is.True);

    [Test]
    public async Task The_query_rpc_is_described_by_operation_and_query_id()
    {
        var authorizer = new RecordingAuthorizer(verdict: true);

        await Invoke(
            Interceptor(authorizer),
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName),
            new TelemetryQueryRequest { QueryId = "lattice.ops.rate" },
            Continuation);

        Assert.Multiple(() =>
        {
            Assert.That(authorizer.LastOperation, Is.EqualTo(LatticeTelemetryApiOperation.Query));
            Assert.That(authorizer.LastTargetId, Is.EqualTo("lattice.ops.rate"));
        });
    }

    [Test]
    public async Task The_catalogue_rpc_is_described_with_no_target()
    {
        var authorizer = new RecordingAuthorizer(verdict: true);

        await Invoke(
            Interceptor(authorizer),
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetCatalogMethodName),
            new TelemetryCatalogRequest(),
            (_, _) => Task.FromResult(TelemetryQueryCatalog.Empty));

        Assert.Multiple(() =>
        {
            Assert.That(authorizer.LastOperation, Is.EqualTo(LatticeTelemetryApiOperation.GetCatalog));
            Assert.That(authorizer.LastTargetId, Is.Null);
        });
    }

    [Test]
    public async Task An_unmapped_method_is_described_as_unknown_so_a_deny_policy_refuses_it()
    {
        var authorizer = new RecordingAuthorizer(verdict: true);

        await Invoke(
            Interceptor(authorizer),
            TelemetryGrpcTestSupport.FullMethod("SomeFutureRpc"),
            new TelemetryCatalogRequest(),
            (_, _) => Task.FromResult(TelemetryQueryCatalog.Empty));

        Assert.That(
            authorizer.LastOperation,
            Is.EqualTo(LatticeTelemetryApiOperation.Unknown),
            "An unmapped RPC must never masquerade as a benign known one.");
    }

    [Test]
    public void A_cancelled_authorization_check_maps_to_cancelled()
    {
        var exception = Assert.ThrowsAsync<RpcException>(() => Invoke(
            Interceptor(new CancellingAuthorizer()),
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName),
            new TelemetryQueryRequest { QueryId = "lattice.ops.rate" },
            Continuation));

        Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void The_interceptor_rejects_null_arguments()
    {
        var interceptor = Interceptor(new AllowAllTelemetryApiAuthorizer());
        var context = new FakeServerCallContext(
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName));
        var request = new TelemetryQueryRequest { QueryId = "q" };

        Assert.Multiple(() =>
        {
            Assert.That(
                () => interceptor.UnaryServerHandler<TelemetryQueryRequest, TelemetryQueryResponse>(
                    null!, context, Continuation),
                Throws.ArgumentNullException);
            Assert.That(
                () => interceptor.UnaryServerHandler<TelemetryQueryRequest, TelemetryQueryResponse>(
                    request, null!, Continuation),
                Throws.ArgumentNullException);
            Assert.That(
                () => interceptor.UnaryServerHandler<TelemetryQueryRequest, TelemetryQueryResponse>(
                    request, context, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_interceptor_rejects_null_construction_arguments()
    {
        var options = new StaticOptionsMonitor(new LatticeTelemetryApiGrpcOptions());

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new LatticeTelemetryApiGrpcAuthInterceptor(
                    null!, options, NullLogger<LatticeTelemetryApiGrpcAuthInterceptor>.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeTelemetryApiGrpcAuthInterceptor(
                    new AllowAllTelemetryApiAuthorizer(), null!, NullLogger<LatticeTelemetryApiGrpcAuthInterceptor>.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeTelemetryApiGrpcAuthInterceptor(
                    new AllowAllTelemetryApiAuthorizer(), options, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void DescribeCall_reads_the_query_id_from_a_query_request()
    {
        var (operation, targetId) = LatticeTelemetryApiGrpcAuthInterceptor.DescribeCall(
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName),
            new TelemetryQueryRequest { QueryId = "lattice.latency.p99" });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeTelemetryApiOperation.Query));
            Assert.That(targetId, Is.EqualTo("lattice.latency.p99"));
        });
    }

    [Test]
    public void DescribeCall_never_surfaces_a_wire_supplied_tenant_as_the_target()
    {
        var (_, targetId) = LatticeTelemetryApiGrpcAuthInterceptor.DescribeCall(
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName),
            new TelemetryQueryRequest
            {
                QueryId = "lattice.ops.rate",
                RequestedVisibility = TelemetryTenantVisibility.SingleTenant,
                RequestedTenantId = "other-tenant",
            });

        Assert.That(
            targetId,
            Is.EqualTo("lattice.ops.rate"),
            "The authorizer target is the curated query id; handing it a caller-controlled tenant "
            + "would invite a host policy to decide on a value the wire controls.");
    }

    [Test]
    public void IsUnauthenticatedMethod_exempts_only_the_advertisement_rpc()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeTelemetryApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                    TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetAuthSchemeMethodName)),
                Is.True);
            Assert.That(
                LatticeTelemetryApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                    TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetCatalogMethodName)),
                Is.False);
            Assert.That(
                LatticeTelemetryApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                    TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName)),
                Is.False);
        });
    }

    /// <summary>An authorizer that always answers cancellation.</summary>
    private sealed class CancellingAuthorizer : ILatticeTelemetryApiAuthorizer
    {
        public Task<bool> IsAuthorizedAsync(
            LatticeTelemetryApiAuthorizationContext authorizationContext,
            CancellationToken cancellationToken)
            => throw new OperationCanceledException();
    }

    /// <summary>A fixed <see cref="IOptionsMonitor{T}"/> over one options instance.</summary>
    private sealed class StaticOptionsMonitor(LatticeTelemetryApiGrpcOptions value)
        : IOptionsMonitor<LatticeTelemetryApiGrpcOptions>
    {
        public LatticeTelemetryApiGrpcOptions CurrentValue => value;

        public LatticeTelemetryApiGrpcOptions Get(string? name) => value;

        public IDisposable? OnChange(Action<LatticeTelemetryApiGrpcOptions, string?> listener) => null;
    }
}
