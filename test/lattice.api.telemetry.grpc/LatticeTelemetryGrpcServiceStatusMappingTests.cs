using System.Net.Http;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Coverage for the server-side service's translation of the telemetry facade's
/// typed refusals onto gRPC status codes. Every arm matters: a refusal that falls
/// through to the catch-all reaches the caller as an opaque <c>Internal</c>, which
/// hides an actionable reason and invites a client to retry a decision that will
/// never change.
/// </summary>
[TestFixture]
public sealed class LatticeTelemetryGrpcServiceStatusMappingTests
{
    private ServiceProvider _serializers = null!;
    private FakeTelemetry _facade = null!;
    private LatticeTelemetryGrpcService _service = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = TelemetryGrpcTestSupport.Serializers();
        _facade = new FakeTelemetry();
        _service = TelemetryGrpcTestSupport.Service(_serializers, _facade);
    }

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private Task<TelemetryQueryResponse> QueryAsync(CancellationToken cancellationToken = default)
        => _service.Query(
            new TelemetryQueryRequest { QueryId = "lattice.ops.rate" },
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName),
                cancellationToken: cancellationToken));

    private static StatusCode StatusOf(Func<Task> call)
    {
        var exception = Assert.ThrowsAsync<RpcException>(() => call());
        return exception!.StatusCode;
    }

    [Test]
    public void An_unknown_or_unoffered_query_maps_to_not_found()
    {
        _facade.Throw = new TelemetryQueryNotFoundException("lattice.ops.rate");

        Assert.That(StatusOf(() => QueryAsync()), Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public void A_backend_fault_maps_to_unavailable_not_a_caller_error()
    {
        _facade.Throw = new TelemetryBackendException(
            "lattice.ops.rate",
            "The telemetry backend request failed: No such host is known (metrics.internal:9090).");

        Assert.That(
            StatusOf(() => QueryAsync()),
            Is.EqualTo(StatusCode.Unavailable),
            "A backend outage is not the caller's fault. Presenting it as a caller error would make "
            + "a client abandon a transient outage as though its query were permanently invalid.");
    }

    [Test]
    public void A_backend_fault_is_mapped_apart_from_every_caller_error()
    {
        // The three typed refusals must land on three different codes, or a client
        // cannot tell "fix your request" from "retry later".
        var codes = new List<StatusCode>();

        _facade.Throw = new TelemetryQueryNotFoundException("q");
        codes.Add(StatusOf(() => QueryAsync()));
        _facade.Throw = new TelemetryQueryBoundsException("q", TelemetryBoundsViolation.RangeTooLong);
        codes.Add(StatusOf(() => QueryAsync()));
        _facade.Throw = new TelemetryBackendException("q", "backend down");
        codes.Add(StatusOf(() => QueryAsync()));

        Assert.Multiple(() =>
        {
            Assert.That(codes, Is.Unique);
            Assert.That(codes, Does.Not.Contain(StatusCode.InvalidArgument));
            Assert.That(
                codes[2],
                Is.EqualTo(StatusCode.Unavailable),
                "Only the backend fault is retryable-with-backoff.");
        });
    }

    [Test]
    public void A_backend_fault_does_not_leak_the_backend_address_to_the_caller()
    {
        _facade.Throw = new TelemetryBackendException(
            "lattice.ops.rate",
            "The telemetry backend request failed: No such host is known (metrics.internal:9090).",
            new HttpRequestException("No such host is known (metrics.internal:9090)."));

        var exception = Assert.ThrowsAsync<RpcException>(() => QueryAsync());

        Assert.Multiple(() =>
        {
            Assert.That(exception!.Status.Detail, Does.Not.Contain("metrics.internal"));
            Assert.That(exception.Status.Detail, Does.Not.Contain("9090"));
            Assert.That(
                exception.Status.Detail,
                Does.Contain("lattice.ops.rate"),
                "The query id came from the caller, so echoing it reveals nothing and aids diagnosis.");
        });
    }

    [Test]
    public void A_backend_fault_on_the_catalogue_rpc_also_maps_to_unavailable()
    {
        _facade.Throw = new TelemetryBackendException("catalog", "backend down");

        var exception = Assert.ThrowsAsync<RpcException>(() => _service.GetCatalog(
            new TelemetryCatalogRequest(),
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetCatalogMethodName))));

        Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.Unavailable));
    }

    [Test]
    public void An_unconfigured_backend_stays_a_not_found_rather_than_an_availability_fault()
    {
        // The facade reports an unconfigured backend as not-found so discovery and
        // execution agree: a catalogue that offered nothing cannot then refuse a
        // query for a different-looking reason. The transport must not "helpfully"
        // upgrade that to Unavailable.
        _facade.Throw = new TelemetryQueryNotFoundException(
            "lattice.ops.rate",
            "Telemetry query 'lattice.ops.rate' is not available.");

        Assert.That(StatusOf(() => QueryAsync()), Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public void A_capability_denial_stays_permission_denied_and_never_becomes_not_found()
    {
        // QueryAsync propagates the capability denial while GetCatalogAsync degrades
        // to an empty catalogue. Collapsing the denial into a not-found would erase
        // the difference between "you may not read telemetry" and "no such query".
        _facade.Throw = new LatticeAuthorizationDeniedException("denied");

        Assert.That(StatusOf(() => QueryAsync()), Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void A_bounds_violation_maps_to_out_of_range()
    {
        _facade.Throw = new TelemetryQueryBoundsException(
            "lattice.ops.rate",
            TelemetryBoundsViolation.RangeTooLong);

        Assert.That(StatusOf(() => QueryAsync()), Is.EqualTo(StatusCode.OutOfRange));
    }

    [Test]
    public void A_bounds_violation_preserves_the_typed_reason_in_the_message()
    {
        _facade.Throw = new TelemetryQueryBoundsException(
            "lattice.ops.rate",
            TelemetryBoundsViolation.StepBelowMinimum);

        var exception = Assert.ThrowsAsync<RpcException>(() => QueryAsync());

        Assert.That(exception!.Status.Detail, Does.Contain(nameof(TelemetryBoundsViolation.StepBelowMinimum)));
    }

    [Test]
    public void An_authorization_denial_maps_to_permission_denied()
    {
        _facade.Throw = new LatticeAuthorizationDeniedException("denied");

        Assert.That(StatusOf(() => QueryAsync()), Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void A_tenant_access_denial_maps_to_permission_denied_not_internal()
    {
        _facade.Throw = new LatticeTenantAccessDeniedException("no such active tenant");

        Assert.That(
            StatusOf(() => QueryAsync()),
            Is.EqualTo(StatusCode.PermissionDenied),
            "A fail-closed tenant resolution is an authorization outcome, not a server fault.");
    }

    [Test]
    public void A_bad_argument_maps_to_invalid_argument()
    {
        _facade.Throw = new ArgumentException("bad step");

        Assert.That(StatusOf(() => QueryAsync()), Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void A_cancellation_maps_to_cancelled()
    {
        _facade.Throw = new OperationCanceledException();

        Assert.That(StatusOf(() => QueryAsync()), Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void An_rpc_exception_from_the_facade_passes_through_unchanged()
    {
        _facade.Throw = new RpcException(new Status(StatusCode.Unavailable, "backend down"));

        var exception = Assert.ThrowsAsync<RpcException>(() => QueryAsync());

        Assert.Multiple(() =>
        {
            Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.Unavailable));
            Assert.That(exception.Status.Detail, Is.EqualTo("backend down"));
        });
    }

    [Test]
    public void An_unexpected_fault_maps_to_internal_without_leaking_the_message()
    {
        _facade.Throw = new InvalidOperationException("connection string secret-value");

        var exception = Assert.ThrowsAsync<RpcException>(() => QueryAsync());

        Assert.Multiple(() =>
        {
            Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(exception.Status.Detail, Does.Not.Contain("secret-value"));
        });
    }

    [Test]
    public void The_catalogue_rpc_maps_refusals_the_same_way()
    {
        _facade.Throw = new LatticeAuthorizationDeniedException("denied");

        var exception = Assert.ThrowsAsync<RpcException>(() => _service.GetCatalog(
            new TelemetryCatalogRequest(),
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetCatalogMethodName))));

        Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }
}
