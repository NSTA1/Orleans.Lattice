using Grpc.Core;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Covers the fault mapping: three different failures stay three different
/// statuses, whichever side of the wire the facade is on.
/// </summary>
/// <remarks>
/// <para>
/// The distinction that matters most is between a caller error and a backend
/// fault. A backend outage presented as an invalid query makes a user abandon a
/// query that was fine; an invalid query presented as an outage makes a user
/// retry one that will never work. The facade separates them, the binding
/// separates them, and this seam must not put them back together.
/// </para>
/// </remarks>
[TestFixture]
public class TelemetryFaultMappingTests
{
    private FakeTelemetryQueryClient _client = null!;

    [SetUp]
    public void SetUp() => _client = new FakeTelemetryQueryClient();

    private async Task<TelemetryOperationResult> QueryFailingWith(Exception fault)
    {
        _client.QueryThrows = fault;
        return await new TelemetryQueryService(_client)
            .QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));
    }

    [Test]
    public async Task An_unknown_or_unoffered_query_is_a_caller_error()
    {
        var typed = await QueryFailingWith(new TelemetryQueryNotFoundException(SampleTelemetry.UnknownQueryId));
        var overTheWire = await QueryFailingWith(new RpcException(new Status(StatusCode.NotFound, "no such query")));

        Assert.Multiple(() =>
        {
            Assert.That(typed.Status, Is.EqualTo(TelemetryQueryStatus.UnknownQuery));
            Assert.That(overTheWire.Status, Is.EqualTo(TelemetryQueryStatus.UnknownQuery));
            Assert.That(typed.IsRetryable, Is.False, "an unknown query never becomes known by retrying");
        });
    }

    [Test]
    public async Task A_bounds_refusal_is_a_caller_error_of_its_own()
    {
        var typed = await QueryFailingWith(
            new TelemetryQueryBoundsException(SampleTelemetry.RangeQueryId, TelemetryBoundsViolation.RangeTooLong));
        var overTheWire = await QueryFailingWith(new RpcException(new Status(StatusCode.OutOfRange, "too long")));

        Assert.Multiple(() =>
        {
            Assert.That(typed.Status, Is.EqualTo(TelemetryQueryStatus.OutOfBounds));
            Assert.That(overTheWire.Status, Is.EqualTo(TelemetryQueryStatus.OutOfBounds));
            Assert.That(typed.IsRetryable, Is.False);
        });
    }

    [Test]
    public async Task A_backend_fault_is_retryable_and_never_reads_as_a_bad_query()
    {
        var typed = await QueryFailingWith(
            new TelemetryBackendException(SampleTelemetry.RangeQueryId, "the metrics store timed out"));
        var overTheWire = await QueryFailingWith(
            new RpcException(new Status(StatusCode.Unavailable, "backend fault; retry with backoff")));

        Assert.Multiple(() =>
        {
            Assert.That(typed.Status, Is.EqualTo(TelemetryQueryStatus.BackendUnavailable));
            Assert.That(overTheWire.Status, Is.EqualTo(TelemetryQueryStatus.BackendUnavailable));
            Assert.That(typed.IsRetryable, Is.True);
            Assert.That(overTheWire.IsRetryable, Is.True);
            Assert.That(
                typed.Status,
                Is.Not.EqualTo(TelemetryQueryStatus.InvalidRequest),
                "presenting an outage as an invalid query makes a user abandon a query that was fine");
            Assert.That(
                typed.Status,
                Is.Not.EqualTo(TelemetryQueryStatus.UnknownQuery));
        });
    }

    [Test]
    public async Task A_backend_fault_is_not_an_absent_capability()
    {
        // Hiding the surface on a transient backend failure would make a metrics
        // store hiccup look like the telemetry add-on being uninstalled.
        var result = await QueryFailingWith(new RpcException(new Status(StatusCode.Unavailable, "connection refused")));

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(TelemetryQueryStatus.BackendUnavailable));
            Assert.That(result.IsUnavailable, Is.False);
        });
    }

    [Test]
    public async Task The_three_failure_kinds_are_three_distinct_statuses()
    {
        var unknown = await QueryFailingWith(new TelemetryQueryNotFoundException(SampleTelemetry.UnknownQueryId));
        var bounds = await QueryFailingWith(
            new TelemetryQueryBoundsException(SampleTelemetry.RangeQueryId, TelemetryBoundsViolation.TooManyPoints));
        var backend = await QueryFailingWith(new TelemetryBackendException(SampleTelemetry.RangeQueryId, "down"));

        Assert.That(
            new[] { unknown.Status, bounds.Status, backend.Status }.Distinct(),
            Has.Exactly(3).Items);
    }

    [Test]
    public async Task An_absent_facade_is_unavailable_rather_than_denied_or_retryable()
    {
        var typed = await QueryFailingWith(new TelemetryUnavailableException("not served here"));
        var overTheWire = await QueryFailingWith(
            new RpcException(new Status(StatusCode.Unimplemented, "not served here")));

        Assert.Multiple(() =>
        {
            Assert.That(typed.Status, Is.EqualTo(TelemetryQueryStatus.Unavailable));
            Assert.That(overTheWire.Status, Is.EqualTo(TelemetryQueryStatus.Unavailable));
            Assert.That(typed.IsUnavailable, Is.True);
            Assert.That(typed.IsRetryable, Is.False);
            Assert.That(typed.Message, Is.EqualTo("not served here"));
        });
    }

    [Test]
    public async Task A_denial_and_a_missing_credential_stay_apart()
    {
        var denied = await QueryFailingWith(new LatticeAuthorizationDeniedException("not yours"));
        var deniedOverTheWire = await QueryFailingWith(
            new RpcException(new Status(StatusCode.PermissionDenied, "not yours")));
        var unauthenticated = await QueryFailingWith(
            new RpcException(new Status(StatusCode.Unauthenticated, "sign in")));

        Assert.Multiple(() =>
        {
            Assert.That(denied.Status, Is.EqualTo(TelemetryQueryStatus.Denied));
            Assert.That(deniedOverTheWire.Status, Is.EqualTo(TelemetryQueryStatus.Denied));
            Assert.That(
                unauthenticated.Status,
                Is.EqualTo(TelemetryQueryStatus.AuthenticationRequired),
                "presenting no credential is recoverable by signing in; being refused one is not");
        });
    }

    [Test]
    public async Task A_malformed_request_and_an_unclassified_fault_stay_apart()
    {
        var invalid = await QueryFailingWith(new RpcException(new Status(StatusCode.InvalidArgument, "bad tree id")));
        var internalFault = await QueryFailingWith(new RpcException(new Status(StatusCode.Internal, "boom")));
        var unconfigured = await QueryFailingWith(new InvalidOperationException("no endpoint yet"));

        Assert.Multiple(() =>
        {
            Assert.That(invalid.Status, Is.EqualTo(TelemetryQueryStatus.InvalidRequest));
            Assert.That(internalFault.Status, Is.EqualTo(TelemetryQueryStatus.Failed));
            Assert.That(unconfigured.Status, Is.EqualTo(TelemetryQueryStatus.Failed));
        });
    }

    [Test]
    public async Task A_deadline_is_treated_as_a_backend_fault_because_retrying_may_work()
    {
        var result = await QueryFailingWith(new RpcException(new Status(StatusCode.DeadlineExceeded, "too slow")));

        Assert.That(result.Status, Is.EqualTo(TelemetryQueryStatus.BackendUnavailable));
    }

    [Test]
    public async Task A_fault_with_no_detail_still_carries_a_message()
    {
        var unavailable = await QueryFailingWith(new RpcException(new Status(StatusCode.Unimplemented, string.Empty)));
        var backend = await QueryFailingWith(new RpcException(new Status(StatusCode.Unavailable, string.Empty)));
        var other = await QueryFailingWith(new RpcException(new Status(StatusCode.Internal, string.Empty)));

        Assert.Multiple(() =>
        {
            Assert.That(unavailable.Message, Is.Not.Empty);
            Assert.That(backend.Message, Is.Not.Empty);
            Assert.That(other.Message, Is.Not.Empty);
        });
    }

    [Test]
    public void The_callers_own_cancellation_escapes_rather_than_becoming_a_rendered_failure()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        _client.QueryThrows = new RpcException(new Status(StatusCode.Cancelled, "cancelled"));

        Assert.That(
            async () => await new TelemetryQueryService(_client)
                .QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId), cts.Token),
            Throws.InstanceOf<RpcException>());
    }

    [Test]
    public async Task A_server_side_cancellation_the_caller_did_not_ask_for_is_a_failure()
    {
        var result = await QueryFailingWith(new RpcException(new Status(StatusCode.Cancelled, "server gave up")));

        Assert.That(result.Status, Is.EqualTo(TelemetryQueryStatus.Failed));
    }

    [Test]
    public void An_operation_cancelled_exception_is_never_swallowed()
    {
        _client.QueryThrows = new OperationCanceledException();

        Assert.That(
            async () => await new TelemetryQueryService(_client)
                .QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task The_catalogue_read_is_classified_by_the_same_rules()
    {
        _client.CatalogThrows = new RpcException(new Status(StatusCode.Unimplemented, "no telemetry here"));

        var result = await new TelemetryQueryService(_client).GetCatalogAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.Status, Is.EqualTo(TelemetryQueryStatus.Unavailable));
            Assert.That(result.Value, Is.Null);
        });
    }

    [Test]
    public void A_result_reports_its_own_shape() =>
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryOperationResult.Success("done").IsSuccess, Is.True);
            Assert.That(TelemetryOperationResult.Success("done").Message, Is.EqualTo("done"));
            Assert.That(TelemetryOperationResult.Success("done").Violation, Is.EqualTo(ExplorerTelemetryBoundsViolation.None));
            Assert.That(
                TelemetryOperationResult.Failure(TelemetryQueryStatus.Unavailable, "gone").IsUnavailable,
                Is.True);
            Assert.That(
                TelemetryOperationResult
                    .Failure(TelemetryQueryStatus.OutOfBounds, "bad", ExplorerTelemetryBoundsViolation.RangeTooLong)
                    .Violation,
                Is.EqualTo(ExplorerTelemetryBoundsViolation.RangeTooLong));
            Assert.That(() => TelemetryOperationResult.Success(null!), Throws.ArgumentNullException);
            Assert.That(
                () => TelemetryOperationResult.Failure(TelemetryQueryStatus.Failed, null!),
                Throws.ArgumentNullException);
        });

    [Test]
    public void A_valued_result_carries_its_value_only_on_success() =>
        Assert.Multiple(() =>
        {
            var success = TelemetryOperationResult<ExplorerTelemetryCatalog>.Success(
                ExplorerTelemetryCatalog.Empty,
                "read");
            var failure = TelemetryOperationResult<ExplorerTelemetryCatalog>.Failure(
                TelemetryQueryStatus.Denied,
                "no");

            Assert.That(success.Value, Is.SameAs(ExplorerTelemetryCatalog.Empty));
            Assert.That(failure.Value, Is.Null);
            Assert.That(failure.Status, Is.EqualTo(TelemetryQueryStatus.Denied));
            Assert.That(
                () => TelemetryOperationResult<ExplorerTelemetryCatalog>.Success(ExplorerTelemetryCatalog.Empty, null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => TelemetryOperationResult<ExplorerTelemetryCatalog>.Failure(TelemetryQueryStatus.Failed, null!),
                Throws.ArgumentNullException);
        });

    [Test]
    public void The_unavailable_exception_carries_a_message_on_every_shape() =>
        Assert.Multiple(() =>
        {
            Assert.That(new TelemetryUnavailableException().Message, Is.Not.Empty);
            Assert.That(new TelemetryUnavailableException("gone").Message, Is.EqualTo("gone"));
            Assert.That(
                new TelemetryUnavailableException("gone", new InvalidOperationException()).InnerException,
                Is.InstanceOf<InvalidOperationException>());
        });

    [Test]
    public void The_classifier_rejects_a_null_fault() =>
        Assert.That(() => TelemetryFaultMapper.Classify(null!), Throws.ArgumentNullException);
}
