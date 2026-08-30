using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Covers the seam's fault mapping: every refusal the tenant-administration
/// facades document lands on its own <see cref="TenantOperationStatus"/>, no two
/// share one, the server's reason survives into the rendered message, and a
/// cancellation the caller asked for propagates rather than being rendered as a
/// failure.
/// </summary>
[TestFixture]
public class TenantFaultMappingTests
{
    private FakeTenantAdminClient _client = null!;
    private TenantAdminService _service = null!;

    /// <summary>
    /// Every documented facade refusal, paired with the status it must classify
    /// to. Used both to assert each mapping and to assert they are all distinct.
    /// </summary>
    private static readonly (Exception Fault, TenantOperationStatus Status)[] DocumentedFaults =
    [
        (new TenantAlreadyExistsException(SampleTenant.TenantId), TenantOperationStatus.AlreadyExists),
        (new TenantNotFoundException(SampleTenant.TenantId), TenantOperationStatus.NotFound),
        (new ReservedTenantOperationException(SampleTenant.TenantId, "delete"), TenantOperationStatus.ReservedTenant),
        (new TenantRegionNotAllowedException(SampleTenant.TenantId, "westeurope"), TenantOperationStatus.RegionNotAllowed),
        (new TenantLastRegionException(SampleTenant.TenantId), TenantOperationStatus.LastRegion),
        (new TenantLastAdminSubjectException(SampleTenant.TenantId, SampleTenant.SubjectId), TenantOperationStatus.LastAdminSubject),
        (new TenantGrantNotFoundException(SampleTenant.TenantId, SampleTenant.OtherTenantId, SampleTenant.Scope), TenantOperationStatus.GrantNotFound),
        (
            new TenantGrantTransitionException(
                SampleTenant.TenantId,
                SampleTenant.OtherTenantId,
                SampleTenant.Scope,
                TenantGrantLifecycleState.Active,
                TenantGrantLifecycleState.Active),
            TenantOperationStatus.GrantTransitionRejected),
        (new LatticeAuthorizationDeniedException("refused"), TenantOperationStatus.Denied),
        (new TenancyUnavailableException("no tenancy here"), TenantOperationStatus.Unavailable),
    ];

    private static IEnumerable<TestCaseData> DocumentedFaultCases()
    {
        foreach (var (fault, status) in DocumentedFaults)
        {
            yield return new TestCaseData(fault, status).SetArgDisplayNames(fault.GetType().Name, status.ToString());
        }
    }

    private static IEnumerable<TestCaseData> TransportStatusCases()
    {
        yield return new TestCaseData(StatusCode.Unimplemented, TenantOperationStatus.Unavailable);
        yield return new TestCaseData(StatusCode.Unauthenticated, TenantOperationStatus.AuthenticationRequired);
        yield return new TestCaseData(StatusCode.NotFound, TenantOperationStatus.NotFound);
        yield return new TestCaseData(StatusCode.AlreadyExists, TenantOperationStatus.AlreadyExists);
        yield return new TestCaseData(StatusCode.InvalidArgument, TenantOperationStatus.InvalidRequest);
        yield return new TestCaseData(StatusCode.FailedPrecondition, TenantOperationStatus.PreconditionFailed);
        yield return new TestCaseData(StatusCode.PermissionDenied, TenantOperationStatus.Denied);

        // A transport "unavailable" is the server being unreachable, which is
        // retryable - never the permanent "this cluster has no such capability".
        yield return new TestCaseData(StatusCode.Unavailable, TenantOperationStatus.Failed);
        yield return new TestCaseData(StatusCode.Internal, TenantOperationStatus.Failed);
        yield return new TestCaseData(StatusCode.DeadlineExceeded, TenantOperationStatus.Failed);
    }

    [SetUp]
    public void SetUp()
    {
        _client = new FakeTenantAdminClient();
        _service = new TenantAdminService(_client);
    }

    [TestCaseSource(nameof(DocumentedFaultCases))]
    public async Task Documented_facade_refusal_maps_to_its_own_status(
        Exception fault,
        TenantOperationStatus expected)
    {
        _client.Throws = fault;

        var result = await _service.GetTenantAsync(SampleTenant.TenantId);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(expected));
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.Value, Is.Null);
            Assert.That(result.Message, Is.EqualTo(fault.Message));
        });
    }

    [Test]
    public void Every_documented_refusal_has_a_status_of_its_own()
    {
        var statuses = DocumentedFaults.Select(pair => pair.Status).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(statuses, Is.Unique, "two documented refusals collapsed onto one status");
            Assert.That(statuses, Has.None.EqualTo(TenantOperationStatus.Succeeded));
            Assert.That(statuses, Has.None.EqualTo(TenantOperationStatus.Failed));
        });
    }

    [TestCaseSource(nameof(TransportStatusCases))]
    public async Task Transport_status_maps_to_its_status(StatusCode code, TenantOperationStatus expected)
    {
        _client.Throws = new RpcException(new Status(code, "the server said so"));

        var result = await _service.GetTenantAsync(SampleTenant.TenantId);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(expected));
            Assert.That(
                result.Message,
                Is.EqualTo("the server said so"),
                "the status detail, not the gRPC status line, is what a panel renders");
        });
    }

    [Test]
    public async Task Transport_precondition_keeps_the_specific_reason_the_wire_could_not_type()
    {
        // The binding collapses all five precondition refusals onto one code and
        // distinguishes them only in the message, so the message is load-bearing.
        const string Detail =
            "The residency change would remove the last resident region of tenant 'acme'.";
        _client.Throws = new RpcException(new Status(StatusCode.FailedPrecondition, Detail));

        var result = await _service.SetResidencyAsync(SampleTenant.TenantId, []);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(TenantOperationStatus.PreconditionFailed));
            Assert.That(result.Message, Is.EqualTo(Detail));
        });
    }

    [Test]
    public async Task A_fault_with_no_detail_still_renders_a_message()
    {
        _client.Throws = new RpcException(new Status(StatusCode.Unimplemented, string.Empty));

        var result = await _service.GetQuotaUsageAsync(SampleTenant.TenantId);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(TenantOperationStatus.Unavailable));
            Assert.That(result.IsUnavailable, Is.True);
            Assert.That(result.Message, Is.Not.Empty);
        });
    }

    [Test]
    public async Task An_unconfigured_endpoint_is_a_failure_not_an_absent_capability()
    {
        _client.Throws = new InvalidOperationException("The explorer is not configured with an endpoint yet.");

        var result = await _service.ListAdminSubjectsAsync(SampleTenant.TenantId);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(TenantOperationStatus.Failed));
            Assert.That(result.IsUnavailable, Is.False);
        });
    }

    [Test]
    public void A_cancellation_the_caller_asked_for_propagates_rather_than_rendering_a_failure()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        _client.Throws = new RpcException(new Status(StatusCode.Cancelled, "cancelled"));

        Assert.That(
            async () => await _service.GetTenantAsync(SampleTenant.TenantId, cts.Token),
            Throws.InstanceOf<RpcException>());
    }

    [Test]
    public async Task A_server_side_cancellation_the_caller_did_not_ask_for_is_a_failure()
    {
        _client.Throws = new RpcException(new Status(StatusCode.Cancelled, "the server gave up"));

        var result = await _service.GetTenantAsync(SampleTenant.TenantId);

        Assert.That(result.Status, Is.EqualTo(TenantOperationStatus.Failed));
    }

    [Test]
    public void An_operation_cancelled_exception_always_propagates()
    {
        _client.Throws = new OperationCanceledException();

        Assert.That(
            async () => await _service.GetCurrentTenantAsync(),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void A_defect_outside_the_known_fault_families_is_not_disguised_as_a_refusal()
    {
        _client.Throws = new NotSupportedException("a genuine defect");

        Assert.That(
            async () => await _service.GetCurrentTenantAsync(),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public async Task Every_operation_folds_a_refusal_into_a_result_rather_than_throwing()
    {
        _client.Throws = new LatticeAuthorizationDeniedException("refused");

        TenantOperationResult[] results =
        [
            await _service.GetCurrentTenantAsync(),
            await _service.ListAccessibleTenantsAsync(),
            await _service.GetTenantAsync(SampleTenant.TenantId),
            await _service.CreateTenantAsync(SampleTenant.TenantId),
            await _service.SuspendTenantAsync(SampleTenant.TenantId),
            await _service.ResumeTenantAsync(SampleTenant.TenantId),
            await _service.DeleteTenantAsync(SampleTenant.TenantId),
            await _service.SetQuotasAsync(SampleTenant.TenantId, ExplorerTenantQuotaLimits.Unbounded),
            await _service.GetQuotaUsageAsync(SampleTenant.TenantId),
            await _service.AuthorizeAllowedRegionsAsync(SampleTenant.TenantId, []),
            await _service.SetResidencyAsync(SampleTenant.TenantId, []),
            await _service.GetRegionStatusAsync(SampleTenant.TenantId),
            await _service.ListAdminSubjectsAsync(SampleTenant.TenantId),
            await _service.AddAdminSubjectAsync(SampleTenant.TenantId, SampleTenant.SubjectId),
            await _service.RemoveAdminSubjectAsync(SampleTenant.TenantId, SampleTenant.SubjectId),
            await _service.ListGrantsAsync(SampleTenant.TenantId),
            await _service.OfferGrantAsync(
                SampleTenant.TenantId,
                SampleTenant.OtherTenantId,
                SampleTenant.Scope,
                ExplorerTenantGrantAccess.Read),
            await _service.ApproveGrantAsync(SampleTenant.TenantId, SampleTenant.OtherTenantId, SampleTenant.Scope),
            await _service.RejectGrantAsync(SampleTenant.TenantId, SampleTenant.OtherTenantId, SampleTenant.Scope),
            await _service.RevokeGrantAsync(SampleTenant.TenantId, SampleTenant.OtherTenantId, SampleTenant.Scope),
        ];

        Assert.Multiple(() =>
        {
            Assert.That(results, Has.Length.EqualTo(20), "every facade operation must be covered here");
            Assert.That(results.Select(r => r.Status), Is.All.EqualTo(TenantOperationStatus.Denied));
            Assert.That(results.Select(r => r.Message), Is.All.EqualTo("refused"));
        });
    }

    [Test]
    public async Task Every_operation_reports_unavailable_when_the_cluster_serves_no_tenancy()
    {
        _client.Throws = new RpcException(new Status(StatusCode.Unimplemented, "not served"));

        TenantOperationResult[] results =
        [
            await _service.GetCurrentTenantAsync(),
            await _service.ListAccessibleTenantsAsync(),
            await _service.GetTenantAsync(SampleTenant.TenantId),
            await _service.GetQuotaUsageAsync(SampleTenant.TenantId),
            await _service.ListAdminSubjectsAsync(SampleTenant.TenantId),
            await _service.ListGrantsAsync(SampleTenant.TenantId),
            await _service.GetRegionStatusAsync(SampleTenant.TenantId),
        ];

        Assert.That(results.Select(r => r.Status), Is.All.EqualTo(TenantOperationStatus.Unavailable));
    }

    [Test]
    public void Fault_mapper_rejects_a_null_exception() =>
        Assert.Multiple(() =>
        {
            Assert.That(() => TenantFaultMapper.Classify(null!), Throws.ArgumentNullException);
            Assert.That(
                () => TenantFaultMapper.Describe(null!, TenantOperationStatus.Failed),
                Throws.ArgumentNullException);
        });

    [Test]
    public void Describe_falls_back_when_the_fault_carries_no_message() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantFaultMapper.Describe(
                    new RpcException(new Status(StatusCode.Unimplemented, string.Empty)),
                    TenantOperationStatus.Unavailable),
                Is.EqualTo("This cluster does not serve tenant administration."));
            Assert.That(
                TenantFaultMapper.Describe(
                    new RpcException(new Status(StatusCode.Internal, "   ")),
                    TenantOperationStatus.Failed),
                Is.EqualTo("The tenancy request failed."));
        });

    [Test]
    public void Valueless_result_helpers_reject_a_null_message() =>
        Assert.Multiple(() =>
        {
            Assert.That(() => TenantOperationResult.Success(null!), Throws.ArgumentNullException);
            Assert.That(
                () => TenantOperationResult.Failure(TenantOperationStatus.Failed, null!),
                Throws.ArgumentNullException);
            Assert.That(() => TenantOperationResult<int>.Success(1, null!), Throws.ArgumentNullException);
            Assert.That(
                () => TenantOperationResult<int>.Failure(TenantOperationStatus.Failed, null!),
                Throws.ArgumentNullException);
        });

    [Test]
    public void A_valueless_success_reports_success_and_a_failure_does_not() =>
        Assert.Multiple(() =>
        {
            Assert.That(TenantOperationResult.Success("done").IsSuccess, Is.True);
            Assert.That(
                TenantOperationResult.Failure(TenantOperationStatus.Unavailable, "gone").IsUnavailable,
                Is.True);
            Assert.That(TenantFaultMapper.Fail(new TenantNotFoundException("x")).Status,
                Is.EqualTo(TenantOperationStatus.NotFound));
        });
}
