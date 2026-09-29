using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Closes the fault-translation parity gap between the service's four optional
/// facade invocation helpers - quota usage, cross-tenant grants, tenant access
/// administration, and region residency.
/// </summary>
/// <remarks>
/// <para>
/// Each of those helpers carries its own private copy of the same catch ladder.
/// The copies are near-identical, which is exactly why their coverage diverged:
/// an arm proved on one helper reads as proved everywhere, so the arms below
/// went untested on one or more helpers even though the ladder they belong to
/// was, as a shape, well covered. The admin, self-service, and region helpers
/// already had the pass-through and cancellation arms proved in
/// <see cref="LatticeTenantAdminGrpcServiceStatusMappingTests"/>; the quota,
/// grant, and access-administration copies did not.
/// </para>
/// <para>
/// The two arms this fixture is mostly about are the ones whose failure mode is
/// silent. An <see cref="RpcException"/> the facade already shaped must pass
/// through with its status <em>and</em> detail intact rather than being
/// re-wrapped as <see cref="StatusCode.Internal"/>, and a cancellation must
/// become <see cref="StatusCode.Cancelled"/> rather than a server fault: both
/// would otherwise report a routine outcome as a bug in the cluster, and a
/// re-wrap additionally discards the detail the caller needed.
/// </para>
/// <para>
/// The service is driven directly rather than through the loopback client so the
/// raised exception is observed exactly as the server produced it, which is the
/// only way to tell a genuine pass-through from a faithful re-wrap. Every
/// pass-through assertion therefore checks the detail too, because the status
/// code alone cannot distinguish them.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeTenantAdminGrpcFaultMappingParityTests
{
    private ServiceProvider _serializers = null!;
    private FakeTenantQuotaUsage _quotaUsage = null!;
    private FakeTenantGrantAdmin _grantAdmin = null!;
    private FakeTenantAccessAdmin _accessAdmin = null!;
    private FakeTenantRegionAdmin _regionAdmin = null!;
    private LatticeTenantAdminGrpcService _service = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _quotaUsage = new FakeTenantQuotaUsage();
        _grantAdmin = new FakeTenantGrantAdmin();
        _accessAdmin = new FakeTenantAccessAdmin();
        _regionAdmin = new FakeTenantRegionAdmin();
        _service = new LatticeTenantAdminGrpcService(
            LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers),
            new FakeTenantAdmin(),
            new FakeTenantSelfService(),
            new NullCredentialBridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
            Options.Create(new LatticeTenantAdminApiGrpcOptions()),
            NullLogger<LatticeTenantAdminGrpcService>.Instance,
            _regionAdmin,
            _quotaUsage,
            _accessAdmin,
            _grantAdmin);
    }

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private static FakeServerCallContext Context(string method) =>
        new("/orleans.lattice.api.tenantadmin/" + method);

    private static TenantAdminTenantRequest Tenant() => new() { TenantId = "acme" };

    private static TenantAdminRegionSetRequest RegionSet(params string[] regions) =>
        new() { TenantId = "acme", Regions = regions };

    // ---- quota usage helper ----------------------------------------------

    [Test]
    public void An_rpc_exception_from_the_quota_usage_facade_passes_through_unchanged()
    {
        _quotaUsage.Throw = new RpcException(new Status(StatusCode.ResourceExhausted, "usage backend saturated"));

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await _service.GetTenantQuotaUsage(
                Tenant(), Context(LatticeTenantAdminGrpcMethods.GetTenantQuotaUsageMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
            Assert.That(
                ex.Status.Detail,
                Is.EqualTo("usage backend saturated"),
                "an already-shaped RpcException must reach the caller intact, not be re-wrapped as Internal");
        });
    }

    [Test]
    public void A_cancelled_quota_usage_call_maps_to_the_cancelled_status()
    {
        _quotaUsage.Throw = new OperationCanceledException();

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await _service.GetTenantQuotaUsage(
                Tenant(), Context(LatticeTenantAdminGrpcMethods.GetTenantQuotaUsageMethodName)));

        Assert.That(
            ex!.StatusCode,
            Is.EqualTo(StatusCode.Cancelled),
            "a cancellation is a routine outcome and must not be reported as a server fault");
    }

    // ---- cross-tenant grant helper ---------------------------------------

    [Test]
    public void An_rpc_exception_from_the_grant_facade_passes_through_unchanged()
    {
        _grantAdmin.Throw = new RpcException(new Status(StatusCode.Unavailable, "grant store draining"));

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await _service.ListCrossTenantGrants(
                Tenant(), Context(LatticeTenantAdminGrpcMethods.ListCrossTenantGrantsMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unavailable));
            Assert.That(ex.Status.Detail, Is.EqualTo("grant store draining"));
        });
    }

    [Test]
    public void A_cancelled_grant_call_maps_to_the_cancelled_status()
    {
        _grantAdmin.Throw = new OperationCanceledException();

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await _service.ListCrossTenantGrants(
                Tenant(), Context(LatticeTenantAdminGrpcMethods.ListCrossTenantGrantsMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void An_invalid_state_on_the_grant_path_maps_to_failed_precondition_not_internal()
    {
        // A well-formed request the cluster state refuses. It must not fall
        // through to the Internal arm below it, which would both misreport a
        // caller-actionable refusal as a server bug and discard its message.
        _grantAdmin.Throw = new InvalidOperationException("the grant scope is not enabled on this tree");

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await _service.ListCrossTenantGrants(
                Tenant(), Context(LatticeTenantAdminGrpcMethods.ListCrossTenantGrantsMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(
                ex.Status.Detail,
                Is.EqualTo("the grant scope is not enabled on this tree"),
                "a precondition breach is caller-actionable, so its message is forwarded");
        });
    }

    [Test]
    public void A_denied_tenant_assertion_on_the_grant_path_maps_to_permission_denied()
    {
        // A fail-closed tenant resolution is an authorization outcome, not a
        // server fault, so it must be caught above the Internal arm.
        _grantAdmin.Throw = new LatticeTenantAccessDeniedException();

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await _service.ListCrossTenantGrants(
                Tenant(), Context(LatticeTenantAdminGrpcMethods.ListCrossTenantGrantsMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    // ---- tenant access-administration helper -----------------------------

    [Test]
    public void An_rpc_exception_from_the_access_admin_facade_passes_through_unchanged()
    {
        _accessAdmin.Throw = new RpcException(new Status(StatusCode.DeadlineExceeded, "directory timed out"));

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await _service.ListTenantAdminSubjects(
                Tenant(), Context(LatticeTenantAdminGrpcMethods.ListTenantAdminSubjectsMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.DeadlineExceeded));
            Assert.That(ex.Status.Detail, Is.EqualTo("directory timed out"));
        });
    }

    [Test]
    public void A_cancelled_access_admin_call_maps_to_the_cancelled_status()
    {
        _accessAdmin.Throw = new OperationCanceledException();

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await _service.ListTenantAdminSubjects(
                Tenant(), Context(LatticeTenantAdminGrpcMethods.ListTenantAdminSubjectsMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void An_invalid_state_on_the_access_admin_path_maps_to_failed_precondition_not_internal()
    {
        _accessAdmin.Throw = new InvalidOperationException("the tenant directory is not provisioned");

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await _service.ListTenantAdminSubjects(
                Tenant(), Context(LatticeTenantAdminGrpcMethods.ListTenantAdminSubjectsMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(ex.Status.Detail, Is.EqualTo("the tenant directory is not provisioned"));
        });
    }

    // ---- region residency helper -----------------------------------------

    [Test]
    public void An_invalid_state_on_the_region_path_maps_to_failed_precondition_not_internal()
    {
        _regionAdmin.Throw = new InvalidOperationException("the region map is still converging");

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await _service.SetTenantResidency(
                RegionSet("eu"), Context(LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(ex.Status.Detail, Is.EqualTo("the region map is still converging"));
        });
    }

    // ---- the positive counterpart ----------------------------------------

    [Test]
    public async Task Each_helper_still_serves_its_facade_when_nothing_is_thrown()
    {
        // Without this, every assertion above is satisfied by a service that
        // refused unconditionally. Each helper is driven once on its success
        // path, so the refusals are shown to be a property of the thrown fault
        // rather than of the call.
        var usage = await _service.GetTenantQuotaUsage(
            Tenant(), Context(LatticeTenantAdminGrpcMethods.GetTenantQuotaUsageMethodName));
        var grants = await _service.ListCrossTenantGrants(
            Tenant(), Context(LatticeTenantAdminGrpcMethods.ListCrossTenantGrantsMethodName));
        var subjects = await _service.ListTenantAdminSubjects(
            Tenant(), Context(LatticeTenantAdminGrpcMethods.ListTenantAdminSubjectsMethodName));
        var residency = await _service.SetTenantResidency(
            RegionSet("eu"), Context(LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName));

        Assert.Multiple(() =>
        {
            Assert.That(usage, Is.Not.Null);
            Assert.That(grants, Is.Not.Null);
            Assert.That(subjects, Is.Not.Null);
            Assert.That(residency, Is.Not.Null);
            Assert.That(_quotaUsage.LastTenantId, Is.EqualTo("acme"));
            Assert.That(_accessAdmin.LastTenantId, Is.EqualTo("acme"));
        });
    }
}
