using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Covers the two exception paths the service shares between its admin and
/// self-service invocation helpers that the loopback round-trip fixture cannot
/// reach: an <see cref="RpcException"/> the facade already produced must pass
/// through unchanged (never be re-wrapped as <see cref="StatusCode.Internal"/>),
/// and a cancellation must be translated into
/// <see cref="StatusCode.Cancelled"/>. The service is driven directly here rather
/// than through the client so the raised exception is observed exactly as the
/// server produced it.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminGrpcServiceStatusMappingTests
{
    private ServiceProvider _serializers = null!;
    private FakeTenantAdmin _facade = null!;
    private FakeTenantSelfService _selfService = null!;
    private FakeTenantRegionAdmin _regionAdmin = null!;
    private LatticeTenantAdminGrpcService _service = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _facade = new FakeTenantAdmin();
        _selfService = new FakeTenantSelfService();
        _regionAdmin = new FakeTenantRegionAdmin();
        _service = new LatticeTenantAdminGrpcService(
            LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers),
            _facade,
            _selfService,
            new NullCredentialBridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
            Options.Create(new LatticeTenantAdminApiGrpcOptions()), NullLogger<LatticeTenantAdminGrpcService>.Instance,
            _regionAdmin);
    }

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private static FakeServerCallContext Context(string method) =>
        new("/orleans.lattice.api.tenantadmin/" + method);

    [Test]
    public void An_rpc_exception_from_the_admin_facade_passes_through_unchanged()
    {
        _facade.Throw = new RpcException(new Status(StatusCode.ResourceExhausted, "quota exceeded"));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.SuspendTenant(
            new TenantAdminTenantRequest { TenantId = "acme" },
            Context(LatticeTenantAdminGrpcMethods.SuspendTenantMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
            Assert.That(ex.Status.Detail, Is.EqualTo("quota exceeded"),
                "an already-shaped RpcException must not be re-wrapped as Internal");
        });
    }

    [Test]
    public void A_cancelled_admin_call_maps_to_the_cancelled_status()
    {
        _facade.Throw = new OperationCanceledException();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.DeleteTenant(
            new TenantAdminTenantRequest { TenantId = "acme" },
            Context(LatticeTenantAdminGrpcMethods.DeleteTenantMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void An_rpc_exception_from_the_self_service_facade_passes_through_unchanged()
    {
        _selfService.Throw = new RpcException(new Status(StatusCode.Unavailable, "region draining"));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.GetCurrentTenant(
            new TenantSelfCurrentRequest(),
            Context(LatticeTenantAdminGrpcMethods.GetCurrentTenantMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unavailable));
            Assert.That(ex.Status.Detail, Is.EqualTo("region draining"));
        });
    }

    [Test]
    public void A_cancelled_self_service_call_maps_to_the_cancelled_status()
    {
        _selfService.Throw = new OperationCanceledException();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.GetTenant(
            new TenantAdminTenantRequest { TenantId = "acme" },
            Context(LatticeTenantAdminGrpcMethods.GetTenantMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void A_denied_tenant_assertion_maps_to_permission_denied_not_internal()
    {
        // The resolver fails closed by raising this when the caller has no valid
        // active tenant, or may not act as the one it asserted. Falling through to
        // the generic handler would report an authorization decision as a server
        // fault, hiding the actionable reason behind a generic message and
        // inviting a client to retry a decision that will never change.
        _selfService.Throw = new LatticeTenantAccessDeniedException();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.GetCurrentTenant(
            new TenantSelfCurrentRequest(),
            Context(LatticeTenantAdminGrpcMethods.GetCurrentTenantMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("active tenant"),
                "the caller-facing reason must survive the mapping");
        });
    }

    [Test]
    public void A_denied_tenant_assertion_on_the_admin_path_maps_to_permission_denied()
    {
        _facade.Throw = new LatticeTenantAccessDeniedException();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.SuspendTenant(
            new TenantAdminTenantRequest { TenantId = "acme" },
            Context(LatticeTenantAdminGrpcMethods.SuspendTenantMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    // ----- region residency -----

    private static TenantAdminRegionSetRequest RegionSet(params string[] regions)
        => new() { TenantId = "acme", Regions = regions };

    [Test]
    public void A_region_outside_the_allowed_set_maps_to_failed_precondition_not_internal()
    {
        // TenantRegionNotAllowedException derives directly from Exception, so
        // without its own arm it would fall to the catch-all and reach the caller
        // as an opaque Internal - the exact defect the lifecycle surface had.
        _regionAdmin.Throw = new TenantRegionNotAllowedException("acme", "eu");

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.SetTenantResidency(
            RegionSet("eu"),
            Context(LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(ex.StatusCode, Is.Not.EqualTo(StatusCode.Internal));
            Assert.That(ex.Status.Detail, Does.Contain("eu"),
                "the actionable region must survive the mapping");
        });
    }

    [Test]
    public void The_last_resident_region_guard_maps_to_failed_precondition_not_internal()
    {
        _regionAdmin.Throw = new TenantLastRegionException("acme");

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.SetTenantResidency(
            RegionSet(),
            Context(LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(ex.StatusCode, Is.Not.EqualTo(StatusCode.Internal));
            Assert.That(ex.Status.Detail, Does.Contain("acme"));
        });
    }

    [Test]
    public void An_unknown_tenant_on_the_region_path_maps_to_not_found()
    {
        _regionAdmin.Throw = new TenantNotFoundException("ghost");

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.GetTenantRegionStatus(
            new TenantAdminTenantRequest { TenantId = "ghost" },
            Context(LatticeTenantAdminGrpcMethods.GetTenantRegionStatusMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public void An_operator_denial_on_the_region_path_maps_to_permission_denied()
    {
        _regionAdmin.Throw = new LatticeAuthorizationDeniedException("operator role required");

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.AuthorizeAllowedRegions(
            RegionSet("eu"),
            Context(LatticeTenantAdminGrpcMethods.AuthorizeAllowedRegionsMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("operator"));
        });
    }

    [Test]
    public void A_denied_tenant_assertion_on_the_region_path_maps_to_permission_denied()
    {
        _regionAdmin.Throw = new LatticeTenantAccessDeniedException();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.GetTenantRegionStatus(
            new TenantAdminTenantRequest { TenantId = "acme" },
            Context(LatticeTenantAdminGrpcMethods.GetTenantRegionStatusMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void The_reserved_default_tenant_on_the_region_path_maps_to_failed_precondition()
    {
        _regionAdmin.Throw = new ReservedTenantOperationException("default", "set-residency");

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.SetTenantResidency(
            RegionSet("eu"),
            Context(LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
    }

    [Test]
    public void A_bad_argument_on_the_region_path_maps_to_invalid_argument()
    {
        _regionAdmin.Throw = new ArgumentException("regionId must not be empty");

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.AuthorizeAllowedRegions(
            RegionSet(string.Empty),
            Context(LatticeTenantAdminGrpcMethods.AuthorizeAllowedRegionsMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void An_rpc_exception_from_the_region_facade_passes_through_unchanged()
    {
        _regionAdmin.Throw = new RpcException(new Status(StatusCode.Unavailable, "region draining"));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.GetTenantRegionStatus(
            new TenantAdminTenantRequest { TenantId = "acme" },
            Context(LatticeTenantAdminGrpcMethods.GetTenantRegionStatusMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unavailable));
            Assert.That(ex.Status.Detail, Is.EqualTo("region draining"));
        });
    }

    [Test]
    public void A_cancelled_region_call_maps_to_the_cancelled_status()
    {
        _regionAdmin.Throw = new OperationCanceledException();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.AuthorizeAllowedRegions(
            RegionSet("eu"),
            Context(LatticeTenantAdminGrpcMethods.AuthorizeAllowedRegionsMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void An_unexpected_region_fault_maps_to_internal_without_leaking_the_message()
    {
        _regionAdmin.Throw = new IOException("connection string user=admin password=hunter2");

        var ex = Assert.ThrowsAsync<RpcException>(async () => await _service.SetTenantResidency(
            RegionSet("eu"),
            Context(LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(ex.Status.Detail, Does.Not.Contain("password"),
                "an unexpected fault must not leak its message to the caller");
        });
    }

    [Test]
    public void The_region_rpcs_reject_a_null_request()
    {
        var context = Context(LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await _service.AuthorizeAllowedRegions(null!, context),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await _service.SetTenantResidency(null!, context),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await _service.GetTenantRegionStatus(null!, context),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_region_rpcs_reject_a_null_context()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await _service.AuthorizeAllowedRegions(RegionSet("eu"), null!),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await _service.SetTenantResidency(RegionSet("eu"), null!),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await _service.GetTenantRegionStatus(
                    new TenantAdminTenantRequest { TenantId = "acme" }, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task Each_region_rpc_reaches_its_own_facade_operation()
    {
        var authorized = await _service.AuthorizeAllowedRegions(
            RegionSet("eu", "ap"),
            Context(LatticeTenantAdminGrpcMethods.AuthorizeAllowedRegionsMethodName));
        var residency = await _service.SetTenantResidency(
            RegionSet("eu"),
            Context(LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName));
        var status = await _service.GetTenantRegionStatus(
            new TenantAdminTenantRequest { TenantId = "acme" },
            Context(LatticeTenantAdminGrpcMethods.GetTenantRegionStatusMethodName));

        Assert.Multiple(() =>
        {
            Assert.That(authorized.AllowedRegions, Is.EqualTo(new[] { "eu", "ap" }));
            Assert.That(_regionAdmin.LastAllowedRegions, Is.EqualTo(new[] { "eu", "ap" }));
            Assert.That(residency.AddedRegions, Is.EqualTo(new[] { "eu" }));
            Assert.That(_regionAdmin.LastResidencyRegions, Is.EqualTo(new[] { "eu" }));
            Assert.That(status.Regions.Select(r => r.RegionId), Is.EqualTo(new[] { "eu-west" }));
        });
    }
}
