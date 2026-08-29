using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Unit tests for the <c>GetTenantQuotaUsage</c> RPC on
/// <see cref="LatticeTenantAdminGrpcService"/> and
/// <see cref="LatticeTenantAdminApiGrpcClient"/>, driven end to end through the
/// in-memory <see cref="LoopbackCallInvoker"/> (no network, no host) so every
/// figure crosses the real Orleans marshallers. Covers the full round-trip of the
/// nullable ceiling and usage modelling (unbounded stays <c>null</c>, capped-at-zero
/// stays <c>0</c>, unmeasured stays <c>null</c>), the enforcement-scope qualifier,
/// the exception-to-<see cref="StatusCode"/> translation, and the
/// <see cref="StatusCode.Unimplemented"/> answer on a host that binds tenant
/// administration without the usage facade. All figures are fixed, so nothing here
/// depends on timing, ordering, or the wall clock.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminGrpcQuotaUsageTests
{
    private ServiceProvider _serializers = null!;
    private FakeTenantQuotaUsage _quotaUsage = null!;
    private LatticeTenantAdminApiGrpcClient _client = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _quotaUsage = new FakeTenantQuotaUsage();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        _client = new LatticeTenantAdminApiGrpcClient(
            new LoopbackCallInvoker(CreateService(methods, _quotaUsage), _serializers), methods);
    }

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private static LatticeTenantAdminGrpcService CreateService(
        LatticeTenantAdminGrpcMethods methods, ILatticeTenantQuotaUsage? quotaUsage) =>
        new(
            methods,
            new FakeTenantAdmin(),
            new FakeTenantSelfService(),
            new NullCredentialBridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement { Schemes = Array.Empty<AuthSchemeDescriptor>() }),
            Options.Create(new LatticeTenantAdminApiGrpcOptions()),
            NullLogger<LatticeTenantAdminGrpcService>.Instance,
            new FakeTenantRegionAdmin(),
            quotaUsage);

    [Test]
    public async Task GetTenantQuotaUsage_round_trips_a_bounded_dimension_through_the_wire()
    {
        var report = await _client.GetTenantQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(_quotaUsage.LastTenantId, Is.EqualTo("acme"));
            Assert.That(report.TenantId, Is.EqualTo("acme"));
            Assert.That(report.HasUsage, Is.True);
            Assert.That(report.Bytes.Usage, Is.EqualTo(4_100));
            Assert.That(report.Bytes.Limit, Is.EqualTo(10_000));
            Assert.That(report.Bytes.BurstLimit, Is.EqualTo(12_000));
            Assert.That(report.Bytes.MeteredOverage, Is.EqualTo(11));
            Assert.That(report.Keys.Overage, Is.EqualTo(100));
            Assert.That(report.BurstPercent, Is.EqualTo(20));
            Assert.That(report.Quotas.MaxBytes, Is.EqualTo(10_000));
        });
    }

    [Test]
    public async Task GetTenantQuotaUsage_round_trips_an_unbounded_dimension_as_no_ceiling()
    {
        var report = await _client.GetTenantQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.MemoryBytes.Limit, Is.Null, "unbounded must not decode as a zero ceiling");
            Assert.That(report.MemoryBytes.BurstLimit, Is.Null);
            Assert.That(report.MemoryBytes.IsBounded, Is.False);
            Assert.That(report.MemoryBytes.Usage, Is.EqualTo(9_000));
        });
    }

    [Test]
    public async Task GetTenantQuotaUsage_round_trips_a_zero_ceiling_as_bounded()
    {
        var report = await _client.GetTenantQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeCount.Limit, Is.EqualTo(0));
            Assert.That(report.TreeCount.IsBounded, Is.True, "a ceiling of zero must not decode as unbounded");
            Assert.That(report.TreeCount.Overage, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task GetTenantQuotaUsage_round_trips_an_unmeasured_dimension_as_no_usage()
    {
        var report = await _client.GetTenantQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.OpsPerSecond.Limit, Is.EqualTo(250));
            Assert.That(report.OpsPerSecond.Usage, Is.Null, "unmeasured must not decode as a usage of zero");
            Assert.That(report.OpsPerSecond.IsMeasured, Is.False);
        });
    }

    [Test]
    public async Task GetTenantQuotaUsage_round_trips_the_enforcement_scope()
    {
        var perCluster = await _client.GetTenantQuotaUsageAsync("acme");

        _quotaUsage.Report = _quotaUsage.Report with
        {
            EnforcementScope = TenantQuotaEnforcementScope.GlobalConverged,
        };
        var global = await _client.GetTenantQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(perCluster.EnforcementScope, Is.EqualTo(TenantQuotaEnforcementScope.PerCluster));
            Assert.That(global.EnforcementScope, Is.EqualTo(TenantQuotaEnforcementScope.GlobalConverged));
        });
    }

    [Test]
    public async Task GetTenantQuotaUsage_round_trips_an_unmeasured_report()
    {
        _quotaUsage.Report = _quotaUsage.Report with
        {
            HasUsage = false,
            Bytes = new TenantQuotaDimensionUsage { Limit = 10_000, BurstLimit = 12_000 },
        };

        var report = await _client.GetTenantQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.HasUsage, Is.False);
            Assert.That(report.Bytes.Limit, Is.EqualTo(10_000));
            Assert.That(report.Bytes.Usage, Is.Null);
        });
    }

    [Test]
    public void GetTenantQuotaUsage_rejects_a_null_tenant_id_at_the_client() =>
        Assert.That(() => _client.GetTenantQuotaUsageAsync(null!), Throws.InstanceOf<ArgumentException>());

    [Test]
    public void GetTenantQuotaUsage_rejects_an_empty_tenant_id_at_the_client() =>
        Assert.That(() => _client.GetTenantQuotaUsageAsync(string.Empty), Throws.InstanceOf<ArgumentException>());

    // ---- status mapping --------------------------------------------------

    [Test]
    public void GetTenantQuotaUsage_maps_a_refusal_to_not_found()
    {
        _quotaUsage.Throw = new TenantNotFoundException("acme");

        var ex = Assert.ThrowsAsync<RpcException>(() => _client.GetTenantQuotaUsageAsync("acme"));

        Assert.That(
            ex!.StatusCode,
            Is.EqualTo(StatusCode.NotFound),
            "the facade already unified unauthorized with absent, so the transport must not widen it");
    }

    [Test]
    public void GetTenantQuotaUsage_maps_an_authorization_denial_to_permission_denied()
    {
        _quotaUsage.Throw = new LatticeAuthorizationDeniedException("operator or tenant-admin authority required");

        var ex = Assert.ThrowsAsync<RpcException>(() => _client.GetTenantQuotaUsageAsync("acme"));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void GetTenantQuotaUsage_maps_a_tenant_access_denial_to_permission_denied()
    {
        _quotaUsage.Throw = new LatticeTenantAccessDeniedException();

        var ex = Assert.ThrowsAsync<RpcException>(() => _client.GetTenantQuotaUsageAsync("acme"));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void GetTenantQuotaUsage_maps_a_bad_tenant_id_to_invalid_argument()
    {
        _quotaUsage.Throw = new ArgumentException("bad id", "tenantId");

        var ex = Assert.ThrowsAsync<RpcException>(() => _client.GetTenantQuotaUsageAsync("acme"));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void GetTenantQuotaUsage_maps_an_unexpected_fault_to_internal()
    {
        _quotaUsage.Throw = new InvalidTimeZoneException("boom");

        var ex = Assert.ThrowsAsync<RpcException>(() => _client.GetTenantQuotaUsageAsync("acme"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(ex.Status.Detail, Does.Not.Contain("boom"), "an internal fault must not leak its message");
        });
    }

    [Test]
    public void GetTenantQuotaUsage_without_the_facade_is_unimplemented()
    {
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        var client = new LatticeTenantAdminApiGrpcClient(
            new LoopbackCallInvoker(CreateService(methods, quotaUsage: null), _serializers), methods);

        var ex = Assert.ThrowsAsync<RpcException>(() => client.GetTenantQuotaUsageAsync("acme"));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void GetTenantQuotaUsage_rejects_null_arguments()
    {
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        var service = CreateService(methods, _quotaUsage);
        var context = new FakeServerCallContext("/svc/GetTenantQuotaUsage");

        Assert.Multiple(() =>
        {
            Assert.That(() => service.GetTenantQuotaUsage(null!, context), Throws.ArgumentNullException);
            Assert.That(
                () => service.GetTenantQuotaUsage(new TenantAdminTenantRequest { TenantId = "acme" }, null!),
                Throws.ArgumentNullException);
        });
    }

    // ---- interceptor operation mapping -----------------------------------

    [Test]
    public void DescribeCall_maps_the_quota_usage_method_and_target()
    {
        var (operation, targetId) = LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
            $"/{LatticeTenantAdminGrpcMethods.ServiceName}/{LatticeTenantAdminGrpcMethods.GetTenantQuotaUsageMethodName}",
            new TenantAdminTenantRequest { TenantId = "acme" });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeTenantAdminApiOperation.GetTenantQuotaUsage));
            Assert.That(targetId, Is.EqualTo("acme"));
        });
    }

    [Test]
    public void The_quota_usage_method_is_not_exempt_from_the_transport_authorizer()
    {
        var fullName = $"/{LatticeTenantAdminGrpcMethods.ServiceName}/{LatticeTenantAdminGrpcMethods.GetTenantQuotaUsageMethodName}";

        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeTenantAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(fullName),
                Is.False,
                "reading a tenant's usage must never be reachable without a credential");
            Assert.That(
                LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(fullName),
                Is.False,
                "it is a tenant-admin-tier read, gated exactly as GetTenantRegionStatus is");
        });
    }
}
