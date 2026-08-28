using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantAdminGrpcService"/> and
/// <see cref="LatticeTenantAdminApiGrpcClient"/> driven end to end through an
/// in-memory <see cref="LoopbackCallInvoker"/> (no network, no host): every
/// lifecycle RPC and the unauthenticated auth-scheme RPC round-trip through the
/// real Orleans marshallers, and every facade exception the service can catch is
/// translated onto the expected gRPC <see cref="StatusCode"/>. Also covers the
/// constructor and static-binding argument guards.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminGrpcServiceTests
{
    private ServiceProvider _serializers = null!;
    private FakeTenantAdmin _facade = null!;
    private FakeTenantSelfService _selfService = null!;
    private FakeTenantRegionAdmin _regionAdmin = null!;
    private LatticeTenantAdminApiGrpcClient _client = null!;
    private LatticeTenantSelfServiceApiGrpcClient _selfClient = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _facade = new FakeTenantAdmin();
        _selfService = new FakeTenantSelfService();
        _regionAdmin = new FakeTenantRegionAdmin();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        var service = new LatticeTenantAdminGrpcService(
            methods,
            _facade,
            _selfService,
            new NullCredentialBridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement
            {
                Schemes = new[] { new AuthSchemeDescriptor { SchemeId = "basic", DisplayName = "Basic" } },
            }),
            Options.Create(new LatticeTenantAdminApiGrpcOptions()), NullLogger<LatticeTenantAdminGrpcService>.Instance,
            _regionAdmin);
        var invoker = new LoopbackCallInvoker(service, _serializers);
        _client = new LatticeTenantAdminApiGrpcClient(invoker, methods);
        _selfClient = new LatticeTenantSelfServiceApiGrpcClient(invoker, methods);
    }

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    [Test]
    public async Task CreateTenant_round_trips_through_the_wire()
    {
        var result = await _client.CreateTenantAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Status, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(_facade.LastTenantId, Is.EqualTo("acme"));
            Assert.That(_facade.LastAdminSubjects, Is.Empty,
                "An omitted set must reach the server empty so it seeds the caller there.");
        });
    }

    [Test]
    public async Task CreateTenant_round_trips_the_requested_admin_subjects()
    {
        var result = await _client.CreateTenantAsync("acme", ["ops@example.com", "sre@example.com"]);

        Assert.Multiple(() =>
        {
            Assert.That(_facade.LastAdminSubjects, Is.EqualTo(new[] { "ops@example.com", "sre@example.com" }));
            Assert.That(result.AdminSubjects, Is.EqualTo(new[] { "ops@example.com", "sre@example.com" }));
        });
    }

    [Test]
    public async Task SuspendTenant_round_trips_through_the_wire()
    {
        var result = await _client.SuspendTenantAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(result.NewStatus, Is.EqualTo(TenantLifecycleStatus.Suspended));
            Assert.That(result.Changed, Is.True);
        });
    }

    [Test]
    public async Task ResumeTenant_round_trips_through_the_wire()
    {
        var result = await _client.ResumeTenantAsync("acme");

        Assert.That(result.NewStatus, Is.EqualTo(TenantLifecycleStatus.Active));
    }

    [Test]
    public async Task DeleteTenant_round_trips_the_cascade_count()
    {
        var result = await _client.DeleteTenantAsync("acme");

        Assert.That(result.CascadedTreeCount, Is.EqualTo(2));
    }

    [Test]
    public async Task SetTenantQuotas_round_trips_the_quotas_through_the_wire()
    {
        var quotas = new TenantQuotasDescriptor
        {
            MaxBytes = 1_000_000,
            MaxKeys = 5_000,
            MaxOpsPerSecond = 250,
            BurstPercent = 20,
        };

        var result = await _client.SetTenantQuotasAsync("acme", quotas);

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Quotas.MaxBytes, Is.EqualTo(1_000_000));
            Assert.That(result.Quotas.MaxKeys, Is.EqualTo(5_000));
            Assert.That(result.Quotas.MaxOpsPerSecond, Is.EqualTo(250));
            Assert.That(result.Quotas.BurstPercent, Is.EqualTo(20));
            Assert.That(_facade.LastTenantId, Is.EqualTo("acme"));
            Assert.That(_facade.LastQuotas, Is.EqualTo(quotas));
        });
    }

    [Test]
    public void SetTenantQuotas_facade_exceptions_map_to_the_expected_status_code()
    {
        _facade.Throw = new ReservedTenantOperationException("default", "set-quotas");

        var rpc = Assert.ThrowsAsync<RpcException>(
            async () => await _client.SetTenantQuotasAsync("default", TenantQuotasDescriptor.Unbounded));
        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
    }

    [Test]
    public async Task GetAuthScheme_round_trips_the_advertisement()
    {
        var schemes = await _client.GetAuthSchemeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(schemes, Has.Count.EqualTo(1));
            Assert.That(schemes[0].SchemeId, Is.EqualTo("basic"));
        });
    }

    [TestCaseSource(nameof(ExceptionMappings))]
    public void Facade_exceptions_map_to_the_expected_status_code(Exception thrown, StatusCode expected)
    {
        _facade.Throw = thrown;

        var rpc = Assert.ThrowsAsync<RpcException>(async () => await _client.CreateTenantAsync("acme"));
        Assert.That(rpc!.StatusCode, Is.EqualTo(expected));
    }

    private static IEnumerable<TestCaseData> ExceptionMappings()
    {
        yield return new TestCaseData(
            new LatticeAuthorizationDeniedException("*", LatticeOperation.Admin, "anon", "denied"),
            StatusCode.PermissionDenied).SetName("AuthorizationDenied_to_PermissionDenied");
        yield return new TestCaseData(
            new TenantAlreadyExistsException("acme"), StatusCode.AlreadyExists).SetName("AlreadyExists_to_AlreadyExists");
        yield return new TestCaseData(
            new TenantNotFoundException("acme"), StatusCode.NotFound).SetName("NotFound_to_NotFound");
        yield return new TestCaseData(
            new ReservedTenantOperationException("default", "delete"), StatusCode.FailedPrecondition).SetName("Reserved_to_FailedPrecondition");
        yield return new TestCaseData(
            new InvalidOperationException("bad state"), StatusCode.FailedPrecondition).SetName("InvalidOperation_to_FailedPrecondition");
        yield return new TestCaseData(
            new ArgumentException("bad arg"), StatusCode.InvalidArgument).SetName("Argument_to_InvalidArgument");
        yield return new TestCaseData(
            new Exception("boom"), StatusCode.Internal).SetName("Unexpected_to_Internal");
    }

    // ----- self-service round-trips -----

    [Test]
    public async Task GetCurrentTenant_round_trips_the_current_descriptor()
    {
        var descriptor = await _selfClient.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.TenantId, Is.EqualTo("acme"));
            Assert.That(descriptor.Status, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(descriptor.IsDefault, Is.False);
        });
    }

    [Test]
    public async Task ListAccessibleTenants_round_trips_and_unwraps_the_descriptor_list()
    {
        var tenants = await _selfClient.ListAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(tenants, Has.Count.EqualTo(2));
            Assert.That(tenants[0].TenantId, Is.EqualTo("acme"));
            Assert.That(tenants[1].TenantId, Is.EqualTo("beta"));
        });
    }

    [Test]
    public async Task GetTenant_round_trips_the_status_report()
    {
        var report = await _selfClient.GetTenantAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo("acme"));
            Assert.That(report.Status, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(report.Regions, Is.Empty);
            Assert.That(_selfService.LastTenantId, Is.EqualTo("acme"));
        });
    }

    [TestCaseSource(nameof(SelfServiceExceptionMappings))]
    public void Self_service_exceptions_map_to_the_expected_status_code(Exception thrown, StatusCode expected)
    {
        _selfService.Throw = thrown;

        var rpc = Assert.ThrowsAsync<RpcException>(async () => await _selfClient.GetTenantAsync("acme"));
        Assert.That(rpc!.StatusCode, Is.EqualTo(expected));
    }

    private static IEnumerable<TestCaseData> SelfServiceExceptionMappings()
    {
        yield return new TestCaseData(
            new TenantNotFoundException("acme"), StatusCode.NotFound).SetName("SelfService_NotFound_to_NotFound");
        yield return new TestCaseData(
            new LatticeAuthorizationDeniedException("*", LatticeOperation.Read, "anon", "denied"),
            StatusCode.PermissionDenied).SetName("SelfService_AuthorizationDenied_to_PermissionDenied");
        yield return new TestCaseData(
            new ArgumentException("bad arg"), StatusCode.InvalidArgument).SetName("SelfService_Argument_to_InvalidArgument");
        yield return new TestCaseData(
            new Exception("boom"), StatusCode.Internal).SetName("SelfService_Unexpected_to_Internal");
    }

    // ----- region-residency round-trips -----

    [Test]
    public async Task AuthorizeAllowedRegions_round_trips_through_the_wire()
    {
        var result = await _client.AuthorizeAllowedRegionsAsync("acme", ["eu-west", "ap-south"]);

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.AllowedRegions, Is.EqualTo(new[] { "eu-west", "ap-south" }));
            Assert.That(_regionAdmin.LastTenantId, Is.EqualTo("acme"));
            Assert.That(_regionAdmin.LastAllowedRegions, Is.EqualTo(new[] { "eu-west", "ap-south" }));
        });
    }

    [Test]
    public async Task AuthorizeAllowedRegions_round_trips_an_empty_set_as_a_full_revocation()
    {
        var result = await _client.AuthorizeAllowedRegionsAsync("acme", []);

        Assert.Multiple(() =>
        {
            Assert.That(result.AllowedRegions, Is.Empty);
            Assert.That(_regionAdmin.LastAllowedRegions, Is.Empty,
                "The empty set must survive the wire as 'revoke everything', not as null.");
        });
    }

    [Test]
    public async Task SetTenantResidency_round_trips_the_added_regions_and_rows()
    {
        var result = await _client.SetTenantResidencyAsync("acme", ["eu-west"]);

        Assert.Multiple(() =>
        {
            Assert.That(result.AddedRegions, Is.EqualTo(new[] { "eu-west" }));
            Assert.That(result.Regions[0].RegionId, Is.EqualTo("eu-west"));
            Assert.That(result.Regions[0].Status, Is.EqualTo(TenantRegionLifecycleStatus.Provisioning));
            Assert.That(_regionAdmin.LastResidencyRegions, Is.EqualTo(new[] { "eu-west" }));
        });
    }

    [Test]
    public async Task GetTenantRegionStatus_round_trips_the_report()
    {
        var report = await _client.GetTenantRegionStatusAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo("acme"));
            Assert.That(report.Regions[0].RegionId, Is.EqualTo("eu-west"));
            Assert.That(report.Regions[0].Status, Is.EqualTo(TenantRegionLifecycleStatus.Online));
            Assert.That(report.Regions[0].IsAllowed, Is.True);
        });
    }

    [TestCaseSource(nameof(RegionExceptionMappings))]
    public void Region_facade_exceptions_map_to_the_expected_status_code(Exception thrown, StatusCode expected)
    {
        _regionAdmin.Throw = thrown;

        var rpc = Assert.ThrowsAsync<RpcException>(
            async () => await _client.SetTenantResidencyAsync("acme", ["eu-west"]));
        Assert.That(rpc!.StatusCode, Is.EqualTo(expected));
    }

    private static IEnumerable<TestCaseData> RegionExceptionMappings()
    {
        // The two region-specific refusals derive directly from Exception, so an
        // absent typed arm would surface them as an opaque Internal.
        yield return new TestCaseData(
            new TenantRegionNotAllowedException("acme", "eu-west"),
            StatusCode.FailedPrecondition).SetName("RegionNotAllowed_to_FailedPrecondition");
        yield return new TestCaseData(
            new TenantLastRegionException("acme"),
            StatusCode.FailedPrecondition).SetName("LastRegion_to_FailedPrecondition");
        yield return new TestCaseData(
            new TenantNotFoundException("acme"), StatusCode.NotFound).SetName("Region_NotFound_to_NotFound");
        yield return new TestCaseData(
            new LatticeAuthorizationDeniedException("*", LatticeOperation.Admin, "anon", "denied"),
            StatusCode.PermissionDenied).SetName("Region_AuthorizationDenied_to_PermissionDenied");
        yield return new TestCaseData(
            new ReservedTenantOperationException("default", "set-residency"),
            StatusCode.FailedPrecondition).SetName("Region_Reserved_to_FailedPrecondition");
        yield return new TestCaseData(
            new ArgumentException("bad arg"), StatusCode.InvalidArgument).SetName("Region_Argument_to_InvalidArgument");
        yield return new TestCaseData(
            new Exception("boom"), StatusCode.Internal).SetName("Region_Unexpected_to_Internal");
    }

    [Test]
    public void Region_client_calls_reject_a_null_or_empty_tenant_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _client.AuthorizeAllowedRegionsAsync(null!, []),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _client.AuthorizeAllowedRegionsAsync(string.Empty, []),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _client.SetTenantResidencyAsync(null!, []),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _client.SetTenantResidencyAsync(string.Empty, []),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _client.GetTenantRegionStatusAsync(null!),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _client.GetTenantRegionStatusAsync(string.Empty),
                Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Region_client_calls_reject_a_null_region_collection()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _client.AuthorizeAllowedRegionsAsync("acme", null!),
                Throws.ArgumentNullException);
            Assert.That(async () => await _client.SetTenantResidencyAsync("acme", null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Self_service_client_GetTenant_rejects_a_null_or_empty_tenant_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _selfClient.GetTenantAsync(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _selfClient.GetTenantAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Self_service_client_Create_rejects_null_arguments()
    {
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        var invoker = new LoopbackCallInvoker(
            new LatticeTenantAdminGrpcService(
                methods, _facade, _selfService, new NullCredentialBridge(),
                new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
                Options.Create(new LatticeTenantAdminApiGrpcOptions()), NullLogger<LatticeTenantAdminGrpcService>.Instance,
                _regionAdmin),
            _serializers);

        Assert.Multiple(() =>
        {
            Assert.That(() => LatticeTenantSelfServiceApiGrpcClient.Create(null!, _serializers), Throws.ArgumentNullException);
            Assert.That(() => LatticeTenantSelfServiceApiGrpcClient.Create(invoker, null!), Throws.ArgumentNullException);
        });
    }

    // ----- constructor / static-binding guards -----

    [Test]
    public void BindService_rejects_a_null_binder()
    {
        Assert.That(
            () => LatticeTenantAdminGrpcServiceBase.BindService(null!, null),
            Throws.ArgumentNullException);
    }

    /// <summary>
    /// Every required dependency is guarded. <c>regionAdmin</c> is deliberately
    /// absent from this list: it is optional so a host that never opted the
    /// region-residency facade in still composes (see
    /// <c>LatticeTenantAdminApiGrpcRegistrationTests</c>), and a null there is a
    /// supported configuration rather than a programming error.
    /// </summary>
    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        var bridge = new NullCredentialBridge();
        var source = new FixedAuthSchemeSource(new AuthSchemeAdvertisement());
        var options = Options.Create(new LatticeTenantAdminApiGrpcOptions());
        var logger = NullLogger<LatticeTenantAdminGrpcService>.Instance;
        var regionAdmin = new FakeTenantRegionAdmin();

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeTenantAdminGrpcService(null!, _facade, _selfService, bridge, source, options, logger, regionAdmin), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, null!, _selfService, bridge, source, options, logger, regionAdmin), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, _facade, null!, bridge, source, options, logger, regionAdmin), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, _facade, _selfService, null!, source, options, logger, regionAdmin), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, _facade, _selfService, bridge, null!, options, logger, regionAdmin), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, _facade, _selfService, bridge, source, null!, logger, regionAdmin), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, _facade, _selfService, bridge, source, options, null!, regionAdmin), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, _facade, _selfService, bridge, source, options, logger, null), Throws.Nothing);
        });
    }

    // ----- client guards -----

    [Test]
    public void Client_Create_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => LatticeTenantAdminApiGrpcClient.Create(null!, _serializers), Throws.ArgumentNullException);
            Assert.That(() => LatticeTenantAdminApiGrpcClient.Create(new LoopbackCallInvoker(
                new LatticeTenantAdminGrpcService(
                    LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers),
                    _facade, _selfService, new NullCredentialBridge(), new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
                    Options.Create(new LatticeTenantAdminApiGrpcOptions()), NullLogger<LatticeTenantAdminGrpcService>.Instance,
                    _regionAdmin),
                _serializers), null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Client_lifecycle_calls_reject_a_null_or_empty_tenant_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _client.CreateTenantAsync(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _client.SuspendTenantAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _client.ResumeTenantAsync(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _client.DeleteTenantAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _client.SetTenantQuotasAsync(null!, TenantQuotasDescriptor.Unbounded), Throws.InstanceOf<ArgumentException>());
        });
    }
}
