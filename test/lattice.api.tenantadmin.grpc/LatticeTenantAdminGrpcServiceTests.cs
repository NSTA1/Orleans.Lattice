using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
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
    private LatticeTenantAdminApiGrpcClient _client = null!;
    private LatticeTenantSelfServiceApiGrpcClient _selfClient = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _facade = new FakeTenantAdmin();
        _selfService = new FakeTenantSelfService();
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
            NullLogger<LatticeTenantAdminGrpcService>.Instance);
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
                NullLogger<LatticeTenantAdminGrpcService>.Instance),
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

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        var bridge = new NullCredentialBridge();
        var source = new FixedAuthSchemeSource(new AuthSchemeAdvertisement());
        var logger = NullLogger<LatticeTenantAdminGrpcService>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeTenantAdminGrpcService(null!, _facade, _selfService, bridge, source, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, null!, _selfService, bridge, source, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, _facade, null!, bridge, source, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, _facade, _selfService, null!, source, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, _facade, _selfService, bridge, null!, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdminGrpcService(methods, _facade, _selfService, bridge, source, null!), Throws.ArgumentNullException);
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
                    NullLogger<LatticeTenantAdminGrpcService>.Instance),
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
        });
    }
}
