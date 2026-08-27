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
    private LatticeTenantAdminGrpcService _service = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _facade = new FakeTenantAdmin();
        _selfService = new FakeTenantSelfService();
        _service = new LatticeTenantAdminGrpcService(
            LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers),
            _facade,
            _selfService,
            new NullCredentialBridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
            Options.Create(new LatticeTenantAdminApiGrpcOptions()), NullLogger<LatticeTenantAdminGrpcService>.Instance);
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
}
