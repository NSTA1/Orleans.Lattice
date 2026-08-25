using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcLatticeTenantSelfService"/>, the remote-host
/// adapter that fronts the read-only <see cref="ILatticeTenantSelfService"/> over
/// the tenant self-service gRPC client. Each read op is proven to forward its
/// request and unwrap its response (including the descriptor-list unwrapping the
/// client performs), plus the constructor guard, the empty-id guard, and
/// cancellation propagation. Deterministic over a <see cref="FakeCallInvoker"/> -
/// no network, no host.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeTenantSelfServiceTests
{
    private static GrpcLatticeTenantSelfService Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.TenantSelfServiceClient(invoker));

    [Test]
    public void Constructor_null_client_throws()
        => Assert.That(() => new GrpcLatticeTenantSelfService(null!), Throws.ArgumentNullException);

    [Test]
    public async Task GetCurrentTenantAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(
            _ => new TenantDescriptor { TenantId = "acme", Status = TenantLifecycleStatus.Active, IsDefault = false });

        var result = await Adapter(invoker).GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.InstanceOf<TenantSelfCurrentRequest>());
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Status, Is.EqualTo(TenantLifecycleStatus.Active));
        });
    }

    [Test]
    public async Task ListAccessibleTenantsAsync_forwards_request_and_unwraps_the_descriptor_list()
    {
        var invoker = new FakeCallInvoker(_ => new TenantSelfDescriptorList
        {
            Tenants = new[]
            {
                new TenantDescriptor { TenantId = "acme", Status = TenantLifecycleStatus.Active, IsDefault = false },
                new TenantDescriptor { TenantId = "beta", Status = TenantLifecycleStatus.Suspended, IsDefault = false },
            },
        });

        var result = await Adapter(invoker).ListAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.InstanceOf<TenantSelfListRequest>());
            Assert.That(result, Has.Count.EqualTo(2));
            Assert.That(result[0].TenantId, Is.EqualTo("acme"));
            Assert.That(result[1].TenantId, Is.EqualTo("beta"));
        });
    }

    [Test]
    public async Task ListAccessibleTenantsAsync_unwraps_an_empty_list()
    {
        var invoker = new FakeCallInvoker(_ => new TenantSelfDescriptorList());

        var result = await Adapter(invoker).ListAccessibleTenantsAsync();

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task GetTenantAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TenantStatusReport
        {
            TenantId = "acme",
            Status = TenantLifecycleStatus.Active,
            IsDefault = false,
            Regions = Array.Empty<TenantRegionStatusDescriptor>(),
        });

        var result = await Adapter(invoker).GetTenantAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(((TenantAdminTenantRequest)invoker.LastRequest!).TenantId, Is.EqualTo("acme"));
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Regions, Is.Empty);
        });
    }

    [Test]
    public void GetTenantAsync_empty_tenant_throws()
        => Assert.ThrowsAsync<ArgumentException>(
            async () => await Adapter(new FakeCallInvoker(_ => new TenantStatusReport
            {
                TenantId = "x",
                Status = TenantLifecycleStatus.Active,
                Regions = Array.Empty<TenantRegionStatusDescriptor>(),
            })).GetTenantAsync(""));

    [Test]
    public void GetCurrentTenantAsync_propagates_cancellation()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var invoker = new FakeCallInvoker(
            _ => new TenantDescriptor { TenantId = "acme", Status = TenantLifecycleStatus.Active, IsDefault = false });

        Assert.CatchAsync<OperationCanceledException>(
            async () => await Adapter(invoker).GetCurrentTenantAsync(cts.Token));
    }
}
