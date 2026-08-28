using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcLatticeTenantRegionAdmin"/>, the remote-host
/// adapter that fronts <see cref="ILatticeTenantRegionAdmin"/> over the
/// tenant-administration-API gRPC client. Each of the three region-residency
/// operations is proven to forward its request and unwrap its response, plus the
/// constructor guard and cancellation propagation. The adapter adds no
/// authorization of its own - the remote cluster re-runs the facade's two-tier
/// gate - so these tests assert pass-through fidelity only. Deterministic over a
/// <see cref="FakeCallInvoker"/> - no network, no host.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeTenantRegionAdminTests
{
    private static GrpcLatticeTenantRegionAdmin Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.TenantAdminClient(invoker));

    private static TenantRegionStatusDescriptor Row(
        string regionId, TenantRegionLifecycleStatus status, bool isAllowed)
        => new() { RegionId = regionId, Status = status, IsAllowed = isAllowed };

    [Test]
    public void Constructor_null_client_throws()
        => Assert.That(() => new GrpcLatticeTenantRegionAdmin(null!), Throws.ArgumentNullException);

    [Test]
    public async Task AuthorizeAllowedRegionsAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(
            _ => new TenantRegionAuthorizationResult
            {
                TenantId = "acme",
                AllowedRegions = ["ap-south", "eu-west"],
            });

        var result = await Adapter(invoker).AuthorizeAllowedRegionsAsync("acme", ["eu-west", "ap-south"]);

        Assert.Multiple(() =>
        {
            var request = (TenantAdminRegionSetRequest)invoker.LastRequest!;
            Assert.That(request.TenantId, Is.EqualTo("acme"));
            Assert.That(request.Regions, Is.EqualTo(new[] { "eu-west", "ap-south" }));
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.AllowedRegions, Is.EqualTo(new[] { "ap-south", "eu-west" }));
        });
    }

    [Test]
    public async Task AuthorizeAllowedRegionsAsync_forwards_an_empty_set_as_a_full_revocation()
    {
        var invoker = new FakeCallInvoker(
            _ => new TenantRegionAuthorizationResult { TenantId = "acme", AllowedRegions = [] });

        var result = await Adapter(invoker).AuthorizeAllowedRegionsAsync("acme", []);

        Assert.Multiple(() =>
        {
            Assert.That(((TenantAdminRegionSetRequest)invoker.LastRequest!).Regions, Is.Empty);
            Assert.That(result.AllowedRegions, Is.Empty);
        });
    }

    [Test]
    public async Task SetResidencyAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(
            _ => new TenantResidencyChangeResult
            {
                TenantId = "acme",
                AddedRegions = ["ap-south"],
                RemovedRegions = ["eu-west"],
                Regions =
                [
                    Row("ap-south", TenantRegionLifecycleStatus.Provisioning, isAllowed: true),
                    Row("eu-west", TenantRegionLifecycleStatus.Draining, isAllowed: true),
                ],
            });

        var result = await Adapter(invoker).SetResidencyAsync("acme", ["ap-south"]);

        Assert.Multiple(() =>
        {
            var request = (TenantAdminRegionSetRequest)invoker.LastRequest!;
            Assert.That(request.TenantId, Is.EqualTo("acme"));
            Assert.That(request.Regions, Is.EqualTo(new[] { "ap-south" }));
            Assert.That(result.AddedRegions, Is.EqualTo(new[] { "ap-south" }));
            Assert.That(result.RemovedRegions, Is.EqualTo(new[] { "eu-west" }));
            Assert.That(result.Regions, Has.Count.EqualTo(2));
            Assert.That(result.Regions[0].Status, Is.EqualTo(TenantRegionLifecycleStatus.Provisioning));
        });
    }

    [Test]
    public async Task GetTenantRegionStatusAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(
            _ => new TenantRegionStatusReport
            {
                TenantId = "acme",
                Regions = [Row("eu-west", TenantRegionLifecycleStatus.Online, isAllowed: true)],
            });

        var result = await Adapter(invoker).GetTenantRegionStatusAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(((TenantAdminTenantRequest)invoker.LastRequest!).TenantId, Is.EqualTo("acme"));
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Regions, Has.Count.EqualTo(1));
            Assert.That(result.Regions[0].RegionId, Is.EqualTo("eu-west"));
            Assert.That(result.Regions[0].Status, Is.EqualTo(TenantRegionLifecycleStatus.Online));
            Assert.That(result.Regions[0].IsAllowed, Is.True);
        });
    }

    [Test]
    public void AuthorizeAllowedRegionsAsync_empty_tenant_throws()
        => Assert.ThrowsAsync<ArgumentException>(
            async () => await Adapter(new FakeCallInvoker(
                    _ => new TenantRegionAuthorizationResult { TenantId = "x", AllowedRegions = [] }))
                .AuthorizeAllowedRegionsAsync("", []));

    [Test]
    public void SetResidencyAsync_empty_tenant_throws()
        => Assert.ThrowsAsync<ArgumentException>(
            async () => await Adapter(new FakeCallInvoker(
                    _ => new TenantResidencyChangeResult
                    {
                        TenantId = "x",
                        AddedRegions = [],
                        RemovedRegions = [],
                        Regions = [],
                    }))
                .SetResidencyAsync("", ["eu-west"]));

    [Test]
    public void GetTenantRegionStatusAsync_empty_tenant_throws()
        => Assert.ThrowsAsync<ArgumentException>(
            async () => await Adapter(new FakeCallInvoker(
                    _ => new TenantRegionStatusReport { TenantId = "x", Regions = [] }))
                .GetTenantRegionStatusAsync(""));

    [Test]
    public void GetTenantRegionStatusAsync_propagates_cancellation()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var invoker = new FakeCallInvoker(
            _ => new TenantRegionStatusReport { TenantId = "acme", Regions = [] });

        Assert.CatchAsync<OperationCanceledException>(
            async () => await Adapter(invoker).GetTenantRegionStatusAsync("acme", cts.Token));
    }

    [Test]
    public void SetResidencyAsync_propagates_cancellation()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var invoker = new FakeCallInvoker(
            _ => new TenantResidencyChangeResult
            {
                TenantId = "acme",
                AddedRegions = [],
                RemovedRegions = [],
                Regions = [],
            });

        Assert.CatchAsync<OperationCanceledException>(
            async () => await Adapter(invoker).SetResidencyAsync("acme", ["eu-west"], cts.Token));
    }
}
