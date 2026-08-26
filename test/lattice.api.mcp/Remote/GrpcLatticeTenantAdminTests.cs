using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcLatticeTenantAdmin"/>, the remote-host adapter
/// that fronts <see cref="ILatticeTenantAdmin"/> over the tenant-administration-API
/// gRPC client. Each mutating lifecycle op is proven to forward its request and
/// unwrap its response, plus the constructor guard and cancellation propagation.
/// Deterministic over a <see cref="FakeCallInvoker"/> - no network, no host.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeTenantAdminTests
{
    private static GrpcLatticeTenantAdmin Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.TenantAdminClient(invoker));

    [Test]
    public void Constructor_null_client_throws()
        => Assert.That(() => new GrpcLatticeTenantAdmin(null!), Throws.ArgumentNullException);

    [Test]
    public async Task CreateTenantAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(
            _ => new TenantCreationResult
            {
                TenantId = "acme",
                Status = TenantLifecycleStatus.Active,
                AdminSubjects = ["ops@example.com"],
            });

        var result = await Adapter(invoker).CreateTenantAsync("acme", ["ops@example.com"]);

        Assert.Multiple(() =>
        {
            var request = (TenantAdminCreateRequest)invoker.LastRequest!;
            Assert.That(request.TenantId, Is.EqualTo("acme"));
            Assert.That(request.AdminSubjects, Is.EqualTo(new[] { "ops@example.com" }));
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Status, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(result.AdminSubjects, Is.EqualTo(new[] { "ops@example.com" }));
        });
    }

    [Test]
    public async Task CreateTenantAsync_with_no_subjects_sends_an_empty_set_for_the_server_to_seed()
    {
        var invoker = new FakeCallInvoker(
            _ => new TenantCreationResult { TenantId = "acme", Status = TenantLifecycleStatus.Active });

        _ = await Adapter(invoker).CreateTenantAsync("acme");

        Assert.That(((TenantAdminCreateRequest)invoker.LastRequest!).AdminSubjects, Is.Empty);
    }

    [Test]
    public async Task SuspendTenantAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TenantStatusChangeResult
        {
            TenantId = "acme",
            PreviousStatus = TenantLifecycleStatus.Active,
            NewStatus = TenantLifecycleStatus.Suspended,
            Changed = true,
        });

        var result = await Adapter(invoker).SuspendTenantAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(((TenantAdminTenantRequest)invoker.LastRequest!).TenantId, Is.EqualTo("acme"));
            Assert.That(result.NewStatus, Is.EqualTo(TenantLifecycleStatus.Suspended));
            Assert.That(result.Changed, Is.True);
        });
    }

    [Test]
    public async Task ResumeTenantAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TenantStatusChangeResult
        {
            TenantId = "acme",
            PreviousStatus = TenantLifecycleStatus.Suspended,
            NewStatus = TenantLifecycleStatus.Active,
            Changed = true,
        });

        var result = await Adapter(invoker).ResumeTenantAsync("acme");

        Assert.That(result.NewStatus, Is.EqualTo(TenantLifecycleStatus.Active));
    }

    [Test]
    public async Task DeleteTenantAsync_forwards_request_and_unwraps_cascade_count()
    {
        var invoker = new FakeCallInvoker(_ => new TenantDeletionResult { TenantId = "acme", CascadedTreeCount = 3 });

        var result = await Adapter(invoker).DeleteTenantAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(((TenantAdminTenantRequest)invoker.LastRequest!).TenantId, Is.EqualTo("acme"));
            Assert.That(result.CascadedTreeCount, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task SetTenantQuotasAsync_forwards_request_and_unwraps_response()
    {
        var quotas = new TenantQuotasDescriptor { MaxBytes = 1_000, MaxOpsPerSecond = 50, BurstPercent = 10 };
        var invoker = new FakeCallInvoker(
            _ => new TenantQuotasUpdateResult { TenantId = "acme", Quotas = quotas });

        var result = await Adapter(invoker).SetTenantQuotasAsync("acme", quotas);

        Assert.Multiple(() =>
        {
            var request = (TenantAdminSetQuotasRequest)invoker.LastRequest!;
            Assert.That(request.TenantId, Is.EqualTo("acme"));
            Assert.That(request.Quotas, Is.EqualTo(quotas));
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Quotas, Is.EqualTo(quotas));
        });
    }

    [Test]
    public void CreateTenantAsync_empty_tenant_throws()
        => Assert.ThrowsAsync<ArgumentException>(
            async () => await Adapter(new FakeCallInvoker(
                _ => new TenantCreationResult { TenantId = "x", Status = TenantLifecycleStatus.Active })).CreateTenantAsync(""));

    [Test]
    public void DeleteTenantAsync_propagates_cancellation()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var invoker = new FakeCallInvoker(_ => new TenantDeletionResult { TenantId = "acme", CascadedTreeCount = 0 });

        Assert.CatchAsync<OperationCanceledException>(
            async () => await Adapter(invoker).DeleteTenantAsync("acme", cts.Token));
    }
}
