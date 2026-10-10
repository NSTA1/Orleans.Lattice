using NSubstitute;
using Orleans.Lattice.BPlusTree;
using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

public sealed partial class TenantUsageMeteringServiceTests
{
    private sealed class QuotaRefinementRate : ITenantRateLimiter
    {
        public bool TryAcquire(TenantId tenant) => true;
    }

    [Test]
    public async Task MeterOnceAsync_changed_resident_reports_publish_merge_and_refuse_later_admission()
    {
        var quotas = new TenantQuotas { MaxBytes = 100 };
        var record = TenantRecord.Create(Acme, TenantStatus.Active, quotas,
            TenantPlacement.Shared, TestClocks.Clock(1), "cluster-a");
        var store = new RecordingStore();
        var factory = GrainFactoryWith(["t/acme/orders"], bytesPerTree: 100, keysPerTree: 1);
        var service = Create(new FakeRegistry(Acme) { Quotas = quotas }, store, factory);
        var index = new FakeTenantUsageIndex();
        var controller = new LatticeTenantAdmissionController(index,
            new FixedScopeResolver(TenantEnforcementScope.GlobalConverged), new QuotaRefinementRate());
        TenantUsageRecord Rebuild()
        {
            var joined = TenantUsageRecord.Create(Acme);
            foreach (var published in store.Published)
            {
                joined.MergeFrom(published);
            }
            var compiled = CompiledTenantUsage.Compile([record], [joined], "cluster-a");
            Assert.That(compiled.TryGetView(Acme, out var view), Is.True);
            index.Views["acme"] = view;
            return joined;
        }

        await service.MeterOnceAsync(CancellationToken.None);
        Assert.That(Rebuild().LocalSample("cluster-a").Bytes, Is.EqualTo(100));
        Assert.That(await controller.IsAdmittedAsync(Acme, "orders"), Is.True);

        factory.GetGrain<ILatticeStorageUsage>("t/acme/orders")
            .GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TreeStorageUsageReport
            {
                TreeId = "t/acme/orders", TotalBytes = 101, LiveKeys = 1, LeafStateBytes = 101,
            }));
        await service.MeterOnceAsync(CancellationToken.None);

        var updated = Rebuild();
        Assert.That(updated.LocalSample("cluster-a").Bytes, Is.EqualTo(101));
        Assert.That(updated.Fold().Bytes, Is.EqualTo(101));
        Assert.That(async () => await controller.IsAdmittedAsync(Acme, "orders"),
            Throws.TypeOf<LatticeQuotaExceededException>());
    }
}
