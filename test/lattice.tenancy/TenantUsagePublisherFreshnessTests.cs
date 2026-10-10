using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;
using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Guards eventual publication of quota crossings below the hysteresis band.</summary>
[TestFixture]
public sealed class TenantUsagePublisherFreshnessTests
{
    private sealed class AdmitRate : ITenantRateLimiter
    {
        public bool TryAcquire(TenantId tenant) => true;
    }

    [TestCase("bytes")]
    [TestCase("keys")]
    [TestCase("memory")]
    [TestCase("trees")]
    public async Task RollUpAndPublishAsync_stable_sub_threshold_crossing_eventually_refuses_admission(string dimension)
    {
        var tenant = TenantId.Parse("acme");
        var store = new FakeTenantUsageStore();
        var options = Substitute.For<IOptionsMonitor<TenantUsageAccountingOptions>>();
        options.CurrentValue.Returns(new TenantUsageAccountingOptions());
        var publisher = new TenantUsagePublisher(store,
            Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ClusterId = "a" }), options);
        var quotas = dimension switch
        {
            "bytes" => new TenantQuotas { MaxBytes = 100 },
            "keys" => new TenantQuotas { MaxKeys = 100 },
            "memory" => new TenantQuotas { MaxMemoryBytes = 100 },
            _ => new TenantQuotas { MaxTreeCount = 1 },
        };
        var registry = TenantRecord.Create(tenant, TenantStatus.Active, quotas,
            TenantPlacement.Shared, Clock(1), "a");
        TreeUsageSample[] Before() => [Tree(100, 100, 100)];
        TreeUsageSample[] After() => dimension switch
        {
            "bytes" => [Tree(101, 100, 100)],
            "keys" => [Tree(100, 101, 100)],
            "memory" => [Tree(100, 100, 101)],
            _ => [Tree(50, 50, 50), Tree(50, 50, 50)],
        };
        var index = new FakeTenantUsageIndex();
        var controller = new LatticeTenantAdmissionController(
            index, new FixedScopeResolver(TenantEnforcementScope.GlobalConverged), new AdmitRate());
        void Rebuild()
        {
            var compiled = CompiledTenantUsage.Compile([registry], store.Records, "a");
            Assert.That(compiled.TryGetView(tenant, out var view), Is.True);
            index.Views["acme"] = view;
        }

        Assert.That(await publisher.RollUpAndPublishAsync(tenant, Before(), Clock(1)), Is.True);
        Assert.That(await publisher.RollUpAndPublishAsync(tenant, After(), Clock(TimeSpan.TicksPerSecond)), Is.False,
            "normal sub-threshold damping is preserved");
        Rebuild();
        Assert.That(await controller.IsAdmittedAsync(tenant, "orders"), Is.True);

        // The same footprint is re-metered, with monotonic cadence stamps, for an hour.
        for (var minute = 1; minute <= 60; minute++)
        {
            await publisher.RollUpAndPublishAsync(tenant, After(), Clock(TimeSpan.FromMinutes(minute).Ticks));
        }

        Rebuild();
        Assert.That(async () => await controller.IsAdmittedAsync(tenant, "orders"),
            Throws.TypeOf<LatticeQuotaExceededException>(),
            "hysteresis must not make a stable authored quota crossing permanently invisible");

        Assert.That(await publisher.RollUpAndPublishAsync(
            tenant, Before(), Clock(TimeSpan.FromMinutes(65).Ticks)), Is.True);
        Rebuild();
        Assert.That(await controller.IsAdmittedAsync(tenant, "orders"), Is.True,
            "a stable sub-threshold recovery must also lift a previous footprint refusal");
    }
}
