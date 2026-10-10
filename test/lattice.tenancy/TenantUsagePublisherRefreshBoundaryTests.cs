using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;
using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Checks age-bounded damping without unnecessary unchanged-sample writes.</summary>
[TestFixture]
public sealed class TenantUsagePublisherRefreshBoundaryTests
{
    [Test]
    public async Task RollUpAndPublishAsync_refreshes_changed_sample_at_five_minutes_not_before()
    {
        var tenant = TenantId.Parse("acme");
        var store = new FakeTenantUsageStore();
        var options = Substitute.For<IOptionsMonitor<TenantUsageAccountingOptions>>();
        options.CurrentValue.Returns(new TenantUsageAccountingOptions());
        var publisher = new TenantUsagePublisher(store,
            Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ClusterId = "a" }), options);
        var refresh = TimeSpan.FromMinutes(5).Ticks;
        Assert.That(await publisher.RollUpAndPublishAsync(tenant, [Tree(100, 1, 1)], Clock(1)), Is.True);
        Assert.That(await publisher.RollUpAndPublishAsync(tenant, [Tree(101, 1, 1)], Clock(refresh)), Is.False);
        Assert.That(await publisher.RollUpAndPublishAsync(tenant, [Tree(101, 1, 1)], Clock(refresh + 1)), Is.True);
        Assert.That(await publisher.RollUpAndPublishAsync(tenant, [Tree(101, 1, 1)], Clock(2 * refresh + 1)), Is.False,
            "unchanged samples remain suppressed even after the refresh age");
        Assert.That(store.Published, Has.Count.EqualTo(2));
        Assert.That(publisher.LastPublished(tenant).Bytes, Is.EqualTo(101));
        Assert.That(await publisher.RollUpAndPublishAsync(tenant, [Tree(100, 1, 1)], Clock(3 * refresh + 1)), Is.True,
            "small decreases refresh too, so a previous refusal can clear");
    }
}
