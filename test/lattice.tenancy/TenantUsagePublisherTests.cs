using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;
using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantUsagePublisher"/>: the cadence-driven roll-up
/// and hysteresis-gated publish of this cluster's usage slot. Covers the
/// constructor guards, that a roll-up sums the per-tree samples into the published
/// slot, that a sub-threshold movement is suppressed (no write) while a significant
/// one is published, and that the last-published memory tracks only real publishes.
/// The store is a synchronous in-memory fake, so no timing is involved.
/// </summary>
[TestFixture]
public sealed class TenantUsagePublisherTests
{
    private const string LocalCluster = "east";
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static IOptionsMonitor<TenantUsageAccountingOptions> Options(long absolute = 0, double relative = 0.0)
    {
        var monitor = Substitute.For<IOptionsMonitor<TenantUsageAccountingOptions>>();
        monitor.CurrentValue.Returns(new TenantUsageAccountingOptions
        {
            PublishMinAbsoluteDelta = absolute,
            PublishMinRelativeDelta = relative,
        });
        return monitor;
    }

    private static TenantUsagePublisher Create(ITenantUsageStore store, IOptionsMonitor<TenantUsageAccountingOptions> options) =>
        new(store, Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ClusterId = LocalCluster }), options);

    [Test]
    public void Constructor_null_arguments_throw()
    {
        var store = new FakeTenantUsageStore();
        var cluster = Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ClusterId = LocalCluster });
        var options = Options();

        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantUsagePublisher(null!, cluster, options), Throws.ArgumentNullException);
            Assert.That(() => new TenantUsagePublisher(store, null!, options), Throws.ArgumentNullException);
            Assert.That(() => new TenantUsagePublisher(store, cluster, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void ClusterId_is_the_configured_cluster()
    {
        Assert.That(Create(new FakeTenantUsageStore(), Options()).ClusterId, Is.EqualTo(LocalCluster));
    }

    [Test]
    public void RollUpAndPublishAsync_null_perTree_throws()
    {
        var publisher = Create(new FakeTenantUsageStore(), Options());

        Assert.That(
            async () => await publisher.RollUpAndPublishAsync(Acme, null!, Clock(1)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void RollUpAndPublishAsync_no_tenant_throws()
    {
        var publisher = Create(new FakeTenantUsageStore(), Options());

        Assert.That(
            async () => await publisher.RollUpAndPublishAsync(default, [Tree(1, 1, 1)], Clock(1)),
            Throws.ArgumentException);
    }

    [Test]
    public async Task RollUpAndPublishAsync_publishes_the_rolled_up_slot_for_this_cluster()
    {
        var store = new FakeTenantUsageStore();
        var publisher = Create(store, Options());

        var published = await publisher.RollUpAndPublishAsync(
            Acme,
            [Tree(100, 1, 10), Tree(200, 2, 20)],
            Clock(1));

        Assert.Multiple(() =>
        {
            Assert.That(published, Is.True);
            Assert.That(store.Published, Has.Count.EqualTo(1));
            Assert.That(store.Published[0].Id, Is.EqualTo(Acme));
            Assert.That(store.Published[0].LocalSample(LocalCluster), Is.EqualTo(Sample(300, 3, 30, 2)), "the published slot is this cluster's roll-up");
            Assert.That(store.Published[0].ClusterCount, Is.EqualTo(1), "only this cluster's slot is written");
        });
    }

    [Test]
    public async Task RollUpAndPublishAsync_updates_last_published_after_a_publish()
    {
        var store = new FakeTenantUsageStore();
        var publisher = Create(store, Options());

        await publisher.RollUpAndPublishAsync(Acme, [Tree(100, 1, 10)], Clock(1));

        Assert.That(publisher.LastPublished(Acme), Is.EqualTo(Sample(100, 1, 10, 1)));
    }

    [Test]
    public async Task RollUpAndPublishAsync_suppresses_a_sub_threshold_movement()
    {
        var store = new FakeTenantUsageStore();
        // 64 KiB absolute floor; the second roll-up moves only 1 KB.
        var publisher = Create(store, Options(absolute: 64 * 1024));

        await publisher.RollUpAndPublishAsync(Acme, [Tree(128 * 1024, 1, 0)], Clock(1));
        var second = await publisher.RollUpAndPublishAsync(Acme, [Tree(129 * 1024, 1, 0)], Clock(2));

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.False, "a 1 KB move is below the 64 KiB band and is suppressed");
            Assert.That(store.Published, Has.Count.EqualTo(1), "the suppressed roll-up does not write");
            Assert.That(publisher.LastPublished(Acme), Is.EqualTo(Sample(128 * 1024, 1, 0, 1)), "last-published stays at the first sample");
        });
    }

    [Test]
    public async Task RollUpAndPublishAsync_publishes_a_movement_that_clears_the_band()
    {
        var store = new FakeTenantUsageStore();
        var publisher = Create(store, Options(absolute: 64 * 1024));

        await publisher.RollUpAndPublishAsync(Acme, [Tree(128 * 1024, 1, 0)], Clock(1));
        var second = await publisher.RollUpAndPublishAsync(Acme, [Tree(512 * 1024, 1, 0)], Clock(2));

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.True, "a large move clears the band and publishes");
            Assert.That(store.Published, Has.Count.EqualTo(2));
            Assert.That(publisher.LastPublished(Acme), Is.EqualTo(Sample(512 * 1024, 1, 0, 1)));
        });
    }

    [Test]
    public void LastPublished_of_an_unseen_tenant_is_empty()
    {
        var publisher = Create(new FakeTenantUsageStore(), Options());

        Assert.That(publisher.LastPublished(Acme), Is.EqualTo(LocalUsageSample.Empty));
    }
}
