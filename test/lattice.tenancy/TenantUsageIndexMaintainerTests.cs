using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;
using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantUsageIndexMaintainer"/> over a substituted
/// in-memory registry and usage store (no cluster). Covers the constructor guards,
/// the cold start, the warm-up build, the monotonic epoch, the change-feed filter
/// (rebuild on both the reserved registry tree and the usage tree, nothing else),
/// and that a rebuild reflects a newly-landed usage slot. Every change-feed rebuild
/// is drained deterministically by awaiting the captured background task, so no
/// test polls or sleeps.
/// </summary>
[TestFixture]
public sealed class TenantUsageIndexMaintainerTests
{
    private const string LocalCluster = "east";

    private static IOptions<ClusterOptions> Cluster => Options.Create(new ClusterOptions { ClusterId = LocalCluster });

    private static TenantUsageIndexMaintainer Create(FakeTenantRegistry registry, FakeTenantUsageStore usage) =>
        new(registry, usage, Cluster, NullLogger<TenantUsageIndexMaintainer>.Instance);

    [Test]
    public void Constructor_null_arguments_throw()
    {
        var registry = new FakeTenantRegistry();
        var usage = new FakeTenantUsageStore();

        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantUsageIndexMaintainer(null!, usage, Cluster, NullLogger<TenantUsageIndexMaintainer>.Instance), Throws.ArgumentNullException);
            Assert.That(() => new TenantUsageIndexMaintainer(registry, null!, Cluster, NullLogger<TenantUsageIndexMaintainer>.Instance), Throws.ArgumentNullException);
            Assert.That(() => new TenantUsageIndexMaintainer(registry, usage, null!, NullLogger<TenantUsageIndexMaintainer>.Instance), Throws.ArgumentNullException);
            Assert.That(() => new TenantUsageIndexMaintainer(registry, usage, Cluster, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Fresh_maintainer_starts_at_epoch_zero_with_the_empty_snapshot()
    {
        var maintainer = Create(new FakeTenantRegistry(), new FakeTenantUsageStore());

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(0));
            Assert.That(maintainer.Current, Is.SameAs(CompiledTenantUsage.Empty));
        });
    }

    [Test]
    public async Task EnsureWarmAsync_builds_the_snapshot_once_and_is_idempotent()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme"));
        var maintainer = Create(registry, new FakeTenantUsageStore());

        await maintainer.EnsureWarmAsync();
        var epochAfterFirst = maintainer.CurrentEpoch;
        await maintainer.EnsureWarmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(epochAfterFirst, Is.EqualTo(1));
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1), "warming an already-warm maintainer does not rebuild");
            Assert.That(maintainer.Current.TenantCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RebuildNowAsync_advances_the_epoch_monotonically()
    {
        var maintainer = Create(new FakeTenantRegistry(), new FakeTenantUsageStore());

        var first = await maintainer.RebuildNowAsync();
        var second = await maintainer.RebuildNowAsync();
        var third = await maintainer.RebuildNowAsync();

        Assert.That(new[] { first, second, third }, Is.EqualTo(new long[] { 1, 2, 3 }));
    }

    [Test]
    public void IsUsageOrRegistryMutation_is_true_for_the_registry_and_usage_trees_only()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantUsageIndexMaintainer.IsUsageOrRegistryMutation(new LatticeMutation { TreeId = TenantTreeNames.RegistryTree }),
                Is.True);
            Assert.That(
                TenantUsageIndexMaintainer.IsUsageOrRegistryMutation(new LatticeMutation { TreeId = TenantTreeNames.UsageTree }),
                Is.True);
            Assert.That(
                TenantUsageIndexMaintainer.IsUsageOrRegistryMutation(new LatticeMutation { TreeId = "some-app-tree" }),
                Is.False);
        });
    }

    [Test]
    public async Task OnMutationAsync_on_the_usage_tree_rebuilds_the_snapshot()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme"));
        var usage = new FakeTenantUsageStore();
        var maintainer = Create(registry, usage);
        await maintainer.EnsureWarmAsync();
        var epochBefore = maintainer.CurrentEpoch;

        // A new slot lands, then the usage-tree mutation drives the rebuild.
        usage.Records.Add(UsageRecord("acme", ("east", Sample(100, 1, 10, 1))));
        await maintainer.OnMutationAsync(new LatticeMutation { TreeId = TenantTreeNames.UsageTree }, CancellationToken.None);
        await maintainer.BackgroundRebuild;

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.CurrentEpoch, Is.GreaterThan(epochBefore));
            Assert.That(maintainer.TryGetView(TenantId.Parse("acme"), out var view), Is.True);
            Assert.That(view.GlobalUsage, Is.EqualTo(Sample(100, 1, 10, 1)), "the rebuild reflects the newly-landed slot");
        });
    }

    [Test]
    public async Task OnMutationAsync_on_the_registry_tree_rebuilds_the_snapshot()
    {
        var registry = new FakeTenantRegistry();
        var usage = new FakeTenantUsageStore();
        var maintainer = Create(registry, usage);
        await maintainer.EnsureWarmAsync();
        var epochBefore = maintainer.CurrentEpoch;

        registry.Records.Add(Record("acme"));
        await maintainer.OnMutationAsync(new LatticeMutation { TreeId = TenantTreeNames.RegistryTree }, CancellationToken.None);
        await maintainer.BackgroundRebuild;

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.CurrentEpoch, Is.GreaterThan(epochBefore));
            Assert.That(maintainer.Current.TenantCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task OnMutationAsync_on_an_unrelated_tree_does_not_rebuild()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme"));
        var maintainer = Create(registry, new FakeTenantUsageStore());
        var epochBefore = maintainer.CurrentEpoch;

        await maintainer.OnMutationAsync(new LatticeMutation { TreeId = "some-app-tree" }, CancellationToken.None);
        await maintainer.BackgroundRebuild;

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(epochBefore), "an unrelated mutation must not rebuild");
            Assert.That(maintainer.Current, Is.SameAs(CompiledTenantUsage.Empty), "the snapshot stays cold");
        });
    }

    [Test]
    public async Task TryGetView_delegates_to_the_current_snapshot()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme"));
        var usage = new FakeTenantUsageStore();
        usage.Records.Add(UsageRecord("acme", ("east", Sample(100, 1, 10, 1))));
        var maintainer = Create(registry, usage);

        Assert.That(maintainer.TryGetView(TenantId.Parse("acme"), out _), Is.False, "cold maintainer has no view");

        await maintainer.EnsureWarmAsync();

        Assert.That(maintainer.TryGetView(TenantId.Parse("acme"), out var view), Is.True);
        Assert.That(view.LocalUsage, Is.EqualTo(Sample(100, 1, 10, 1)));
    }
}
