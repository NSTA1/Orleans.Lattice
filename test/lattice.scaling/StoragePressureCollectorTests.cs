using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for <see cref="StoragePressureCollector"/>: the reduction of a
/// cluster-aggregate WAL storage sample into a normalised
/// <see cref="StoragePressure"/> - aggregate and per-account retained bytes,
/// throughput-bound vs capacity-bound classification, over-threshold detection,
/// and the rebalance recommendation (target-key selection with and without
/// headroom, and the provision-another-account fallback). Uses a substituted
/// <see cref="IWalStorageStateSource"/> so no cluster is required.
/// </summary>
[TestFixture]
public sealed class StoragePressureCollectorTests
{
    private const string AcctA = "acct-a";
    private const string AcctB = "acct-b";
    private const string Default = "default";

    private sealed class FakeSource(WalStorageSample sample) : IWalStorageStateSource
    {
        public ValueTask<WalStorageSample> SampleAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(sample);
    }

    private sealed class ThrowingSource : IWalStorageStateSource
    {
        public ValueTask<WalStorageSample> SampleAsync(CancellationToken cancellationToken)
            => throw new InvalidOperationException("boom");
    }

    private static StoragePressureCollector Collector(
        IWalStorageStateSource source,
        long? walMaxRetainedBytes = null,
        Action<LatticeScalingSignalOptions>? configure = null)
    {
        var scaling = new LatticeScalingSignalOptions();
        configure?.Invoke(scaling);
        var lattice = new LatticeOptions { WalMaxRetainedBytes = walMaxRetainedBytes };
        return new StoragePressureCollector(
            source,
            Options.Create(scaling),
            Options.Create(lattice));
    }

    private static WalTreeSample Tree(
        string treeId,
        long bytes,
        WalSaturationState saturation = WalSaturationState.Healthy,
        TimeSpan saturatedFor = default,
        params (int Partition, string Key)[] partitions)
    {
        var mapped = new WalPartitionSample[partitions.Length];
        for (var i = 0; i < partitions.Length; i++)
        {
            mapped[i] = new WalPartitionSample { Partition = partitions[i].Partition, ProviderKey = partitions[i].Key };
        }

        return new WalTreeSample
        {
            TreeId = treeId,
            WalRetainedBytes = bytes,
            Saturation = saturation,
            SaturatedFor = saturatedFor,
            Partitions = mapped,
        };
    }

    private static WalStorageSample Sample(IEnumerable<string> catalogKeys, params WalTreeSample[] trees)
        => new() { Trees = trees, CatalogKeys = catalogKeys.ToArray() };

    private static WalAccountPressure Account(StoragePressure pressure, string key)
    {
        foreach (var account in pressure.Accounts)
        {
            if (account.ProviderKey == key)
            {
                return account;
            }
        }

        Assert.Fail($"No account for key '{key}'.");
        return default;
    }

    [Test]
    public async Task Empty_sample_yields_zero_pressure()
    {
        var collector = Collector(new FakeSource(default));

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.OverThreshold, Is.False);
            Assert.That(pressure.WalRetainedBytes, Is.Zero);
            Assert.That(pressure.Accounts, Is.Empty);
            Assert.That(pressure.Recommendation, Is.Null);
        });
    }

    [Test]
    public async Task Source_failure_yields_zero_pressure()
    {
        var collector = Collector(new ThrowingSource());

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.OverThreshold, Is.False);
            Assert.That(pressure.Accounts, Is.Empty);
            Assert.That(pressure.Recommendation, Is.Null);
        });
    }

    [Test]
    public async Task Aggregate_retained_bytes_sum_every_tree()
    {
        var sample = Sample(
            [AcctA],
            Tree("t1", 100, partitions: (0, AcctA)),
            Tree("t2", 250, partitions: (0, AcctA)));
        var collector = Collector(new FakeSource(sample));

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.That(pressure.WalRetainedBytes, Is.EqualTo(350));
        Assert.That(Account(pressure, AcctA).WalRetainedBytes, Is.EqualTo(350));
    }

    [Test]
    public async Task Multi_account_one_hot_partition_recommends_target_with_headroom()
    {
        // acct-a is throughput-bound (saturated longer than the window); acct-b is
        // idle and registered, so it is the headroom target.
        var sample = Sample(
            [AcctA, AcctB],
            Tree("hot", 1000, WalSaturationState.Saturated, TimeSpan.FromMinutes(1), (2, AcctA)),
            Tree("cool", 10, WalSaturationState.Healthy, default, (0, AcctB)));
        var collector = Collector(new FakeSource(sample),
            configure: o => o.AccountSaturationWindow = TimeSpan.FromSeconds(30));

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Account(pressure, AcctA).Classification,
                Is.EqualTo(WalPressureClassification.ThroughputBound));
            Assert.That(pressure.Recommendation, Is.Not.Null);
            var rec = pressure.Recommendation!.Value;
            Assert.That(rec.CurrentProviderKey, Is.EqualTo(AcctA));
            Assert.That(rec.TargetProviderKey, Is.EqualTo(AcctB));
            Assert.That(rec.HasHeadroom, Is.True);
            Assert.That(rec.Tree, Is.EqualTo("hot"));
            Assert.That(rec.Partition, Is.EqualTo(2));
            Assert.That(rec.Classification, Is.EqualTo(WalPressureClassification.ThroughputBound));
            Assert.That(rec.Rationale, Does.Contain("ExecuteWalMoveAsync"));
        });
    }

    [Test]
    public async Task Single_account_over_threshold_recommends_provisioning_another_account()
    {
        // One registered account, retained bytes over the advisory ratio of the
        // ceiling: capacity-bound with nowhere to move to.
        var sample = Sample(
            [Default],
            Tree("t1", 900, partitions: (0, Default)));
        var collector = Collector(new FakeSource(sample), walMaxRetainedBytes: 1000);

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.OverThreshold, Is.True);
            var account = Account(pressure, Default);
            Assert.That(account.OverThreshold, Is.True);
            Assert.That(account.Classification, Is.EqualTo(WalPressureClassification.CapacityBound));
            Assert.That(pressure.Recommendation, Is.Not.Null);
            var rec = pressure.Recommendation!.Value;
            Assert.That(rec.HasHeadroom, Is.False);
            Assert.That(rec.TargetProviderKey, Is.Empty);
            Assert.That(rec.Rationale, Does.Contain("AddLatticeWalStorageProvider"));
        });
    }

    [Test]
    public async Task Throughput_and_capacity_bound_accounts_classify_independently()
    {
        var sample = Sample(
            [AcctA, AcctB],
            Tree("hot", 10, WalSaturationState.Saturated, TimeSpan.FromMinutes(1), (0, AcctA)),
            Tree("full", 900, WalSaturationState.Healthy, default, (0, AcctB)));
        var collector = Collector(new FakeSource(sample), walMaxRetainedBytes: 1000,
            configure: o => o.AccountSaturationWindow = TimeSpan.FromSeconds(30));

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Account(pressure, AcctA).Classification,
                Is.EqualTo(WalPressureClassification.ThroughputBound));
            Assert.That(Account(pressure, AcctB).Classification,
                Is.EqualTo(WalPressureClassification.CapacityBound));
            Assert.That(Account(pressure, AcctB).OverThreshold, Is.True);
            // Throughput-bound is the acute case: it wins the single recommendation.
            Assert.That(pressure.Recommendation!.Value.CurrentProviderKey, Is.EqualTo(AcctA));
        });
    }

    [Test]
    public async Task Saturation_shorter_than_window_is_not_throughput_bound()
    {
        var sample = Sample(
            [AcctA, AcctB],
            Tree("blip", 10, WalSaturationState.Saturated, TimeSpan.FromSeconds(5), (0, AcctA)));
        var collector = Collector(new FakeSource(sample),
            configure: o => o.AccountSaturationWindow = TimeSpan.FromSeconds(30));

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Account(pressure, AcctA).Classification,
                Is.EqualTo(WalPressureClassification.None));
            Assert.That(pressure.Recommendation, Is.Null);
        });
    }

    [Test]
    public async Task Zero_window_classifies_on_first_saturated_sample()
    {
        var sample = Sample(
            [AcctA, AcctB],
            Tree("hot", 10, WalSaturationState.Throttled, TimeSpan.Zero, (0, AcctA)));
        var collector = Collector(new FakeSource(sample),
            configure: o => o.AccountSaturationWindow = TimeSpan.Zero);

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.That(Account(pressure, AcctA).Classification,
            Is.EqualTo(WalPressureClassification.ThroughputBound));
    }

    [Test]
    public async Task All_accounts_hot_leaves_recommendation_without_headroom()
    {
        var sample = Sample(
            [AcctA, AcctB],
            Tree("hot-a", 10, WalSaturationState.Saturated, TimeSpan.FromMinutes(1), (0, AcctA)),
            Tree("hot-b", 10, WalSaturationState.Saturated, TimeSpan.FromMinutes(1), (0, AcctB)));
        var collector = Collector(new FakeSource(sample),
            configure: o => o.AccountSaturationWindow = TimeSpan.FromSeconds(30));

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.Recommendation, Is.Not.Null);
            Assert.That(pressure.Recommendation!.Value.HasHeadroom, Is.False);
            Assert.That(pressure.Recommendation!.Value.TargetProviderKey, Is.Empty);
        });
    }

    [Test]
    public async Task Idle_registered_account_is_a_headroom_target()
    {
        // acct-b backs no partition at all but is registered: it has full headroom.
        var sample = Sample(
            [AcctA, AcctB],
            Tree("hot", 10, WalSaturationState.Saturated, TimeSpan.FromMinutes(1), (0, AcctA)));
        var collector = Collector(new FakeSource(sample),
            configure: o => o.AccountSaturationWindow = TimeSpan.FromSeconds(30));

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.That(pressure.Recommendation!.Value.TargetProviderKey, Is.EqualTo(AcctB));
        Assert.That(pressure.Recommendation!.Value.HasHeadroom, Is.True);
    }

    [Test]
    public async Task Recommendations_disabled_suppresses_recommendation_but_keeps_over_threshold()
    {
        var sample = Sample(
            [Default],
            Tree("t1", 900, partitions: (0, Default)));
        var collector = Collector(new FakeSource(sample), walMaxRetainedBytes: 1000,
            configure: o => o.StorageRecommendationsEnabled = false);

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.OverThreshold, Is.True);
            Assert.That(Account(pressure, Default).OverThreshold, Is.True);
            Assert.That(pressure.Recommendation, Is.Null);
        });
    }

    [Test]
    public async Task No_capacity_ceiling_never_reports_over_threshold()
    {
        var sample = Sample(
            [Default],
            Tree("t1", long.MaxValue / 2, partitions: (0, Default)));
        var collector = Collector(new FakeSource(sample), walMaxRetainedBytes: null);

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.OverThreshold, Is.False);
            Assert.That(Account(pressure, Default).OverThreshold, Is.False);
            Assert.That(Account(pressure, Default).Classification, Is.EqualTo(WalPressureClassification.None));
            Assert.That(pressure.Recommendation, Is.Null);
        });
    }

    [Test]
    public async Task Advisory_ratio_scales_the_capacity_threshold()
    {
        // 700 bytes is under 0.8*1000=800 but over 0.5*1000=500.
        var sample = Sample([Default], Tree("t1", 700, partitions: (0, Default)));

        var lenient = await Collector(new FakeSource(sample), walMaxRetainedBytes: 1000)
            .CollectAsync(CancellationToken.None);
        var strict = await Collector(new FakeSource(sample), walMaxRetainedBytes: 1000,
                configure: o => o.RetainedBytesAdvisoryRatio = 0.5)
            .CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(lenient.OverThreshold, Is.False);
            Assert.That(strict.OverThreshold, Is.True);
        });
    }

    [Test]
    public async Task Accounts_are_ordered_by_provider_key()
    {
        var sample = Sample(
            [AcctA, AcctB],
            Tree("t2", 10, partitions: (0, AcctB)),
            Tree("t1", 10, partitions: (0, AcctA)));
        var collector = Collector(new FakeSource(sample));

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.That(pressure.Accounts.Select(a => a.ProviderKey), Is.EqualTo(new[] { AcctA, AcctB }));
    }
}
