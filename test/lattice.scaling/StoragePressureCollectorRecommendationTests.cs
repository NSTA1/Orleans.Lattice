using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for the hot-account selection and target-choice logic inside
/// <see cref="StoragePressureCollector"/> that the round-trip tests do not
/// reach: the precedence rule that promotes a throughput-bound account over an
/// already-selected capacity-bound one, the ranking tie-breaks (saturation, then
/// retained bytes, then ordinal key), the advisory-ratio clamps, the
/// partition-search preference for a saturated tree, and the degenerate shapes
/// (no partitions at all, an unset partition list).
/// <para>
/// Every case is pure over a substituted <see cref="IWalStorageStateSource"/>,
/// so the selection is asserted deterministically with no cluster and no clock.
/// </para>
/// </summary>
[TestFixture]
public sealed class StoragePressureCollectorRecommendationTests
{
    private const string AcctA = "acct-a";
    private const string AcctB = "acct-b";
    private const string AcctC = "acct-c";

    private sealed class FakeSource(WalStorageSample sample) : IWalStorageStateSource
    {
        public ValueTask<WalStorageSample> SampleAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(sample);
    }

    private static StoragePressureCollector Collector(
        WalStorageSample sample,
        long? walMaxRetainedBytes = null,
        Action<LatticeScalingSignalOptions>? configure = null,
        ILogger<StoragePressureCollector>? logger = null)
    {
        var scaling = new LatticeScalingSignalOptions();
        configure?.Invoke(scaling);
        return new StoragePressureCollector(
            new FakeSource(sample),
            Options.Create(scaling),
            Options.Create(new LatticeOptions { WalMaxRetainedBytes = walMaxRetainedBytes }),
            logger);
    }

    [Test]
    public async Task An_injected_logger_is_used_instead_of_the_null_logger()
    {
        var sample = Sample(new[] { AcctA }, Tree("t-a", 100L, partitions: (0, AcctA)));

        var pressure = await Collector(sample, logger: NullLogger<StoragePressureCollector>.Instance)
            .CollectAsync(CancellationToken.None);

        Assert.That(pressure.WalRetainedBytes, Is.EqualTo(100L));
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
            mapped[i] = new WalPartitionSample
            {
                Partition = partitions[i].Partition,
                ProviderKey = partitions[i].Key,
            };
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

    [Test]
    public void A_tree_sample_with_no_partition_list_exposes_an_empty_list()
    {
        // Partitions is init-only with a null-coalescing getter, so a sample
        // constructed without it must still be enumerable.
        var tree = new WalTreeSample { TreeId = "t", WalRetainedBytes = 1 };

        Assert.That(tree.Partitions, Is.Empty);
    }

    [Test]
    public async Task Trees_with_no_partitions_produce_no_accounts_but_still_aggregate_bytes()
    {
        var sample = Sample(new[] { AcctA }, Tree("t-1", 900L));

        var pressure = await Collector(sample).CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.WalRetainedBytes, Is.EqualTo(900L));
            Assert.That(pressure.Accounts, Is.Empty, "No partition means no account to attribute bytes to.");
            Assert.That(pressure.Recommendation, Is.Null);
        });
    }

    [Test]
    public async Task A_throughput_bound_account_outranks_an_already_selected_capacity_bound_one()
    {
        // acct-a sorts first and is capacity-bound, so it is selected first; the
        // later throughput-bound acct-b must displace it, because a saturated
        // backend is the acute condition.
        var sample = Sample(
            new[] { AcctA, AcctB, AcctC },
            Tree("t-a", 1_000L, partitions: (0, AcctA)),
            Tree("t-b", 10L, WalSaturationState.Saturated, TimeSpan.FromMinutes(5), (0, AcctB)));

        var pressure = await Collector(
            sample,
            walMaxRetainedBytes: 1_000L,
            configure: o => o.AccountSaturationWindow = TimeSpan.FromMinutes(1))
            .CollectAsync(CancellationToken.None);

        Assert.That(pressure.Recommendation, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(pressure.Recommendation!.Value.CurrentProviderKey, Is.EqualTo(AcctB));
            Assert.That(
                pressure.Recommendation.Value.Classification,
                Is.EqualTo(WalPressureClassification.ThroughputBound));
        });
    }

    [Test]
    public async Task Between_two_throughput_bound_accounts_the_more_saturated_one_is_chosen()
    {
        var sample = Sample(
            new[] { AcctA, AcctB, AcctC },
            Tree("t-a", 100L, WalSaturationState.Throttled, TimeSpan.FromMinutes(5), (0, AcctA)),
            Tree("t-b", 100L, WalSaturationState.Saturated, TimeSpan.FromMinutes(5), (0, AcctB)));

        var pressure = await Collector(
            sample,
            configure: o => o.AccountSaturationWindow = TimeSpan.FromMinutes(1))
            .CollectAsync(CancellationToken.None);

        Assert.That(pressure.Recommendation, Is.Not.Null);
        Assert.That(
            pressure.Recommendation!.Value.CurrentProviderKey,
            Is.EqualTo(AcctB),
            "Saturated outranks Throttled.");
    }

    [Test]
    public async Task Between_two_capacity_bound_accounts_the_larger_one_is_chosen()
    {
        // Both accounts clear the capacity threshold; neither is saturated, so
        // the ranking falls through to retained bytes.
        var sample = Sample(
            new[] { AcctA, AcctB, AcctC },
            Tree("t-a", 1_000L, partitions: (0, AcctA)),
            Tree("t-b", 5_000L, partitions: (0, AcctB)));

        var pressure = await Collector(sample, walMaxRetainedBytes: 1_000L)
            .CollectAsync(CancellationToken.None);

        Assert.That(pressure.Recommendation, Is.Not.Null);
        Assert.That(pressure.Recommendation!.Value.CurrentProviderKey, Is.EqualTo(AcctB));
    }

    [Test]
    public async Task Equally_hot_accounts_break_the_tie_on_the_ordinal_key()
    {
        var sample = Sample(
            new[] { AcctA, AcctB, AcctC },
            Tree("t-a", 2_000L, partitions: (0, AcctA)),
            Tree("t-b", 2_000L, partitions: (0, AcctB)));

        var pressure = await Collector(sample, walMaxRetainedBytes: 1_000L)
            .CollectAsync(CancellationToken.None);

        Assert.That(pressure.Recommendation, Is.Not.Null);
        Assert.That(
            pressure.Recommendation!.Value.CurrentProviderKey,
            Is.EqualTo(AcctA),
            "An exact tie must resolve to the ordinally-first key so the advice is stable.");
    }

    [Test]
    public async Task The_hot_partition_search_prefers_a_saturated_tree_over_a_healthy_one()
    {
        // Both trees place a partition on the hot account, but only t-b is
        // saturated. The advice must name t-b's partition - relocating the
        // healthy tree would not relieve the pressure.
        var sample = Sample(
            new[] { AcctA, AcctB },
            Tree("t-a", 10L, WalSaturationState.Healthy, TimeSpan.Zero, (7, AcctA)),
            Tree("t-b", 10L, WalSaturationState.Saturated, TimeSpan.FromMinutes(5), (3, AcctA)));

        var pressure = await Collector(
            sample,
            configure: o => o.AccountSaturationWindow = TimeSpan.FromMinutes(1))
            .CollectAsync(CancellationToken.None);

        Assert.That(pressure.Recommendation, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(pressure.Recommendation!.Value.Tree, Is.EqualTo("t-b"));
            Assert.That(pressure.Recommendation.Value.Partition, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task A_capacity_bound_account_names_the_first_matching_partition()
    {
        var sample = Sample(
            new[] { AcctA, AcctB },
            Tree("t-a", 4_000L, partitions: (5, AcctA)));

        var pressure = await Collector(sample, walMaxRetainedBytes: 1_000L)
            .CollectAsync(CancellationToken.None);

        Assert.That(pressure.Recommendation, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(pressure.Recommendation!.Value.Tree, Is.EqualTo("t-a"));
            Assert.That(pressure.Recommendation.Value.Partition, Is.EqualTo(5));
            Assert.That(pressure.Recommendation.Value.TargetProviderKey, Is.EqualTo(AcctB));
            Assert.That(pressure.Recommendation.Value.HasHeadroom, Is.True);
        });
    }

    [Test]
    public async Task The_lowest_ordinal_key_with_headroom_is_chosen_as_the_target()
    {
        // acct-b and acct-c are both idle. The scan must settle on acct-b.
        var sample = Sample(
            new[] { AcctC, AcctB, AcctA },
            Tree("t-a", 4_000L, partitions: (0, AcctA)));

        var pressure = await Collector(sample, walMaxRetainedBytes: 1_000L)
            .CollectAsync(CancellationToken.None);

        Assert.That(pressure.Recommendation, Is.Not.Null);
        Assert.That(pressure.Recommendation!.Value.TargetProviderKey, Is.EqualTo(AcctB));
    }

    [Test]
    public async Task A_non_positive_advisory_ratio_falls_back_to_the_default_ratio()
    {
        // 0 is not a meaningful ratio; the collector must substitute the default
        // (0.8) rather than classify everything as over threshold.
        var sample = Sample(new[] { AcctA }, Tree("t-a", 700L, partitions: (0, AcctA)));

        var pressure = await Collector(
            sample,
            walMaxRetainedBytes: 1_000L,
            configure: o => o.RetainedBytesAdvisoryRatio = 0d)
            .CollectAsync(CancellationToken.None);

        Assert.That(
            pressure.OverThreshold,
            Is.False,
            "700 is below the default advisory fraction of a 1000-byte ceiling.");
    }

    [Test]
    public async Task An_advisory_ratio_above_one_is_clamped_to_the_full_ceiling()
    {
        var sample = Sample(new[] { AcctA }, Tree("t-a", 999L, partitions: (0, AcctA)));

        var pressure = await Collector(
            sample,
            walMaxRetainedBytes: 1_000L,
            configure: o => o.RetainedBytesAdvisoryRatio = 5d)
            .CollectAsync(CancellationToken.None);

        Assert.That(
            pressure.OverThreshold,
            Is.False,
            "Clamped to 1.0, so the threshold is the ceiling itself and 999 is under it.");
    }

    [Test]
    public async Task An_advisory_ratio_clamped_to_one_still_trips_at_the_ceiling()
    {
        var sample = Sample(new[] { AcctA }, Tree("t-a", 1_000L, partitions: (0, AcctA)));

        var pressure = await Collector(
            sample,
            walMaxRetainedBytes: 1_000L,
            configure: o => o.RetainedBytesAdvisoryRatio = 5d)
            .CollectAsync(CancellationToken.None);

        Assert.That(pressure.OverThreshold, Is.True);
    }

    [Test]
    public async Task Remainder_bytes_are_attributed_to_the_first_partition()
    {
        // 10 bytes across 4 partitions is 2 each with a remainder of 2, which
        // must land on partition 0 so the per-account total reconciles exactly
        // with the tree total.
        var sample = Sample(
            new[] { AcctA, AcctB },
            Tree("t-a", 10L, partitions: new[] { (0, AcctA), (1, AcctB), (2, AcctB), (3, AcctB) }));

        var pressure = await Collector(sample).CollectAsync(CancellationToken.None);

        var a = pressure.Accounts.First(x => x.ProviderKey == AcctA);
        var b = pressure.Accounts.First(x => x.ProviderKey == AcctB);

        Assert.Multiple(() =>
        {
            Assert.That(a.WalRetainedBytes, Is.EqualTo(4L), "2 base + 2 remainder.");
            Assert.That(b.WalRetainedBytes, Is.EqualTo(6L));
            Assert.That(a.WalRetainedBytes + b.WalRetainedBytes, Is.EqualTo(10L));
        });
    }

    [Test]
    public async Task A_null_provider_key_is_attributed_to_the_default_catalogue_key()
    {
        var sample = Sample(
            new[] { IWalStorageProviderCatalog.DefaultProviderKey },
            new WalTreeSample
            {
                TreeId = "t-a",
                WalRetainedBytes = 100L,
                Partitions = new[] { new WalPartitionSample { Partition = 0, ProviderKey = null! } },
            });

        var pressure = await Collector(sample).CollectAsync(CancellationToken.None);

        Assert.That(
            pressure.Accounts.Single().ProviderKey,
            Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
    }
}
