using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the leaf-access tracking knobs (<see
/// cref="LatticeOptions.LeafCachePreWarmCount"/> and <see
/// cref="LatticeOptions.LeafAccessModelFlushIntervalMs"/>): their validation
/// rules and their resolution into a <see cref="LeafAccessTrackingSettings"/>
/// through <see cref="LatticeOptionsResolver.GetLeafAccessTrackingSettings"/>.
/// </summary>
[TestFixture]
public class LatticeOptionsResolverLeafAccessTests
{
    private static ValidateOptionsResult Validate(Action<LatticeOptions> configure)
    {
        var options = new LatticeOptions();
        configure(options);
        return new LatticeOptionsValidator().Validate(null, options);
    }

    private static LatticeOptionsResolver Build(LatticeOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        return new LatticeOptionsResolver(Substitute.For<IGrainFactory>(), monitor);
    }

    // ---- validation

    [Test]
    public void Defaults_pass_validation()
    {
        Assert.That(Validate(_ => { }).Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(32)]
    [TestCase(64)]
    public void LeafCachePreWarmCount_within_bounds_succeeds(int value)
    {
        Assert.That(Validate(o => o.LeafCachePreWarmCount = value).Succeeded, Is.True);
    }

    [TestCase(-1)]
    [TestCase(65)]
    [TestCase(int.MaxValue)]
    public void LeafCachePreWarmCount_outside_bounds_fails(int value)
    {
        var result = Validate(o => o.LeafCachePreWarmCount = value);
        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("LeafCachePreWarmCount"));
        });
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(30_000)]
    public void LeafAccessModelFlushIntervalMs_non_negative_succeeds(int value)
    {
        Assert.That(Validate(o => o.LeafAccessModelFlushIntervalMs = value).Succeeded, Is.True);
    }

    [TestCase(-1)]
    [TestCase(int.MinValue)]
    public void LeafAccessModelFlushIntervalMs_negative_fails(int value)
    {
        var result = Validate(o => o.LeafAccessModelFlushIntervalMs = value);
        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("LeafAccessModelFlushIntervalMs"));
        });
    }

    // ---- resolution

    [Test]
    public void GetLeafAccessTrackingSettings_rejects_a_null_tree_id()
    {
        var resolver = Build(new LatticeOptions());
        Assert.That(() => resolver.GetLeafAccessTrackingSettings(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetLeafAccessTrackingSettings_is_enabled_by_default()
    {
        var settings = Build(new LatticeOptions()).GetLeafAccessTrackingSettings("t");

        Assert.Multiple(() =>
        {
            Assert.That(settings.IsEnabled, Is.True);
            Assert.That(settings.PreWarmCount, Is.EqualTo(LatticeOptions.DefaultLeafCachePreWarmCount));
            Assert.That(settings.FlushIntervalMs, Is.EqualTo(LatticeOptions.DefaultLeafAccessModelFlushIntervalMs));
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void GetLeafAccessTrackingSettings_treats_a_non_positive_count_as_disabled(int value)
    {
        var settings = Build(new LatticeOptions { LeafCachePreWarmCount = value })
            .GetLeafAccessTrackingSettings("t");

        Assert.That(settings.IsEnabled, Is.False);
    }

    [Test]
    public void GetLeafAccessTrackingSettings_passes_a_configured_count_through()
    {
        var settings = Build(new LatticeOptions
        {
            LeafCachePreWarmCount = 12,
            LeafAccessModelFlushIntervalMs = 5_000,
        }).GetLeafAccessTrackingSettings("t");

        Assert.Multiple(() =>
        {
            Assert.That(settings.IsEnabled, Is.True);
            Assert.That(settings.PreWarmCount, Is.EqualTo(12));
            Assert.That(settings.FlushIntervalMs, Is.EqualTo(5_000));
        });
    }

    [Test]
    public void GetLeafAccessTrackingSettings_clamps_a_count_above_the_ceiling()
    {
        // Defence in depth: the validator already rejects this, but a caller
        // that bypassed validation must still not ask for more leaves than the
        // shard root persists.
        var settings = Build(new LatticeOptions { LeafCachePreWarmCount = 5_000 })
            .GetLeafAccessTrackingSettings("t");

        Assert.That(settings.PreWarmCount, Is.EqualTo(64));
    }

    [Test]
    public void GetLeafAccessTrackingSettings_floors_a_negative_flush_interval()
    {
        var settings = Build(new LatticeOptions
        {
            LeafCachePreWarmCount = 4,
            LeafAccessModelFlushIntervalMs = -1,
        }).GetLeafAccessTrackingSettings("t");

        Assert.That(settings.FlushIntervalMs, Is.Zero);
    }

    // ---- the settings struct itself

    [Test]
    public void Disabled_settings_report_a_zero_count()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LeafAccessTrackingSettings.Disabled.PreWarmCount, Is.Zero);
            Assert.That(LeafAccessTrackingSettings.Disabled.IsEnabled, Is.False);
        });
    }

    [Test]
    public void Settings_with_a_positive_count_are_enabled()
    {
        Assert.That(new LeafAccessTrackingSettings(1, 0).IsEnabled, Is.True);
    }
}
