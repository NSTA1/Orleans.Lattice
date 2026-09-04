using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the scan-page bounds (<see
/// cref="LatticeOptions.MaxLeavesPerScanPage"/>, <see
/// cref="LatticeOptions.MaxScanPageDuration"/> and <see
/// cref="LatticeOptions.MaxScanPageStallDuration"/>): their validation rules
/// and their synchronous resolution into a <see cref="ScanPageBounds"/> through
/// <see cref="LatticeOptionsResolver.GetScanPageBounds"/>.
/// <para>
/// The synchronous resolve exists so the shard root can arm both bounds as the
/// first statement of a page-fill grain call, with no <c>await</c> in front of
/// the clock they start (issue 2002). That only stays correct while it agrees
/// with the asynchronous <see cref="LatticeOptionsResolver.ResolveAsync"/>
/// path, so the drift guard below is load-bearing rather than incidental.
/// </para>
/// </summary>
[TestFixture]
public class LatticeOptionsResolverScanPageBoundsTests
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
    public void Default_stall_duration_passes_validation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Validate(_ => { }).Succeeded, Is.True);
            Assert.That(
                new LatticeOptions().MaxScanPageStallDuration,
                Is.EqualTo(LatticeOptions.DefaultMaxScanPageStallDuration));
        });
    }

    [Test]
    public void Default_stall_duration_exceeds_the_default_graceful_budget()
    {
        // The graceful budget must get the chance to return a partial page
        // before the hard ceiling faults the call, or the cooperative bound is
        // dead configuration.
        Assert.That(
            LatticeOptions.DefaultMaxScanPageStallDuration,
            Is.GreaterThan(LatticeOptions.DefaultMaxScanPageDuration));
    }

    [Test]
    public void Infinite_stall_duration_succeeds_and_disables_the_ceiling()
    {
        var result = Validate(o => o.MaxScanPageStallDuration = Timeout.InfiniteTimeSpan);

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.True);
            Assert.That(
                Build(new LatticeOptions { MaxScanPageStallDuration = Timeout.InfiniteTimeSpan })
                    .GetScanPageBounds("t").IsStallGuarded,
                Is.False);
        });
    }

    [Test]
    public void Non_positive_stall_duration_fails()
    {
        // A non-positive ceiling would fault every scan before it read a leaf,
        // so it is rejected rather than silently treated as "disabled" -
        // TimeSpan.Zero disables the graceful budget, but the two knobs must
        // not use the same sentinel for opposite meanings.
        foreach (var value in new[] { TimeSpan.Zero, TimeSpan.FromSeconds(-1) })
        {
            var result = Validate(o => o.MaxScanPageStallDuration = value);
            Assert.Multiple(() =>
            {
                Assert.That(result.Failed, Is.True, $"{value} should fail");
                Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.MaxScanPageStallDuration)));
            });
        }
    }

    [Test]
    public void Stall_duration_not_greater_than_the_graceful_budget_fails()
    {
        foreach (var stall in new[] { TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(4) })
        {
            var result = Validate(o =>
            {
                o.MaxScanPageDuration = TimeSpan.FromSeconds(5);
                o.MaxScanPageStallDuration = stall;
            });

            Assert.Multiple(() =>
            {
                Assert.That(result.Failed, Is.True, $"{stall} should fail");
                Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.MaxScanPageDuration)));
            });
        }
    }

    [Test]
    public void Stall_duration_below_a_disabled_graceful_budget_succeeds()
    {
        // With the cooperative budget off there is nothing to order against,
        // so the ordering rule must not fire.
        var result = Validate(o =>
        {
            o.MaxScanPageDuration = TimeSpan.Zero;
            o.MaxScanPageStallDuration = TimeSpan.FromMilliseconds(1);
        });

        Assert.That(result.Succeeded, Is.True);
    }

    // ---- resolution

    [Test]
    public void GetScanPageBounds_rejects_a_null_tree_id()
    {
        var resolver = Build(new LatticeOptions());
        Assert.That(() => resolver.GetScanPageBounds(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetScanPageBounds_returns_the_defaults()
    {
        var bounds = Build(new LatticeOptions()).GetScanPageBounds("t");

        Assert.Multiple(() =>
        {
            Assert.That(bounds.MaxLeaves, Is.EqualTo(LatticeOptions.DefaultMaxLeavesPerScanPage));
            Assert.That(bounds.MaxDuration, Is.EqualTo(LatticeOptions.DefaultMaxScanPageDuration));
            Assert.That(bounds.StallDuration, Is.EqualTo(LatticeOptions.DefaultMaxScanPageStallDuration));
            Assert.That(bounds.IsStallGuarded, Is.True);
        });
    }

    [Test]
    public void GetScanPageBounds_passes_configured_values_through()
    {
        var bounds = Build(new LatticeOptions
        {
            MaxLeavesPerScanPage = 7,
            MaxScanPageDuration = TimeSpan.FromSeconds(2),
            MaxScanPageStallDuration = TimeSpan.FromSeconds(11),
        }).GetScanPageBounds("t");

        Assert.Multiple(() =>
        {
            Assert.That(bounds.MaxLeaves, Is.EqualTo(7));
            Assert.That(bounds.MaxDuration, Is.EqualTo(TimeSpan.FromSeconds(2)));
            Assert.That(bounds.StallDuration, Is.EqualTo(TimeSpan.FromSeconds(11)));
        });
    }

    [Test]
    public async Task GetScanPageBounds_agrees_with_the_async_resolve()
    {
        // Drift guard. The synchronous path exists only because all three
        // bounds are non-structural passthroughs; if one ever gains a
        // per-tree registry override, this fails rather than silently arming
        // the wrong bound at every scan-page site.
        var options = new LatticeOptions
        {
            MaxLeavesPerScanPage = 13,
            MaxScanPageDuration = TimeSpan.FromSeconds(3),
            MaxScanPageStallDuration = TimeSpan.FromSeconds(19),
        };
        var resolver = Build(options);

        var sync = resolver.GetScanPageBounds("t");
        var async = await resolver.ResolveAsync("t");

        Assert.Multiple(() =>
        {
            Assert.That(sync.MaxLeaves, Is.EqualTo(async.MaxLeavesPerScanPage));
            Assert.That(sync.MaxDuration, Is.EqualTo(async.MaxScanPageDuration));
            Assert.That(sync.StallDuration, Is.EqualTo(async.MaxScanPageStallDuration));
        });
    }

    // ---- the bounds struct itself

    [Test]
    public void IsStallGuarded_is_false_only_for_the_infinite_sentinel()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                new ScanPageBounds(64, TimeSpan.FromSeconds(5), Timeout.InfiniteTimeSpan).IsStallGuarded,
                Is.False);
            Assert.That(
                new ScanPageBounds(64, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30)).IsStallGuarded,
                Is.True);
        });
    }

    [Test]
    public void Bounds_compare_by_value()
    {
        var a = new ScanPageBounds(64, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30));
        var b = new ScanPageBounds(64, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30));

        Assert.That(a, Is.EqualTo(b));
    }
}
