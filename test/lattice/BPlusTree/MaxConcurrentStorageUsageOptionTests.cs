using NUnit.Framework;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins the defaults and mutability of the two storage-usage roll-up
/// concurrency knobs
/// (<see cref="LatticeOptions.MaxConcurrentStorageUsageTrees"/> and
/// <see cref="LatticeOptions.MaxConcurrentStorageUsageSurfaces"/>) added to
/// bound the previously unbounded, multiplicative roll-up fan-out (issue
/// #1728). Their product is the cluster-wide peak in-flight grain-call count,
/// so a future default flip is a conscious, test-visible change.
/// </summary>
[TestFixture]
public sealed class MaxConcurrentStorageUsageOptionTests
{
    [Test]
    public void Default_tree_constant_is_eight()
    {
        Assert.That(LatticeOptions.DefaultMaxConcurrentStorageUsageTrees, Is.EqualTo(8));
    }

    [Test]
    public void Default_surface_constant_is_sixteen()
    {
        Assert.That(LatticeOptions.DefaultMaxConcurrentStorageUsageSurfaces, Is.EqualTo(16));
    }

    [Test]
    public void New_options_instance_uses_the_documented_defaults()
    {
        var options = new LatticeOptions();
        Assert.Multiple(() =>
        {
            Assert.That(
                options.MaxConcurrentStorageUsageTrees,
                Is.EqualTo(LatticeOptions.DefaultMaxConcurrentStorageUsageTrees));
            Assert.That(
                options.MaxConcurrentStorageUsageSurfaces,
                Is.EqualTo(LatticeOptions.DefaultMaxConcurrentStorageUsageSurfaces));
        });
    }

    [Test]
    public void Default_peak_in_flight_calls_stay_bounded()
    {
        // The two levels multiply. The documented ceiling is the product, and
        // it must stay small enough that a cluster-wide roll-up cannot saturate
        // a silo's scheduler the way the unbounded shape did (roughly 6,500
        // concurrent calls at 90 trees).
        var peak = LatticeOptions.DefaultMaxConcurrentStorageUsageTrees
                   * LatticeOptions.DefaultMaxConcurrentStorageUsageSurfaces;
        Assert.That(peak, Is.EqualTo(128));
    }

    [Test]
    public void Values_are_settable()
    {
        var options = new LatticeOptions
        {
            MaxConcurrentStorageUsageTrees = 2,
            MaxConcurrentStorageUsageSurfaces = 3,
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.MaxConcurrentStorageUsageTrees, Is.EqualTo(2));
            Assert.That(options.MaxConcurrentStorageUsageSurfaces, Is.EqualTo(3));
        });
    }
}
