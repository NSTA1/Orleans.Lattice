using Orleans.Lattice;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="MonotonicTenantAdminClock"/>: every
/// <see cref="MonotonicTenantAdminClock.Next"/> stamp must be strictly greater than
/// the one before it, so successive control-plane writes each supersede the last
/// under the registry's last-writer-wins join. Deterministic - no wall-time
/// dependence in the assertions.
/// </summary>
[TestFixture]
public sealed class MonotonicTenantAdminClockTests
{
    [Test]
    public void Next_is_strictly_increasing_across_a_burst()
    {
        var clock = new MonotonicTenantAdminClock();

        var previous = clock.Next();
        for (var i = 0; i < 100; i++)
        {
            var current = clock.Next();
            Assert.That(current, Is.GreaterThan(previous),
                "Each stamp must strictly supersede the previous one.");
            previous = current;
        }
    }

    [Test]
    public void Next_seeds_above_the_zero_clock()
    {
        var clock = new MonotonicTenantAdminClock();

        Assert.That(clock.Next(), Is.GreaterThan(HybridLogicalClock.Zero));
    }
}
