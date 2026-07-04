using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticePolicyEpochFenceContext"/>: the ambient
/// required-epoch floor defaults to absent, a scope sets and restores it, nesting
/// never weakens an outer floor, and a negative floor is rejected.
/// </summary>
[TestFixture]
public class LatticePolicyEpochFenceContextTests
{
    [Test]
    public void RequiredEpoch_is_null_by_default()
    {
        Assert.That(LatticePolicyEpochFenceContext.RequiredEpoch, Is.Null);
    }

    [Test]
    public void RequireAtLeast_sets_the_floor_for_the_scope()
    {
        using (LatticePolicyEpochFenceContext.RequireAtLeast(7))
        {
            Assert.That(LatticePolicyEpochFenceContext.RequiredEpoch, Is.EqualTo(7));
        }

        Assert.That(LatticePolicyEpochFenceContext.RequiredEpoch, Is.Null);
    }

    [Test]
    public void RequireAtLeast_restores_the_previous_floor_on_dispose()
    {
        using (LatticePolicyEpochFenceContext.RequireAtLeast(5))
        {
            using (LatticePolicyEpochFenceContext.RequireAtLeast(9))
            {
                Assert.That(LatticePolicyEpochFenceContext.RequiredEpoch, Is.EqualTo(9));
            }

            Assert.That(LatticePolicyEpochFenceContext.RequiredEpoch, Is.EqualTo(5));
        }
    }

    [Test]
    public void RequireAtLeast_nesting_never_weakens_an_outer_floor()
    {
        using (LatticePolicyEpochFenceContext.RequireAtLeast(9))
        {
            using (LatticePolicyEpochFenceContext.RequireAtLeast(3))
            {
                // A weaker inner requirement must not lower the effective floor.
                Assert.That(LatticePolicyEpochFenceContext.RequiredEpoch, Is.EqualTo(9));
            }
        }
    }

    [Test]
    public void RequireAtLeast_allows_zero()
    {
        using (LatticePolicyEpochFenceContext.RequireAtLeast(0))
        {
            Assert.That(LatticePolicyEpochFenceContext.RequiredEpoch, Is.EqualTo(0));
        }
    }

    [Test]
    public void RequireAtLeast_throws_for_a_negative_floor()
    {
        Assert.That(
            () => LatticePolicyEpochFenceContext.RequireAtLeast(-1),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var scope = LatticePolicyEpochFenceContext.RequireAtLeast(4);
        scope.Dispose();
        scope.Dispose();

        Assert.That(LatticePolicyEpochFenceContext.RequiredEpoch, Is.Null);
    }
}
