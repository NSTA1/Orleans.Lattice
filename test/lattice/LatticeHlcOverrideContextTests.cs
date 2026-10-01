using NUnit.Framework;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeHlcOverrideContext"/>, the ambient
/// hybrid-logical-clock override that source-HLC-preserving apply paths use to
/// stamp a replicated entry's timestamp verbatim rather than taking a fresh
/// local tick.
/// <para>
/// Each per-key apply opens exactly one scope, so the nested and
/// double-dispose arms were cold. They matter because the replication applier
/// drives these scopes in a tight loop: a scope that failed to restore would
/// leak one entry's source clock onto the next key, which LWW resolution would
/// then honour as if it were authored at that time.
/// </para>
/// </summary>
[TestFixture]
public class LatticeHlcOverrideContextTests
{
    private static readonly HybridLogicalClock Remote =
        new() { WallClockTicks = 1_000, Counter = 7 };

    private static readonly HybridLogicalClock Later =
        new() { WallClockTicks = 2_000, Counter = 1 };

    [SetUp]
    [TearDown]
    public void EnsureCleanContext()
        => RequestContext.Remove(LatticeEventConstants.HlcOverrideRequestContextKey);

    [Test]
    public void Current_is_null_for_a_direct_foreground_caller()
    {
        Assert.That(LatticeHlcOverrideContext.Current, Is.Null);
    }

    [Test]
    public void Current_is_null_when_the_context_carries_a_foreign_value()
    {
        RequestContext.Set(LatticeEventConstants.HlcOverrideRequestContextKey, "not-a-clock");

        Assert.That(LatticeHlcOverrideContext.Current, Is.Null);
    }

    [Test]
    public void Current_round_trips_an_override_verbatim()
    {
        LatticeHlcOverrideContext.Current = Remote;

        Assert.That(LatticeHlcOverrideContext.Current, Is.EqualTo(Remote));
    }

    [Test]
    public void Setting_null_removes_the_entry()
    {
        LatticeHlcOverrideContext.Current = Remote;
        LatticeHlcOverrideContext.Current = null;

        Assert.That(
            RequestContext.Get(LatticeEventConstants.HlcOverrideRequestContextKey),
            Is.Null,
            "the documented contract is removal, so the leaf falls back to a fresh local tick");
    }

    [Test]
    public void With_publishes_the_override_for_the_lifetime_of_the_scope()
    {
        using (LatticeHlcOverrideContext.With(Remote))
        {
            Assert.That(LatticeHlcOverrideContext.Current, Is.EqualTo(Remote));
        }

        Assert.That(LatticeHlcOverrideContext.Current, Is.Null);
    }

    [Test]
    public void With_null_explicitly_clears_the_ambient()
    {
        using (LatticeHlcOverrideContext.With(Remote))
        {
            using (LatticeHlcOverrideContext.With(null))
            {
                Assert.That(LatticeHlcOverrideContext.Current, Is.Null);
            }

            Assert.That(LatticeHlcOverrideContext.Current, Is.EqualTo(Remote));
        }
    }

    [Test]
    public void Nested_scope_restores_the_enclosing_override()
    {
        using (LatticeHlcOverrideContext.With(Remote))
        {
            using (LatticeHlcOverrideContext.With(Later))
            {
                Assert.That(LatticeHlcOverrideContext.Current, Is.EqualTo(Later));
            }

            Assert.That(
                LatticeHlcOverrideContext.Current,
                Is.EqualTo(Remote),
                "a per-key scope closing must not leave the next key stamped with this key's source clock");
        }

        Assert.That(LatticeHlcOverrideContext.Current, Is.Null);
    }

    [Test]
    public void Scope_dispose_is_idempotent()
    {
        var scope = LatticeHlcOverrideContext.With(Remote);
        scope.Dispose();
        scope.Dispose();

        Assert.That(LatticeHlcOverrideContext.Current, Is.Null);
    }

    [Test]
    public void Redundant_dispose_of_an_inner_scope_cannot_clear_the_outer_override()
    {
        using (LatticeHlcOverrideContext.With(Remote))
        {
            var inner = LatticeHlcOverrideContext.With(Later);
            inner.Dispose();
            inner.Dispose();

            Assert.That(LatticeHlcOverrideContext.Current, Is.EqualTo(Remote));
        }
    }

    [Test]
    public async Task Scope_propagates_across_async_boundaries()
    {
        using (LatticeHlcOverrideContext.With(Remote))
        {
            await Task.Yield();
            Assert.That(LatticeHlcOverrideContext.Current, Is.EqualTo(Remote));
        }

        await Task.Yield();
        Assert.That(LatticeHlcOverrideContext.Current, Is.Null);
    }
}
