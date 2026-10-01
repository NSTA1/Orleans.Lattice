using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the internal <see cref="LatticeAtomicShardCountContext"/>
/// ambient that carries a saga's authoritative touched-shard count down into
/// the per-shard terminal publish helpers, where it is stamped onto
/// <see cref="LatticeMutation.AtomicShardCount"/> for the receiver-side
/// cross-cluster visibility gate.
/// <para>
/// The getter's defensive arms were cold. It reads the ambient as
/// <c>raw is int count &amp;&amp; count &gt; 0</c>, and neither the
/// non-positive nor the foreign-type arm had ever been taken, so the only
/// evidence that a stale or foreign value degrades to "no gating information"
/// (rather than throwing, or worse, returning a bogus expected total) was the
/// source reading as though it would.
/// </para>
/// </summary>
[TestFixture]
public class LatticeAtomicShardCountContextTests
{
    [SetUp]
    [TearDown]
    public void EnsureCleanContext()
        => RequestContext.Remove(LatticeEventConstants.AtomicShardCountRequestContextKey);

    [Test]
    public void Current_is_null_outside_a_saga()
    {
        Assert.That(LatticeAtomicShardCountContext.Current, Is.Null);
    }

    [Test]
    public void Current_round_trips_a_positive_count()
    {
        LatticeAtomicShardCountContext.Current = 3;

        Assert.That(LatticeAtomicShardCountContext.Current, Is.EqualTo(3));
    }

    [Test]
    public void Setting_null_removes_the_entry()
    {
        LatticeAtomicShardCountContext.Current = 4;
        LatticeAtomicShardCountContext.Current = null;

        Assert.That(
            RequestContext.Get(LatticeEventConstants.AtomicShardCountRequestContextKey),
            Is.Null,
            "the documented contract is removal, not storing a zero");
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Setting_a_non_positive_count_removes_the_entry(int count)
    {
        LatticeAtomicShardCountContext.Current = 5;
        LatticeAtomicShardCountContext.Current = count;

        Assert.That(LatticeAtomicShardCountContext.Current, Is.Null);
        Assert.That(
            RequestContext.Get(LatticeEventConstants.AtomicShardCountRequestContextKey),
            Is.Null);
    }

    [Test]
    public void Current_reads_a_non_positive_ambient_as_no_gating_information()
    {
        // A zero can only reach the ambient from another build or a hand-rolled
        // caller, because the setter above refuses to store one. The getter has
        // to refuse it too: a zero expected-total would gate on nothing.
        RequestContext.Set(LatticeEventConstants.AtomicShardCountRequestContextKey, 0);

        Assert.That(LatticeAtomicShardCountContext.Current, Is.Null);
    }

    [Test]
    public void Current_reads_a_foreign_typed_ambient_as_no_gating_information()
    {
        RequestContext.Set(LatticeEventConstants.AtomicShardCountRequestContextKey, "2");

        Assert.That(LatticeAtomicShardCountContext.Current, Is.Null);
    }

    [Test]
    public void With_publishes_the_count_for_the_lifetime_of_the_scope()
    {
        using (LatticeAtomicShardCountContext.With(7))
        {
            Assert.That(LatticeAtomicShardCountContext.Current, Is.EqualTo(7));
        }

        Assert.That(LatticeAtomicShardCountContext.Current, Is.Null);
    }

    [Test]
    public void With_null_explicitly_clears_the_ambient()
    {
        using (LatticeAtomicShardCountContext.With(2))
        {
            using (LatticeAtomicShardCountContext.With(null))
            {
                Assert.That(LatticeAtomicShardCountContext.Current, Is.Null);
            }

            Assert.That(
                LatticeAtomicShardCountContext.Current,
                Is.EqualTo(2),
                "clearing inside a saga must not strip the enclosing count on the way out");
        }
    }

    [Test]
    public void Nested_scope_restores_the_enclosing_count()
    {
        using (LatticeAtomicShardCountContext.With(2))
        {
            using (LatticeAtomicShardCountContext.With(5))
            {
                Assert.That(LatticeAtomicShardCountContext.Current, Is.EqualTo(5));
            }

            Assert.That(LatticeAtomicShardCountContext.Current, Is.EqualTo(2));
        }

        Assert.That(LatticeAtomicShardCountContext.Current, Is.Null);
    }

    [Test]
    public void Scope_dispose_is_idempotent()
    {
        var scope = LatticeAtomicShardCountContext.With(6);
        scope.Dispose();
        scope.Dispose();

        Assert.That(LatticeAtomicShardCountContext.Current, Is.Null);
    }

    [Test]
    public void Redundant_dispose_of_an_inner_scope_cannot_clear_the_outer_count()
    {
        using (LatticeAtomicShardCountContext.With(2))
        {
            var inner = LatticeAtomicShardCountContext.With(9);
            inner.Dispose();
            inner.Dispose();

            Assert.That(LatticeAtomicShardCountContext.Current, Is.EqualTo(2));
        }
    }

    [Test]
    public async Task Scope_propagates_across_async_boundaries()
    {
        using (LatticeAtomicShardCountContext.With(4))
        {
            await Task.Yield();
            Assert.That(LatticeAtomicShardCountContext.Current, Is.EqualTo(4));
        }

        await Task.Yield();
        Assert.That(LatticeAtomicShardCountContext.Current, Is.Null);
    }
}
