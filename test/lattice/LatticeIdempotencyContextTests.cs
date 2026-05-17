using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the ambient <see cref="LatticeIdempotencyContext"/>
/// helper that carries a caller-supplied
/// <see cref="LatticeIdempotencyKey"/> through the public
/// <see cref="ILattice"/> mutating entry-points.
/// </summary>
[TestFixture]
public class LatticeIdempotencyContextTests
{
    [SetUp]
    public void Reset() => LatticeIdempotencyContext.Current = null;

    private static LatticeIdempotencyKey KeyA()
    {
        var hlc = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        return new LatticeIdempotencyKey { Timestamp = hlc };
    }

    private static LatticeIdempotencyKey KeyB()
    {
        // Tick twice so KeyB() != KeyA() even with no origin field.
        var hlc = HybridLogicalClock.Tick(HybridLogicalClock.Tick(HybridLogicalClock.Zero));
        return new LatticeIdempotencyKey { Timestamp = hlc };
    }

    [Test]
    public void Current_defaults_to_null()
    {
        Assert.That(LatticeIdempotencyContext.Current, Is.Null);
    }

    [Test]
    public void Setting_Current_reads_back_the_same_value()
    {
        var key = KeyA();
        LatticeIdempotencyContext.Current = key;
        Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(key));
    }

    [Test]
    public void Setting_Current_to_null_clears_the_ambient_value()
    {
        LatticeIdempotencyContext.Current = KeyA();
        LatticeIdempotencyContext.Current = null;
        Assert.That(LatticeIdempotencyContext.Current, Is.Null);
    }

    [Test]
    public void With_sets_the_value_for_the_scope()
    {
        var key = KeyA();
        using (LatticeIdempotencyContext.With(key))
        {
            Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(key));
        }
    }

    [Test]
    public void With_restores_previous_value_on_dispose()
    {
        var outer = KeyA();
        var inner = KeyB();
        LatticeIdempotencyContext.Current = outer;
        using (LatticeIdempotencyContext.With(inner))
        {
            Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(inner));
        }
        Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(outer));
    }

    [Test]
    public void With_null_clears_the_value_for_the_scope_and_restores_on_dispose()
    {
        var outer = KeyA();
        LatticeIdempotencyContext.Current = outer;
        using (LatticeIdempotencyContext.With(null))
        {
            Assert.That(LatticeIdempotencyContext.Current, Is.Null);
        }
        Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(outer));
    }

    [Test]
    public void With_nested_scopes_restore_in_reverse_order()
    {
        var a = KeyA();
        var b = KeyB();
        using (LatticeIdempotencyContext.With(a))
        {
            Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(a));
            using (LatticeIdempotencyContext.With(b))
            {
                Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(b));
            }
            Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(a));
        }
        Assert.That(LatticeIdempotencyContext.Current, Is.Null);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var outer = KeyA();
        var inner = KeyB();
        LatticeIdempotencyContext.Current = outer;
        var scope = LatticeIdempotencyContext.With(inner);

        scope.Dispose();
        Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(outer));

        LatticeIdempotencyContext.Current = null;
        scope.Dispose();
        Assert.That(LatticeIdempotencyContext.Current, Is.Null);
    }

    [Test]
    public async Task Current_flows_across_async_await_boundary()
    {
        var key = KeyA();
        using (LatticeIdempotencyContext.With(key))
        {
            await Task.Yield();
            Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(key));
        }
    }

    [Test]
    public void IsActive_is_false_when_no_scope_is_set()
    {
        Assert.That(LatticeIdempotencyContext.IsActive, Is.False);
    }

    [Test]
    public void IsActive_is_true_when_Current_is_set()
    {
        LatticeIdempotencyContext.Current = KeyA();
        Assert.That(LatticeIdempotencyContext.IsActive, Is.True);
    }

    [Test]
    public void IsActive_tracks_With_scope_entry_and_exit()
    {
        Assert.That(LatticeIdempotencyContext.IsActive, Is.False);
        using (LatticeIdempotencyContext.With(KeyA()))
        {
            Assert.That(LatticeIdempotencyContext.IsActive, Is.True);
        }
        Assert.That(LatticeIdempotencyContext.IsActive, Is.False);
    }

    [Test]
    public void IsActive_is_false_when_With_null_clears_outer_scope()
    {
        LatticeIdempotencyContext.Current = KeyA();
        Assert.That(LatticeIdempotencyContext.IsActive, Is.True);
        using (LatticeIdempotencyContext.With(null))
        {
            Assert.That(LatticeIdempotencyContext.IsActive, Is.False);
        }
        Assert.That(LatticeIdempotencyContext.IsActive, Is.True);
    }

    [Test]
    public void NewScope_opens_a_scope_with_a_fresh_key()
    {
        Assert.That(LatticeIdempotencyContext.IsActive, Is.False);
        using (LatticeIdempotencyContext.NewScope())
        {
            Assert.That(LatticeIdempotencyContext.IsActive, Is.True);
            Assert.That(LatticeIdempotencyContext.Current, Is.Not.Null);
            Assert.That(LatticeIdempotencyContext.Current!.Value.Timestamp,
                Is.Not.EqualTo(default(HybridLogicalClock)),
                "NewScope() must mint a non-default HLC via Fresh().");
        }
        Assert.That(LatticeIdempotencyContext.IsActive, Is.False);
    }

    [Test]
    public void NewScope_restores_previous_ambient_value_on_dispose()
    {
        var outer = KeyA();
        LatticeIdempotencyContext.Current = outer;
        using (LatticeIdempotencyContext.NewScope())
        {
            Assert.That(LatticeIdempotencyContext.Current, Is.Not.EqualTo(outer));
        }
        Assert.That(LatticeIdempotencyContext.Current, Is.EqualTo(outer));
    }

    [Test]
    public void NewScope_calls_produce_distinct_keys_per_invocation()
    {
        LatticeIdempotencyKey first, second;
        using (LatticeIdempotencyContext.NewScope())
        {
            first = LatticeIdempotencyContext.Current!.Value;
        }
        using (LatticeIdempotencyContext.NewScope())
        {
            second = LatticeIdempotencyContext.Current!.Value;
        }
        Assert.That(first, Is.Not.EqualTo(second),
            "Each NewScope() invocation must mint a fresh key.");
    }
}
