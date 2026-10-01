namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeAtomicBatchContext"/> helper
/// that stamps atomic-batch <c>(Size, Index)</c> metadata onto mutations via
/// the ambient Orleans <c>RequestContext</c>. Mirrors the shape of
/// <c>LatticeVectorClockContextTests</c> - the two helpers share the
/// "Current get/set + With(...) IDisposable scope" contract and the same
/// per-test reset hygiene.
/// </summary>
[TestFixture]
public class LatticeAtomicBatchContextTests
{
    [SetUp]
    public void Reset()
    {
        // Clear any ambient value leaking from a previous test on this logical thread.
        LatticeAtomicBatchContext.Current = null;
        LatticeAtomicBatchContext.CurrentIndexMap = null;
        LatticeAtomicBatchContext.CurrentDeltaMap = null;
        LatticeAtomicBatchContext.CurrentDeleteSet = null;
    }

    [Test]
    public void Current_defaults_to_null()
    {
        Assert.That(LatticeAtomicBatchContext.Current, Is.Null);
    }

    [Test]
    public void Setting_Current_reads_back_the_same_pair()
    {
        LatticeAtomicBatchContext.Current = (5, 2);
        Assert.That(LatticeAtomicBatchContext.Current,
            Is.EqualTo(((int Size, int Index)?)(5, 2)));
    }

    [Test]
    public void Setting_Current_to_null_removes_the_ambient_value()
    {
        LatticeAtomicBatchContext.Current = (3, 0);
        LatticeAtomicBatchContext.Current = null;
        Assert.That(LatticeAtomicBatchContext.Current, Is.Null);
    }

    [Test]
    public void With_sets_the_value_for_the_scope()
    {
        using (LatticeAtomicBatchContext.With((4, 1)))
        {
            Assert.That(LatticeAtomicBatchContext.Current,
                Is.EqualTo(((int Size, int Index)?)(4, 1)));
        }
    }

    [Test]
    public void With_restores_previous_value_on_dispose()
    {
        // Outer saga-wide ambient - mirrors AtomicWriteGrain.RunSagaAsync's
        // saga-wide `(Size, 0)` stamp.
        LatticeAtomicBatchContext.Current = (5, 0);
        using (LatticeAtomicBatchContext.With((5, 3)))
        {
            // Per-key override - mirrors ExecutePhaseAsync's per-step scope.
            Assert.That(LatticeAtomicBatchContext.Current,
                Is.EqualTo(((int Size, int Index)?)(5, 3)));
        }
        // Saga-wide stamp restored on disposal - the contract
        // ExecutePhaseAsync relies on so the stamp survives a per-key
        // exception thrown out of the using block.
        Assert.That(LatticeAtomicBatchContext.Current,
            Is.EqualTo(((int Size, int Index)?)(5, 0)));
    }

    [Test]
    public void With_null_restores_previous_non_null_value_on_dispose()
    {
        LatticeAtomicBatchContext.Current = (7, 0);
        using (LatticeAtomicBatchContext.With(null))
        {
            Assert.That(LatticeAtomicBatchContext.Current, Is.Null);
        }
        Assert.That(LatticeAtomicBatchContext.Current,
            Is.EqualTo(((int Size, int Index)?)(7, 0)));
    }

    [Test]
    public void With_nested_scopes_restore_in_reverse_order()
    {
        using (LatticeAtomicBatchContext.With((10, 0)))
        {
            Assert.That(LatticeAtomicBatchContext.Current,
                Is.EqualTo(((int Size, int Index)?)(10, 0)));
            using (LatticeAtomicBatchContext.With((10, 4)))
            {
                Assert.That(LatticeAtomicBatchContext.Current,
                    Is.EqualTo(((int Size, int Index)?)(10, 4)));
            }
            Assert.That(LatticeAtomicBatchContext.Current,
                Is.EqualTo(((int Size, int Index)?)(10, 0)));
        }
        Assert.That(LatticeAtomicBatchContext.Current, Is.Null);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        LatticeAtomicBatchContext.Current = (5, 0);
        var scope = LatticeAtomicBatchContext.With((5, 2));

        scope.Dispose();
        Assert.That(LatticeAtomicBatchContext.Current,
            Is.EqualTo(((int Size, int Index)?)(5, 0)));

        // Second dispose must not re-apply the restore - otherwise it would
        // overwrite any value set after the first dispose returned.
        LatticeAtomicBatchContext.Current = (9, 1);
        scope.Dispose();
        Assert.That(LatticeAtomicBatchContext.Current,
            Is.EqualTo(((int Size, int Index)?)(9, 1)));
    }

    [Test]
    public void Zero_size_zero_index_pair_is_distinct_from_null()
    {
        // The "not-in-a-saga" sentinel a publish helper reads is
        // `null` (ambient unset). An explicit `(0, 0)` pair stored via
        // `Current = (0, 0)` is a *different* state - the publish
        // helper would still default both wire slots to 0, but the
        // ambient itself round-trips faithfully and is observably
        // non-null. Pin the distinction so a future refactor that
        // collapses `(0, 0)` to `null` (or vice versa) regresses
        // visibly.
        LatticeAtomicBatchContext.Current = (0, 0);
        Assert.That(LatticeAtomicBatchContext.Current,
            Is.EqualTo(((int Size, int Index)?)(0, 0)));
        Assert.That(LatticeAtomicBatchContext.Current, Is.Not.Null);
    }

    [Test]
    public void CurrentDeltaMap_defaults_to_null()
    {
        Assert.That(LatticeAtomicBatchContext.CurrentDeltaMap, Is.Null);
    }

    [Test]
    public void With_delta_map_sets_and_restores_all_three_carries()
    {
        var indexMap = new Dictionary<string, int> { ["k"] = 7 };
        var deltaMap = new Dictionary<string, byte[]> { ["k"] = [1, 2, 3] };

        using (LatticeAtomicBatchContext.With((4, 1), indexMap, deltaMap))
        {
            Assert.That(LatticeAtomicBatchContext.Current,
                Is.EqualTo(((int Size, int Index)?)(4, 1)));
            Assert.That(LatticeAtomicBatchContext.CurrentIndexMap, Is.SameAs(indexMap));
            Assert.That(LatticeAtomicBatchContext.CurrentDeltaMap, Is.SameAs(deltaMap));
        }

        Assert.That(LatticeAtomicBatchContext.Current, Is.Null);
        Assert.That(LatticeAtomicBatchContext.CurrentIndexMap, Is.Null);
        Assert.That(LatticeAtomicBatchContext.CurrentDeltaMap, Is.Null);
    }

    [Test]
    public void With_null_delta_map_clears_the_carry_for_the_scope()
    {
        // A value-only saga passes a null delta map; the scope must remove the
        // ambient key so the leaf publish helpers fall back to the saga-wide
        // delta carry rather than reading a stale per-entry map.
        using (LatticeAtomicBatchContext.With((2, 0), indexMap: null, deltaMap: null))
        {
            Assert.That(LatticeAtomicBatchContext.CurrentDeltaMap, Is.Null);
        }
        Assert.That(LatticeAtomicBatchContext.CurrentDeltaMap, Is.Null);
    }

    // The two-argument With(batch, indexMap) overload below had no caller in
    // any fixture. It is the overload whose restore set is easiest to misread,
    // because it restores exactly two of the four ambients and deliberately
    // leaves the delta map and delete set alone - an omission a reader checking
    // only the happy path cannot tell apart from a missing line.

    [Test]
    public void With_index_map_sets_and_restores_both_carries()
    {
        var indexMap = new Dictionary<string, int> { ["a"] = 0, ["b"] = 1 };

        using (LatticeAtomicBatchContext.With((2, 0), indexMap))
        {
            Assert.That(LatticeAtomicBatchContext.Current,
                Is.EqualTo(((int Size, int Index)?)(2, 0)));
            Assert.That(LatticeAtomicBatchContext.CurrentIndexMap, Is.SameAs(indexMap));
        }

        Assert.That(LatticeAtomicBatchContext.Current, Is.Null);
        Assert.That(LatticeAtomicBatchContext.CurrentIndexMap, Is.Null);
    }

    [Test]
    public void With_index_map_restores_an_enclosing_index_map()
    {
        var outer = new Dictionary<string, int> { ["a"] = 0 };
        var inner = new Dictionary<string, int> { ["c"] = 9 };

        using (LatticeAtomicBatchContext.With((4, 0), outer))
        {
            using (LatticeAtomicBatchContext.With((4, 2), inner))
            {
                Assert.That(LatticeAtomicBatchContext.CurrentIndexMap, Is.SameAs(inner));
            }

            // The per-shard fan-out closing must not strip the saga-wide
            // key -> globalIndex lookup; without it the publish helpers fall
            // back to BaseIndex + bucketLocal and mis-stamp every later entry.
            Assert.That(LatticeAtomicBatchContext.CurrentIndexMap, Is.SameAs(outer));
            Assert.That(LatticeAtomicBatchContext.Current,
                Is.EqualTo(((int Size, int Index)?)(4, 0)));
        }
    }

    [Test]
    public void With_index_map_leaves_the_delta_map_and_delete_set_untouched()
    {
        var deltaMap = new Dictionary<string, byte[]> { ["a"] = [1, 2, 3] };
        var deleteSet = new HashSet<string> { "b" };
        LatticeAtomicBatchContext.CurrentDeltaMap = deltaMap;
        LatticeAtomicBatchContext.CurrentDeleteSet = deleteSet;

        using (LatticeAtomicBatchContext.With((2, 0), new Dictionary<string, int> { ["a"] = 0 }))
        {
            Assert.That(LatticeAtomicBatchContext.CurrentDeltaMap, Is.SameAs(deltaMap));
            Assert.That(LatticeAtomicBatchContext.CurrentDeleteSet, Is.SameAs(deleteSet));
        }

        Assert.That(LatticeAtomicBatchContext.CurrentDeltaMap, Is.SameAs(deltaMap));
        Assert.That(LatticeAtomicBatchContext.CurrentDeleteSet, Is.SameAs(deleteSet));
    }

    [Test]
    public void With_index_map_null_clears_the_lookup_for_the_scope()
    {
        var outer = new Dictionary<string, int> { ["a"] = 0 };

        using (LatticeAtomicBatchContext.With((2, 0), outer))
        {
            using (LatticeAtomicBatchContext.With((2, 0), indexMap: null))
            {
                Assert.That(LatticeAtomicBatchContext.CurrentIndexMap, Is.Null);
            }

            Assert.That(LatticeAtomicBatchContext.CurrentIndexMap, Is.SameAs(outer));
        }
    }

    [Test]
    public void With_index_map_dispose_is_idempotent()
    {
        var outer = new Dictionary<string, int> { ["a"] = 0 };

        using (LatticeAtomicBatchContext.With((4, 0), outer))
        {
            var inner = LatticeAtomicBatchContext.With(
                (4, 2), new Dictionary<string, int> { ["c"] = 9 });
            inner.Dispose();
            inner.Dispose();

            Assert.That(LatticeAtomicBatchContext.Current,
                Is.EqualTo(((int Size, int Index)?)(4, 0)));
            Assert.That(LatticeAtomicBatchContext.CurrentIndexMap, Is.SameAs(outer));
        }
    }

    [Test]
    public async Task With_index_map_propagates_across_async_boundaries()
    {
        var indexMap = new Dictionary<string, int> { ["a"] = 0 };

        using (LatticeAtomicBatchContext.With((2, 0), indexMap))
        {
            await Task.Yield();
            Assert.That(LatticeAtomicBatchContext.Current,
                Is.EqualTo(((int Size, int Index)?)(2, 0)));
            Assert.That(LatticeAtomicBatchContext.CurrentIndexMap, Is.SameAs(indexMap));
        }

        await Task.Yield();
        Assert.That(LatticeAtomicBatchContext.Current, Is.Null);
    }

    [Test]
    public void CurrentDeleteSet_round_trips_and_clears()
    {
        var deleteSet = new HashSet<string> { "b" };
        LatticeAtomicBatchContext.CurrentDeleteSet = deleteSet;
        Assert.That(LatticeAtomicBatchContext.CurrentDeleteSet, Is.SameAs(deleteSet));

        LatticeAtomicBatchContext.CurrentDeleteSet = null;
        Assert.That(LatticeAtomicBatchContext.CurrentDeleteSet, Is.Null);
    }
}
