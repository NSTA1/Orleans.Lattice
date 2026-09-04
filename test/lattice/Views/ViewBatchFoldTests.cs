namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="ViewBatchFold"/>, the fused single-pass form of
/// <see cref="ViewWriteCoalescer.Coalesce(IEnumerable{ViewWrite})"/> and
/// <see cref="ViewKeyCollisionDetector.Detect(IEnumerable{ViewWrite})"/>.
/// <para>
/// The load-bearing property is <b>equivalence</b>: the fold must return exactly
/// what the two standalone helpers return for the same batch, because the drain
/// path replaced two passes with this one. The randomised equivalence test at the
/// end is the real guard - the hand-written cases pin the specific edges that
/// make the fusion non-trivial (an unattributed write that precedes an
/// attributable one for the same key, ties, and repeated collisions).
/// </para>
/// </summary>
[TestFixture]
public class ViewBatchFoldTests
{
    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    /// <summary>
    /// Asserts the fold agrees element-for-element with the two helpers it
    /// replaces, which is the whole contract of the fusion.
    /// </summary>
    private static void AssertMatchesSeparatePasses(List<ViewWrite> writes)
    {
        var fold = ViewBatchFold.Fold(writes);

        Assert.Multiple(() =>
        {
            Assert.That(
                fold.Survivors,
                Is.EqualTo(ViewWriteCoalescer.Coalesce(writes)),
                "survivors must match ViewWriteCoalescer.Coalesce");
            Assert.That(
                fold.Collisions,
                Is.EqualTo(ViewKeyCollisionDetector.Detect(writes)),
                "collisions must match ViewKeyCollisionDetector.Detect");
        });
    }

    [Test]
    public void Fold_null_writes_throws()
    {
        Assert.That(() => ViewBatchFold.Fold(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Fold_empty_batch_yields_no_survivors_and_no_collisions()
    {
        var fold = ViewBatchFold.Fold([]);

        Assert.Multiple(() =>
        {
            Assert.That(fold.Survivors, Is.Empty);
            Assert.That(fold.Collisions, Is.Empty);
        });
    }

    [Test]
    public void Fold_keeps_highest_timestamp_per_key_in_first_seen_key_order()
    {
        var writes = new List<ViewWrite>
        {
            ViewWrite.Upsert("b", [1], Clock(10)),
            ViewWrite.Upsert("a", [2], Clock(20)),
            ViewWrite.Upsert("b", [3], Clock(30)),
        };

        var fold = ViewBatchFold.Fold(writes);

        Assert.Multiple(() =>
        {
            Assert.That(fold.Survivors.Select(w => w.Key), Is.EqualTo(new[] { "b", "a" }));
            Assert.That(fold.Survivors[0].Value, Is.EqualTo(new byte[] { 3 }));
        });
        AssertMatchesSeparatePasses(writes);
    }

    [Test]
    public void Fold_tie_does_not_displace_the_incumbent()
    {
        var writes = new List<ViewWrite>
        {
            ViewWrite.Upsert("k", [1], Clock(5)),
            ViewWrite.Upsert("k", [2], Clock(5)),
        };

        var fold = ViewBatchFold.Fold(writes);

        Assert.That(fold.Survivors.Single().Value, Is.EqualTo(new byte[] { 1 }));
        AssertMatchesSeparatePasses(writes);
    }

    [Test]
    public void Fold_same_source_key_repeated_is_not_a_collision()
    {
        var writes = new List<ViewWrite>
        {
            ViewWrite.Upsert("v", [1], Clock(1), sourceKey: "a"),
            ViewWrite.Upsert("v", [2], Clock(2), sourceKey: "a"),
        };

        Assert.That(ViewBatchFold.Fold(writes).Collisions, Is.Empty);
        AssertMatchesSeparatePasses(writes);
    }

    [Test]
    public void Fold_two_distinct_source_keys_one_view_key_is_a_collision()
    {
        var writes = new List<ViewWrite>
        {
            ViewWrite.Upsert("v", [1], Clock(1), sourceKey: "a"),
            ViewWrite.Upsert("v", [2], Clock(2), sourceKey: "b"),
        };

        Assert.That(ViewBatchFold.Fold(writes).Collisions, Is.EqualTo(new[] { "v" }));
        AssertMatchesSeparatePasses(writes);
    }

    [Test]
    public void Fold_reports_each_colliding_view_key_once()
    {
        var writes = new List<ViewWrite>
        {
            ViewWrite.Upsert("v", [1], Clock(1), sourceKey: "a"),
            ViewWrite.Upsert("v", [2], Clock(2), sourceKey: "b"),
            ViewWrite.Upsert("v", [3], Clock(3), sourceKey: "c"),
        };

        Assert.That(ViewBatchFold.Fold(writes).Collisions, Is.EqualTo(new[] { "v" }));
        AssertMatchesSeparatePasses(writes);
    }

    [Test]
    public void Fold_ignores_writes_without_a_source_key()
    {
        var writes = new List<ViewWrite>
        {
            ViewWrite.Upsert("v", [1], Clock(1)),
            ViewWrite.Upsert("v", [2], Clock(2)),
        };

        Assert.That(ViewBatchFold.Fold(writes).Collisions, Is.Empty);
        AssertMatchesSeparatePasses(writes);
    }

    [Test]
    public void Fold_unattributed_write_first_does_not_suppress_a_later_collision()
    {
        // The fused slot is created by the FIRST write for a key regardless of
        // attribution (the coalescer needs it), so a leading unattributed write
        // must still leave the first-source field open for the next attributable
        // write - otherwise the collision below would be missed.
        var writes = new List<ViewWrite>
        {
            ViewWrite.Upsert("v", [1], Clock(1)),
            ViewWrite.Upsert("v", [2], Clock(2), sourceKey: "a"),
            ViewWrite.Upsert("v", [3], Clock(3), sourceKey: "b"),
        };

        Assert.That(ViewBatchFold.Fold(writes).Collisions, Is.EqualTo(new[] { "v" }));
        AssertMatchesSeparatePasses(writes);
    }

    [Test]
    public void Fold_unattributed_write_between_two_sources_does_not_mask_the_collision()
    {
        var writes = new List<ViewWrite>
        {
            ViewWrite.Upsert("v", [1], Clock(1), sourceKey: "a"),
            ViewWrite.Upsert("v", [2], Clock(2)),
            ViewWrite.Upsert("v", [3], Clock(3), sourceKey: "b"),
        };

        Assert.That(ViewBatchFold.Fold(writes).Collisions, Is.EqualTo(new[] { "v" }));
        AssertMatchesSeparatePasses(writes);
    }

    [Test]
    public void Fold_reports_multiple_colliding_keys_in_first_seen_order()
    {
        var writes = new List<ViewWrite>
        {
            ViewWrite.Upsert("x", [1], Clock(1), sourceKey: "s1"),
            ViewWrite.Upsert("y", [2], Clock(2), sourceKey: "s2"),
            ViewWrite.Upsert("y", [3], Clock(3), sourceKey: "s3"),
            ViewWrite.Upsert("x", [4], Clock(4), sourceKey: "s4"),
        };

        Assert.That(ViewBatchFold.Fold(writes).Collisions, Is.EqualTo(new[] { "y", "x" }));
        AssertMatchesSeparatePasses(writes);
    }

    [Test]
    public void Fold_deletes_and_upserts_coalesce_together_by_view_key()
    {
        var writes = new List<ViewWrite>
        {
            ViewWrite.Upsert("k", [1], Clock(1), sourceKey: "a"),
            ViewWrite.Delete("k", Clock(9), sourceKey: "a"),
        };

        var fold = ViewBatchFold.Fold(writes);

        Assert.That(fold.Survivors.Single().Kind, Is.EqualTo(ViewWriteKind.Delete));
        AssertMatchesSeparatePasses(writes);
    }

    [Test]
    public void Fold_matches_the_two_separate_passes_across_randomised_batches()
    {
        // The equivalence guard: exercise the full cross product of the shapes
        // that make the fusion non-trivial - key reuse, attributed and
        // unattributed writes, repeated and distinct source keys, deletes, and
        // heavy timestamp ties - and assert the fold never disagrees with the
        // helpers it replaced.
        var random = new Random(20260904);

        for (var iteration = 0; iteration < 400; iteration++)
        {
            var count = random.Next(0, 40);
            var writes = new List<ViewWrite>(count);
            for (var i = 0; i < count; i++)
            {
                // Deliberately tiny key and source spaces so collisions, update
                // streams and ties are frequent rather than rare.
                var key = "k" + random.Next(0, 4);
                var sourceKey = random.Next(0, 4) switch
                {
                    0 => null,
                    var s => "s" + s,
                };
                var hlc = Clock(random.Next(0, 6));

                writes.Add(random.Next(0, 4) == 0
                    ? ViewWrite.Delete(key, hlc, sourceKey)
                    : ViewWrite.Upsert(key, [(byte)i], hlc, sourceKey: sourceKey));
            }

            AssertMatchesSeparatePasses(writes);
        }
    }
}
