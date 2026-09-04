using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LeafAccessFrequencyModel"/> - the bounded
/// visit-count histogram that ranks which leaf caches a shard root should
/// pre-warm after a restart (issue #332).
/// </summary>
[TestFixture]
public class LeafAccessFrequencyModelTests
{
    private static GrainId Leaf(int n) => GrainId.Create("leaf", $"tree/0/leaf-{n:D4}");

    private static void RecordSequence(LeafAccessFrequencyModel model, params int[] leaves)
    {
        foreach (var n in leaves) model.Record(Leaf(n));
    }

    // ---- caps: pinned literals, then a separate assertion that the constants
    // match them. Deriving an expectation from the constant under test would
    // make the test vacuous.

    [Test]
    public void Caps_are_the_documented_literals()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LeafAccessFrequencyModel.MaxTrackedLeaves, Is.EqualTo(256));
            Assert.That(LeafAccessFrequencyModel.MaxPersistedLeaves, Is.EqualTo(64));
        });
    }

    [Test]
    public void MaxPersistedLeaves_matches_the_options_pre_warm_upper_bound()
    {
        // The option can never ask for more leaves than the snapshot persists,
        // otherwise a configured pre-warm count would be silently unsatisfiable
        // after a restart.
        Assert.That(
            LatticeOptions.MaxLeafCachePreWarmCount,
            Is.EqualTo(LeafAccessFrequencyModel.MaxPersistedLeaves));
    }

    // ---- empty / degenerate

    [Test]
    public void New_model_is_empty_and_clean()
    {
        var model = new LeafAccessFrequencyModel();

        Assert.Multiple(() =>
        {
            Assert.That(model.TrackedLeafCount, Is.Zero);
            Assert.That(model.Observations, Is.Zero);
            Assert.That(model.IsDirty, Is.False);
            Assert.That(model.RankTopLeaves(8), Is.Empty);
            Assert.That(model.CaptureSnapshot().Leaves, Is.Empty);
        });
    }

    [Test]
    public void RankTopLeaves_returns_empty_for_non_positive_count()
    {
        var model = new LeafAccessFrequencyModel();
        RecordSequence(model, 1, 2, 3);

        Assert.Multiple(() =>
        {
            Assert.That(model.RankTopLeaves(0), Is.Empty);
            Assert.That(model.RankTopLeaves(-1), Is.Empty);
        });
    }

    [Test]
    public void RankTopLeaves_clamps_to_the_tracked_leaf_count()
    {
        var model = new LeafAccessFrequencyModel();
        RecordSequence(model, 1, 2, 3);

        Assert.That(model.RankTopLeaves(100), Has.Length.EqualTo(3));
    }

    // ---- recording semantics

    [Test]
    public void Record_counts_a_visit_and_marks_the_model_dirty()
    {
        var model = new LeafAccessFrequencyModel();
        model.Record(Leaf(1));

        Assert.Multiple(() =>
        {
            Assert.That(model.TrackedLeafCount, Is.EqualTo(1));
            Assert.That(model.Observations, Is.EqualTo(1));
            Assert.That(model.IsDirty, Is.True);
        });
    }

    [Test]
    public void MarkPersisted_clears_the_dirty_flag_until_the_next_record()
    {
        var model = new LeafAccessFrequencyModel();
        model.Record(Leaf(1));
        model.MarkPersisted();

        Assert.That(model.IsDirty, Is.False);

        model.Record(Leaf(2));

        Assert.That(model.IsDirty, Is.True);
    }

    [Test]
    public void Record_of_the_same_leaf_twice_counts_two_visits_on_one_leaf()
    {
        var model = new LeafAccessFrequencyModel();
        RecordSequence(model, 1, 1);

        var snapshot = model.CaptureSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(model.TrackedLeafCount, Is.EqualTo(1));
            Assert.That(model.Observations, Is.EqualTo(2));
            Assert.That(snapshot.Visits, Is.EqualTo(new[] { 2L }));
        });
    }

    // ---- ranking correctness

    [Test]
    public void RankTopLeaves_ranks_a_skewed_workload_by_frequency_not_recency()
    {
        // Leaf 1 and 2 are read constantly; leaf 9 is touched exactly once,
        // last. A recency oracle would rank leaf 9 first. Frequency must not.
        var model = new LeafAccessFrequencyModel();
        for (var i = 0; i < 50; i++)
        {
            model.Record(Leaf(1));
            model.Record(Leaf(2));
        }
        model.Record(Leaf(9));

        var ranked = model.RankTopLeaves(3);

        Assert.Multiple(() =>
        {
            Assert.That(ranked.Take(2), Is.EquivalentTo(new[] { Leaf(1), Leaf(2) }),
                "the two constantly-read leaves must occupy the top two ranks, in either order");
            Assert.That(ranked[2], Is.EqualTo(Leaf(9)));
        });
    }

    [Test]
    public void RankTopLeaves_recovers_more_of_a_skewed_hot_set_than_a_recency_list()
    {
        // The design claim that selected this model over an LRU list, pinned as
        // a deterministic property: on a skewed trace whose tail is a burst of
        // cold one-shot leaves, frequency recovers the whole hot set and a
        // recency list of the same size recovers almost none of it.
        const int topN = 8;
        var model = new LeafAccessFrequencyModel();
        var recency = new List<GrainId>();

        void Access(GrainId leaf)
        {
            model.Record(leaf);
            recency.Remove(leaf);
            recency.Add(leaf);
        }

        var hotSet = Enumerable.Range(0, topN).Select(Leaf).ToHashSet();

        // A long skewed phase over the hot set...
        for (var round = 0; round < 100; round++)
        {
            for (var i = 0; i < topN; i++) Access(Leaf(i));
        }

        // ...then a tail of cold one-shot leaves, which is exactly what
        // saturates a recency list just before a shutdown.
        for (var i = 500; i < 500 + topN; i++) Access(Leaf(i));

        var byFrequency = model.RankTopLeaves(topN).Count(hotSet.Contains);
        var byRecency = Enumerable.Reverse(recency).Take(topN).Count(hotSet.Contains);

        Assert.Multiple(() =>
        {
            Assert.That(byFrequency, Is.EqualTo(topN),
                "frequency must recover the entire hot set");
            Assert.That(byRecency, Is.Zero,
                "recency must be fully displaced by the cold tail, which is the failure mode frequency exists to avoid");
        });
    }

    [Test]
    public void RankTopLeaves_orders_strictly_by_descending_visit_count()
    {
        var model = new LeafAccessFrequencyModel();
        for (var i = 0; i < 5; i++)
        {
            // Leaf i gets (5 - i) visits, so the expected order is 0, 1, 2, 3, 4.
            for (var v = 0; v < 5 - i; v++) model.Record(Leaf(i));
        }

        Assert.That(
            model.RankTopLeaves(5),
            Is.EqualTo(new[] { Leaf(0), Leaf(1), Leaf(2), Leaf(3), Leaf(4) }));
    }

    [Test]
    public void RankTopLeaves_is_deterministic_across_repeated_calls()
    {
        var model = new LeafAccessFrequencyModel();
        for (var i = 0; i < 40; i++) model.Record(Leaf(i % 7));

        var first = model.RankTopLeaves(7);
        var second = model.RankTopLeaves(7);
        var third = model.RankTopLeaves(7);

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.EqualTo(first));
            Assert.That(third, Is.EqualTo(first));
        });
    }

    [Test]
    public void RankTopLeaves_breaks_an_exact_tie_by_ascending_grain_identity()
    {
        // Every leaf has exactly one visit, so the tie-break is the only thing
        // deciding the order. It must be the identity order, not the hash order.
        var model = new LeafAccessFrequencyModel();
        foreach (var n in new[] { 7, 3, 9, 1, 5 }) model.Record(Leaf(n));

        Assert.That(
            model.RankTopLeaves(5),
            Is.EqualTo(new[] { Leaf(1), Leaf(3), Leaf(5), Leaf(7), Leaf(9) }));
    }

    // ---- caps and eviction

    [Test]
    public void Record_never_exceeds_the_tracked_leaf_cap()
    {
        var model = new LeafAccessFrequencyModel();
        for (var i = 0; i < LeafAccessFrequencyModel.MaxTrackedLeaves * 4; i++)
        {
            model.Record(Leaf(i));
        }

        Assert.That(model.TrackedLeafCount, Is.LessThanOrEqualTo(LeafAccessFrequencyModel.MaxTrackedLeaves));
    }

    [Test]
    public void Pruning_retains_the_hottest_leaves_and_evicts_the_coldest()
    {
        var model = new LeafAccessFrequencyModel();

        // A small set of leaves read very often.
        for (var round = 0; round < 100; round++)
        {
            for (var hot = 0; hot < 4; hot++) model.Record(Leaf(hot));
        }
        // Then a flood of one-shot leaves that must force several prune passes.
        for (var i = 1000; i < 1000 + (LeafAccessFrequencyModel.MaxTrackedLeaves * 4); i++)
        {
            model.Record(Leaf(i));
        }

        var ranked = model.RankTopLeaves(LeafAccessFrequencyModel.MaxTrackedLeaves);

        Assert.Multiple(() =>
        {
            Assert.That(model.TrackedLeafCount, Is.LessThanOrEqualTo(LeafAccessFrequencyModel.MaxTrackedLeaves));
            for (var hot = 0; hot < 4; hot++)
            {
                Assert.That(ranked, Does.Contain(Leaf(hot)),
                    $"hot leaf {hot} must survive pruning");
            }
        });
    }

    [Test]
    public void Record_is_deterministic_for_a_given_access_sequence()
    {
        // Two models fed the identical sequence must be indistinguishable
        // through every observable, including after pruning has fired.
        static LeafAccessFrequencyModel Build()
        {
            var model = new LeafAccessFrequencyModel();
            for (var i = 0; i < LeafAccessFrequencyModel.MaxTrackedLeaves * 3; i++)
            {
                model.Record(Leaf(i % 400));
                model.Record(Leaf(i % 13));
            }
            return model;
        }

        var a = Build();
        var b = Build();

        Assert.Multiple(() =>
        {
            Assert.That(b.TrackedLeafCount, Is.EqualTo(a.TrackedLeafCount));
            Assert.That(b.Observations, Is.EqualTo(a.Observations));
            Assert.That(b.RankTopLeaves(64), Is.EqualTo(a.RankTopLeaves(64)));
            Assert.That(b.CaptureSnapshot().Leaves, Is.EqualTo(a.CaptureSnapshot().Leaves));
            Assert.That(b.CaptureSnapshot().Visits, Is.EqualTo(a.CaptureSnapshot().Visits));
        });
    }

    // ---- snapshot / restore

    [Test]
    public void CaptureSnapshot_never_exceeds_the_persisted_leaf_cap()
    {
        var model = new LeafAccessFrequencyModel();
        for (var i = 0; i < LeafAccessFrequencyModel.MaxTrackedLeaves; i++)
        {
            model.Record(Leaf(i));
        }

        var snapshot = model.CaptureSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Leaves, Has.Count.EqualTo(LeafAccessFrequencyModel.MaxPersistedLeaves));
            Assert.That(snapshot.Visits, Has.Count.EqualTo(LeafAccessFrequencyModel.MaxPersistedLeaves));
        });
    }

    [Test]
    public void CaptureSnapshot_writes_parallel_lists_ordered_most_visited_first()
    {
        var model = new LeafAccessFrequencyModel();
        RecordSequence(model, 1, 1, 1, 2, 2, 3);

        var snapshot = model.CaptureSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Leaves, Has.Count.EqualTo(snapshot.Visits.Count));
            Assert.That(snapshot.Visits, Is.EqualTo(new[] { 3L, 2L, 1L }));
            Assert.That(snapshot.Leaves[0], Is.EqualTo(Leaf(1).ToString()));
        });
    }

    [Test]
    public void CaptureSnapshot_persists_only_the_hottest_leaves_when_over_the_cap()
    {
        var model = new LeafAccessFrequencyModel();
        // Leaf 0 is the hottest, leaf 199 the coldest.
        for (var i = 0; i < 200; i++)
        {
            for (var v = 0; v < 200 - i; v++) model.Record(Leaf(i));
        }

        var snapshot = model.CaptureSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Leaves[0], Is.EqualTo(Leaf(0).ToString()));
            Assert.That(snapshot.Leaves, Does.Not.Contain(Leaf(199).ToString()));
            Assert.That(snapshot.Visits, Is.Ordered.Descending);
        });
    }

    [Test]
    public void Restore_round_trips_the_ranking_of_a_captured_snapshot()
    {
        var model = new LeafAccessFrequencyModel();
        for (var i = 0; i < 30; i++) model.Record(Leaf(i % 6));

        var restored = LeafAccessFrequencyModel.Restore(model.CaptureSnapshot());

        Assert.Multiple(() =>
        {
            Assert.That(restored.RankTopLeaves(6), Is.EqualTo(model.RankTopLeaves(6)));
            Assert.That(restored.Observations, Is.EqualTo(model.Observations));
            Assert.That(restored.TrackedLeafCount, Is.EqualTo(model.TrackedLeafCount));
        });
    }

    [Test]
    public void Restore_of_a_freshly_captured_snapshot_starts_clean()
    {
        // A restored model already matches what is durable, so it must not
        // provoke a redundant storage write on the next flush tick.
        var model = new LeafAccessFrequencyModel();
        RecordSequence(model, 1, 2, 3);

        Assert.That(LeafAccessFrequencyModel.Restore(model.CaptureSnapshot()).IsDirty, Is.False);
    }

    [Test]
    public void Restore_of_null_yields_an_empty_model()
    {
        var restored = LeafAccessFrequencyModel.Restore(null);

        Assert.Multiple(() =>
        {
            Assert.That(restored.TrackedLeafCount, Is.Zero);
            Assert.That(restored.Observations, Is.Zero);
            Assert.That(restored.IsDirty, Is.False);
        });
    }

    [Test]
    public void Restore_of_the_empty_snapshot_yields_an_empty_model()
    {
        var restored = LeafAccessFrequencyModel.Restore(LeafAccessModelSnapshot.Empty);

        Assert.That(restored.TrackedLeafCount, Is.Zero);
    }

    [Test]
    public void Restore_tolerates_a_visits_list_shorter_than_the_leaf_list()
    {
        // A truncated parallel list must degrade to the entries that are
        // readable - which are the hottest, since they are written first -
        // rather than discarding the whole snapshot.
        var restored = LeafAccessFrequencyModel.Restore(new LeafAccessModelSnapshot
        {
            Leaves = [Leaf(1).ToString(), Leaf(2).ToString(), Leaf(3).ToString()],
            Visits = [9L, 4L],
        });

        Assert.Multiple(() =>
        {
            Assert.That(restored.TrackedLeafCount, Is.EqualTo(2));
            Assert.That(restored.Observations, Is.EqualTo(13));
            Assert.That(restored.RankTopLeaves(2), Is.EqualTo(new[] { Leaf(1), Leaf(2) }));
        });
    }

    [Test]
    public void Restore_skips_unparsable_leaf_identities_without_throwing()
    {
        var restored = LeafAccessFrequencyModel.Restore(new LeafAccessModelSnapshot
        {
            Leaves = ["", "not a grain id", Leaf(4).ToString()],
            Visits = [5L, 6L, 7L],
        });

        Assert.Multiple(() =>
        {
            Assert.That(restored.TrackedLeafCount, Is.EqualTo(1));
            Assert.That(restored.RankTopLeaves(1), Is.EqualTo(new[] { Leaf(4) }));
            Assert.That(restored.Observations, Is.EqualTo(7));
        });
    }

    [Test]
    public void Restore_ignores_non_positive_visit_counts()
    {
        var restored = LeafAccessFrequencyModel.Restore(new LeafAccessModelSnapshot
        {
            Leaves = [Leaf(1).ToString(), Leaf(2).ToString(), Leaf(3).ToString()],
            Visits = [0L, -4L, 3L],
        });

        Assert.Multiple(() =>
        {
            Assert.That(restored.TrackedLeafCount, Is.EqualTo(1));
            Assert.That(restored.Observations, Is.EqualTo(3));
        });
    }

    [Test]
    public void Restore_never_exceeds_the_tracked_leaf_cap()
    {
        // A hand-rolled or future snapshot must not be able to inflate the
        // resident model past its own bound.
        var leaves = new List<string>();
        var visits = new List<long>();
        for (var i = 0; i < LeafAccessFrequencyModel.MaxTrackedLeaves * 3; i++)
        {
            leaves.Add(Leaf(i).ToString());
            visits.Add(1L);
        }

        var restored = LeafAccessFrequencyModel.Restore(
            new LeafAccessModelSnapshot { Leaves = leaves, Visits = visits });

        Assert.That(
            restored.TrackedLeafCount,
            Is.LessThanOrEqualTo(LeafAccessFrequencyModel.MaxTrackedLeaves));
    }

    [Test]
    public void Restored_model_continues_to_accumulate_from_live_traffic()
    {
        var model = new LeafAccessFrequencyModel();
        RecordSequence(model, 1, 1, 1, 2);

        var restored = LeafAccessFrequencyModel.Restore(model.CaptureSnapshot());
        restored.Record(Leaf(3));

        Assert.Multiple(() =>
        {
            Assert.That(restored.TrackedLeafCount, Is.EqualTo(3));
            Assert.That(restored.IsDirty, Is.True);
            Assert.That(restored.Observations, Is.EqualTo(5));
        });
    }

    [Test]
    public void Persisted_snapshot_stays_within_its_documented_size_bound()
    {
        // The snapshot rides inside the shard root's own durable state, so its
        // worst case must stay small regardless of how wide the read set gets.
        var model = new LeafAccessFrequencyModel();
        for (var round = 0; round < 20; round++)
        {
            for (var i = 0; i < LeafAccessFrequencyModel.MaxTrackedLeaves; i++)
            {
                model.Record(Leaf(i));
            }
        }

        var snapshot = model.CaptureSnapshot();
        var approximateBytes = snapshot.Leaves.Sum(l => l.Length + 4) + (snapshot.Visits.Count * 8);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Leaves, Has.Count.EqualTo(LeafAccessFrequencyModel.MaxPersistedLeaves));
            Assert.That(approximateBytes, Is.LessThan(3 * 1024),
                "the persisted leaf-access model must stay a small fraction of the shard-root state budget");
        });
    }
}
