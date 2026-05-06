namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="InMemoryInFlightSagaTracker"/> — the
/// default in-process tracker for in-flight atomic-batch sagas
/// consumed by <see cref="LatticeSnapshotProvider"/>'s quiesce path.
/// </summary>
[TestFixture]
public class InMemoryInFlightSagaTrackerTests
{
    private const string Tree = "tree-x";

    [Test]
    public void GetInFlightTransactions_returns_empty_when_no_emissions_observed()
    {
        var tracker = new InMemoryInFlightSagaTracker();

        Assert.That(tracker.GetInFlightTransactions(Tree), Is.Empty);
    }

    [Test]
    public void ObserveEmission_first_emission_is_in_flight_until_batch_size_reached()
    {
        var tracker = new InMemoryInFlightSagaTracker();
        var tx = Guid.NewGuid();

        tracker.ObserveEmission(Tree, tx, batchSize: 3);

        Assert.That(tracker.GetInFlightTransactions(Tree), Is.EqualTo(new[] { tx }));
    }

    [Test]
    public void ObserveEmission_completes_after_batch_size_observations()
    {
        var tracker = new InMemoryInFlightSagaTracker();
        var tx = Guid.NewGuid();

        tracker.ObserveEmission(Tree, tx, batchSize: 3);
        tracker.ObserveEmission(Tree, tx, batchSize: 3);
        tracker.ObserveEmission(Tree, tx, batchSize: 3);

        Assert.That(tracker.GetInFlightTransactions(Tree), Is.Empty);
    }

    [Test]
    public void ObserveEmission_isolates_per_tree()
    {
        var tracker = new InMemoryInFlightSagaTracker();
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();

        tracker.ObserveEmission("tree-a", txA, batchSize: 3);
        tracker.ObserveEmission("tree-b", txB, batchSize: 3);

        Assert.Multiple(() =>
        {
            Assert.That(tracker.GetInFlightTransactions("tree-a"), Is.EqualTo(new[] { txA }));
            Assert.That(tracker.GetInFlightTransactions("tree-b"), Is.EqualTo(new[] { txB }));
            Assert.That(tracker.GetInFlightTransactions("tree-c"), Is.Empty);
        });
    }

    [Test]
    public void ObserveEmission_isolates_per_transaction_id()
    {
        var tracker = new InMemoryInFlightSagaTracker();
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();

        tracker.ObserveEmission(Tree, txA, batchSize: 2);
        tracker.ObserveEmission(Tree, txB, batchSize: 2);

        var inFlight = tracker.GetInFlightTransactions(Tree);

        Assert.That(inFlight, Is.EquivalentTo(new[] { txA, txB }));
    }

    [Test]
    public void ObserveEmission_throws_on_null_tree_name()
    {
        var tracker = new InMemoryInFlightSagaTracker();

        Assert.That(
            () => tracker.ObserveEmission(null!, Guid.NewGuid(), 3),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ObserveEmission_throws_on_empty_tree_name()
    {
        var tracker = new InMemoryInFlightSagaTracker();

        Assert.That(
            () => tracker.ObserveEmission("", Guid.NewGuid(), 3),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ObserveEmission_throws_on_empty_transaction_id()
    {
        var tracker = new InMemoryInFlightSagaTracker();

        Assert.That(
            () => tracker.ObserveEmission(Tree, Guid.Empty, 3),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ObserveEmission_throws_on_zero_batch_size()
    {
        var tracker = new InMemoryInFlightSagaTracker();

        Assert.That(
            () => tracker.ObserveEmission(Tree, Guid.NewGuid(), 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ObserveEmission_throws_on_negative_batch_size()
    {
        var tracker = new InMemoryInFlightSagaTracker();

        Assert.That(
            () => tracker.ObserveEmission(Tree, Guid.NewGuid(), -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void GetInFlightTransactions_throws_on_null_tree_name()
    {
        var tracker = new InMemoryInFlightSagaTracker();

        Assert.That(
            () => tracker.GetInFlightTransactions(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetInFlightTransactions_throws_on_empty_tree_name()
    {
        var tracker = new InMemoryInFlightSagaTracker();

        Assert.That(
            () => tracker.GetInFlightTransactions(""),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Concurrent_emissions_converge_to_completion()
    {
        // Stress: 100 sagas of size 8 emitted concurrently from
        // multiple threads. Each saga's 8 emissions are interleaved
        // with every other saga's emissions; the tracker must end
        // with an empty in-flight set when every saga has
        // contributed all 8 of its emissions.
        var tracker = new InMemoryInFlightSagaTracker();
        const int sagas = 100;
        const int batchSize = 8;
        var ids = Enumerable.Range(0, sagas).Select(_ => Guid.NewGuid()).ToArray();

        Parallel.ForEach(
            Enumerable.Range(0, sagas * batchSize),
            i =>
            {
                tracker.ObserveEmission(Tree, ids[i % sagas], batchSize);
            });

        Assert.That(tracker.GetInFlightTransactions(Tree), Is.Empty);
    }

    // -------- Stale-entry eviction (defense-in-depth) --------

    [Test]
    public void Re_emit_after_completion_does_not_grow_dictionary()
    {
        // Producer-bug guard: once a saga completes (count reaches
        // BatchSize) the row is removed. A spurious extra emission
        // for the completed transaction id reinserts a row with
        // count=1 but the snapshot quiesce path still reads it as
        // "in flight" until BatchSize is reached again. Verify the
        // tracker does not silently double-count or grow unbounded
        // — re-emit is idempotent in the sense that the in-flight
        // count for the tree only reflects partial sagas.
        var tracker = new InMemoryInFlightSagaTracker();
        var tx = Guid.NewGuid();

        for (var i = 0; i < 3; i++)
        {
            tracker.ObserveEmission(Tree, tx, batchSize: 3);
        }

        Assert.That(tracker.GetInFlightTransactions(Tree), Is.Empty);

        // A late, duplicate emission re-creates a partial row -
        // observable as "in flight" again with count=1 of 3.
        tracker.ObserveEmission(Tree, tx, batchSize: 3);
        Assert.That(tracker.GetInFlightTransactions(Tree), Is.EqualTo(new[] { tx }));
    }

    [Test]
    public void Stale_entries_are_evicted_after_timeout_via_observe()
    {
        // White-box test: drive the dictionary into a state where
        // a row's LastObservedAtTicks is older than the stale
        // ceiling, then trigger a fresh observe to exercise the
        // prune-on-observe path. The stale ceiling is hard-coded
        // to 10 minutes; the test verifies the prune path runs by
        // checking that an unrelated emission against the same
        // tracker after a sub-microsecond delay does not leak the
        // stale row when tested through the public surface.
        //
        // Because the ceiling is wall-clock 10 minutes, a
        // deterministic test cannot wait for it. Instead, the test
        // uses internal-visible reflection on the tracker's
        // dictionary to poke a stale timestamp directly. A more
        // hermetic harness would expose StaleEntryTimeout as a
        // ctor parameter — left for a future refactor when the
        // 10-minute ceiling proves operationally insufficient.
        var tracker = new InMemoryInFlightSagaTracker();
        var staleTx = Guid.NewGuid();

        tracker.ObserveEmission(Tree, staleTx, batchSize: 5); // 1 of 5 — in flight
        Assert.That(tracker.GetInFlightTransactions(Tree), Has.Count.EqualTo(1));

        // Backdate every existing row past the stale ceiling.
        BackdateAllRows(tracker, ageBeyondTimeout: TimeSpan.FromSeconds(1));

        // Trigger prune via a fresh observe on a different tx.
        var freshTx = Guid.NewGuid();
        tracker.ObserveEmission(Tree, freshTx, batchSize: 2);

        // Stale tx is gone; only the fresh tx remains.
        Assert.That(tracker.GetInFlightTransactions(Tree), Is.EqualTo(new[] { freshTx }));
    }

    [Test]
    public void Stale_entries_are_evicted_after_timeout_via_get()
    {
        // The prune-on-get path runs even without a fresh observe.
        var tracker = new InMemoryInFlightSagaTracker();
        var staleTx = Guid.NewGuid();
        tracker.ObserveEmission(Tree, staleTx, batchSize: 5);
        BackdateAllRows(tracker, ageBeyondTimeout: TimeSpan.FromSeconds(1));

        Assert.That(tracker.GetInFlightTransactions(Tree), Is.Empty);
    }

    /// <summary>
    /// Reaches into the tracker's private dictionary via reflection
    /// and rewrites every row's <c>LastObservedAtTicks</c> to a
    /// stopwatch-timestamp that is older than the stale ceiling.
    /// Used to exercise the prune path without sleeping for 10
    /// minutes. The reflection coupling is acceptable: the
    /// internal field name is documented in the implementation's
    /// XML comment as load-bearing, and the test is co-located
    /// with that implementation in the same repo.
    /// </summary>
    private static void BackdateAllRows(InMemoryInFlightSagaTracker tracker, TimeSpan ageBeyondTimeout)
    {
        var dictField = typeof(InMemoryInFlightSagaTracker)
            .GetField("_byTransaction", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        var dict = dictField.GetValue(tracker)!;
        var dictType = dict.GetType();

        var sagaEntryType = typeof(InMemoryInFlightSagaTracker)
            .GetNestedType("SagaEntry", System.Reflection.BindingFlags.NonPublic)!;
        var transactionKeyType = typeof(InMemoryInFlightSagaTracker)
            .GetNestedType("TransactionKey", System.Reflection.BindingFlags.NonPublic)!;
        var batchSizeProp = sagaEntryType.GetProperty("BatchSize")!
            .GetMethod!;
        var countProp = sagaEntryType.GetProperty("Count")!
            .GetMethod!;

        var staleCeiling = (TimeSpan)typeof(InMemoryInFlightSagaTracker)
            .GetField("StaleEntryTimeout", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(null)!;

        var staleAt = System.Diagnostics.Stopwatch.GetTimestamp()
            - (long)((staleCeiling + ageBeyondTimeout).TotalSeconds * System.Diagnostics.Stopwatch.Frequency);

        var indexer = dictType.GetProperty("Item", new[] { transactionKeyType })!;
        var keysProp = dictType.GetProperty("Keys")!;
        var keys = ((System.Collections.IEnumerable)keysProp.GetValue(dict)!).Cast<object>().ToArray();

        var sagaCtor = sagaEntryType.GetConstructors().Single();
        foreach (var key in keys)
        {
            var existing = indexer.GetValue(dict, new[] { key })!;
            var rebuilt = sagaCtor.Invoke(new object[]
            {
                batchSizeProp.Invoke(existing, null)!,
                countProp.Invoke(existing, null)!,
                staleAt,
            });
            indexer.SetValue(dict, rebuilt, new[] { key });
        }
    }

    // -------- AnyInFlight (allocation-free overlap probe) --------

    [Test]
    public void AnyInFlight_returns_false_when_tree_has_no_in_flight_sagas()
    {
        var tracker = new InMemoryInFlightSagaTracker();

        Assert.That(
            tracker.AnyInFlight(Tree, new HashSet<Guid> { Guid.NewGuid() }),
            Is.False);
    }

    [Test]
    public void AnyInFlight_returns_false_when_candidates_empty()
    {
        var tracker = new InMemoryInFlightSagaTracker();
        tracker.ObserveEmission(Tree, Guid.NewGuid(), batchSize: 2);

        Assert.That(
            tracker.AnyInFlight(Tree, Array.Empty<Guid>()),
            Is.False);
    }

    [Test]
    public void AnyInFlight_returns_true_when_candidate_overlaps_in_flight_set()
    {
        var tracker = new InMemoryInFlightSagaTracker();
        var inFlight = Guid.NewGuid();
        var unrelated = Guid.NewGuid();
        tracker.ObserveEmission(Tree, inFlight, batchSize: 5);

        Assert.That(
            tracker.AnyInFlight(Tree, new HashSet<Guid> { unrelated, inFlight }),
            Is.True);
    }

    [Test]
    public void AnyInFlight_returns_false_when_no_candidate_matches()
    {
        var tracker = new InMemoryInFlightSagaTracker();
        tracker.ObserveEmission(Tree, Guid.NewGuid(), batchSize: 5);

        Assert.That(
            tracker.AnyInFlight(Tree, new HashSet<Guid> { Guid.NewGuid(), Guid.NewGuid() }),
            Is.False);
    }

    [Test]
    public void AnyInFlight_isolates_per_tree()
    {
        var tracker = new InMemoryInFlightSagaTracker();
        var tx = Guid.NewGuid();
        tracker.ObserveEmission("tree-a", tx, batchSize: 5);

        Assert.Multiple(() =>
        {
            Assert.That(tracker.AnyInFlight("tree-a", new HashSet<Guid> { tx }), Is.True);
            Assert.That(tracker.AnyInFlight("tree-b", new HashSet<Guid> { tx }), Is.False);
        });
    }

    [Test]
    public void AnyInFlight_excludes_completed_sagas()
    {
        var tracker = new InMemoryInFlightSagaTracker();
        var tx = Guid.NewGuid();
        tracker.ObserveEmission(Tree, tx, batchSize: 2);
        tracker.ObserveEmission(Tree, tx, batchSize: 2); // completes

        Assert.That(
            tracker.AnyInFlight(Tree, new HashSet<Guid> { tx }),
            Is.False);
    }

    [Test]
    public void AnyInFlight_throws_on_null_tree_name()
    {
        var tracker = new InMemoryInFlightSagaTracker();

        Assert.That(
            () => tracker.AnyInFlight(null!, new HashSet<Guid> { Guid.NewGuid() }),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void AnyInFlight_throws_on_null_candidates()
    {
        var tracker = new InMemoryInFlightSagaTracker();

        Assert.That(
            () => tracker.AnyInFlight(Tree, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AnyInFlight_accepts_non_hashset_collection()
    {
        // Default-method fallback path: caller passes a List<Guid>
        // rather than a HashSet. The implementation must still
        // work — promoting to a HashSet internally for O(1) probes.
        var tracker = new InMemoryInFlightSagaTracker();
        var tx = Guid.NewGuid();
        tracker.ObserveEmission(Tree, tx, batchSize: 5);

        Assert.That(
            tracker.AnyInFlight(Tree, new List<Guid> { tx }),
            Is.True);
    }
}
