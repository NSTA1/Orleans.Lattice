using System.Diagnostics.Metrics;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Discriminator tests for the cold/warm arm of
/// <see cref="LatticeMetrics.LeafActivationReplays"/> (issue #2148).
/// <para>
/// Classified in advance as <b>discriminator</b> tests: each one asserts that a
/// cold activation lands on the cold arm and <b>not</b> the warm one, and the
/// converse, so a change that stopped distinguishing the two - or that tagged
/// every activation the same way - fails. The control is the opposite-arm
/// assertion inside each test; without it, an implementation that emitted both
/// arms on every activation would pass.
/// </para>
/// <para>
/// Both tests drive the <b>real</b> condition the production code branches on
/// (<c>rehydratedFromSnapshot</c> and an empty entry cache, evaluated inside
/// <c>OnActivateAsync</c>) through a full activation, rather than calling an
/// internal helper with a flag. A test that passed the flag in would only prove
/// the flag was passed to itself.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static string UniqueTemperatureTree() => $"activation-temperature-{Guid.NewGuid():N}";

    [Test]
    public async Task Activation_with_no_snapshot_and_an_empty_cache_counts_on_the_cold_arm()
    {
        var tree = UniqueTemperatureTree();

        // No snapshot to rehydrate from and nothing in the cache, so the
        // activation computes the -1 replay-start sentinel and replays the
        // whole readable WAL window. That is the definition of cold.
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 5,
            walHead: 5);
        state.State.TreeId = tree;

        using var recorder = new ActivationTemperatureRecorder(tree);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Total("cold"), Is.EqualTo(1),
                "A cold activation must be counted on the cold arm.");
            Assert.That(recorder.Total("warm"), Is.Zero,
                "and must NOT also be counted on the warm arm - otherwise the tag discriminates "
                + "nothing and the ratio it exists to produce is meaningless.");
        });
    }

    [Test]
    public async Task Activation_rehydrated_from_a_snapshot_counts_on_the_warm_arm()
    {
        var tree = UniqueTemperatureTree();

        // A snapshot whose offset exceeds the persisted checkpoint rehydrates
        // the entry cache, so the activation resumes above that anchor and
        // replays only the tail. That is the definition of warm.
        var blob = NewSnapshotBlob(
            offset: 50,
            ("a", new byte[] { 1 }),
            ("b", new byte[] { 2 }));
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: blob,
            persistedCheckpoint: 10,
            walHead: 50);
        state.State.TreeId = tree;

        using var recorder = new ActivationTemperatureRecorder(tree);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Proves the rehydrate really happened, so "warm" is the outcome of
            // the production condition and not of an inert code path.
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "b" }),
                "The snapshot must actually have rehydrated the cache,");
            Assert.That(recorder.Total("warm"), Is.EqualTo(1),
                "so the activation is counted on the warm arm,");
            Assert.That(recorder.Total("cold"), Is.Zero,
                "and not on the cold one.");
        });
    }

    [Test]
    public async Task Every_counted_activation_carries_exactly_one_temperature_tag()
    {
        var tree = UniqueTemperatureTree();
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 0,
            walHead: 0);
        state.State.TreeId = tree;

        using var recorder = new ActivationTemperatureRecorder(tree);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // The cold:warm ratio is only a property of a single scrape while every
        // increment lands on exactly one arm: an untagged or double-tagged
        // emission would make the two arms fail to sum to the counter.
        Assert.That(recorder.Temperatures, Has.Count.EqualTo(1),
            "the activation must emit exactly one measurement for this tree");
        Assert.That(recorder.Temperatures[0], Is.EqualTo("cold"));
    }

    [Test]
    public async Task Activation_from_a_snapshot_at_the_persisted_checkpoint_counts_on_the_warm_arm()
    {
        var tree = UniqueTemperatureTree();

        // Issue #2278, expressed in the units the issue is actually reported
        // in. offset == checkpoint is the CONVERGED steady state, not an edge
        // case: a capture stamps the checkpoint it covers, so this is where a
        // healthy leaf sits on every reactivation after its first. This arm
        // previously landed on "cold", and because a cold activation replays
        // from zero and re-captures at the new checkpoint, it returned to this
        // same state and went cold again on the next activation - which is the
        // self-perpetuating loop behind repo-context-vector-metadata's 2.04
        // cold-activations-per-distinct-leaf against vector-membership's 1.00.
        var blob = NewSnapshotBlob(
            offset: 40,
            ("a", new byte[] { 1 }),
            ("b", new byte[] { 2 }));
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: blob,
            persistedCheckpoint: 40,
            walHead: 40);
        state.State.TreeId = tree;

        using var recorder = new ActivationTemperatureRecorder(tree);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Proves the rehydrate is what produced the warm tag, so a "warm"
            // reading cannot come from an inert path that never loaded anything.
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "b" }),
                "the at-checkpoint snapshot must fill the empty cache,");
            Assert.That(recorder.Total("warm"), Is.EqualTo(1),
                "so this activation is warm,");
            Assert.That(recorder.Total("cold"), Is.Zero,
                "and NOT cold - the whole-window WAL replay is exactly what issue #2278 removes.");
        });
    }

    [Test]
    public async Task Activation_with_a_populated_cache_and_a_redundant_snapshot_is_still_warm()
    {
        var tree = UniqueTemperatureTree();

        // The control for the test above. It shares the same offset ==
        // checkpoint input and the same expected arm, but reaches it down the
        // OTHER branch - the snapshot is declined, and the activation is warm
        // because the cache was already populated. Without this arm, a change
        // that made every activation report "warm" regardless of what happened
        // would pass the test above; with it, the two branches are shown to be
        // distinguishable by their effect on the cache rather than only by their
        // tag.
        var blob = NewSnapshotBlob(offset: 40, ("fromSnapshot", new byte[] { 1 }));
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: blob,
            persistedCheckpoint: 40,
            walHead: 40);
        state.State.TreeId = tree;

        grain.CacheForTest.StoreRow("fromCache", new LwwValue<byte[]>
        {
            Value = [9],
            Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
        });

        using var recorder = new ActivationTemperatureRecorder(tree);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "fromCache" }),
                "the snapshot must still be declined against a populated cache,");
            Assert.That(recorder.Total("warm"), Is.EqualTo(1),
                "and the activation is warm anyway, because a populated cache is not cold,");
            Assert.That(recorder.Total("cold"), Is.Zero,
                "so the fix did not merely relabel the cold arm.");
        });
    }

    /// <summary>
    /// Captures <see cref="LatticeMetrics.LeafActivationReplays"/> measurements
    /// for one tree. The Lattice meter is a process-wide static, so the tree
    /// filter (a per-test unique id) is what makes the assertions immune to
    /// fixtures running in parallel.
    /// </summary>
    private sealed class ActivationTemperatureRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<string> _temperatures = [];
        private readonly object _lock = new();
        private readonly string _treeTagValue;

        public ActivationTemperatureRecorder(string treeTagValue)
        {
            _treeTagValue = treeTagValue;

            // Read the instrument into a local BEFORE the listener starts. The
            // instruments are created by LatticeMetrics' static initialiser, and
            // MeterListener.Start only replays instruments that already exist.
            // Touching the field first forces that initialiser to run, so the
            // instrument is published while the listener can still see it;
            // dereferencing it inside the callback instead would leave the
            // static field null on the very publication being filtered, and the
            // first test in the process to construct a recorder would silently
            // capture nothing.
            var replays = LatticeMetrics.LeafActivationReplays;
            _listener = new MeterListener
            {
                InstrumentPublished = (published, listener) =>
                {
                    if (ReferenceEquals(published, replays))
                    {
                        listener.EnableMeasurementEvents(published);
                    }
                },
            };

            _listener.SetMeasurementEventCallback<long>(OnMeasurement);
            _listener.Start();
        }

        /// <summary>The temperature tag of every captured measurement, in order.</summary>
        public IReadOnlyList<string> Temperatures
        {
            get { lock (_lock) return _temperatures.ToArray(); }
        }

        /// <summary>How many measurements carried <paramref name="temperature"/>.</summary>
        public int Total(string temperature)
        {
            lock (_lock)
            {
                return _temperatures.Count(t => string.Equals(t, temperature, StringComparison.Ordinal));
            }
        }

        public void Dispose() => _listener.Dispose();

        private void OnMeasurement(
            Instrument instrument,
            long measurement,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state)
        {
            string? tree = null;
            var temperatures = new List<string>();
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal))
                {
                    tree = tag.Value as string;
                }
                else if (string.Equals(tag.Key, LatticeMetrics.TagActivationTemperature, StringComparison.Ordinal)
                    && tag.Value is string temperature)
                {
                    temperatures.Add(temperature);
                }
            }

            if (!string.Equals(tree, _treeTagValue, StringComparison.Ordinal))
            {
                return;
            }

            // One entry per temperature tag seen (normally exactly one), so a
            // double-tagged emission is observable rather than collapsed.
            lock (_lock) _temperatures.AddRange(temperatures);
        }
    }
}
