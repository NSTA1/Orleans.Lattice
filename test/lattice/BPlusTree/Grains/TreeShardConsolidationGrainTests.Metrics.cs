using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the one meter instrument online shard consolidation publishes,
/// <c>orleans.lattice.shard.consolidations_committed</c>.
/// <para>
/// It is the exact inverse of <c>orleans.lattice.shard.splits_committed</c> and
/// the signal that proves a tree an over-eager splitter shattered is actually
/// being healed rather than merely attempted. Because that is the claim an
/// operator (and the epic's end-to-end verification) reads it for, the counter
/// must fire once per <em>durably committed</em> fold and never for an attempt,
/// an abandoned fold, or a retry - which is what these tests pin.
/// </para>
/// </summary>
public partial class TreeShardConsolidationGrainTests
{
    private const string ConsolidationsCommittedInstrument = "orleans.lattice.shard.consolidations_committed";

    /// <summary>
    /// Captures long-valued measurements on the Lattice meter for the duration
    /// of one test, so a fold's metric emission is observed directly rather
    /// than inferred.
    /// </summary>
    private sealed class MetricRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)> _records = [];
        private readonly Lock _gate = new();

        public MetricRecorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (ReferenceEquals(instrument.Meter, LatticeMetrics.Meter))
                        listener.EnableMeasurementEvents(instrument);
                },
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                lock (_gate) _records.Add((instrument.Name, value, tags.ToArray()));
            });
            _listener.Start();
        }

        public long TotalFor(string instrumentName, string treeId)
        {
            lock (_gate)
            {
                long total = 0;
                foreach (var record in _records)
                {
                    if (record.Name != instrumentName) continue;
                    foreach (var tag in record.Tags)
                    {
                        if (tag.Key == LatticeMetrics.TagTree && (tag.Value as string) == treeId)
                        {
                            total += record.Value;
                            break;
                        }
                    }
                }
                return total;
            }
        }

        public IReadOnlyList<KeyValuePair<string, object?>> TagsOfSingle(string instrumentName, string treeId)
        {
            lock (_gate)
            {
                foreach (var record in _records)
                {
                    if (record.Name != instrumentName) continue;
                    foreach (var tag in record.Tags)
                    {
                        if (tag.Key == LatticeMetrics.TagTree && (tag.Value as string) == treeId)
                            return record.Tags;
                    }
                }
                return [];
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    [Test]
    public async Task A_committed_fold_increments_the_consolidations_committed_counter_once()
    {
        using var recorder = new MetricRecorder();

        var h = CreateGrain(leafEntries: [Entries("a", "b")]);
        await h.Grain.StartAsync(0);
        await h.Grain.RunConsolidationPassAsync();

        Assert.That(h.State.State.Complete, Is.True, "Precondition: the fold must have landed.");
        Assert.That(recorder.TotalFor(ConsolidationsCommittedInstrument, TreeId), Is.EqualTo(1),
            "A committed fold must be observable exactly once - it is the signal an operator reads "
            + "to tell a healing tree from one whose shard count is still climbing.");
    }

    [Test]
    public async Task The_consolidations_committed_counter_carries_the_tree_and_donor_shard_tags()
    {
        using var recorder = new MetricRecorder();

        var h = CreateGrain(donorShardIndex: 1, survivorShardIndex: 0, leafEntries: [Entries("a")]);
        await h.Grain.StartAsync(0);
        await h.Grain.RunConsolidationPassAsync();

        var tags = recorder.TagsOfSingle(ConsolidationsCommittedInstrument, TreeId);

        Assert.That(tags, Is.Not.Empty, "The counter must have been recorded.");
        Assert.That(tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == TreeId), Is.True);
        Assert.That(tags.Any(t => t.Key == LatticeMetrics.TagShard && t.Value is 1), Is.True,
            "The shard tag must carry the DONOR index - the shard this fold retired - so an operator "
            + "can see which shard came out of the routing map.");
    }

    [Test]
    public async Task An_in_flight_fold_does_not_increment_the_counter()
    {
        using var recorder = new MetricRecorder();

        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [Entries("a")]);
        await h.Grain.DrainAsync();
        await h.Grain.SwapAsync();

        Assert.That(h.State.State.Complete, Is.False, "Precondition: the fold has not finalised yet.");
        Assert.That(recorder.TotalFor(ConsolidationsCommittedInstrument, TreeId), Is.Zero,
            "The counter measures commits, not attempts; incrementing before finalise would let a "
            + "stalled fold look like a healed tree.");
    }

    [Test]
    public async Task An_abandoned_fold_does_not_increment_the_counter()
    {
        using var recorder = new MetricRecorder();

        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [Entries("a")]);
        await h.Grain.CancelAsync();
        await h.Grain.RunConsolidationPassAsync();

        Assert.That(h.State.State.Cancelled, Is.True, "Precondition: the fold must have been abandoned.");
        Assert.That(recorder.TotalFor(ConsolidationsCommittedInstrument, TreeId), Is.Zero,
            "An abandoned fold changed no routing, so it must not count as a healed shard.");
    }

    [Test]
    public async Task A_failed_finalise_persist_does_not_increment_the_counter()
    {
        // The increment sits after the terminal write, so a fold whose commit
        // never reached storage is never counted. Otherwise a retry loop
        // against failing storage would inflate the healed-shard figure without
        // a single shard actually leaving the routing map.
        using var recorder = new MetricRecorder();

        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Complete),
            leafEntries: [Entries("a")]);
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.FinaliseAsync());
        Assert.That(recorder.TotalFor(ConsolidationsCommittedInstrument, TreeId), Is.Zero);

        await h.Grain.RunConsolidationPassAsync();

        Assert.That(h.State.State.Complete, Is.True);
        Assert.That(recorder.TotalFor(ConsolidationsCommittedInstrument, TreeId), Is.EqualTo(1),
            "Once the commit is durable the fold must be counted exactly once, not twice for the retry.");
    }
}
