using System.Linq;
using System.Runtime.CompilerServices;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>Tests for the scoped bootstrap-snapshot fallback engine.</summary>
[TestFixture]
public sealed class BootstrapFallbackPlannerTests
{
    private const string Tree = "orders";
    private const string Peer = "cluster-b";
    private const string Origin = "cluster-a";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    private static SnapshotEntry Committed(string key, long ticks, int valueBytes = 8)
        => new()
        {
            Key = key,
            Value = new byte[valueBytes],
            Timestamp = Hlc(ticks),
        };

    private static SnapshotEntry Prepared(string key, long ticks)
        => new()
        {
            Key = key,
            Value = new byte[8],
            Timestamp = Hlc(ticks),
            IsPrepared = true,
            TransactionId = Guid.NewGuid(),
        };

    private static SnapshotEntry Tombstone(string key, long ticks)
        => new()
        {
            Key = key,
            Value = Array.Empty<byte>(),
            Timestamp = Hlc(ticks),
            IsTombstone = true,
        };

    private static LeafReReplayRange Range(string? start, string? end)
        => new() { StartKey = start, EndKey = end };

    private static async Task<(BootstrapFallbackOutcome Outcome, RecordingSink Sink)> RunAsync(
        IReadOnlyList<SnapshotEntry> entries,
        IReadOnlyList<LeafReReplayRange> ranges,
        int maxEntries = 4096,
        long maxBytes = 1024 * 1024,
        bool ackAccepted = true)
    {
        var provider = new StubProvider(entries);
        var sink = new RecordingSink(ackAccepted);

        var outcome = await BootstrapFallbackPlanner.PlanAsync(
            Tree, Peer, Origin, ranges, provider, sink, maxEntries, maxBytes, CancellationToken.None);

        return (outcome, sink);
    }

    [Test]
    public async Task PlanAsync_empty_ranges_skips_range_empty_without_triggering()
    {
        using var triggered = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackTriggeredName);
        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackSkippedName);

        var (outcome, sink) = await RunAsync(
            Array.Empty<SnapshotEntry>(), Array.Empty<LeafReReplayRange>());

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.False);
            Assert.That(outcome.SkipReason, Is.EqualTo(BootstrapFallbackSkipReason.RangeEmpty));
            Assert.That(sink.Calls, Is.Zero);
            Assert.That(triggered.Measurements, Is.Empty);
        });
        Assert.That(skipped.Measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagReason
            && (string?)t.Value == LatticeReplicationMetrics.BootstrapFallbackSkipRangeEmpty));
    }

    [Test]
    public async Task PlanAsync_no_committed_entries_triggers_then_skips_empty()
    {
        using var triggered = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackTriggeredName);
        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackSkippedName);

        var (outcome, sink) = await RunAsync(
            Array.Empty<SnapshotEntry>(), new[] { Range(null, null) });

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.False);
            Assert.That(outcome.RangesProcessed, Is.EqualTo(1));
            Assert.That(outcome.SkipReason, Is.EqualTo(BootstrapFallbackSkipReason.Empty));
            Assert.That(sink.Calls, Is.Zero);
            Assert.That(triggered.Measurements.Single().Value, Is.EqualTo(1L));
        });
        Assert.That(skipped.Measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagReason
            && (string?)t.Value == LatticeReplicationMetrics.BootstrapFallbackSkipEmpty));
    }

    [Test]
    public async Task PlanAsync_reships_in_range_committed_entries()
    {
        using var triggered = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackTriggeredName);
        using var entries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackEntriesName);

        var rows = new[]
        {
            Committed("a", ticks: 100),
            Committed("b", ticks: 110),
            Committed("z", ticks: 120),   // out of range -> filtered by the scoped export
        };

        var (outcome, sink) = await RunAsync(rows, new[] { Range("a", "m") });

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.True);
            Assert.That(outcome.RangesProcessed, Is.EqualTo(1));
            Assert.That(outcome.EntriesShipped, Is.EqualTo(2));
            Assert.That(sink.LastEntries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(triggered.Measurements.Single().Value, Is.EqualTo(1L));
            Assert.That(entries.Measurements.Single().Value, Is.EqualTo(2L));
        });
    }

    [Test]
    public async Task PlanAsync_skips_prepared_and_tombstone_rows()
    {
        var rows = new[]
        {
            Committed("b", ticks: 110),
            Prepared("c", ticks: 111),
            Tombstone("d", ticks: 112),
        };

        var (outcome, sink) = await RunAsync(rows, new[] { Range("a", "m") });

        Assert.Multiple(() =>
        {
            Assert.That(outcome.EntriesShipped, Is.EqualTo(1));
            Assert.That(sink.LastEntries.Single().Key, Is.EqualTo("b"));
        });
    }

    [Test]
    public async Task PlanAsync_reships_set_records_stamped_with_origin()
    {
        var rows = new[] { Committed("b", ticks: 110) };

        var (_, sink) = await RunAsync(rows, new[] { Range("a", "m") });

        var record = sink.LastEntries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(record.Op, Is.EqualTo(MutationKind.Set));
            Assert.That(record.TreeId, Is.EqualTo(Tree));
            Assert.That(record.OriginClusterId, Is.EqualTo(Origin));
            Assert.That(record.Timestamp, Is.EqualTo(Hlc(110)));
        });
    }

    [Test]
    public async Task PlanAsync_entry_cap_ships_a_prefix()
    {
        var rows = new[]
        {
            Committed("a", ticks: 100),
            Committed("b", ticks: 110),
            Committed("c", ticks: 120),
        };

        var (outcome, sink) = await RunAsync(rows, new[] { Range(null, null) }, maxEntries: 2);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.EntriesShipped, Is.EqualTo(2));
            Assert.That(sink.LastEntries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
        });
    }

    [Test]
    public async Task PlanAsync_byte_cap_always_ships_at_least_one()
    {
        var rows = new[]
        {
            Committed("a", ticks: 100, valueBytes: 256),
            Committed("b", ticks: 110, valueBytes: 256),
        };

        // A 1-byte budget is below a single entry's estimate, but the first
        // entry always ships.
        var (outcome, sink) = await RunAsync(rows, new[] { Range(null, null) }, maxBytes: 1);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.EntriesShipped, Is.EqualTo(1));
            Assert.That(sink.LastEntries.Single().Key, Is.EqualTo("a"));
        });
    }

    [Test]
    public async Task PlanAsync_ack_rejected_reports_zero_shipped()
    {
        var rows = new[] { Committed("b", ticks: 110) };

        var (outcome, sink) = await RunAsync(rows, new[] { Range("a", "m") }, ackAccepted: false);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.True);
            Assert.That(outcome.EntriesShipped, Is.Zero);
            Assert.That(sink.Calls, Is.EqualTo(1));
        });
    }

    [Test]
    public void PlanAsync_throws_on_null_required_args()
    {
        var provider = new StubProvider(Array.Empty<SnapshotEntry>());
        var sink = new RecordingSink(ackAccepted: true);
        var ranges = new[] { Range(null, null) };

        Assert.Multiple(() =>
        {
            Assert.That(async () => await BootstrapFallbackPlanner.PlanAsync(
                null!, Peer, Origin, ranges, provider, sink, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(async () => await BootstrapFallbackPlanner.PlanAsync(
                Tree, null!, Origin, ranges, provider, sink, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(async () => await BootstrapFallbackPlanner.PlanAsync(
                Tree, Peer, null!, ranges, provider, sink, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(async () => await BootstrapFallbackPlanner.PlanAsync(
                Tree, Peer, Origin, null!, provider, sink, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(async () => await BootstrapFallbackPlanner.PlanAsync(
                Tree, Peer, Origin, ranges, null!, sink, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(async () => await BootstrapFallbackPlanner.PlanAsync(
                Tree, Peer, Origin, ranges, provider, null!, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
        });
    }

    private sealed class StubProvider(IReadOnlyList<SnapshotEntry> entries) : ISnapshotProvider
    {
        public Task<SnapshotStream> ExportAsync(
            string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new SnapshotStream(
                treeName, asOfHlc, new VersionVector(), Emit(entries, cancellationToken)));
        }

        private static async IAsyncEnumerable<SnapshotEntry> Emit(
            IReadOnlyList<SnapshotEntry> entries,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var e in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return e;
            }
            await Task.CompletedTask;
        }
    }

    private sealed class RecordingSink(bool ackAccepted) : ILeafReReplaySink
    {
        public int Calls { get; private set; }

        public IReadOnlyList<WalRecord> LastEntries { get; private set; } = Array.Empty<WalRecord>();

        public ValueTask<int> ReplayAsync(
            string peer, string treeName, IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken)
        {
            Calls++;
            LastEntries = entries;
            return new ValueTask<int>(ackAccepted ? entries.Count : 0);
        }
    }
}
