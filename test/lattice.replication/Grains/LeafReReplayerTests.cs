using System.Linq;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>Tests for the targeted leaf re-replay repair engine.</summary>
[TestFixture]
public sealed class LeafReReplayerTests
{
    private const string Tree = "orders";
    private const string Peer = "cluster-b";
    private const string Origin = "cluster-a";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    private static WalRecord Entry(
        string key,
        long ticks,
        string origin = Origin,
        int atomicBatchSize = 0,
        int atomicBatchIndex = 0,
        Guid transactionId = default,
        int valueBytes = 8)
        => new()
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[valueBytes],
            Timestamp = Hlc(ticks),
            OriginClusterId = origin,
            AtomicBatchSize = atomicBatchSize,
            AtomicBatchIndex = atomicBatchIndex,
            TransactionId = transactionId,
        };

    private static LeafReReplayRange Range(string? start, string? end)
        => new() { StartKey = start, EndKey = end };

    private static async Task<(LeafReReplayOutcome Outcome, RecordingSink Sink)> RunAsync(
        WalReReplayReadResult read,
        IReadOnlyList<LeafReReplayRange> ranges,
        HybridLogicalClock peerCursor,
        int maxEntries = 4096,
        long maxBytes = 1024 * 1024,
        bool ackAccepted = true)
    {
        var source = new StubSource(read);
        var sink = new RecordingSink(ackAccepted);

        var outcome = await LeafReReplayer.ReplayAsync(
            Tree, Peer, Origin, ranges, peerCursor, source, sink, maxEntries, maxBytes, CancellationToken.None);

        return (outcome, sink);
    }

    [Test]
    public async Task ReplayAsync_empty_ranges_skips_range_empty()
    {
        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.LeafReReplaySkippedName);

        var (outcome, sink) = await RunAsync(
            new WalReReplayReadResult { Entries = Array.Empty<WalRecord>() },
            Array.Empty<LeafReReplayRange>(),
            HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.False);
            Assert.That(outcome.SkipReason, Is.EqualTo(LeafReReplaySkipReason.RangeEmpty));
            Assert.That(sink.Calls, Is.Zero);
        });
        Assert.That(skipped.Measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagReason
            && (string?)t.Value == LatticeReplicationMetrics.LeafReReplaySkipRangeEmpty));
    }

    [Test]
    public async Task ReplayAsync_retained_log_empty_skips_range_empty_after_counting_the_ranges()
    {
        // Non-empty ranges but nothing retained to select from: the pass is
        // still a skip, but it reports the ranges it was given so the caller can
        // tell "no ranges to repair" from "ranges given, nothing retained".
        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.LeafReReplaySkippedName);

        var (outcome, sink) = await RunAsync(
            new WalReReplayReadResult { Entries = Array.Empty<WalRecord>() },
            new[] { Range("a", "m"), Range("m", null) },
            HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.False);
            Assert.That(outcome.RangesProcessed, Is.EqualTo(2));
            Assert.That(outcome.SkipReason, Is.EqualTo(LeafReReplaySkipReason.RangeEmpty));
            Assert.That(sink.Calls, Is.Zero);
        });
        Assert.That(skipped.Measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagReason
            && (string?)t.Value == LatticeReplicationMetrics.LeafReReplaySkipRangeEmpty));
    }

    [Test]
    public async Task ReplayAsync_null_entry_list_skips_range_empty()
    {
        // A source that reports no entry list at all is treated exactly like an
        // empty one rather than faulting the repair pass.
        var (outcome, sink) = await RunAsync(
            new WalReReplayReadResult { Entries = null! },
            new[] { Range(null, null) },
            HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.False);
            Assert.That(outcome.RangesProcessed, Is.EqualTo(1));
            Assert.That(outcome.SkipReason, Is.EqualTo(LeafReReplaySkipReason.RangeEmpty));
            Assert.That(sink.Calls, Is.Zero);
        });
    }

    [Test]
    public async Task ReplayAsync_wal_trimmed_past_cursor_skips_with_operator_alert()
    {
        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.LeafReReplaySkippedName);

        var read = new WalReReplayReadResult
        {
            Entries = new[] { Entry("k", ticks: 200) },
            WasTrimmed = true,
            OldestRetainedHlc = Hlc(150),
        };

        var (outcome, sink) = await RunAsync(read, new[] { Range(null, null) }, peerCursor: Hlc(100));

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.False);
            Assert.That(outcome.SkipReason, Is.EqualTo(LeafReReplaySkipReason.WalTrimmed));
            Assert.That(sink.Calls, Is.Zero);
        });
        Assert.That(skipped.Measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagReason
            && (string?)t.Value == LatticeReplicationMetrics.LeafReReplaySkipWalTrimmed));
    }

    [Test]
    public async Task ReplayAsync_trimmed_but_cursor_above_oldest_still_repairs()
    {
        var read = new WalReReplayReadResult
        {
            Entries = new[] { Entry("k", ticks: 200) },
            WasTrimmed = true,
            OldestRetainedHlc = Hlc(150),
        };

        // Peer cursor is at or above the oldest retained entry, so there is no gap.
        var (outcome, sink) = await RunAsync(read, new[] { Range(null, null) }, peerCursor: Hlc(150));

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.True);
            Assert.That(outcome.EntriesReReplayed, Is.EqualTo(1));
            Assert.That(sink.Calls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ReplayAsync_reships_in_range_entries_above_cursor()
    {
        using var entries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.LeafReReplayEntriesName);

        var read = new WalReReplayReadResult
        {
            Entries = new[]
            {
                Entry("a", ticks: 50),    // below cursor -> excluded
                Entry("b", ticks: 150),   // in range, above cursor -> included
                Entry("z", ticks: 200),   // out of range -> excluded
            },
        };

        var (outcome, sink) = await RunAsync(read, new[] { Range("a", "m") }, peerCursor: Hlc(100));

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.True);
            Assert.That(outcome.RangesProcessed, Is.EqualTo(1));
            Assert.That(outcome.EntriesReReplayed, Is.EqualTo(1));
            Assert.That(sink.LastEntries.Single().Key, Is.EqualTo("b"));
        });
        Assert.That(entries.Measurements.Single().Value, Is.EqualTo(1L));
    }

    [Test]
    public async Task ReplayAsync_excludes_foreign_origin_entries()
    {
        var read = new WalReReplayReadResult
        {
            Entries = new[]
            {
                Entry("b", ticks: 150, origin: "cluster-c"),
            },
        };

        var (outcome, _) = await RunAsync(read, new[] { Range("a", "m") }, peerCursor: Hlc(100));

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.False);
            Assert.That(outcome.SkipReason, Is.EqualTo(LeafReReplaySkipReason.RangeEmpty));
        });
    }

    [Test]
    public async Task ReplayAsync_ships_whole_atomic_batch_when_one_member_in_range()
    {
        var tx = Guid.NewGuid();
        var read = new WalReReplayReadResult
        {
            Entries = new[]
            {
                Entry("b", ticks: 150, atomicBatchSize: 2, atomicBatchIndex: 0, transactionId: tx), // in range
                Entry("z", ticks: 151, atomicBatchSize: 2, atomicBatchIndex: 1, transactionId: tx), // out of range sibling
            },
        };

        var (outcome, sink) = await RunAsync(read, new[] { Range("a", "m") }, peerCursor: Hlc(100));

        Assert.Multiple(() =>
        {
            Assert.That(outcome.EntriesReReplayed, Is.EqualTo(2));
            Assert.That(sink.LastEntries, Has.Count.EqualTo(2));
            Assert.That(sink.LastEntries.Select(e => e.Key), Is.EquivalentTo(new[] { "b", "z" }));
        });
    }

    [Test]
    public async Task ReplayAsync_entry_cap_never_splits_an_atomic_batch()
    {
        var tx = Guid.NewGuid();
        var read = new WalReReplayReadResult
        {
            Entries = new[]
            {
                Entry("a", ticks: 150, atomicBatchSize: 3, atomicBatchIndex: 0, transactionId: tx),
                Entry("b", ticks: 151, atomicBatchSize: 3, atomicBatchIndex: 1, transactionId: tx),
                Entry("c", ticks: 152, atomicBatchSize: 3, atomicBatchIndex: 2, transactionId: tx),
            },
        };

        // Cap of 1 is below the batch size, but the batch ships whole as the
        // first (always-shipped) unit.
        var (outcome, sink) = await RunAsync(read, new[] { Range(null, null) }, peerCursor: Hlc(100), maxEntries: 1);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.EntriesReReplayed, Is.EqualTo(3));
            Assert.That(sink.LastEntries, Has.Count.EqualTo(3));
        });
    }

    [Test]
    public async Task ReplayAsync_entry_cap_ships_a_prefix_of_single_entry_units()
    {
        var read = new WalReReplayReadResult
        {
            Entries = new[]
            {
                Entry("a", ticks: 110),
                Entry("b", ticks: 120),
                Entry("c", ticks: 130),
            },
        };

        var (outcome, sink) = await RunAsync(read, new[] { Range(null, null) }, peerCursor: Hlc(100), maxEntries: 2);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.EntriesReReplayed, Is.EqualTo(2));
            Assert.That(sink.LastEntries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
        });
    }

    [Test]
    public async Task ReplayAsync_ack_rejected_reports_zero_shipped()
    {
        var read = new WalReReplayReadResult { Entries = new[] { Entry("b", ticks: 150) } };

        var (outcome, sink) = await RunAsync(
            read, new[] { Range("a", "m") }, peerCursor: Hlc(100), ackAccepted: false);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.True);
            Assert.That(outcome.EntriesReReplayed, Is.Zero);
            Assert.That(sink.Calls, Is.EqualTo(1));
        });
    }

    [Test]
    public void ReplayAsync_throws_on_null_required_args()
    {
        var read = new WalReReplayReadResult { Entries = Array.Empty<WalRecord>() };
        var source = new StubSource(read);
        var sink = new RecordingSink(ackAccepted: true);
        var ranges = new[] { Range(null, null) };

        Assert.Multiple(() =>
        {
            Assert.That(async () => await LeafReReplayer.ReplayAsync(
                null!, Peer, Origin, ranges, HybridLogicalClock.Zero, source, sink, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(async () => await LeafReReplayer.ReplayAsync(
                Tree, null!, Origin, ranges, HybridLogicalClock.Zero, source, sink, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(async () => await LeafReReplayer.ReplayAsync(
                Tree, Peer, null!, ranges, HybridLogicalClock.Zero, source, sink, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(async () => await LeafReReplayer.ReplayAsync(
                Tree, Peer, Origin, null!, HybridLogicalClock.Zero, source, sink, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(async () => await LeafReReplayer.ReplayAsync(
                Tree, Peer, Origin, ranges, HybridLogicalClock.Zero, null!, sink, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(async () => await LeafReReplayer.ReplayAsync(
                Tree, Peer, Origin, ranges, HybridLogicalClock.Zero, source, null!, 1, 1, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
        });
    }

    private sealed class StubSource(WalReReplayReadResult result) : IWalReReplaySource
    {
        public ValueTask<WalReReplayReadResult> ReadAsync(CancellationToken cancellationToken) =>
            new(result);
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
