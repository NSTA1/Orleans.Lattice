using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Pins the soft-cap truncation arm of the targeted leaf re-replay engine: the
/// point at which a pass stops adding units because the next one would breach
/// the per-pass entry or byte cap.
/// <para>
/// The cap is deliberately soft and unit-granular. The engine always ships the
/// first unit even when it alone exceeds a cap (otherwise a single oversized
/// entry would wedge the repair forever), and it never splits an atomic batch
/// across the boundary, so an over-cap batch is either shipped whole or not at
/// all. What ships is a timestamp-ordered prefix; the remainder is repaired on
/// the next cadence once the peer's cursor has advanced.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafReReplayerCapTests
{
    private const string Tree = "orders";
    private const string Peer = "cluster-b";
    private const string Origin = "cluster-a";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    private static WalRecord Entry(
        string key,
        long ticks,
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
            OriginClusterId = Origin,
            AtomicBatchSize = atomicBatchSize,
            AtomicBatchIndex = atomicBatchIndex,
            TransactionId = transactionId,
        };

    private static LeafReReplayRange Range(string? start, string? end)
        => new() { StartKey = start, EndKey = end };

    private sealed class StubSource(WalReReplayReadResult result) : IWalReReplaySource
    {
        public ValueTask<WalReReplayReadResult> ReadAsync(CancellationToken cancellationToken) => new(result);
    }

    private sealed class RecordingSink : ILeafReReplaySink
    {
        public int Calls { get; private set; }

        public IReadOnlyList<WalRecord> LastEntries { get; private set; } = Array.Empty<WalRecord>();

        public ValueTask<int> ReplayAsync(
            string peer, string treeName, IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken)
        {
            Calls++;
            LastEntries = entries;
            return new ValueTask<int>(entries.Count);
        }
    }

    private static async Task<(LeafReReplayOutcome Outcome, RecordingSink Sink)> RunAsync(
        IReadOnlyList<WalRecord> entries, int maxEntries, long maxBytes)
    {
        var sink = new RecordingSink();
        var outcome = await LeafReReplayer.ReplayAsync(
            Tree,
            Peer,
            Origin,
            new[] { Range(null, null) },
            HybridLogicalClock.Zero,
            new StubSource(new WalReReplayReadResult { Entries = entries }),
            sink,
            maxEntries,
            maxBytes,
            CancellationToken.None);
        return (outcome, sink);
    }

    [Test]
    public async Task Entry_cap_ships_a_timestamp_ordered_prefix_and_stops()
    {
        var entries = new[] { Entry("a", 10), Entry("b", 20), Entry("c", 30), Entry("d", 40) };

        var (outcome, sink) = await RunAsync(entries, maxEntries: 2, maxBytes: 1024 * 1024);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.True);
            Assert.That(outcome.EntriesReReplayed, Is.EqualTo(2), "the cap must truncate the pass, not the selection");
            Assert.That(sink.LastEntries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }).AsCollection,
                "the shipped prefix must be the causally earliest entries so the peer's cursor advances monotonically");
        });
    }

    [Test]
    public async Task Byte_cap_stops_the_pass_before_the_unit_that_would_breach_it()
    {
        // Each entry estimates at 128 bytes of framing plus its value, so a cap
        // just above one entry admits the first and rejects the second.
        var entries = new[] { Entry("a", 10, valueBytes: 64), Entry("b", 20, valueBytes: 64) };

        var (outcome, sink) = await RunAsync(entries, maxEntries: 4096, maxBytes: 200);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.EntriesReReplayed, Is.EqualTo(1));
            Assert.That(sink.LastEntries.Single().Key, Is.EqualTo("a"));
        });
    }

    [Test]
    public async Task The_first_unit_always_ships_even_when_it_alone_exceeds_both_caps()
    {
        var entries = new[] { Entry("a", 10, valueBytes: 4096), Entry("b", 20) };

        var (outcome, sink) = await RunAsync(entries, maxEntries: 1, maxBytes: 1);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.True);
            Assert.That(outcome.EntriesReReplayed, Is.EqualTo(1),
                "refusing the first over-cap unit would wedge the repair permanently");
            Assert.That(sink.LastEntries.Single().Key, Is.EqualTo("a"));
        });
    }

    [Test]
    public async Task An_atomic_batch_that_would_breach_the_entry_cap_is_left_whole_for_the_next_pass()
    {
        var tx = Guid.NewGuid();
        var entries = new[]
        {
            Entry("a", 10),
            Entry("b", 20, atomicBatchSize: 2, atomicBatchIndex: 0, transactionId: tx),
            Entry("c", 21, atomicBatchSize: 2, atomicBatchIndex: 1, transactionId: tx),
        };

        var (outcome, sink) = await RunAsync(entries, maxEntries: 2, maxBytes: 1024 * 1024);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.EntriesReReplayed, Is.EqualTo(1),
                "the two-member batch does not fit under the remaining budget, so it is deferred whole");
            Assert.That(sink.LastEntries.Select(e => e.Key), Is.EqualTo(new[] { "a" }).AsCollection);
            Assert.That(sink.LastEntries.Any(e => e.TransactionId == tx), Is.False,
                "shipping half an atomic batch would expose a torn transaction at the peer");
        });
    }

    [Test]
    public async Task A_generous_cap_ships_every_selected_entry()
    {
        var entries = new[] { Entry("a", 10), Entry("b", 20), Entry("c", 30) };

        var (outcome, sink) = await RunAsync(entries, maxEntries: 4096, maxBytes: 1024 * 1024);

        Assert.That(outcome.EntriesReReplayed, Is.EqualTo(3));
        Assert.That(sink.Calls, Is.EqualTo(1));
    }
}
