using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Replication.Tests.Coyote;

/// <summary>
/// Which point-write dedup rule a <see cref="ReplicationDedupConvergenceModel"/> run applies.
/// </summary>
public enum ReplicationDedupMode
{
    /// <summary>
    /// The fix: no HLC threshold at all. Only the identity cache and the idempotent merge
    /// deduplicate, as <see cref="ReplicationApplier"/> does since #1060 and #4463.
    /// </summary>
    IdentityAndMerge,

    /// <summary>
    /// The guard removed, reproducing the receiver half of #1060: the threshold is the
    /// incrementally advanced per-origin high-water mark (the diagonal).
    /// </summary>
    IncrementalDiagonal,

    /// <summary>
    /// The anti-vacuity witness: the fix, plus an assertion that no entry is ever delivered
    /// with an HLC below its origin's high-water mark. Coyote must refute it, which proves the
    /// exploration reaches the non-monotonic delivery the diagonal mistakes for a duplicate.
    /// </summary>
    MonotonicDeliveryProbe,
}

/// <summary>
/// A Coyote model of the receiver's point-write dedup and merge over a lossy, duplicating,
/// reordering transport. It drives the real <see cref="ReplicationReceiveDedup"/>,
/// <see cref="RecentApplyCache"/>, <c>LwwValue.Merge</c> and <see cref="GCounter.MergeFrom"/> -
/// the decisions and merges the production <see cref="ReplicationApplier"/> runs - and is the
/// executable counterpart of the TLA+ properties <c>DedupNeverDropsNew</c> and
/// <c>EventualConvergence</c> in <c>spec/replication/Replication.tla</c>.
/// <para>
/// Two origins each commit one write to a last-writer-wins key and one increment to a
/// grow-only-counter key. Each key lives on its own leaf with its own clock, and the scheduler
/// picks each leaf's starting tick and each origin's commit order, so an origin's HLCs are not
/// monotonic in delivery order. Entries cross a <see cref="FaultDeliveryQueue{T}"/> that
/// reorders, drops and duplicates; once the fault budget is spent, the shipper re-sends every
/// entry the receiver has not acknowledged (its cursor never passed them), which is the
/// backstop the bounded-progress liveness encoding requires.
/// </para>
/// <para>
/// Safety, at every drop: an entry the receiver discards as a duplicate would not change the receiver's value. Liveness, at the end: the receiver's
/// values equal the merge of every write. The identity cache holds fewer entries than the run
/// delivers, so eviction and the idempotent-merge fallback behind it are exercised.
/// </para>
/// </summary>
public sealed class ReplicationDedupConvergenceModel : ICoyoteModel
{
    private const string Tree = "coyote-replication-dedup";
    private const string Receiver = "site-c";
    private const string LwwKey = "k1";
    private const string CounterKey = "k2";
    private static readonly string[] Origins = ["site-a", "site-b"];

    private readonly ReplicationDedupMode _mode;
    private readonly int _drops;
    private readonly int _duplicates;

    /// <summary>Creates the model for one dedup mode and a transport fault allowance.</summary>
    public ReplicationDedupConvergenceModel(ReplicationDedupMode mode, int drops = 1, int duplicates = 2)
    {
        _mode = mode;
        _drops = drops;
        _duplicates = duplicates;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        // All per-iteration state is local: the engine reuses this instance across schedules.
        var budget = new FaultBudget(_drops, _duplicates, restarts: 0);
        var transport = new FaultDeliveryQueue<WalRecord>(budget);
        var authored = new List<WalRecord>();

        foreach (var origin in Origins)
        {
            var own = new List<WalRecord>
            {
                Write(origin, LwwKey, 1 + Choose(runtime, 3)),
                Write(origin, CounterKey, 1 + Choose(runtime, 3)),
            };

            // The scheduler picks which key the origin commits first, so WAL (shipping) order
            // and HLC order disagree.
            if (runtime.RandomBoolean())
            {
                own.Reverse();
            }

            foreach (var entry in own)
            {
                authored.Add(entry);
                transport.Enqueue(entry);
            }
        }

        var cache = new RecentApplyCache(capacity: 2);
        var hwm = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        LwwValue<byte[]>? lww = null;
        var counter = new GCounter();
        var acked = new HashSet<WalRecord>();

        void Receive(WalRecord entry)
        {
            var origin = entry.OriginClusterId!;
            if (_mode == ReplicationDedupMode.MonotonicDeliveryProbe)
            {
                Specification.Assert(
                    entry.Timestamp.CompareTo(HighWaterMark(hwm, origin)) >= 0,
                    "PROBE: an entry was delivered below its origin's high-water mark");
            }

            if (ReplicationReceiveDedup.IsOwnOrigin(origin, Receiver))
            {
                acked.Add(entry);
                return;
            }

            // The guard reintroduces the removed rule, so it is written here rather than in the
            // product: a drop at or below the origin's high-water mark.
            var droppedOnDiagonal = _mode == ReplicationDedupMode.IncrementalDiagonal
                && entry.Timestamp.CompareTo(HighWaterMark(hwm, origin)) <= 0;
            if (droppedOnDiagonal || !cache.TryAdd(entry))
            {
                AssertSubsumed(entry, lww, counter);
                acked.Add(entry);
                return;
            }

            if (entry.Key == LwwKey)
            {
                var incoming = LwwValue<byte[]>.Create(Payload(origin), entry.Timestamp);
                lww = lww is { } held ? LwwValue<byte[]>.Merge(held, incoming) : incoming;
            }
            else
            {
                counter.MergeFrom(Delta(entry));
            }

            if (ReplicationReceiveDedup.AdvancesHighWaterMark(HighWaterMark(hwm, origin), entry.Timestamp))
            {
                hwm[origin] = entry.Timestamp;
            }

            acked.Add(entry);
        }

        while (transport.HasPending)
        {
            if (transport.TryDeliverNext(runtime.RandomBoolean, out var delivered))
            {
                Receive(delivered);
            }
        }

        // The fault budget is spent: the shipper re-sends every unacknowledged entry over a
        // transport that is now reliable.
        foreach (var entry in authored.Where(e => !acked.Contains(e)).ToList())
        {
            Receive(entry);
        }

        var expectedLww = authored
            .Where(e => e.Key == LwwKey)
            .Select(e => LwwValue<byte[]>.Create(Payload(e.OriginClusterId!), e.Timestamp))
            .Aggregate(LwwValue<byte[]>.Merge);
        var expectedCounter = new GCounter();
        foreach (var e in authored.Where(e => e.Key == CounterKey))
        {
            expectedCounter.MergeFrom(Delta(e));
        }

        Specification.Assert(
            lww is { } final && SameValue(final, expectedLww),
            $"EventualConvergence: the last-writer-wins key did not converge: got {Describe(lww)}, "
            + $"expected {Describe(expectedLww)}");
        Specification.Assert(
            counter.Value == expectedCounter.Value,
            $"EventualConvergence: the counter key did not converge: got {counter.Value}, expected {expectedCounter.Value}");
    }

    private static void AssertSubsumed(WalRecord entry, LwwValue<byte[]>? lww, GCounter counter)
    {
        if (entry.Key == LwwKey)
        {
            var incoming = LwwValue<byte[]>.Create(Payload(entry.OriginClusterId!), entry.Timestamp);
            Specification.Assert(
                lww is { } held
                    && LwwValue<byte[]>.Merge(held, incoming) is var merged
                    && SameValue(merged, held),
                $"DedupNeverDropsNew: a new write {entry.OriginClusterId}@{entry.Timestamp} to {entry.Key} was dropped as a duplicate");
        }
        else
        {
            var probe = counter.Clone();
            probe.MergeFrom(Delta(entry));
            Specification.Assert(
                probe.Value == counter.Value,
                $"DedupNeverDropsNew: a new increment {entry.OriginClusterId}@{entry.Timestamp} to {entry.Key} was dropped as a duplicate");
        }
    }

    // The production value type: the payload bytes are the merge's replica-invariant tie-break.
    private static byte[] Payload(string origin) => System.Text.Encoding.UTF8.GetBytes(origin);

    private static bool SameValue(LwwValue<byte[]> left, LwwValue<byte[]> right) =>
        left.Timestamp == right.Timestamp && left.Value.AsSpan().SequenceEqual(right.Value);

    private static string Describe(LwwValue<byte[]>? value) =>
        value is { } v ? $"{System.Text.Encoding.UTF8.GetString(v.Value!)}@{v.Timestamp}" : "nothing";

    // A grow-only-counter delta carries the origin's running count; each origin increments once.
    private static GCounter Delta(WalRecord entry)
    {
        var delta = new GCounter();
        delta.Increment(entry.OriginClusterId!, 1);
        return delta;
    }

    private static HybridLogicalClock HighWaterMark(Dictionary<string, HybridLogicalClock> hwm, string origin) =>
        hwm.TryGetValue(origin, out var mark) ? mark : HybridLogicalClock.Zero;

    private static WalRecord Write(string origin, string key, long tick) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Set,
        Key = key,
        Value = [1],
        Timestamp = new HybridLogicalClock { WallClockTicks = tick },
        OriginClusterId = origin,
    };

    private static int Choose(ICoyoteRuntime runtime, int count)
    {
        for (var i = 0; i < count - 1; i++)
        {
            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return count - 1;
    }
}
