using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Replication.Tests.Coyote;

/// <summary>
/// Which point-write dedup rule a <see cref="ReplicationDedupConvergenceModel"/> run applies.
/// </summary>
public enum ReplicationDedupMode
{
    /// <summary>
    /// The fix: the production <see cref="ReplicationApplier"/> alone, which since #1060 and
    /// #4463 has no HLC threshold: only the identity cache and the idempotent merge deduplicate.
    /// </summary>
    IdentityAndMerge,

    /// <summary>
    /// The guard removed, reproducing the receiver half of #1060: a filter in front of the
    /// production applier drops an entry at or below the incrementally advanced per-origin
    /// high-water mark (the diagonal).
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
/// A Coyote model of the receiver's dedup and merge over a lossy, duplicating, reordering
/// transport. It drives the production <see cref="ReplicationApplier"/> - both the per-entry
/// <see cref="ReplicationApplier.ApplyAsync"/> path and the batched
/// <see cref="ReplicationApplier.ApplyBatchAsync"/> run path - over the real
/// <see cref="ReplicationHighWaterMarkGrain"/>, with an in-memory leaf behind the apply grain
/// that merges by <c>LwwValue.Merge</c>. It is the executable counterpart of the TLA+
/// properties <c>DedupNeverDropsNew</c> and <c>EventualConvergence</c> in
/// <c>spec/replication/Replication.tla</c>, and because it runs the product's receive path, a
/// drop threshold reintroduced into either path makes the fixed arm fail (#4439 F6).
/// <para>
/// Two origins each commit one write to each of two last-writer-wins keys. Each key lives on
/// its own leaf with its own clock, and the scheduler picks each leaf's starting tick and each
/// origin's commit order, so an origin's HLCs are not monotonic in delivery order. Entries
/// cross a <see cref="FaultDeliveryQueue{T}"/> that reorders, drops and duplicates, and each
/// delivery is a single entry or a two-entry batch. Once the fault budget is spent, the
/// shipper re-sends every entry the receiver has not acknowledged.
/// </para>
/// <para>
/// Safety, at every drop: an entry the applier did not forward to the leaf would not change the
/// leaf's value. After every delivery: the high-water mark covers every entry forwarded.
/// Liveness, at the end: each key's value equals the merge of every write to it. The identity
/// cache holds fewer entries than the run delivers, so eviction and the idempotent-merge
/// fallback behind it are exercised.
/// </para>
/// </summary>
public sealed class ReplicationDedupConvergenceModel : ICoyoteModel
{
    private const string Tree = "coyote-replication-dedup";
    private const string Receiver = "site-c";
    private static readonly string[] Keys = ["k1", "k2"];
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
            var own = Keys.Select(key => Write(origin, key, 1 + Choose(runtime, 3))).ToList();

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

        var leaf = new LeafApplyGrain();
        var hwmGrain = HighWaterMarkTestGrains.Real(treeId: Tree);
        var applier = CreateApplier(leaf, hwmGrain);
        var acked = new HashSet<WalRecord>();

        void Receive(IReadOnlyList<WalRecord> batch)
        {
            if (_mode == ReplicationDedupMode.MonotonicDeliveryProbe)
            {
                foreach (var entry in batch)
                {
                    Specification.Assert(
                        entry.Timestamp.CompareTo(HighWaterMark(hwmGrain, entry.OriginClusterId!)) >= 0,
                        "PROBE: an entry was delivered below its origin's high-water mark");
                }
            }

            // The guard reintroduces the removed rule in front of the product, so the product
            // itself is never modified: a drop at or below the origin's high-water mark.
            var admitted = _mode == ReplicationDedupMode.IncrementalDiagonal
                ? batch.Where(e => e.Timestamp.CompareTo(HighWaterMark(hwmGrain, e.OriginClusterId!)) > 0).ToList()
                : batch.ToList();

            var before = leaf.Snapshot();
            leaf.Forwarded.Clear();
            if (admitted.Count == 1)
            {
                applier.ApplyAsync(admitted[0]).GetAwaiter().GetResult();
            }
            else if (admitted.Count > 1)
            {
                applier.ApplyBatchAsync(admitted).GetAwaiter().GetResult();
            }

            foreach (var entry in batch)
            {
                if (!leaf.Forwarded.Contains((entry.Key, entry.OriginClusterId!, entry.Timestamp)))
                {
                    AssertSubsumed(entry, before);
                }
                else
                {
                    Specification.Assert(
                        HighWaterMark(hwmGrain, entry.OriginClusterId!).CompareTo(entry.Timestamp) >= 0,
                        $"HighWaterMarkCoversApplied: {entry.OriginClusterId}@{entry.Timestamp} was applied above the high-water mark");
                }

                acked.Add(entry);
            }
        }

        while (transport.HasPending)
        {
            if (!transport.TryDeliverNext(runtime.RandomBoolean, out var first))
            {
                continue;
            }

            // A delivery is one entry or, when the scheduler chooses, a two-entry batch, so the
            // batched run path is exercised as well as the per-entry path.
            if (transport.HasPending && runtime.RandomBoolean()
                && transport.TryDeliverNext(runtime.RandomBoolean, out var second))
            {
                Receive([first, second]);
            }
            else
            {
                Receive([first]);
            }
        }

        // The fault budget is spent: the shipper re-sends every unacknowledged entry over a
        // transport that is now reliable.
        foreach (var entry in authored.Where(e => !acked.Contains(e)).ToList())
        {
            Receive([entry]);
        }

        foreach (var key in Keys)
        {
            var expected = authored
                .Where(e => e.Key == key)
                .Select(e => LwwValue<byte[]>.Create(Payload(e.OriginClusterId!), e.Timestamp))
                .Aggregate(LwwValue<byte[]>.Merge);
            var held = leaf.Get(key);
            Specification.Assert(
                held is { } final && SameValue(final, expected),
                $"EventualConvergence: key {key} did not converge: got {Describe(held)}, expected {Describe(expected)}");
        }
    }

    private static ReplicationApplier CreateApplier(LeafApplyGrain leaf, ReplicationHighWaterMarkGrain hwm)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(leaf);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        var options = new LatticeReplicationOptions { ClusterId = Receiver, ShadowForwardDedupeCacheSize = 2 };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return new ReplicationApplier(factory, monitor, replicationContext: new LwwContext());
    }

    private static void AssertSubsumed(WalRecord entry, IReadOnlyDictionary<string, LwwValue<byte[]>> before)
    {
        if (string.Equals(entry.OriginClusterId, Receiver, StringComparison.Ordinal))
        {
            return;
        }

        var incoming = LwwValue<byte[]>.Create(Payload(entry.OriginClusterId!), entry.Timestamp);
        Specification.Assert(
            before.TryGetValue(entry.Key, out var held)
                && LwwValue<byte[]>.Merge(held, incoming) is var merged
                && SameValue(merged, held),
            $"DedupNeverDropsNew: a new write {entry.OriginClusterId}@{entry.Timestamp} to {entry.Key} was dropped as a duplicate");
    }

    // The production value type: the payload bytes are the merge's replica-invariant tie-break.
    private static byte[] Payload(string origin) => System.Text.Encoding.UTF8.GetBytes(origin);

    private static bool SameValue(LwwValue<byte[]> left, LwwValue<byte[]> right) =>
        left.Timestamp == right.Timestamp && left.Value.AsSpan().SequenceEqual(right.Value);

    private static string Describe(LwwValue<byte[]>? value) =>
        value is { } v ? $"{System.Text.Encoding.UTF8.GetString(v.Value!)}@{v.Timestamp}" : "nothing";

    private static HybridLogicalClock HighWaterMark(ReplicationHighWaterMarkGrain hwm, string origin) =>
        hwm.GetAsync(origin, CancellationToken.None).GetAwaiter().GetResult();

    private static WalRecord Write(string origin, string key, long tick) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Set,
        Key = key,
        Value = Payload(origin),
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

    /// <summary>Every tree is last-writer-wins and replication is enabled.</summary>
    private sealed class LwwContext : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => Receiver;

        public LatticeMergeMode? ResolveMergeMode(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>
    /// The leaf behind <see cref="IReplicationApplyGrain"/>: it merges every forwarded write by
    /// <c>LwwValue.Merge</c> and records which writes the applier forwarded.
    /// </summary>
    private sealed class LeafApplyGrain : IReplicationApplyGrain
    {
        private readonly Dictionary<string, LwwValue<byte[]>> _values = new(StringComparer.Ordinal);

        public HashSet<(string Key, string Origin, HybridLogicalClock Hlc)> Forwarded { get; } = [];

        public LwwValue<byte[]>? Get(string key) => _values.TryGetValue(key, out var value) ? value : null;

        public IReadOnlyDictionary<string, LwwValue<byte[]>> Snapshot() =>
            new Dictionary<string, LwwValue<byte[]>>(_values, StringComparer.Ordinal);

        public Task ApplySetAsync(
            string key, byte[] value, HybridLogicalClock sourceHlc, string originClusterId,
            VersionVector? sourceVectorClock, long expiresAtTicks)
        {
            Merge(key, value, sourceHlc, originClusterId);
            return Task.CompletedTask;
        }

        public Task ApplyMergeManyAsync(IReadOnlyList<ApplyMergeItem> items)
        {
            foreach (var item in items)
            {
                Merge(item.Key, item.Value!, item.SourceHlc, item.OriginClusterId);
            }

            return Task.CompletedTask;
        }

        private void Merge(string key, byte[] value, HybridLogicalClock hlc, string origin)
        {
            Forwarded.Add((key, origin, hlc));
            var incoming = LwwValue<byte[]>.Create(value, hlc);
            _values[key] = _values.TryGetValue(key, out var held) ? LwwValue<byte[]>.Merge(held, incoming) : incoming;
        }

        public Task ApplyDeleteAsync(string key, HybridLogicalClock sourceHlc, string originClusterId, VersionVector? sourceVectorClock) =>
            throw new NotSupportedException();

        public Task<VersionedValue> ReadStoredWithVersionAsync(string key) => throw new NotSupportedException();

        public Task ApplyDeleteRangeAsync(
            string startInclusive, string endExclusive, HybridLogicalClock sourceHlc, string originClusterId,
            VersionVector? sourceVectorClock, IReadOnlyList<string>? explicitMatchedKeys = null) =>
            throw new NotSupportedException();

        public Task ApplyCrdtDeltaManyAsync(IReadOnlyList<ApplyCrdtDeltaItem> items) => throw new NotSupportedException();

        public Task ApplyCrdtDeltaWithExpiryAsync(string key, LatticeMergeMode mode, byte[] deltaBytes, long expiresAtTicks) =>
            throw new NotSupportedException();

        public Task ApplyPreparedSetAsync(
            string key, byte[] value, HybridLogicalClock sourceHlc, string originClusterId,
            VersionVector? sourceVectorClock, long expiresAtTicks, Guid transactionId, int atomicBatchSize,
            int atomicBatchIndex, byte[]? delta = null, LatticeMergeMode mode = LatticeMergeMode.LwwRegister) =>
            throw new NotSupportedException();

        public Task ApplyPreparedDeleteAsync(
            string key, HybridLogicalClock sourceHlc, string originClusterId, VersionVector? sourceVectorClock,
            Guid transactionId, int atomicBatchSize, int atomicBatchIndex) =>
            throw new NotSupportedException();

        public Task ApplyTxTerminalAsync(
            Guid transactionId, bool committed, int shardIndex, HybridLogicalClock terminalHlc, string originClusterId,
            int atomicShardCount = 0, string? crossTreeOperationId = null, IReadOnlyList<string>? crossTreeWaitSet = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task FinalizeCrossTreeTerminalAsync(
            Guid transactionId, bool committed, IReadOnlyList<int> observedSourceShards, HybridLogicalClock terminalHlc,
            string originClusterId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
