using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Receiver-side wire-routing tests for saga prepare-phase entries
/// (<see cref="WalRecord.IsPrepared"/>=<see langword="true"/>) and saga
/// terminal-mark records (<see cref="MutationKind.TxCommit"/> /
/// <see cref="MutationKind.TxAbort"/>) flowing through
/// <see cref="ReplicationApplier.ApplyAsync"/>.
///
/// These tests guard the receiver-side routing layer that delivers
/// cross-cluster atomic visibility: prepared entries route through the
/// <see cref="IReplicationApplyGrain.ApplyPreparedSetAsync"/> /
/// <see cref="IReplicationApplyGrain.ApplyPreparedDeleteAsync"/> seam
/// so the receiver leaf parks them in its per-tx pending bucket;
/// terminals route through the
/// <see cref="IReplicationApplyGrain.ApplyTxTerminalAsync"/> seam,
/// bypass the per-origin HWM dedup, and re-stamp the source cluster's
/// terminal HLC verbatim onto the receiver's local WAL.
/// </summary>
public partial class ReplicationApplierTests
{
    private static WalRecord PreparedSetEntry(
        string key,
        HybridLogicalClock ts,
        Guid transactionId,
        int atomicBatchSize = 0,
        int atomicBatchIndex = 0,
        long expiresAtTicks = 0,
        string origin = RemoteCluster) => new()
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = origin,
            IsPrepared = true,
            TransactionId = transactionId,
            AtomicBatchSize = atomicBatchSize,
            AtomicBatchIndex = atomicBatchIndex,
            ExpiresAtTicks = expiresAtTicks,
        };

    private static WalRecord PreparedDeleteEntry(
        string key,
        HybridLogicalClock ts,
        Guid transactionId,
        int atomicBatchSize = 0,
        int atomicBatchIndex = 0,
        string origin = RemoteCluster) => new()
        {
            TreeId = Tree,
            Op = MutationKind.Delete,
            Key = key,
            Timestamp = ts,
            IsTombstone = true,
            OriginClusterId = origin,
            IsPrepared = true,
            TransactionId = transactionId,
            AtomicBatchSize = atomicBatchSize,
            AtomicBatchIndex = atomicBatchIndex,
        };

    private static WalRecord TerminalEntry(
        MutationKind kind,
        Guid transactionId,
        HybridLogicalClock ts,
        int shardIndex,
        bool stampTypedShardIndex = true,
        string origin = RemoteCluster) => new()
        {
            TreeId = Tree,
            Op = kind,
            Key = shardIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Timestamp = ts,
            OriginClusterId = origin,
            TransactionId = transactionId,
            ShardIndex = stampTypedShardIndex ? shardIndex : 0,
            IsPrepared = false,
        };

    [Test]
    public async Task ApplyAsync_routes_prepared_set_through_apply_grain_with_full_atomic_batch_metadata()
    {
        var (applier, _, apply, _) = CreateApplier();
        var ts = Hlc(10, 1);
        var txid = Guid.NewGuid();

        var entry = PreparedSetEntry(
            key: "k",
            ts: ts,
            transactionId: txid,
            atomicBatchSize: 3,
            atomicBatchIndex: 1,
            expiresAtTicks: 99);

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyPreparedSetAsync(
            "k",
            Arg.Any<byte[]>(),
            ts,
            RemoteCluster,
            null,
            99,
            txid,
            3,
            1);
        // The non-prepared seam must NOT be called for IsPrepared=true.
        await apply.DidNotReceive().ApplySetAsync(
            Arg.Any<string>(),
            Arg.Any<byte[]>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<string>(),
            Arg.Any<VersionVector?>(),
            Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_routes_prepared_delete_through_apply_grain_with_full_atomic_batch_metadata()
    {
        var (applier, _, apply, _) = CreateApplier();
        var ts = Hlc(20, 2);
        var txid = Guid.NewGuid();

        var entry = PreparedDeleteEntry(
            key: "k",
            ts: ts,
            transactionId: txid,
            atomicBatchSize: 3,
            atomicBatchIndex: 2);

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyPreparedDeleteAsync(
            "k",
            ts,
            RemoteCluster,
            null,
            txid,
            3,
            2);
        await apply.DidNotReceive().ApplyDeleteAsync(
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<string>(),
            Arg.Any<VersionVector?>());
    }

    [Test]
    public void ApplyAsync_throws_when_prepared_set_carries_empty_transaction_id()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = PreparedSetEntry("k", Hlc(10), Guid.Empty);

        Assert.That(
            async () => await applier.ApplyAsync(entry),
            Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_throws_when_prepared_delete_carries_empty_transaction_id()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = PreparedDeleteEntry("k", Hlc(10), Guid.Empty);

        Assert.That(
            async () => await applier.ApplyAsync(entry),
            Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_throws_when_prepared_set_carries_null_value()
    {
        var (applier, _, _, _) = CreateApplier();
        // Hand-build the entry rather than using the helper so we can
        // null-out Value while keeping IsPrepared+TransactionId valid.
        var entry = new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = "k",
            Value = null,
            Timestamp = Hlc(10),
            OriginClusterId = RemoteCluster,
            IsPrepared = true,
            TransactionId = Guid.NewGuid(),
        };

        Assert.That(
            async () => await applier.ApplyAsync(entry),
            Throws.ArgumentException);
    }

    [Test]
    public async Task ApplyAsync_routes_TxCommit_through_apply_grain_with_typed_shard_index_slot()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(50, 3);
        var txid = Guid.NewGuid();

        var entry = TerminalEntry(MutationKind.TxCommit, txid, ts, shardIndex: 7);

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        await apply.Received(1).ApplyTxTerminalAsync(
            txid,
            true,
            7,
            ts,
            RemoteCluster,
            Arg.Any<int>(),
            Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(),
            Arg.Any<CancellationToken>());
        // Terminals bypass the per-origin HWM check entirely - neither
        // a HWM read nor a HWM advance must be issued.
        await hwm.DidNotReceive().GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await hwm.DidNotReceive().TryAdvanceAsync(
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_routes_TxAbort_through_apply_grain_with_typed_shard_index_slot()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(60);
        var txid = Guid.NewGuid();

        var entry = TerminalEntry(MutationKind.TxAbort, txid, ts, shardIndex: 4);

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        await apply.Received(1).ApplyTxTerminalAsync(
            txid,
            false,
            4,
            ts,
            RemoteCluster,
            Arg.Any<int>(),
            Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(),
            Arg.Any<CancellationToken>());
        await hwm.DidNotReceive().GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await hwm.DidNotReceive().TryAdvanceAsync(
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_falls_back_to_numeric_key_parse_when_terminal_lacks_typed_ShardIndex()
    {
        // Back-compat path: a pre-Option A WAL record carries the shard
        // index only in the legacy mutation.Key encoding (typed slot=0).
        // The applier must parse the numeric key and route the terminal
        // correctly.
        var (applier, _, apply, _) = CreateApplier();
        var ts = Hlc(70);
        var txid = Guid.NewGuid();

        var entry = TerminalEntry(
            MutationKind.TxCommit,
            txid,
            ts,
            shardIndex: 5,
            stampTypedShardIndex: false);

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyTxTerminalAsync(
            txid,
            true,
            5,
            ts,
            RemoteCluster,
            Arg.Any<int>(),
            Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_routes_TxCommit_for_zero_shard_via_numeric_key_parse()
    {
        // Edge case: shard index 0 is a valid shard but its typed-slot
        // representation (default(int)=0) collides with the
        // "no typed slot stamped" sentinel. The applier must still
        // accept it via the numeric-key fallback, which permits
        // non-negative parses including 0.
        var (applier, _, apply, _) = CreateApplier();
        var ts = Hlc(80);
        var txid = Guid.NewGuid();

        var entry = TerminalEntry(
            MutationKind.TxCommit,
            txid,
            ts,
            shardIndex: 0,
            stampTypedShardIndex: false);

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyTxTerminalAsync(
            txid,
            true,
            0,
            ts,
            RemoteCluster,
            Arg.Any<int>(),
            Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void ApplyAsync_throws_when_TxCommit_carries_empty_transaction_id()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = TerminalEntry(MutationKind.TxCommit, Guid.Empty, Hlc(50), shardIndex: 7);

        Assert.That(
            async () => await applier.ApplyAsync(entry),
            Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_throws_when_TxAbort_carries_unresolvable_shard_index()
    {
        // Neither typed slot nor a parseable numeric key - the applier
        // has no way to address the receiver's per-shard root, so it
        // must surface the malformed record explicitly rather than
        // silently misroute or no-op.
        var (applier, _, _, _) = CreateApplier();
        var entry = new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.TxAbort,
            Key = "not-a-number",
            Timestamp = Hlc(50),
            OriginClusterId = RemoteCluster,
            TransactionId = Guid.NewGuid(),
            ShardIndex = 0,
            IsPrepared = false,
        };

        Assert.That(
            async () => await applier.ApplyAsync(entry),
            Throws.ArgumentException);
    }

    [Test]
    public async Task ApplyAsync_skips_local_origin_TxCommit_as_dedup_no_op()
    {
        // The local-origin guard at the top of ApplyAsync runs BEFORE
        // the terminal branch - so a TxCommit tagged with the receiver's
        // own ClusterId must surface as a dedup no-op, not flow through
        // ApplyTxTerminalAsync. Defence-in-depth against hand-built
        // pipelines that bypass the producer's outbound origin filter.
        var (applier, _, apply, hwm) = CreateApplier();
        var entry = TerminalEntry(
            MutationKind.TxCommit,
            Guid.NewGuid(),
            Hlc(50),
            shardIndex: 7,
            origin: LocalCluster);

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.False);
        Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        await apply.DidNotReceive().ApplyTxTerminalAsync(
            Arg.Any<Guid>(),
            Arg.Any<bool>(),
            Arg.Any<int>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<string>(),
            Arg.Any<int>(),
            Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(),
            Arg.Any<CancellationToken>());
        await hwm.DidNotReceive().TryAdvanceAsync(
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_skips_local_origin_prepared_set_as_dedup_no_op()
    {
        // Symmetric to the terminal local-origin guard: a prepared Set
        // tagged with the receiver's own ClusterId must surface as a
        // dedup no-op rather than flow through ApplyPreparedSetAsync.
        var (applier, _, apply, _) = CreateApplier();
        var entry = PreparedSetEntry(
            "k",
            Hlc(10),
            Guid.NewGuid(),
            origin: LocalCluster);

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceive().ApplyPreparedSetAsync(
            Arg.Any<string>(),
            Arg.Any<byte[]>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<string>(),
            Arg.Any<VersionVector?>(),
            Arg.Any<long>(),
            Arg.Any<Guid>(),
            Arg.Any<int>(),
            Arg.Any<int>());
    }

    [Test]
    public async Task ApplyBatchAsync_routes_batched_TxCommit_terminal_through_terminal_seam()
    {
        // Cross-cluster atomic-visibility regression (issue #1525). The
        // single-entry ApplyAsync path intercepts saga terminals and routes
        // them through ApplyTxTerminalAsync, but the multi-entry batch path
        // (ApplyOriginRunAsync) previously had no terminal branch: a
        // TxCommit / TxAbort fell through to ApplyPointAsync, whose op
        // switch has no terminal case and throws "Unsupported point-apply
        // op". The production shipper coalesces a saga's contiguous WAL
        // entries (its prepared writes AND its terminal) into one inbound
        // batch, so the terminal is delivered batched - and the whole batch
        // then faulted, the terminal was never applied, and the saga's keys
        // stayed pending (invisible) on the peer forever. This test drives a
        // batch shaped exactly like that coalesced saga and asserts the
        // terminal reaches the ApplyTxTerminalAsync seam.
        var (applier, _, apply, _) = CreateApplier();
        var txid = Guid.NewGuid();
        var entries = new[]
        {
            PreparedSetEntry("k0", Hlc(10), txid, atomicBatchSize: 2, atomicBatchIndex: 0),
            PreparedSetEntry("k1", Hlc(20), txid, atomicBatchSize: 2, atomicBatchIndex: 1),
            TerminalEntry(MutationKind.TxCommit, txid, Hlc(30), shardIndex: 0),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.That(result.Applied, Is.True);
        // The prepared entries still park via the prepared-apply seam.
        await apply.Received(1).ApplyPreparedSetAsync(
            "k0", Arg.Any<byte[]>(), Hlc(10), RemoteCluster, null, 0, txid, 2, 0);
        await apply.Received(1).ApplyPreparedSetAsync(
            "k1", Arg.Any<byte[]>(), Hlc(20), RemoteCluster, null, 0, txid, 2, 1);
        // The terminal must reach the terminal seam so the receiver's
        // per-tree TxRegistry mark (and, for cross-tree, the barrier) fires
        // and the pending saga keys flip visible.
        await apply.Received(1).ApplyTxTerminalAsync(
            txid,
            true,
            0,
            Hlc(30),
            RemoteCluster,
            Arg.Any<int>(),
            Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_forwards_cross_tree_barrier_metadata_for_batched_terminal()
    {
        // Issue #1525, cross-tree specific. A cross-tree atomic write stamps
        // its terminal with CrossTreeOperationId + CrossTreeParticipants;
        // the applier turns that into the receiver barrier's wait set
        // (scoped to trees replicated here). When the terminal is delivered
        // batched together with its prepared entries, the batch path must
        // still forward that barrier metadata to ApplyTxTerminalAsync -
        // otherwise the receiver coordinator never flips the participating
        // trees together and the peer never sees the cross-tree keys.
        var (applier, _, apply, _) = CreateApplier();
        var txid = Guid.NewGuid();
        const string operationId = "xt-op-1525";
        const string otherTree = "chaos-xt-b";
        var participants = new[] { Tree, otherTree };

        var terminal = new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.TxCommit,
            Key = "0",
            Timestamp = Hlc(40),
            OriginClusterId = RemoteCluster,
            TransactionId = txid,
            ShardIndex = 0,
            AtomicShardCount = 1,
            CrossTreeOperationId = operationId,
            CrossTreeParticipants = participants,
            IsPrepared = false,
        };

        var entries = new[]
        {
            PreparedSetEntry("k0", Hlc(20), txid, atomicBatchSize: 1, atomicBatchIndex: 0),
            terminal,
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyTxTerminalAsync(
            txid,
            true,
            0,
            Hlc(40),
            RemoteCluster,
            1,
            operationId,
            Arg.Is<IReadOnlyList<string>?>(w =>
                w != null && w.Contains(Tree) && w.Contains(otherTree)),
            Arg.Any<CancellationToken>());
    }
}
