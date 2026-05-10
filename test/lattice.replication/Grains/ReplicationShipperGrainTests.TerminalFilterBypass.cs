using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Producer-side filter-bypass tests for saga terminal-mark records
/// (<see cref="MutationKind.TxCommit"/> /
/// <see cref="MutationKind.TxAbort"/>) flowing through
/// <c>ReplicationShipperGrain.ShouldShip</c>.
///
/// Terminal records carry <c>Key=ShardIndex.ToString()</c> - an
/// internal shard-routing token, not a user key. Trees configured
/// with restrictive <see cref="LatticeReplicationOptions.KeyFilter"/>
/// or <see cref="LatticeReplicationOptions.KeyPrefixes"/> filters
/// would otherwise drop every terminal at the producer, breaking
/// cross-cluster atomic visibility on the receiver side because the
/// linearization point that flips pending into visible never arrives.
/// The shipper short-circuits <c>ShouldShip</c> for TxCommit /
/// TxAbort kinds so terminals always ship through, but the empty-
/// origin guard and the peer-cycle-break still run first - terminals
/// authored locally for a different peer must not loop back.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static WalRecord MakeTerminalEntry(
        MutationKind kind,
        int shardIndex = 0,
        string origin = LocalCluster,
        long ticks = 1,
        int counter = 0,
        Guid? transactionId = null) => new()
        {
            TreeId = Tree,
            Op = kind,
            Key = shardIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = counter },
            OriginClusterId = origin,
            TransactionId = transactionId ?? Guid.NewGuid(),
            ShardIndex = shardIndex,
            IsPrepared = false,
        };

    [Test]
    public async Task PumpOnceAsync_ships_TxCommit_even_when_KeyFilter_rejects_shard_index_key()
    {
        // KeyFilter only admits user keys under "repl/"; the terminal's
        // Key="0" (shard index 0) does NOT pass the filter. ShouldShip
        // must short-circuit for TxCommit so the terminal still ships,
        // otherwise cross-cluster atomic visibility breaks on the
        // receiver side (the linearization point that flips pending
        // into visible never arrives).
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            KeyFilter = key => key.StartsWith("repl/"),
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        ReplicationBatch? captured = null;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured = call.Arg<ReplicationBatch>();
                return new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
            });
        feed.Append(MakeTerminalEntry(MutationKind.TxCommit, shardIndex: 0, ticks: 5));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(1).SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
        Assert.That(captured, Is.Not.Null);
    }

    [Test]
    public async Task PumpOnceAsync_ships_TxAbort_even_when_KeyFilter_rejects_shard_index_key()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            KeyFilter = key => key.StartsWith("repl/"),
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeTerminalEntry(MutationKind.TxAbort, shardIndex: 3, ticks: 7));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(1).SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_ships_TxCommit_even_when_entry_key_outside_KeyPrefixes()
    {
        // KeyPrefixes admits only "repl/" / "ops/"; the terminal's
        // Key="2" matches neither prefix. The bypass applies the same
        // way - terminals route by shard-index slot, not by user key,
        // so they must ship regardless of the user-key filter.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            KeyPrefixes = new[] { "repl/", "ops/" },
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeTerminalEntry(MutationKind.TxCommit, shardIndex: 2, ticks: 5));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(1).SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_ships_TxAbort_even_when_entry_key_outside_KeyPrefixes()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            KeyPrefixes = new[] { "repl/", "ops/" },
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeTerminalEntry(MutationKind.TxAbort, shardIndex: 1, ticks: 9));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(1).SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_ships_terminal_alongside_filtered_user_keys()
    {
        // Mixed batch: a user Set that fails KeyFilter (Key="other/x")
        // and a TxCommit terminal (Key="0"). ShouldShip drops the user
        // Set and ships only the terminal. Asserts the bypass is applied
        // per-entry, not per-batch, so a single failing user key does
        // not poison the surrounding terminal delivery.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            KeyFilter = key => key.StartsWith("repl/"),
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeEntry("other/x", ticks: 5));
        feed.Append(MakeTerminalEntry(MutationKind.TxCommit, shardIndex: 0, ticks: 7));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(1).SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_skips_TxCommit_originating_from_peer()
    {
        // Cycle-break runs BEFORE the TxCommit/TxAbort bypass: a
        // terminal whose origin matches the peer's cluster id must
        // still be filtered out. Otherwise the peer would receive its
        // own terminal back, the receiver would no-op via the registry
        // repeat-same-outcome guard, but the wasted send would be
        // observable as redundant network traffic.
        var (grain, _, feed, transport, _, _, _) = Create();
        feed.Append(MakeTerminalEntry(MutationKind.TxCommit, shardIndex: 0, origin: Peer, ticks: 5));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_skips_TxAbort_originating_from_peer()
    {
        var (grain, _, feed, transport, _, _, _) = Create();
        feed.Append(MakeTerminalEntry(MutationKind.TxAbort, shardIndex: 2, origin: Peer, ticks: 5));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_skips_TxCommit_with_empty_origin()
    {
        // Empty-origin guard runs BEFORE the TxCommit/TxAbort bypass:
        // a terminal authored by a path that did not stamp an origin
        // would surface as ArgumentException on the receiver's
        // OriginClusterId-required check. Drop it at the producer.
        var (grain, _, feed, transport, _, _, _) = Create();
        feed.Append(MakeTerminalEntry(MutationKind.TxCommit, shardIndex: 0, origin: string.Empty, ticks: 5));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }
}