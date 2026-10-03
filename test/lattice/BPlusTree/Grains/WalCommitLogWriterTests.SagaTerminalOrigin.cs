using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Saga terminal records carry the local origin onto the WAL (issue #2324).
/// <para>
/// The replication shipper drops an empty-origin WAL entry and the change feed
/// keeps one, so a saga terminal written with an empty origin would reach a
/// bootstrap consumer but never a peer - the dropped-terminal input to the
/// stranded-prepare hazard. What keeps that from happening is not either drain
/// but the writer: <c>ShardRootGrain</c> stamps a terminal with the ambient
/// <c>LatticeOriginContext</c>, which is empty for a locally-authored saga, and
/// the writer then fills the origin from the resolver exactly as it does for a
/// foreground write. These tests pin that for the terminal kinds and for both
/// append paths, because the saga coordinator dispatches its terminals through
/// the batched one.
/// </para>
/// </summary>
public partial class WalCommitLogWriterTests
{
    private static WalRecord MakeSagaTerminal(MutationKind kind) => new()
    {
        TreeId = TreeId,
        Op = kind,
        Key = "0",
        ShardIndex = 0,
        TransactionId = Guid.NewGuid(),
        AtomicShardCount = 2,
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = null,
    };

    [TestCase(MutationKind.TxCommit)]
    [TestCase(MutationKind.TxAbort)]
    public async Task AppendAsync_stamps_the_local_origin_on_a_saga_terminal(MutationKind kind)
    {
        var (writer, captured) = CreateWriter(clusterId: "site-test");

        await writer.AppendAsync(MakeSagaTerminal(kind));

        Assert.That(captured, Has.Count.EqualTo(1));
        Assert.That(captured[0].OriginClusterId, Is.EqualTo("site-test"),
            "a locally-authored saga terminal must leave the writer with the local origin, or the shipper drops it");
    }

    [TestCase(MutationKind.TxCommit)]
    [TestCase(MutationKind.TxAbort)]
    public async Task AppendManyAsync_stamps_the_local_origin_on_every_saga_terminal(MutationKind kind)
    {
        var (writer, captured) = CreateWriter(clusterId: "site-test");

        await writer.AppendManyAsync(new[] { MakeSagaTerminal(kind), MakeSagaTerminal(kind) with { Key = "1", ShardIndex = 1 } });

        Assert.That(captured, Has.Count.EqualTo(2));
        Assert.That(captured.Select(r => r.OriginClusterId), Is.All.EqualTo("site-test"),
            "the saga coordinator dispatches its terminals through the batched path, so it must stamp too");
    }
}
