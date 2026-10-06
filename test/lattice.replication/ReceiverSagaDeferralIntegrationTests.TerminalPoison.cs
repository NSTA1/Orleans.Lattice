using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4692: a saga terminal that fails deterministically must not defer for
/// ever. Cross-tree wait-set drift - the receiver's replicated trees changing
/// between two terminals of one operation - no longer fails at all: the barrier's
/// frozen wait set is authoritative. Every other terminal that keeps failing gets
/// the bound a prepare has: past <c>SagaDeferralTimeout</c> its saga is poisoned,
/// counted, and a re-seed is started, while the terminal itself stays withheld
/// rather than parked. Each cause below is real - a configuration change, a
/// per-tree cluster id, a recorded decision, a malformed record - driven through
/// the real dead-letter-tracking applier over the real canonical applier.
/// </summary>
public partial class ReceiverSagaDeferralIntegrationTests
{
    /// <summary>Trees the applier treats as not replicated here; see <see cref="AllowAllLwwRegisterResolver"/>.</summary>
    private static readonly ConcurrentDictionary<string, bool> NotReplicatedHere = new(StringComparer.Ordinal);

    /// <summary>A tree whose per-tree <c>ClusterId</c> differs from every other tree's here.</summary>
    private const string OtherClusterTree = "rsd-xc-other-cluster";

    [TearDown]
    public void RestoreReplicatedTrees() => NotReplicatedHere.Clear();

    private static WalRecord CrossTreePrepare(string tree, string key, byte value, Guid txid, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new[] { value },
        Timestamp = Hlc(ticks),
        OriginClusterId = Origin,
        TransactionId = txid,
        IsPrepared = true,
        AtomicBatchSize = 1,
        AtomicBatchIndex = 0,
    };

    private static WalRecord CrossTreeCommit(string tree, string key, Guid txid, long ticks, string operationId, params string[] participants) =>
        Commit(tree, key, txid, ticks) with
        {
            AtomicShardCount = 1,
            CrossTreeOperationId = operationId,
            CrossTreeParticipants = participants,
        };

    private static MeterListener ListenForPoison(string tree, ConcurrentBag<string> outcomes) =>
        MeterListening.StartForInstrument(LatticeReplicationMetrics.ReceiverSagaPoisoned, listener =>
            listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string? treeTag = null;
                string? outcome = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeReplicationMetrics.TagTree) treeTag = tag.Value as string;
                    if (tag.Key == LatticeReplicationMetrics.TagOutcome) outcome = tag.Value as string;
                }

                if (treeTag == tree && outcome is not null)
                {
                    outcomes.Add(outcome);
                }
            }));

    private Task<byte[]?> ReadAsync(string tree, string key) => _cluster.Client.GetGrain<ILattice>(tree).GetAsync(key);

    [Test]
    public async Task A_cross_tree_terminal_whose_wait_set_shrank_mid_operation_settles_the_saga()
    {
        const string treeA = "rsd-xc-shrink-a";
        const string treeB = "rsd-xc-shrink-b";
        const string operation = "rsd-xc-shrink-op";
        var (txA, txB) = (Guid.NewGuid(), Guid.NewGuid());

        Assert.That(await DeliverAsync(CrossTreePrepare(treeA, "k", 1, txA, 20_000)), Is.True);
        Assert.That(await DeliverAsync(CrossTreePrepare(treeB, "k", 2, txB, 20_000)), Is.True);
        Assert.That(await DeliverAsync(CrossTreeCommit(treeA, "k", txA, 20_100, operation, treeA, treeB)), Is.True,
            "precondition: tree A's terminal freezes the wait set [A, B]");

        // The receiver stops replicating tree A, so tree B's terminal is scoped to [B].
        NotReplicatedHere[treeA] = true;
        var acked = await DeliverAsync(CrossTreeCommit(treeB, "k", txB, 20_101, operation, treeA, treeB));

        Assert.Multiple(async () =>
        {
            Assert.That(acked, Is.True, "a drifted wait set must not fail the terminal on every retry");
            Assert.That(await ReadAsync(treeA, "k"), Is.EqualTo(new byte[] { 1 }), "the frozen barrier completes and finalizes tree A");
            Assert.That(await ReadAsync(treeB, "k"), Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task A_cross_tree_terminal_for_a_tree_that_became_replicated_mid_operation_joins_and_settles_the_saga()
    {
        const string treeA = "rsd-xc-grow-a";
        const string treeB = "rsd-xc-grow-b";
        const string treeC = "rsd-xc-grow-c";
        const string operation = "rsd-xc-grow-op";
        var (txA, txB, txC) = (Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

        // Tree A is not replicated here when tree B's terminal freezes the wait set [B, C].
        NotReplicatedHere[treeA] = true;
        Assert.That(await DeliverAsync(CrossTreePrepare(treeB, "k", 2, txB, 21_000)), Is.True);
        Assert.That(await DeliverAsync(CrossTreePrepare(treeC, "k", 3, txC, 21_000)), Is.True);
        Assert.That(await DeliverAsync(CrossTreeCommit(treeB, "k", txB, 21_100, operation, treeA, treeB, treeC)), Is.True);

        // Then it is, and its sub-saga arrives with the wait set [A, B, C].
        NotReplicatedHere.TryRemove(treeA, out _);
        Assert.That(await DeliverAsync(CrossTreePrepare(treeA, "k", 1, txA, 21_000)), Is.True);
        var ackedA = await DeliverAsync(CrossTreeCommit(treeA, "k", txA, 21_101, operation, treeA, treeB, treeC));
        var midC = await ReadAsync(treeC, "k");
        var ackedC = await DeliverAsync(CrossTreeCommit(treeC, "k", txC, 21_102, operation, treeA, treeB, treeC));

        Assert.Multiple(async () =>
        {
            Assert.That(ackedA, Is.True, "the late tree joins the frozen wait set instead of failing on every retry");
            Assert.That(midC, Is.Null, "joining adds nothing to wait for, but the barrier still waits for tree C");
            Assert.That(ackedC, Is.True);
            Assert.That(await ReadAsync(treeA, "k"), Is.EqualTo(new byte[] { 1 }));
            Assert.That(await ReadAsync(treeB, "k"), Is.EqualTo(new byte[] { 2 }));
            Assert.That(await ReadAsync(treeC, "k"), Is.EqualTo(new byte[] { 3 }));
        });
    }

    [Test]
    public async Task A_cross_tree_terminal_whose_trees_resolve_different_cluster_ids_is_poisoned_after_the_bound_and_withheld()
    {
        const string tree = "rsd-xc-cluster-a";
        const string operation = "rsd-xc-cluster-op";
        var txid = Guid.NewGuid();
        var terminal = CrossTreeCommit(tree, "k", txid, 22_100, operation, tree, OtherClusterTree);

        Assert.That(await DeliverAsync(CrossTreePrepare(tree, "k", 1, txid, 22_000)), Is.True);
        await AssertPoisonedAfterTheBoundAndWithheldAsync(tree, txid, terminal);
    }

    [Test]
    public async Task A_terminal_that_conflicts_with_a_recorded_decision_is_poisoned_after_the_bound_and_withheld()
    {
        const string tree = "rsd-terminal-conflict";
        var (keyA, keyB) = KeysOnDistinctShards("terminal-conflict");
        var txid = Guid.NewGuid();
        Assert.That(await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: 23_000)), Is.True);
        Assert.That(await DeliverAsync(Prepare(tree, keyB, 2, txid, index: 1, ticks: 23_001)), Is.True);

        // The receiver registry already holds the opposite decision.
        await TxRegistryRouting.GetRegistry(_cluster.Client, tree, txid).MarkAbortedAsync(txid);

        await AssertPoisonedAfterTheBoundAndWithheldAsync(tree, txid, Commit(tree, keyA, txid, 23_100));
    }

    [Test]
    public async Task A_malformed_terminal_is_poisoned_after_the_bound_and_withheld()
    {
        const string tree = "rsd-terminal-malformed";
        var txid = Guid.NewGuid();
        Assert.That(await DeliverAsync(CrossTreePrepare(tree, "k", 1, txid, 24_000)), Is.True);

        // Neither a positive ShardIndex nor a numeric Key.
        var malformed = Commit(tree, "k", txid, 24_100) with { ShardIndex = 0, Key = "not-a-shard" };
        await AssertPoisonedAfterTheBoundAndWithheldAsync(tree, txid, malformed);
    }

    [Test]
    public async Task A_terminal_that_keeps_failing_is_poisoned_after_the_bound_and_withheld()
    {
        const string tree = "rsd-terminal-poison-bound";
        var (keyA, keyB) = KeysOnDistinctShards("terminal-bound");
        var txid = Guid.NewGuid();
        var commitB = Commit(tree, keyB, txid, 8_101);

        Assert.That(await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: 8_000)), Is.True);
        Assert.That(await DeliverAsync(Prepare(tree, keyB, 2, txid, index: 1, ticks: 8_001)), Is.True);
        Assert.That(await DeliverAsync(Commit(tree, keyA, txid, 8_100)), Is.True);

        _failing.Fail = r => r.Op == MutationKind.TxCommit && r.ShardIndex == commitB.ShardIndex && r.TransactionId == txid;
        await AssertPoisonedAfterTheBoundAndWithheldAsync(tree, txid, commitB);
    }

    private async Task AssertPoisonedAfterTheBoundAndWithheldAsync(string tree, Guid txid, WalRecord terminal)
    {
        Assert.That(await DeliverAsync(terminal), Is.False, "before the bound the failing terminal is deferred");
        Assert.That(await GetPoisonedAsync(tree), Does.Not.Contain(txid), "precondition: not poisoned before the bound");

        await WaitPastSagaDeferralTimeoutAsync();

        var outcomes = new ConcurrentBag<string>();
        bool afterBound;
        using (ListenForPoison(tree, outcomes))
        {
            afterBound = await PushAsync(terminal);
        }

        var later = await PushAsync(terminal);
        var parked = await _deadLetters.ListAsync(tree);
        Assert.Multiple(async () =>
        {
            Assert.That(await GetPoisonedAsync(tree), Does.Contain(txid),
                "past the bound the saga is poisoned, so a re-seed settles it rather than the terminal wedging the stream for ever");
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.OutcomeReceiverSagaPoisonedTerminalTimeout),
                "the poison is counted as a terminal timeout");
            Assert.That((afterBound, later), Is.EqualTo((false, false)),
                "the terminal is withheld unacknowledged, never dropped: the sender keeps it until the re-seed retires the poison");
            Assert.That(parked.Count(e => e.Entry.TransactionId == txid && e.Entry.Op is MutationKind.TxCommit or MutationKind.TxAbort),
                Is.Zero, "a withheld terminal is not parked");
        });
    }
}
