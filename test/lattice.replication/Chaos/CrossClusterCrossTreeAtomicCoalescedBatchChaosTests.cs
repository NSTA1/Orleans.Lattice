using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Cross-cluster, cross-tree atomic-visibility chaos coverage over the
/// <b>coalescing batch-delivery path</b>
/// (<see cref="ReplicationApplier.ApplyBatchAsync"/>).
/// <para>
/// <b>The gap this fills.</b> Every other chaos test delivers replicated
/// entries to the receiver one at a time through
/// <c>ReplicationApplier.ApplyAsync</c> (the in-process
/// <see cref="ChaosDeliveryPump"/> applies each WAL entry individually). But
/// the production shipper <em>coalesces</em> a saga's contiguous WAL entries -
/// its prepared writes <em>and</em> its <c>TxCommit</c> / <c>TxAbort</c>
/// terminal - into a single inbound <c>ReplicationBatch</c> that the receiver
/// applies through <see cref="ReplicationApplier.ApplyBatchAsync"/>. That
/// multi-entry apply path had no terminal case, so a terminal coalesced behind
/// its saga's prepared entries fell through to the point-apply switch, faulted
/// the whole batch, and was never applied - the cross-tree receiver barrier
/// never released and the saga stayed invisible on the peer forever
/// (issue #1525). Because no chaos test drove <c>ApplyBatchAsync</c> with a
/// coalesced terminal, none could observe the defect. These tests drive that
/// exact seam: they collect an authoring site's per-tree WAL backlog and hand
/// each tree's whole run to the receiver's <c>ApplyBatchAsync</c> as one
/// multi-entry batch, so every saga's terminal is coalesced behind its prepared
/// writes.
/// </para>
/// <para>
/// <b>What they prove.</b> Two clusters replicate two trees. Site 0 authors
/// cross-tree
/// <see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAsync(IGrainFactory, System.Collections.Generic.IReadOnlyList{LatticeTreeBatch}, string, System.Threading.CancellationToken)"/>
/// sagas spanning both trees. The receiver barrier must hold every
/// participating tree invisible until all of a saga's replicated terminals have
/// arrived, then flip them together. The invariant asserted on the receiver is
/// the cross-tree all-or-nothing rule: for every saga, tree A's keys and tree
/// B's keys have <em>identical</em> presence - both fully visible or both fully
/// absent - and every saga ultimately becomes fully visible on both trees. A
/// dropped coalesced terminal (#1525) manifests as a saga that never converges;
/// a torn barrier manifests as a stable partial cross-tree view.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class CrossClusterCrossTreeAtomicCoalescedBatchChaosTests
{
    private const string TreeA = "chaos-xtb-a";
    private const string TreeB = "chaos-xtb-b";
    private const int SiteCount = 2;
    private const int SagaCount = 6;
    private const int KeysPerSaga = 3;
    private const int Author = 0;
    private const int Receiver = 1;
    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The direct #1525 reproduction: author every cross-tree saga on site 0,
    /// then deliver each tree's <em>entire</em> WAL backlog to the receiver as a
    /// single coalesced <see cref="ReplicationApplier.ApplyBatchAsync"/> call.
    /// Every saga's prepared writes and its terminal ride in one multi-entry
    /// batch - the exact shape that dropped the terminal - and the receiver must
    /// still converge on all sagas atomically across both trees.
    /// </summary>
    [Test]
    public async Task Cross_tree_atomic_sagas_converge_when_terminals_are_coalesced_in_a_batch()
    {
        await using var runner = await AtomicBatchRunner.StartAsync();
        var (keysA, keysB) = BuildKeyNamespaces();

        await AuthorSagasAsync(runner.Fixture, keysA, keysB, "op-conv");
        await AssertAuthorSiteFullyVisibleAsync(runner.Fixture, keysA, keysB);

        // Coalesce each tree's whole backlog into one multi-entry batch so
        // every saga terminal is delivered behind its prepared writes.
        await runner.DeliverTreeBacklogAsBatchAsync(TreeA);
        await runner.DeliverTreeBacklogAsBatchAsync(TreeB);

        await AssertPeerConvergesAtomicallyAsync(runner.Fixture, keysA, keysB, ConvergenceTimeout);
    }

    /// <summary>
    /// The barrier proof: deliver only tree A's coalesced backlog batch (which
    /// carries tree A's saga terminals) and assert the receiver keeps every saga
    /// invisible on <em>both</em> trees, because tree B's terminals have not
    /// arrived. Then deliver tree B's coalesced batch and assert every saga
    /// flips fully visible on both trees together. This exercises the same
    /// coalescing batch path as the convergence test while proving the
    /// cross-tree barrier holds and releases correctly over it.
    /// </summary>
    [Test]
    public async Task Cross_tree_atomic_saga_batch_keeps_barrier_closed_until_second_tree_terminal_arrives()
    {
        await using var runner = await AtomicBatchRunner.StartAsync();
        var (keysA, keysB) = BuildKeyNamespaces();

        await AuthorSagasAsync(runner.Fixture, keysA, keysB, "op-barrier");
        await AssertAuthorSiteFullyVisibleAsync(runner.Fixture, keysA, keysB);

        // Only tree A's coalesced batch (tree A terminals present, tree B
        // terminals absent): the barrier must hold both trees invisible.
        await runner.DeliverTreeBacklogAsBatchAsync(TreeA);

        var premature = await FindStableCrossTreeViolationAsync(runner.Fixture, keysA, keysB);
        Assert.That(premature, Is.Null,
            "Receiver revealed a cross-tree saga before every participating tree's terminal " +
            $"was applied - the barrier leaked over the coalescing batch path: {premature}");
        Assert.That(await AnySagaFullyVisibleAsync(runner.Fixture, keysA, keysB), Is.False,
            "No saga may be visible after only tree A's terminals have been delivered.");

        // Tree B's coalesced batch releases the barrier: both trees flip together.
        await runner.DeliverTreeBacklogAsBatchAsync(TreeB);

        await AssertPeerConvergesAtomicallyAsync(runner.Fixture, keysA, keysB, ConvergenceTimeout);
    }

    private static async Task AuthorSagasAsync(
        MultiSiteClusterFixture fixture, string[][] keysA, string[][] keysB, string opPrefix)
    {
        var author = fixture.ClientOf(Author);
        for (var n = 0; n < SagaCount; n++)
        {
            var outcome = await author.SetManyAtomicAsync(BuildSaga(keysA[n], keysB[n], n), $"{opPrefix}-{n:D2}");
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed),
                $"Saga {n} must commit locally on the author site.");
        }
    }

    private static (string[][] KeysA, string[][] KeysB) BuildKeyNamespaces()
    {
        var keysA = new string[SagaCount][];
        var keysB = new string[SagaCount][];
        for (var n = 0; n < SagaCount; n++)
        {
            var a = new string[KeysPerSaga];
            var b = new string[KeysPerSaga];
            for (var k = 0; k < KeysPerSaga; k++)
            {
                a[k] = $"xt{n:D2}-a{k}";
                b[k] = $"xt{n:D2}-b{k}";
            }
            keysA[n] = a;
            keysB[n] = b;
        }
        return (keysA, keysB);
    }

    private static IReadOnlyList<LatticeTreeBatch> BuildSaga(string[] aKeys, string[] bKeys, int n)
    {
        var entriesA = new List<KeyValuePair<string, byte[]>>(KeysPerSaga);
        var entriesB = new List<KeyValuePair<string, byte[]>>(KeysPerSaga);
        for (var k = 0; k < KeysPerSaga; k++)
        {
            entriesA.Add(new KeyValuePair<string, byte[]>(aKeys[k], Encoding.UTF8.GetBytes($"a-{n}-{k}")));
            entriesB.Add(new KeyValuePair<string, byte[]>(bKeys[k], Encoding.UTF8.GetBytes($"b-{n}-{k}")));
        }
        return new List<LatticeTreeBatch>
        {
            new(TreeA, entriesA),
            new(TreeB, entriesB),
        };
    }

    private static async Task AssertAuthorSiteFullyVisibleAsync(
        MultiSiteClusterFixture fixture, string[][] keysA, string[][] keysB)
    {
        var treeA = fixture.ClientOf(Author).GetGrain<ILattice>(TreeA);
        var treeB = fixture.ClientOf(Author).GetGrain<ILattice>(TreeB);
        for (var n = 0; n < SagaCount; n++)
        {
            Assert.That(await CountPresentAsync(treeA, keysA[n]), Is.EqualTo(KeysPerSaga),
                $"Author site missing tree-A keys for saga {n}.");
            Assert.That(await CountPresentAsync(treeB, keysB[n]), Is.EqualTo(KeysPerSaga),
                $"Author site missing tree-B keys for saga {n}.");
        }
    }

    private static async Task AssertPeerConvergesAtomicallyAsync(
        MultiSiteClusterFixture fixture, string[][] keysA, string[][] keysB, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var violation = await FindStableCrossTreeViolationAsync(fixture, keysA, keysB);
            Assert.That(violation, Is.Null, $"Cross-tree barrier violation on the receiver: {violation}");

            if (await AllSagasFullyVisibleAsync(fixture, keysA, keysB))
            {
                return;
            }
            await Task.Delay(50);
        }

        var treeA = fixture.ClientOf(Receiver).GetGrain<ILattice>(TreeA);
        var treeB = fixture.ClientOf(Receiver).GetGrain<ILattice>(TreeB);
        var sb = new StringBuilder();
        for (var n = 0; n < SagaCount; n++)
        {
            sb.Append($" saga{n}:A={await CountPresentAsync(treeA, keysA[n])}/B={await CountPresentAsync(treeB, keysB[n])}");
        }
        Assert.Fail(
            $"Receiver did not converge on all {SagaCount} cross-tree sagas within {timeout.TotalSeconds}s - " +
            "coalesced saga terminals were never applied (regression of #1525). Receiver state:" + sb);
    }

    private static async Task<bool> AllSagasFullyVisibleAsync(
        MultiSiteClusterFixture fixture, string[][] keysA, string[][] keysB)
    {
        var treeA = fixture.ClientOf(Receiver).GetGrain<ILattice>(TreeA);
        var treeB = fixture.ClientOf(Receiver).GetGrain<ILattice>(TreeB);
        for (var n = 0; n < SagaCount; n++)
        {
            if (await CountPresentAsync(treeA, keysA[n]) != KeysPerSaga) return false;
            if (await CountPresentAsync(treeB, keysB[n]) != KeysPerSaga) return false;
        }
        return true;
    }

    private static async Task<bool> AnySagaFullyVisibleAsync(
        MultiSiteClusterFixture fixture, string[][] keysA, string[][] keysB)
    {
        var treeA = fixture.ClientOf(Receiver).GetGrain<ILattice>(TreeA);
        var treeB = fixture.ClientOf(Receiver).GetGrain<ILattice>(TreeB);
        for (var n = 0; n < SagaCount; n++)
        {
            if (await CountPresentAsync(treeA, keysA[n]) == KeysPerSaga) return true;
            if (await CountPresentAsync(treeB, keysB[n]) == KeysPerSaga) return true;
        }
        return false;
    }

    /// <summary>
    /// Returns a description of a <em>stable</em> cross-tree atomicity violation
    /// on the receiver, or <see langword="null"/> if none. A single observation
    /// of a torn state can be a benign read skew - the barrier flips two distinct
    /// tree grains and a reader can catch the window between them - so a candidate
    /// violation is re-read several times and only reported if it persists, which
    /// a real barrier defect does and read skew does not. Torn means a tree's
    /// keys are partially present, or one tree's saga is fully visible while the
    /// sibling tree's is not.
    /// </summary>
    private static async Task<string?> FindStableCrossTreeViolationAsync(
        MultiSiteClusterFixture fixture, string[][] keysA, string[][] keysB)
    {
        var treeA = fixture.ClientOf(Receiver).GetGrain<ILattice>(TreeA);
        var treeB = fixture.ClientOf(Receiver).GetGrain<ILattice>(TreeB);

        for (var n = 0; n < SagaCount; n++)
        {
            const int confirmReads = 5;
            string? lastDetail = null;
            var stable = true;
            for (var attempt = 0; attempt < confirmReads; attempt++)
            {
                var presentA = await CountPresentAsync(treeA, keysA[n]);
                var presentB = await CountPresentAsync(treeB, keysB[n]);

                var wholeA = presentA is 0 or KeysPerSaga;
                var wholeB = presentB is 0 or KeysPerSaga;
                var aVisible = presentA == KeysPerSaga;
                var bVisible = presentB == KeysPerSaga;

                if (wholeA && wholeB && aVisible == bVisible)
                {
                    stable = false;
                    break;
                }

                lastDetail =
                    $"saga {n}: A={presentA}/{KeysPerSaga}, B={presentB}/{KeysPerSaga} " +
                    $"(wholeA={wholeA}, wholeB={wholeB}, aVisible={aVisible}, bVisible={bVisible})";
                await Task.Delay(20);
            }

            if (stable && lastDetail is not null)
            {
                return lastDetail;
            }
        }

        return null;
    }

    private static async Task<int> CountPresentAsync(ILattice lattice, string[] keys)
    {
        var present = 0;
        foreach (var key in keys)
        {
            if (await lattice.GetAsync(key) is not null)
            {
                present++;
            }
        }
        return present;
    }

    /// <summary>
    /// Owns the two-site, two-tree cluster fixture and delivers an authoring
    /// site's per-tree WAL backlog to the receiver as one coalesced
    /// <see cref="ReplicationApplier.ApplyBatchAsync"/> call - the production
    /// coalescing seam that dropped a saga terminal in #1525.
    /// </summary>
    private sealed class AtomicBatchRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(
            LatticeMergeMode.LwwRegister,
            SiteCount,
            configureClient: static o => o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                [TreeA] = LatticeMergeMode.LwwRegister,
                [TreeB] = LatticeMergeMode.LwwRegister,
            });

        public static async Task<AtomicBatchRunner> StartAsync()
        {
            var runner = new AtomicBatchRunner();
            await runner.Fixture.InitializeAsync();
            return runner;
        }

        /// <summary>
        /// Reads the author site's entire WAL backlog for <paramref name="tree"/>
        /// from its change feed and applies the whole run to the receiver in a
        /// single <see cref="ReplicationApplier.ApplyBatchAsync"/> call, so each
        /// saga's terminal is coalesced behind its prepared writes.
        /// </summary>
        public async Task DeliverTreeBacklogAsBatchAsync(string tree)
        {
            var feed = Fixture.ChangeFeedOf(Author);
            var applier = Fixture.ApplierOf(Receiver);
            var receiverClusterId = MultiSiteClusterFixture.ClusterIdFor(Receiver);

            var batch = new List<WalRecord>();
            await foreach (var entry in feed.Subscribe(tree, HybridLogicalClock.Zero, includeLocalOrigin: true))
            {
                // Never forward an entry back to the cluster that authored it
                // (defence-in-depth; the author here is always site 0).
                if (string.Equals(entry.OriginClusterId, receiverClusterId, StringComparison.Ordinal))
                {
                    continue;
                }
                batch.Add(entry);
            }

            if (batch.Count > 0)
            {
                await applier.ApplyBatchAsync(batch);
            }
        }

        public async ValueTask DisposeAsync() => await Fixture.DisposeAsync();
    }
}
