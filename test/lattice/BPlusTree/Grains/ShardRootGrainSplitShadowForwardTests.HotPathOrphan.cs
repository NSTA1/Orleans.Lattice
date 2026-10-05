using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detector for the <b>hot-path</b> half of the TLA+ action
/// <c>ShadowForwardOrphan(t,k)</c> (<c>spec/atomic-commit/AtomicCommit.tla</c>, mapped in
/// <c>spec/atomic-commit/Refinement.md</c>).
/// <para>
/// The row maps that action onto <b>two</b> production paths that both land a
/// prepared write on a destination leaf which has already applied the saga's
/// terminal, re-installing a pending bucket there:
/// </para>
/// <list type="number">
/// <item><description>the retroactive sweep
/// <c>TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync</c>, detected
/// by <c>TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight</c>;
/// </description></item>
/// <item><description>the <b>hot-path shadow-forward on an active split</b> -
/// <c>ShardRootGrain.SetAsync</c> into
/// <c>ShardRootGrain.ForwardLocalWriteToShadowIfNeededAsync</c>'s prepared
/// branch - which is what this file detects.</description></item>
/// </list>
/// <para>
/// The two paths cover different windows (the sweep is retroactive, the hot
/// path is concurrent with the split), so the sweep's detector cannot stand in
/// for this one.
/// </para>
/// <para>
/// <b>What was already covered, and what was not.</b> The hot path is not
/// uncovered as a <em>forwarding mechanism</em>: the 22 tests in
/// <c>ShardRootGrainSplitShadowForwardTests.cs</c> (the PR #197 Failure 2
/// regression suite) assert it thoroughly. But they resolve the split
/// destination to an <c>NSubstitute</c> mock of <see cref="IShardRootGrain"/>,
/// so they observe only that the source <em>emitted</em> a
/// <c>target.SetAsync</c> call, and none of them sequences a terminal onto the
/// destination before driving the forward. Symmetrically, the orphan
/// <em>condition</em> is well covered at the leaf
/// (<c>BPlusLeafGrainTests.OrphanBucketDiscard</c> and siblings), but every one
/// of those plants the orphan bucket by hand rather than producing it through a
/// forward.
/// </para>
/// <para>
/// The gap this file closes is the seam between the two: the hot-path
/// shadow-forward <b>arriving after the destination has already applied the
/// terminal</b>, and thereby producing the orphan the row models. The ordering
/// is the whole assertion, which is why the harness composes a real destination
/// <see cref="ShardRootGrain"/> over a real <see cref="BPlusLeafGrain"/> and
/// applies the terminal first. The postcondition asserted is the concrete
/// counterpart of the spec's <c>pend' = [pend EXCEPT ![t][k] = "pending"]</c>.
/// </para>
/// <para>
/// That seam is what makes this test non-redundant, demonstrated by two
/// perturbations pulling in opposite directions. Suppressing the orphan's
/// <em>production</em> at the destination leaf leaves all 22 sibling tests green
/// and turns this one red; removing the hot-path forward leaves the leaf orphan
/// fixtures green and turns this one red. No other single test goes red for
/// both.
/// </para>
/// <para>
/// The complementary <c>OrphanDrain</c> half - the duplicate terminal
/// discarding that bucket without surfacing its prepare-time value - remains
/// detected by <c>BPlusLeafGrainTests.OrphanBucketDiscard</c>.
/// </para>
/// <para>
/// Since issue #4385 a destination leaf whose current activation applied the
/// terminal refuses the late prepare rather than bucketing it, and since issue
/// #4445 a forwarded prepare the leaf has no memory of is also refused when the
/// registry reports the saga decided. The detector therefore stages the one
/// remaining realisable case - a destination leaf whose terminal predates its
/// activation and whose registry does not report the decision (pruned past
/// retention, or unreachable) - and sibling tests pin both refusals.
/// </para>
/// </summary>
public partial class ShardRootGrainSplitShadowForwardTests
{
    private const string DestinationLeafKey = "dest-leaf";

    /// <summary>
    /// Two-shard harness: a source shard root with an in-flight split whose
    /// destination is a <b>real</b> shard root over a <b>real</b> leaf, so the
    /// end state of the hot-path shadow-forward is observable on the
    /// destination rather than only as a call on a mock.
    /// </summary>
    private sealed class HotPathOrphanHarness
    {
        public required ShardRootGrain Source { get; init; }
        public required BPlusLeafGrain DestinationLeaf { get; set; }
        public required FakePersistentState<ShardRootState> SourceState { get; init; }

        /// <summary>
        /// The destination tree's saga registry. Answers
        /// <see cref="TxStatus.InFlight"/> unless a test reconfigures it.
        /// </summary>
        public required ITxRegistryGrain DestinationRegistry { get; init; }

        /// <summary>
        /// Replaces the destination leaf with a fresh activation over the same
        /// persisted state. It loses the per-activation memory of which saga
        /// terminals it has applied; this harness runs no replay, so it also
        /// starts with no rows.
        /// </summary>
        public required Func<BPlusLeafGrain> ReactivateDestinationLeaf { get; init; }
    }

    private static HotPathOrphanHarness CreateHotPathOrphanHarness(ShardSplitPhase phase)
    {
        // ---------- destination shard: real root grain over a real leaf ----------
        var destinationFactory = Substitute.For<IGrainFactory>();
        var destinationResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            shardCount: 2,
            factory: destinationFactory);

        var destinationLeafContext = Substitute.For<IGrainContext>();
        destinationLeafContext.GrainId.Returns(GrainId.Create("leaf", DestinationLeafKey));
        var destinationLeafState = new FakePersistentState<LeafNodeState>();
        destinationLeafState.State.TreeId = TreeId;
        BPlusLeafGrain NewDestinationLeaf() => new(
            destinationLeafContext,
            destinationLeafState,
            destinationFactory,
            destinationResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        var destinationLeaf = NewDestinationLeaf();

        destinationFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(destinationLeaf);
        destinationFactory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(Substitute.For<ILeafCacheGrain>());
        var destinationRegistry = Substitute.For<ITxRegistryGrain>();
        destinationRegistry.GetStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(TxStatus.InFlight));
        destinationRegistry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(TxStatus.InFlight));
        destinationFactory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(destinationRegistry);

        var destinationRootContext = Substitute.For<IGrainContext>();
        destinationRootContext.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{TargetShardIndex}"));
        var destinationRootState = new FakePersistentState<ShardRootState>();
        destinationRootState.State.RootNodeId = GrainId.Create("leaf", DestinationLeafKey);
        destinationRootState.State.RootIsLeaf = true;

        var destinationRoot = new ShardRootGrain(
            destinationRootContext,
            destinationRootState,
            destinationFactory,
            destinationResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        // ---------- source shard: real root grain mid-split ----------
        var sourceFactory = Substitute.For<IGrainFactory>();
        var sourceResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            shardCount: 2,
            factory: sourceFactory);

        var sourceContext = Substitute.For<IGrainContext>();
        sourceContext.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{SourceShardIndex}"));
        var sourceState = new FakePersistentState<ShardRootState>();
        sourceState.State.RootNodeId = GrainId.Create("leaf", "source-leaf");
        sourceState.State.RootIsLeaf = true;
        sourceState.State.SplitInProgress = NewSplit(phase);

        // The source leaf stays a stub: this fixture is about what reaches the
        // destination, and a prepared write leaves the source's visible
        // Entries untouched by construction.
        var sourceLeaf = Substitute.For<IBPlusLeafGrain>();
        sourceLeaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(null));
        sourceLeaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        StubLocalPrepareLookup(sourceLeaf);
        sourceFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(sourceLeaf);
        sourceFactory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(Substitute.For<ILeafCacheGrain>());

        // The split destination resolves to the REAL destination shard root.
        sourceFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(destinationRoot);

        var source = new ShardRootGrain(
            sourceContext,
            sourceState,
            sourceFactory,
            sourceResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new HotPathOrphanHarness
        {
            Source = source,
            DestinationLeaf = destinationLeaf,
            SourceState = sourceState,
            DestinationRegistry = destinationRegistry,
            ReactivateDestinationLeaf = () =>
            {
                var reactivated = NewDestinationLeaf();
                destinationFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(reactivated);
                return reactivated;
            },
        };
    }

    private static async Task PreparedShardSetAsync(
        ShardRootGrain shard, Guid txid, string key, byte[] value)
    {
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                await shard.SetAsync(key, value);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }
    }

    [TearDown]
    public void ClearHotPathOrphanAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeRegistrySnapshotContext.Current = null;
    }

    [Test]
    public async Task Hot_path_shadow_forward_installs_orphan_pending_bucket_on_destination_leaf_whose_terminal_predates_its_activation()
    {
        // This is the refinement detector for ShadowForwardOrphan(t,k)'s
        // hot-path production realisation. Every conjunct of the spec action
        // has a concrete counterpart below.
        //
        // Since issue #4385 a destination leaf that remembers the saga's
        // terminal refuses the late prepare (see the next test), and since
        // issue #4445 one that has forgotten it asks the registry. So the
        // action is realised only where the leaf's current activation does not
        // know the terminal (it landed before a reactivation) AND the registry
        // does not report the decision - here it answers InFlight, standing in
        // for a row pruned past retention.
        var h = CreateHotPathOrphanHarness(ShardSplitPhase.Swap);
        var txid = Guid.NewGuid();

        // Spec precondition: terminal[t][k] # "none" and pend[t][k] = "none".
        // Concretely - the saga's terminal has ALREADY reached the destination
        // leaf (typically via the saga's per-key backstop fan-out, which writes
        // Entries and records the txid in _recentlyTerminal without ever seeing
        // a bucket), and the destination holds no pending bucket for it.
        await h.DestinationLeaf.SetAsync("k", [99]);
        await h.DestinationLeaf.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);
        Assert.Multiple(() =>
        {
            Assert.That(h.DestinationLeaf.RecentlyTerminalCount, Is.EqualTo(1),
                "Setup: the destination leaf must have applied the saga's terminal before the forward runs.");
            Assert.That(h.DestinationLeaf.PendingTransactionCount, Is.Zero,
                "Setup: the destination must hold no pending bucket for the saga (spec pend[t][k] = \"none\").");
        });

        // The leaf reactivates and no longer remembers the terminal.
        h.DestinationLeaf = h.ReactivateDestinationLeaf();
        Assert.That(h.DestinationLeaf.RecentlyTerminalCount, Is.Zero,
            "Setup: the reactivated destination leaf must not remember the terminal.");

        // The action: a prepared write for the same saga arrives at the SOURCE
        // shard while its split is active, and the hot-path shadow-forward
        // carries it to the destination.
        await PreparedShardSetAsync(h.Source, txid, "k", [11]);

        // Spec postcondition: pend'[t][k] = "pending" - a pending bucket is
        // (re-)installed on the destination leaf. This is the discriminator.
        // If the hot-path prepared branch stops forwarding, or reverts to
        // MergeManyAsync (which lands the value straight in the destination's
        // visible Entries), no bucket appears and this reads 0.
        Assert.That(h.DestinationLeaf.PendingTransactionCount, Is.EqualTo(1),
            "The hot-path shadow-forward of a prepared write must install a pending bucket on the destination leaf.");

        // UNCHANGED terminal projection: the late bucket must not be published
        // into the destination's visible Entries. A forward that published [11]
        // would be the pre-fix MergeManyAsync behaviour, and the mid-saga
        // atomic-visibility violation of #1584. (The reactivated leaf runs no
        // replay here, so it holds no row for the key at all.)
        Assert.That(h.DestinationLeaf.EntriesForTest.ContainsKey("k"), Is.False,
            "The forwarded prepared value must be bucketed, never published into the destination's visible Entries.");
    }

    [Test]
    public async Task Hot_path_shadow_forward_after_a_destination_reactivation_installs_no_orphan_when_the_registry_reports_the_saga_decided()
    {
        // Issue #4445. The detector above with the registry reporting the
        // saga's decision. The reactivated destination leaf has forgotten the
        // terminal, so its per-activation memory cannot refuse the forward; the
        // shadow-forward marks it as forwarded, and the leaf refuses it on the
        // registry's answer instead. Without that, the bucket's prepared value
        // [11] would be served over the newer row as the committed value.
        var h = CreateHotPathOrphanHarness(ShardSplitPhase.Swap);
        h.DestinationRegistry.GetStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(TxStatus.Committed));
        var txid = Guid.NewGuid();

        await h.DestinationLeaf.SetAsync("k", [99]);
        await h.DestinationLeaf.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);
        h.DestinationLeaf = h.ReactivateDestinationLeaf();
        await h.DestinationLeaf.SetAsync("k", [22]);

        await PreparedShardSetAsync(h.Source, txid, "k", [11]);
        var read = await h.DestinationLeaf.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(h.DestinationLeaf.PendingTransactionCount, Is.Zero,
                "a forwarded prepare for a decided saga must not leave an orphan on a reactivated destination");
            Assert.That(read, Is.EqualTo(new byte[] { 22 }),
                "the committed saga's stale prepared value must not be served over the newer row");
        });
        await h.DestinationRegistry.Received().GetStatusAsync(txid);
    }

    [Test]
    public async Task Hot_path_shadow_forward_trailing_the_terminal_installs_no_orphan_on_a_destination_leaf_that_remembers_it()
    {
        // Issue #4385. The same forward as the detector above, onto a
        // destination leaf whose current activation applied the saga's
        // terminal. The leaf refuses the prepare instead of bucketing it:
        // nothing would ever drain that bucket, and once the leaf forgets the
        // terminal (a reactivation replaying the logged prepare, or a split
        // that strands the key outside its span) the orphan surfaces as the
        // committed value - a stale round, a duplicate key in a count or scan.
        var h = CreateHotPathOrphanHarness(ShardSplitPhase.Swap);
        var txid = Guid.NewGuid();

        await h.DestinationLeaf.SetAsync("k", [99]);
        await h.DestinationLeaf.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        await PreparedShardSetAsync(h.Source, txid, "k", [11]);
        var pendingKeys = await h.DestinationLeaf.GetPendingKeysAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.DestinationLeaf.PendingTransactionCount, Is.Zero,
                "A prepare that trails its saga's terminal must not leave an orphan pending bucket on the destination leaf.");
            Assert.That(pendingKeys, Is.Empty,
                "The refused prepare must not be visible as a pending key.");
            Assert.That(h.DestinationLeaf.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 99 }),
                "The refused prepare must not touch the destination's visible Entries.");
            Assert.That(h.DestinationLeaf.RecentlyTerminalCount, Is.EqualTo(1),
                "The forward must not disturb the terminal the destination already applied.");
        });
    }

    [Test]
    public async Task Hot_path_shadow_forward_of_a_prepared_write_buckets_rather_than_publishes_on_the_destination_leaf()
    {
        // Control for the test above, and the pre-orphan window of the same
        // path: the saga's terminal has NOT yet reached the destination. It is
        // deliberately the same composition minus the ordering, so that a
        // perturbation which suppresses orphan PRODUCTION specifically (rather
        // than the forward as a whole) turns the orphan test red and leaves
        // this one green - which is what shows the ordering is the assertion.
        //
        // It also carries the end-to-end claim the mock-based sibling tests
        // cannot: the prepared write must arrive at the real destination leaf
        // as a bucket and stay invisible, so the saga's later terminal is what
        // makes it visible, on both shards at once.
        var h = CreateHotPathOrphanHarness(ShardSplitPhase.BeginShadowWrite);
        var txid = Guid.NewGuid();

        await h.DestinationLeaf.SetAsync("k", [7]);

        await PreparedShardSetAsync(h.Source, txid, "k", [42]);

        Assert.Multiple(() =>
        {
            Assert.That(h.DestinationLeaf.PendingTransactionCount, Is.EqualTo(1),
                "The prepared write must land in the destination leaf's pending bucket for the saga.");
            Assert.That(h.DestinationLeaf.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 7 }),
                "An undecided prepared write must not be visible in the destination's Entries.");
            Assert.That(h.DestinationLeaf.RecentlyTerminalCount, Is.Zero,
                "No terminal has been delivered, so the destination must record none.");
        });

        // And the saga's terminal - the concrete BroadcastStep(t,k) - is what
        // makes it visible, drained atomically with the source's own copy.
        await h.DestinationLeaf.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);
        Assert.Multiple(() =>
        {
            Assert.That(h.DestinationLeaf.PendingTransactionCount, Is.Zero,
                "The terminal must drain the bucket the hot-path forward installed.");
            Assert.That(h.DestinationLeaf.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 42 }),
                "The terminal must publish the forwarded prepared value on the destination.");
        });
    }
}
