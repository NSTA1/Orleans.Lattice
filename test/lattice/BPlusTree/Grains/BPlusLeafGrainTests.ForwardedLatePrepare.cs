using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4445. A forwarded saga prepare (a shadow-forward, or a split's
/// retroactive sweep replay) that trails its saga's terminal to a leaf which
/// has <b>reactivated</b> since applying that terminal must still be refused.
/// <para>
/// Issue #4385 made the leaf refuse such a prepare, but only from its
/// per-activation memory of applied terminals. The read gate's orphan guard
/// reads the same memory, so every orphan the leaf still installed was
/// invisible to that guard: after a reactivation the bucket was installed and
/// <c>GetAsync</c> served the committed saga's old prepared value over a newer
/// row. The leaf now asks the registry - the same one the read gate consults -
/// for a forwarded prepare it has no memory of.
/// </para>
/// <para>
/// The reactivation is modelled as a second grain over the same persisted
/// state. This harness runs no WAL replay, so the newer row is written on the
/// new activation, standing in for a replayed row plus a later write.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string ForwardedPrepareLogicalTreeId = "fp-logical";

    /// <summary>
    /// The leaf's own tree id: the physical copy of a resized tree, whose
    /// registry holds no decisions (issue #4368). Sagas record their decisions
    /// under <see cref="ForwardedPrepareLogicalTreeId"/>.
    /// </summary>
    private const string ForwardedPreparePhysicalTreeId = "fp-physical";

    private sealed class ReactivatingLeafHarness
    {
        /// <summary>The logical tree's registry, where decisions are recorded.</summary>
        public required ITxRegistryGrain Registry { get; init; }

        /// <summary>The physical copy's registry, which records nothing.</summary>
        public required ITxRegistryGrain PhysicalRegistry { get; init; }

        public required Func<BPlusLeafGrain> Activate { get; init; }
    }

    /// <summary>
    /// Builds a leaf factory whose activations share one persisted state, over a
    /// logical-tree registry substitute answering <paramref name="status"/> from
    /// the read path and <paramref name="recorded"/> from the recorded-verdict
    /// path. The leaf itself belongs to the physical copy, whose registry answers
    /// <see cref="TxStatus.InFlight"/> to everything.
    /// </summary>
    private static ReactivatingLeafHarness BuildReactivatingLeaf(TxStatus status, TxStatus recorded = TxStatus.InFlight)
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(status));
        registry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(recorded));
        // A live saga's coordinator holds its participant row until it is
        // forgotten (issue #4632); tests of a forgotten saga clear it.
        registry.GetParticipantsAsync(Arg.Any<Guid>()).Returns(Task.FromResult<IReadOnlyList<int>>([0]));
        registry.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(call =>
        {
            var answers = new Dictionary<Guid, TxStatus>();
            foreach (var t in call.Arg<IReadOnlyList<Guid>>()) answers[t] = status;
            return Task.FromResult(answers);
        });

        var physicalRegistry = Substitute.For<ITxRegistryGrain>();
        physicalRegistry.GetStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(TxStatus.InFlight));
        physicalRegistry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(TxStatus.InFlight));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(call =>
            call.ArgAt<string>(0).EndsWith(ForwardedPrepareLogicalTreeId, StringComparison.Ordinal) ? registry : physicalRegistry);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ForwardedPreparePhysicalTreeId;

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "forwarded-prepare-leaf"));
        context.ActivationServices.Returns(new ServiceCollection().BuildServiceProvider());

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        return new ReactivatingLeafHarness
        {
            Registry = registry,
            PhysicalRegistry = physicalRegistry,
            Activate = () => new BPlusLeafGrain(
                context,
                state,
                grainFactory,
                optionsResolver,
                TestMutationObservers.NoObservers(),
                TestOriginClusterIdResolver.Default()),
        };
    }

    private static async Task ForwardedPrepareSetAsync(BPlusLeafGrain grain, Guid txid, string key, byte[] value)
    {
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            using (LatticeForwardedPrepareContext.BeginScope(ForwardedPrepareLogicalTreeId))
            {
                await grain.SetAsync(key, value);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }
    }

    private static async Task ForwardedPrepareSetManyAsync(BPlusLeafGrain grain, Guid txid, string key, byte[] value)
    {
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            using (LatticeForwardedPrepareContext.BeginScope(ForwardedPrepareLogicalTreeId))
            {
                await grain.SetManyAsync([new KeyValuePair<string, byte[]>(key, value)]);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }
    }

    private static async Task ForwardedPrepareDeleteAsync(BPlusLeafGrain grain, Guid txid, string key)
    {
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            using (LatticeForwardedPrepareContext.BeginScope(ForwardedPrepareLogicalTreeId))
            {
                await grain.DeleteAsync(key);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }
    }

    /// <summary>
    /// Applies the saga's terminal on a first activation, then returns a fresh
    /// activation over the same state holding the newer row [22].
    /// </summary>
    private static async Task<BPlusLeafGrain> ReactivateAfterTerminalAsync(ReactivatingLeafHarness h, Guid txid, bool committed = true)
    {
        var first = h.Activate();
        await first.SetAsync("k", [11]);
        await first.ApplyTxTerminalAsync(txid, committed, committedValues: null);
        Assert.That(first.RecentlyTerminalCount, Is.EqualTo(1),
            "Setup: the first activation must have applied the saga's terminal.");

        var second = h.Activate();
        Assert.That(second.RecentlyTerminalCount, Is.Zero,
            "Setup: the reactivated leaf must not remember the terminal.");
        await second.SetAsync("k", [22]);
        return second;
    }

    /// <summary>
    /// Reads <c>k</c> the way a multi-key reader does on a resized tree: under
    /// the logical tree's registry snapshot, which reports
    /// <paramref name="txid"/> as committed. (A point read resolves against the
    /// leaf's own physical tree, whose registry has no rows.)
    /// </summary>
    private static async Task<byte[]?> ReadUnderLogicalSnapshotAsync(BPlusLeafGrain leaf, Guid txid)
    {
        LatticeRegistrySnapshotContext.Current = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        try
        {
            return await leaf.GetAsync("k");
        }
        finally
        {
            LatticeRegistrySnapshotContext.Current = null;
        }
    }

    [Test]
    public async Task Forwarded_prepare_after_a_reactivation_is_refused_when_the_registry_reports_the_saga_committed()
    {
        var h = BuildReactivatingLeaf(TxStatus.Committed);
        var txid = Guid.NewGuid();
        var leaf = await ReactivateAfterTerminalAsync(h, txid);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [11]);
        var read = await ReadUnderLogicalSnapshotAsync(leaf, txid);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.PendingTransactionCount, Is.Zero,
                "the late forwarded prepare must not install an orphan bucket the orphan guard cannot see");
            Assert.That(read, Is.EqualTo(new byte[] { 22 }),
                "the committed saga's stale prepared value must not be served over the newer row");
            Assert.That(leaf.RecentlyTerminalCount, Is.Zero,
                "the refusal must not record the terminal as applied, since that set also dedups terminals");
        });
        await h.Registry.Received().GetStatusAsync(txid);
    }

    [Test]
    public async Task Forwarded_prepare_is_checked_against_the_logical_tree_named_by_the_forwarder()
    {
        // The leaf belongs to a resized tree's physical copy, whose registry has
        // no rows; the saga recorded its decision under the logical tree. Asking
        // the leaf's own registry would read InFlight and bucket the orphan,
        // which a multi-key reader (resolving under the logical tree) would then
        // surface as committed.
        var h = BuildReactivatingLeaf(TxStatus.Committed);
        var txid = Guid.NewGuid();
        var leaf = await ReactivateAfterTerminalAsync(h, txid);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [11]);

        Assert.That(leaf.PendingTransactionCount, Is.Zero);
        await h.Registry.Received().GetStatusAsync(txid);
        await h.PhysicalRegistry.DidNotReceive().GetStatusAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task Delayed_forwarded_prepare_that_outruns_the_terminal_is_refused_once_the_saga_has_decided()
    {
        // The no-reactivation trace on #4445 (shard-ownership TLA+, #4434): the
        // saga decides, a later write of the key is acknowledged, and a delayed
        // duplicate of the shadow-forward reaches the destination BEFORE the
        // saga's terminal does. Nothing on the leaf has seen the terminal, so
        // its memory cannot refuse it; bucketed, the prepare would be stamped
        // newer than the acknowledged write and surface over it.
        var h = BuildReactivatingLeaf(TxStatus.Committed);
        var txid = Guid.NewGuid();
        var leaf = h.Activate();
        await leaf.SetAsync("k", [22]);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [11]);
        var read = await ReadUnderLogicalSnapshotAsync(leaf, txid);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.RecentlyTerminalCount, Is.Zero, "Setup: no terminal has reached this leaf.");
            Assert.That(leaf.PendingTransactionCount, Is.Zero,
                "a forwarded prepare for a decided saga must be refused even before its terminal arrives");
            Assert.That(read, Is.EqualTo(new byte[] { 22 }),
                "the acknowledged later write must not be overridden by the delayed prepare");
        });
    }

    [Test]
    public async Task Forwarded_prepare_after_a_reactivation_is_refused_when_the_registry_reports_the_saga_aborted()
    {
        var h = BuildReactivatingLeaf(TxStatus.Aborted);
        var txid = Guid.NewGuid();
        var leaf = await ReactivateAfterTerminalAsync(h, txid, committed: false);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [11]);

        Assert.That(leaf.PendingTransactionCount, Is.Zero,
            "an aborted saga needs nothing, so a late prepare for it would only leak a bucket");
    }

    [Test]
    public async Task Forwarded_prepare_is_refused_on_the_recorded_verdict_behind_a_masked_decision()
    {
        var h = BuildReactivatingLeaf(TxStatus.Indeterminate, recorded: TxStatus.Committed);
        var txid = Guid.NewGuid();
        var leaf = await ReactivateAfterTerminalAsync(h, txid);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [11]);

        Assert.That(leaf.PendingTransactionCount, Is.Zero,
            "a decision masked by the retention window is still a decision");
        await h.Registry.Received().GetRecordedStatusAsync(txid);
    }

    [Test]
    public async Task Forwarded_set_many_and_delete_prepares_after_a_reactivation_are_refused()
    {
        var h = BuildReactivatingLeaf(TxStatus.Committed);
        var txid = Guid.NewGuid();
        var leaf = await ReactivateAfterTerminalAsync(h, txid);

        await ForwardedPrepareSetManyAsync(leaf, txid, "k", [11]);
        await ForwardedPrepareDeleteAsync(leaf, txid, "k");

        Assert.That(leaf.PendingTransactionCount, Is.Zero,
            "every prepared commit path must refuse the late forwarded prepare");
        Assert.That(await ReadUnderLogicalSnapshotAsync(leaf, txid), Is.EqualTo(new byte[] { 22 }));
    }

    [Test]
    public async Task Forwarded_prepare_for_an_undecided_saga_is_bucketed()
    {
        var h = BuildReactivatingLeaf(TxStatus.InFlight);
        var txid = Guid.NewGuid();
        var leaf = h.Activate();
        await leaf.SetAsync("k", [1]);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [2]);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.PendingTransactionCount, Is.EqualTo(1),
                "a forwarded prepare for a saga still in flight is a live prepare the terminal will drain");
            Assert.That(leaf.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 1 }),
                "it must stay hidden until the terminal");
        });
    }

    [Test]
    public async Task Forwarded_prepare_is_bucketed_when_the_registry_cannot_be_reached()
    {
        var h = BuildReactivatingLeaf(TxStatus.Committed);
        h.Registry.GetStatusAsync(Arg.Any<Guid>()).Throws(new TimeoutException("registry unreachable"));
        var txid = Guid.NewGuid();
        var leaf = h.Activate();
        await leaf.SetAsync("k", [1]);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [2]);

        Assert.That(leaf.PendingTransactionCount, Is.EqualTo(1),
            "refusing on an unknown decision could drop a write the saga still needs, so it fails open");
    }

    [Test]
    public async Task Coordinator_prepare_does_not_consult_the_registry()
    {
        var h = BuildReactivatingLeaf(TxStatus.Committed);
        var txid = Guid.NewGuid();
        var leaf = h.Activate();
        await leaf.SetAsync("k", [1]);

        await PreparePendingSetAsync(leaf, txid, "k", [2]);

        Assert.That(leaf.PendingTransactionCount, Is.EqualTo(1),
            "the coordinator's own prepare is acknowledged before the saga decides, so it is never late");
        await h.Registry.DidNotReceive().GetStatusAsync(Arg.Any<Guid>());
        await h.Registry.DidNotReceive().GetRecordedStatusAsync(Arg.Any<Guid>());
        await h.PhysicalRegistry.DidNotReceive().GetStatusAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task Forwarded_prepare_remembered_as_terminal_is_refused_without_a_registry_call()
    {
        var h = BuildReactivatingLeaf(TxStatus.InFlight);
        var txid = Guid.NewGuid();
        var leaf = h.Activate();
        await leaf.SetAsync("k", [1]);
        await leaf.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [2]);

        Assert.That(leaf.PendingTransactionCount, Is.Zero,
            "the per-activation memory answers first (issue #4385)");
        await h.Registry.DidNotReceive().GetStatusAsync(Arg.Any<Guid>());
    }
    [Test]
    public async Task Forwarded_prepare_of_a_forgotten_saga_whose_decision_was_pruned_is_refused()
    {
        // Issue #4632: the saga committed, completed and was forgotten, and its
        // decision was pruned, so the registry can only answer InFlight. Its
        // participant row went with the forget, which tells it apart from a live
        // saga.
        var h = BuildReactivatingLeaf(TxStatus.InFlight);
        h.Registry.GetParticipantsAsync(Arg.Any<Guid>()).Returns(Task.FromResult<IReadOnlyList<int>>([]));
        var txid = Guid.NewGuid();
        var leaf = await ReactivateAfterTerminalAsync(h, txid);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [11]);
        await ForwardedPrepareSetManyAsync(leaf, txid, "k", [11]);
        await ForwardedPrepareDeleteAsync(leaf, txid, "k");

        Assert.Multiple(() =>
        {
            Assert.That(leaf.PendingTransactionCount, Is.Zero,
                "nothing will ever settle a bucket of a forgotten saga, so it must not be installed");
            Assert.That(leaf.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 22 }),
                "the refusal must not touch the key's row");
            Assert.That(leaf.RecentlyTerminalCount, Is.Zero,
                "the refusal must not record a terminal");
        });
        await h.Registry.Received().GetParticipantsAsync(txid);
        await h.PhysicalRegistry.DidNotReceive().GetParticipantsAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task Forwarded_prepare_of_a_replicated_saga_is_bucketed_without_reading_participants()
    {
        // A replicated prepare's saga is never forgotten on this cluster, and its
        // participant row may be held by another registry, so an absent row
        // proves nothing.
        var h = BuildReactivatingLeaf(TxStatus.InFlight);
        h.Registry.GetParticipantsAsync(Arg.Any<Guid>()).Returns(Task.FromResult<IReadOnlyList<int>>([]));
        var txid = Guid.NewGuid();
        var leaf = h.Activate();
        await leaf.SetAsync("k", [1]);

        using (LatticeOriginContext.With("peer-cluster"))
        {
            await ForwardedPrepareSetAsync(leaf, txid, "k", [2]);
        }

        Assert.That(leaf.PendingTransactionCount, Is.EqualTo(1));
        await h.Registry.DidNotReceive().GetParticipantsAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task Forwarded_prepare_is_bucketed_when_the_participant_row_cannot_be_read()
    {
        var h = BuildReactivatingLeaf(TxStatus.InFlight);
        h.Registry.GetParticipantsAsync(Arg.Any<Guid>()).Throws(new TimeoutException("registry unreachable"));
        var txid = Guid.NewGuid();
        var leaf = h.Activate();
        await leaf.SetAsync("k", [1]);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [2]);

        Assert.That(leaf.PendingTransactionCount, Is.EqualTo(1),
            "refusing on an unknown row could drop a write a live saga still needs, so it fails open");
    }

    [Test]
    public async Task Forwarded_prepare_of_a_decided_saga_is_refused_without_reading_participants()
    {
        var h = BuildReactivatingLeaf(TxStatus.Committed);
        var txid = Guid.NewGuid();
        var leaf = h.Activate();
        await leaf.SetAsync("k", [1]);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [2]);

        Assert.That(leaf.PendingTransactionCount, Is.Zero);
        await h.Registry.DidNotReceive().GetParticipantsAsync(Arg.Any<Guid>());
    }
}
