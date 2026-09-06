using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using System.Reflection;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Split-time transfer of saga isolation state onto the new sibling.
/// <para>
/// A leaf split moves only the committed <c>Entries</c> row for every
/// key at or above the split key. The per-key isolation that makes a
/// mid-flight cross-shard saga safe lives in two other places on the
/// donor - the destination-side shadow markers installed by the shard
/// shadow-forward (<c>_shadowedSagas</c>) and the locally prepared,
/// not-yet-terminal saga buckets (<c>_pendingTx</c>) - and neither
/// rides along with the migrated row. Without re-arming the sibling,
/// the sibling would surface the migrated pre-saga value ungated and a
/// concurrent reader could observe some keys at their pre-saga round
/// and others at their post-saga round: the torn read the reshard
/// chaos fixture catches.
/// </para>
/// <para>
/// These tests pin that the donor re-arms the sibling with a shadow
/// marker for every saga that still claims a migrated key, that the
/// two marker sources are unioned per saga without duplicating a key,
/// and that a saga touching only keys the donor keeps produces no
/// sibling round-trip at all.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearSplitShadowTransferAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
        LatticeRegistrySnapshotContext.Current = null;
    }

    /// <summary>
    /// A sibling stub whose grain identity is stable, wired so the
    /// donor's split chain (initialize, merge, checkpoint hints) can
    /// run to completion against it.
    /// </summary>
    private static IBPlusLeafGrain CreateSplitSiblingStub()
    {
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(Task.CompletedTask);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        sibling.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>()).Returns(Task.CompletedTask);
        sibling.MarkSagaShadowAsync(Arg.Any<Guid>(), Arg.Any<IReadOnlyList<string>>())
            .Returns(Task.CompletedTask);
        return sibling;
    }

    /// <summary>
    /// Arms the persisted mid-split intent so the next mutation resumes
    /// the split through the recovery path, migrating every key at or
    /// above <paramref name="splitKey"/> to <paramref name="sibling"/>.
    /// </summary>
    private static void ArmInterruptedSplit(
        FakePersistentState<LeafNodeState> state,
        IBPlusLeafGrain sibling,
        string splitKey)
    {
        var siblingId = ((IGrainBase)sibling).GrainContext.GrainId;
        state.State.SplitState = SplitState.SplitInProgress;
        state.State.SplitKey = splitKey;
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.TreeId = "test-tree";
    }

    /// <summary>
    /// Installs a single prepared (saga-phase) mutation on the leaf so
    /// it lands in the pending-tx map rather than the visible cache.
    /// </summary>
    private static async Task PreparedSetForSplitAsync(
        BPlusLeafGrain grain, Guid txid, string key, byte[] value)
    {
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                await grain.SetAsync(key, value);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }
    }

    /// <summary>
    /// Every <c>MarkSagaShadowAsync</c> call the donor made on the
    /// sibling, as (transaction id, keys) pairs.
    /// </summary>
    private static List<(Guid Txid, List<string> Keys)> ShadowMarksOn(IBPlusLeafGrain sibling)
        => sibling.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.MarkSagaShadowAsync))
            .Select(c =>
            {
                var args = c.GetArguments();
                return ((Guid)args[0]!, ((IReadOnlyList<string>)args[1]!).ToList());
            })
            .ToList();

    [Test]
    public async Task Split_transfers_destination_side_shadow_markers_for_migrated_keys()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("3"));

        // A source-side saga claims one key that stays on the donor and
        // two that migrate; only the migrating pair must be re-armed.
        var txid = Guid.NewGuid();
        await grain.MarkSagaShadowAsync(txid, new[] { "a", "m", "z" });

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("4"));

        var marks = ShadowMarksOn(sibling);
        Assert.That(marks, Has.Count.EqualTo(1), "one marker round-trip per claiming saga");
        Assert.That(marks[0].Txid, Is.EqualTo(txid));
        Assert.That(marks[0].Keys, Is.EquivalentTo(new[] { "m", "z" }),
            "only the keys that migrated to the sibling are re-armed there");
    }

    [Test]
    public async Task Split_transfers_a_marker_per_saga_when_several_sagas_claim_migrated_keys()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);

        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));

        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        await grain.MarkSagaShadowAsync(first, new[] { "m" });
        await grain.MarkSagaShadowAsync(second, new[] { "z" });

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        var marks = ShadowMarksOn(sibling);
        Assert.That(marks, Has.Count.EqualTo(2));
        Assert.That(marks.Single(m => m.Txid == first).Keys, Is.EquivalentTo(new[] { "m" }));
        Assert.That(marks.Single(m => m.Txid == second).Keys, Is.EquivalentTo(new[] { "z" }));
    }

    [Test]
    public async Task Split_transfers_locally_prepared_saga_buckets_for_migrated_keys()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));

        // A same-shard prepare has no explicit shadow marker of its own -
        // its isolation is the pending bucket, which the split leaves
        // behind on the donor.
        var txid = Guid.NewGuid();
        await PreparedSetForSplitAsync(grain, txid, "z", Encoding.UTF8.GetBytes("prepared"));
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1),
            "the prepared write must be buffered, not committed");

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        var marks = ShadowMarksOn(sibling);
        Assert.That(marks, Has.Count.EqualTo(1));
        Assert.That(marks[0].Txid, Is.EqualTo(txid));
        Assert.That(marks[0].Keys, Is.EquivalentTo(new[] { "z" }));
    }

    [Test]
    public async Task Split_unions_marker_and_pending_sources_without_duplicating_a_key()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);

        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));

        // The same saga is visible through BOTH sources for key "z":
        // an explicit destination-side marker and a local prepare.
        var txid = Guid.NewGuid();
        await grain.MarkSagaShadowAsync(txid, new[] { "m", "z" });
        await PreparedSetForSplitAsync(grain, txid, "z", Encoding.UTF8.GetBytes("prepared"));

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        var marks = ShadowMarksOn(sibling);
        Assert.That(marks, Has.Count.EqualTo(1));
        Assert.That(marks[0].Keys, Is.EquivalentTo(new[] { "m", "z" }),
            "a key claimed through both sources is carried exactly once");
    }

    [Test]
    public async Task Split_does_not_mark_the_sibling_when_no_claimed_key_migrates()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));

        // Both sources are populated, but only for a key the donor keeps.
        var txid = Guid.NewGuid();
        await grain.MarkSagaShadowAsync(txid, new[] { "a" });
        await PreparedSetForSplitAsync(grain, Guid.NewGuid(), "a", Encoding.UTF8.GetBytes("prepared"));

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(ShadowMarksOn(sibling), Is.Empty,
            "no migrated key is claimed, so the sibling needs no read gate");
        await sibling.Received(1).MergeEntriesAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.ContainsKey("z")));
    }

    [Test]
    public async Task Split_does_not_mark_the_sibling_when_no_saga_state_exists()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(ShadowMarksOn(sibling), Is.Empty);
    }

    [Test]
    public async Task Recovery_split_returns_null_when_a_concurrent_turn_already_completed_it()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        ArmInterruptedSplit(state, sibling, splitKey: "m");

        // Stand in for the concurrent turn that owns the recovery: hold
        // the split gate so the arriving write parks on it.
        var gate = SplitGateOf(grain);
        Assert.That(gate.Wait(0), Is.True, "gate should start free");

        Task<SplitResult?> pending;
        try
        {
            pending = grain.SetAsync("z2", Encoding.UTF8.GetBytes("3"));
            Assert.That(pending.IsCompleted, Is.False,
                "the arriving write must park on the contended recovery gate");

            // The gate owner finishes the split while we are parked.
            state.State.SplitState = SplitState.SplitComplete;
        }
        finally
        {
            gate.Release();
        }

        var result = await pending;

        Assert.That(result, Is.Null,
            "the turn that did not perform the recovery reports no split");
        await sibling.DidNotReceive().InitializeSiblingAsync(Arg.Any<SiblingInitialization>());
        await sibling.Received(1).SetAsync("z2", Arg.Any<byte[]>(), Arg.Any<long>());
    }

    private static SemaphoreSlim SplitGateOf(BPlusLeafGrain grain)
    {
        var field = typeof(BPlusLeafGrain).GetField(
            "_splitGate",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(field, Is.Not.Null, "_splitGate field not found - was it renamed?");
        return (SemaphoreSlim)field!.GetValue(grain)!;
    }
}
