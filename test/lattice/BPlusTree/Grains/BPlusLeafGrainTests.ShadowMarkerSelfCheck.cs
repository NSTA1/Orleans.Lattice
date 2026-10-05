using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4545: a destination-side shadow marker must never gate a key after its
/// saga's terminal can no longer clear it.
/// <para>
/// Three rules close it:
/// </para>
/// <list type="number">
/// <item><description>A marker installed after the leaf applied the saga's terminal is not installed.</description></item>
/// <item><description>A leaf split never carries a marker for a saga whose terminal the donor applied.</description></item>
/// <item><description>A marker that carries the saga's marked original prepare stamp P is released once the row it guards is stamped at or above P. This is the rule that holds when the first two cannot: a reactivation forgets the terminal, and a marker installed afterwards is indistinguishable from a live one.</description></item>
/// </list>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly HybridLogicalClock MarkedPrepare = new() { WallClockTicks = 5_000_000, Counter = 4 };

    /// <summary>Seeds a migrated row for <paramref name="key"/> stamped <paramref name="stamp"/>.</summary>
    private static void SeedMigratedRowAt(BPlusLeafGrain grain, string key, byte[] value, HybridLogicalClock stamp) =>
        grain.EntriesForTest[key] = LwwValue<byte[]>.Create(value, stamp) with { IsMigrated = true };

    private static async Task MarkWithStampAsync(BPlusLeafGrain grain, Guid txid, string key, HybridLogicalClock? stamp)
    {
        using (LatticeOriginalPrepareStampContext.With(
            stamp is { } p ? new Dictionary<string, HybridLogicalClock> { [key] = p } : null))
        {
            await grain.MarkSagaShadowAsync(txid, [key]);
        }
    }

    private static Task<byte[]?> ReadUnderAsync(BPlusLeafGrain grain, string key, Guid txid, TxStatus status)
    {
        using (LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus> { [txid] = status }))
        {
            return grain.GetAsync(key);
        }
    }

    // ---- Rule 3: the self-verifying marker

    /// <summary>
    /// The reactivation counter-trace the shard-ownership retention model found
    /// (b0c753b8, depth 14): the leaf applied the saga's terminal, a reactivation
    /// forgot it, and the delayed marker install arrives. The leaf cannot tell it
    /// from a live marker, and no terminal will come again. The row already holds
    /// the saga's value at its prepare stamp, so the read must be served.
    /// </summary>
    [Test]
    public async Task A_marker_carrying_its_marked_prepare_stamp_is_released_once_the_row_is_at_that_stamp(
        [Values] bool committed)
    {
        var status = committed ? TxStatus.Committed : TxStatus.Indeterminate;
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        SeedMigratedRowAt(grain, "k", [7], MarkedPrepare);

        await MarkWithStampAsync(grain, txid, "k", MarkedPrepare);

        Assert.That(await ReadUnderAsync(grain, "k", txid, status), Is.EqualTo(new byte[] { 7 }),
            "the row is the saga's own value; a terminal this leaf never sees must not hide it");
    }

    [Test]
    public async Task A_marker_carrying_its_marked_prepare_stamp_releases_a_later_write()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        SeedMigratedRowAt(grain, "k", [8], new HybridLogicalClock { WallClockTicks = 6_000_000, Counter = 0 });

        await MarkWithStampAsync(grain, txid, "k", MarkedPrepare);

        Assert.That(await ReadUnderAsync(grain, "k", txid, TxStatus.Committed), Is.EqualTo(new byte[] { 8 }));
    }

    /// <summary>
    /// The direction the self-check must never relax: a migrated row stamped below
    /// the saga's prepare is the pre-saga value. Serving it under a committed
    /// saga would tear the batch and lose the saga's write for this key.
    /// </summary>
    [Test]
    public async Task A_marker_carrying_its_marked_prepare_stamp_still_gates_a_row_below_that_stamp()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        SeedMigratedRowAt(grain, "k", [1], new HybridLogicalClock { WallClockTicks = 4_000_000, Counter = 0 });

        await MarkWithStampAsync(grain, txid, "k", MarkedPrepare);

        Assert.That(async () => await ReadUnderAsync(grain, "k", txid, TxStatus.Committed),
            Throws.InstanceOf<StaleShardRoutingException>());
    }

    /// <summary>
    /// A marker installed without a marked stamp - by an older silo, from an
    /// unmarked prepare, or from a resize copy whose writes P does not order -
    /// keeps the original gate, whatever the row's stamp.
    /// </summary>
    [Test]
    public async Task A_marker_without_a_marked_prepare_stamp_keeps_the_original_gate()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        SeedMigratedRowAt(grain, "k", [1], new HybridLogicalClock { WallClockTicks = 9_000_000, Counter = 0 });

        await MarkWithStampAsync(grain, txid, "k", stamp: null);

        Assert.That(async () => await ReadUnderAsync(grain, "k", txid, TxStatus.Committed),
            Throws.InstanceOf<StaleShardRoutingException>());
    }

    [Test]
    public async Task Installing_a_marker_with_a_marked_prepare_stamp_moves_the_leaf_clock_past_it()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 1_000, Counter = 0 };

        await MarkWithStampAsync(grain, Guid.NewGuid(), "k", MarkedPrepare);

        Assert.That(state.State.Clock.CompareTo(MarkedPrepare), Is.GreaterThanOrEqualTo(0),
            "property H: every write this leaf acknowledges afterwards is stamped above the saga's prepare");
    }

    [Test]
    public async Task A_terminal_clears_the_marker_stamp_with_the_marker()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        var txid = Guid.NewGuid();
        await MarkWithStampAsync(grain, txid, "z", MarkedPrepare);

        await grain.ApplyTxTerminalAsync(txid, committed: true);
        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(ShadowMarksOn(sibling), Is.Empty, "the terminal cleared the marker, so a split has nothing to carry");
    }

    // ---- Rule 1: no marker for a key the saga's terminal already settled here

    [Test]
    public async Task A_marker_installed_after_its_terminal_settled_the_key_is_not_installed_and_a_split_does_not_carry_it()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        var txid = Guid.NewGuid();

        await grain.ApplyTxTerminalAsync(txid, committed: true, new Dictionary<string, byte[]> { ["z"] = [9] });
        await grain.MarkSagaShadowAsync(txid, ["z"]);

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(ShadowMarksOn(sibling), Is.Empty,
            "a marker the leaf received after the saga's terminal settled the key guards nothing; a split must not "
            + "carry it onto a sibling the terminal will never reach");
    }

    /// <summary>
    /// The terminal for a saga reaches this leaf for one key while its committed
    /// value for another key is still on its way (a second delivery, forwarded
    /// from another shard). A marker for that other key must still be installed,
    /// gate its pre-saga row, and be carried across a split.
    /// </summary>
    [Test]
    public async Task A_marker_for_a_key_the_saga_terminal_did_not_settle_here_is_installed_gates_and_is_carried()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        var txid = Guid.NewGuid();

        await grain.ApplyTxTerminalAsync(txid, committed: true, new Dictionary<string, byte[]> { ["z"] = [9] });
        SeedMigratedRowAt(grain, "y", [0], new HybridLogicalClock { WallClockTicks = 4_000_000, Counter = 0 });
        await grain.MarkSagaShadowAsync(txid, ["y", "z"]);

        Assert.That(async () => await ReadUnderAsync(grain, "y", txid, TxStatus.Committed),
            Throws.InstanceOf<StaleShardRoutingException>(),
            "y's committed value has not reached this leaf; serving its pre-saga row tears the batch against z");

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        var marks = ShadowMarksOn(sibling);
        Assert.That(marks, Has.Count.EqualTo(1));
        Assert.That(marks[0].Keys, Is.EquivalentTo(new[] { "y" }), "only the key the terminal did not settle is carried");
    }

    // ---- The durable witness

    /// <summary>
    /// The witness is written with the leaf state, so a fresh activation over the
    /// same persisted state - which has no memory of the terminal - still refuses
    /// a delayed marker for a key the terminal settled and serves the key past an
    /// unstamped marker installed before the reactivation could be told apart.
    /// </summary>
    [Test]
    public async Task The_applied_terminal_witness_survives_a_reactivation_through_the_persisted_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var first = CreateGrain(state);
        var txid = Guid.NewGuid();
        await first.ApplyTxTerminalAsync(txid, committed: true, new Dictionary<string, byte[]> { ["z"] = [9] });
        first.MaterialiseTerminalWitnessForPersist();

        Assert.That(state.State.AppliedTerminalWitnesses, Has.Count.EqualTo(1));
        Assert.That(state.State.AppliedTerminalWitnesses![0].TransactionId, Is.EqualTo(txid));
        Assert.That(state.State.AppliedTerminalWitnesses![0].Keys, Is.EquivalentTo(new[] { "z" }));

        var reactivated = CreateGrain(state);
        Assert.That(reactivated.IsTerminalWitnessed(txid, "z"), Is.True);
        Assert.That(reactivated.IsTerminalWitnessed(txid, "y"), Is.False);

        SeedMigratedRowAt(reactivated, "z", [9], new HybridLogicalClock { WallClockTicks = 9_000_000, Counter = 0 });
        await reactivated.MarkSagaShadowAsync(txid, ["z"]);
        Assert.That(await ReadUnderAsync(reactivated, "z", txid, TxStatus.Committed), Is.EqualTo(new byte[] { 9 }));
    }

    [Test]
    public async Task A_drained_bucket_and_a_discarded_abort_are_witnessed_per_key()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        await PreparedSetForSplitAsync(grain, committed, "a", [1]);
        await PreparedSetForSplitAsync(grain, aborted, "b", [2]);

        await grain.ApplyTxTerminalAsync(committed, committed: true);
        await grain.ApplyTxTerminalAsync(aborted, committed: false);

        Assert.Multiple(() =>
        {
            Assert.That(grain.IsTerminalWitnessed(committed, "a"), Is.True);
            Assert.That(grain.IsTerminalWitnessed(committed, "b"), Is.False);
            Assert.That(grain.IsTerminalWitnessed(aborted, "b"), Is.True);
        });
    }

    [Test]
    public async Task A_leaf_split_carries_the_witnesses_of_the_moved_keys_to_the_sibling()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        SiblingInitialization? init = null;
        sibling.When(s => s.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()))
            .Do(c => init = c.Arg<SiblingInitialization>());
        var grain = CreateGrain(state, siblingStub: sibling);
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        var txid = Guid.NewGuid();
        await grain.ApplyTxTerminalAsync(
            txid, committed: true, new Dictionary<string, byte[]> { ["c"] = [1], ["z"] = [2] });

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(init, Is.Not.Null);
        Assert.That(init!.Value.TerminalWitnesses, Has.Length.EqualTo(1));
        Assert.That(init.Value.TerminalWitnesses![0].TransactionId, Is.EqualTo(txid));
        Assert.That(init.Value.TerminalWitnesses![0].Keys, Is.EquivalentTo(new[] { "z" }),
            "only the witness for a key that moves rides to the sibling");
    }

    [Test]
    public async Task A_split_sibling_adopts_and_persists_its_donors_witnesses()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateGrain(state);
        var txid = Guid.NewGuid();

        await sibling.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "test-tree",
            LowKeyInclusive = "m",
            TerminalWitnesses = [new AppliedTerminalWitness(txid, ["z"], DateTime.UtcNow.Ticks)],
        });

        Assert.That(sibling.IsTerminalWitnessed(txid, "z"), Is.True);
        Assert.That(state.State.AppliedTerminalWitnesses, Has.Count.EqualTo(1), "the adoption is persisted with the sibling's birth state");

        await sibling.MarkSagaShadowAsync(txid, ["z"]);
        SeedMigratedRowAt(sibling, "z", [2], new HybridLogicalClock { WallClockTicks = 9_000_000, Counter = 0 });
        Assert.That(await ReadUnderAsync(sibling, "z", txid, TxStatus.Committed), Is.EqualTo(new byte[] { 2 }),
            "a delayed marker routed to the sibling for a key the donor's terminal settled does not gate it");
    }

    [Test]
    public void A_replayed_terminal_backstop_is_witnessed_and_an_ordinary_write_is_not()
    {
        var grain = CreateGrain(new FakePersistentState<LeafNodeState>());
        var txid = Guid.NewGuid();
        var stamp = new HybridLogicalClock { WallClockTicks = 7_000_000, Counter = 0 };

        ((ILeafProjection)grain).Apply(new LatticeMutation
        {
            Kind = MutationKind.Set, Key = "z", Value = [1], Timestamp = stamp, TransactionId = txid, IsBackstop = true,
        });
        ((ILeafProjection)grain).Apply(new LatticeMutation
        {
            Kind = MutationKind.Set, Key = "y", Value = [2], Timestamp = stamp, TransactionId = txid,
        });

        Assert.Multiple(() =>
        {
            Assert.That(grain.IsTerminalWitnessed(txid, "z"), Is.True, "a replayed backstop settled its key");
            Assert.That(grain.IsTerminalWitnessed(txid, "y"), Is.False);
        });
    }

    [Test]
    public async Task The_witness_is_pruned_only_once_the_registry_reads_its_saga_as_absent()
    {
        var forgotten = Guid.NewGuid();
        var retained = Guid.NewGuid();
        var recent = Guid.NewGuid();
        var old = DateTime.UtcNow.AddHours(-1).Ticks;
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "test-tree";
        state.State.AppliedTerminalWitnesses =
        [
            new AppliedTerminalWitness(forgotten, ["a"], old),
            new AppliedTerminalWitness(retained, ["b"], old),
            new AppliedTerminalWitness(recent, ["c"], DateTime.UtcNow.Ticks),
        ];
        IReadOnlyList<Guid>? asked = null;
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(c =>
        {
            asked = c.Arg<IReadOnlyList<Guid>>().ToList();
            return Task.FromResult(new Dictionary<Guid, TxStatus>
            {
                [forgotten] = TxStatus.InFlight,
                [retained] = TxStatus.Indeterminate,
            });
        });
        var grain = CreateGrain(state, configureGrainFactory: f =>
            f.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));
        grain.TerminalWitnessPruneIntervalOverride = TimeSpan.FromMinutes(1);

        await grain.PruneTerminalWitnessAsync();
        grain.MaterialiseTerminalWitnessForPersist();

        Assert.Multiple(() =>
        {
            Assert.That(asked, Is.EquivalentTo(new[] { forgotten, retained }), "a witness younger than the interval is not asked about");
            Assert.That(grain.IsTerminalWitnessed(forgotten, "a"), Is.False);
            Assert.That(grain.IsTerminalWitnessed(retained, "b"), Is.True, "an undetermined decision keeps the witness");
            Assert.That(grain.IsTerminalWitnessed(recent, "c"), Is.True);
            Assert.That(state.State.AppliedTerminalWitnesses!.Select(w => w.TransactionId), Is.EquivalentTo(new[] { retained, recent }));
        });
    }

    [Test]
    public async Task A_registry_fault_keeps_every_witness()
    {
        var txid = Guid.NewGuid();
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "test-tree";
        state.State.AppliedTerminalWitnesses = [new AppliedTerminalWitness(txid, ["a"], DateTime.UtcNow.AddHours(-1).Ticks)];
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(Task.FromException<Dictionary<Guid, TxStatus>>(new TimeoutException("registry down")));
        var grain = CreateGrain(state, configureGrainFactory: f =>
            f.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));
        grain.TerminalWitnessPruneIntervalOverride = TimeSpan.FromMinutes(1);

        await grain.PruneTerminalWitnessAsync();

        Assert.That(grain.IsTerminalWitnessed(txid, "a"), Is.True);
    }

    // ---- Rule 2 and the transfer carrying P

    /// <summary>
    /// A leaf split carries each marker's marked prepare stamp, so the sibling's
    /// gate can release the marker it inherits even though the terminal for it
    /// was, or will be, delivered elsewhere.
    /// </summary>
    [Test]
    public async Task A_leaf_split_carries_each_markers_marked_prepare_stamp_to_the_sibling()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        HybridLogicalClock? carried = null;
        sibling
            .When(s => s.MarkSagaShadowAsync(Arg.Any<Guid>(), Arg.Any<IReadOnlyList<string>>()))
            .Do(_ => carried = LatticeOriginalPrepareStampContext.TryGetStamp("z", out var p) ? p : null);
        var grain = CreateGrain(state, siblingStub: sibling);
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        var txid = Guid.NewGuid();
        await MarkWithStampAsync(grain, txid, "z", MarkedPrepare);

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(ShadowMarksOn(sibling).Single().Txid, Is.EqualTo(txid));
        Assert.That(carried, Is.EqualTo(MarkedPrepare));
        Assert.That(LatticeOriginalPrepareStampContext.HasStamps, Is.False,
            "the carried stamps are scoped to the transfer and must not leak into the donor's own context");
    }

    [Test]
    public async Task A_leaf_split_carries_a_marker_without_a_stamp_without_one()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var sawStamp = false;
        sibling
            .When(s => s.MarkSagaShadowAsync(Arg.Any<Guid>(), Arg.Any<IReadOnlyList<string>>()))
            .Do(_ => sawStamp = LatticeOriginalPrepareStampContext.HasStamps);
        var grain = CreateGrain(state, siblingStub: sibling);
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        await MarkWithStampAsync(grain, Guid.NewGuid(), "z", stamp: null);

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(ShadowMarksOn(sibling), Has.Count.EqualTo(1));
        Assert.That(sawStamp, Is.False, "an unmarked marker stays on the original gate on the sibling too");
    }
}
