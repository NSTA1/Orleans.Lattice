using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the saga prepare/terminal shadow-forward path
/// during a shard split. The split-shadow path
/// (<c>ShardRootGrain.ForwardLocalWriteToShadowIfNeededAsync</c>) is
/// distinct from the resize-shadow path
/// (<c>ShardRootGrain.ForwardShadowAsync</c> over
/// <see cref="ShardRootState.ShadowForward"/>) - the former targets a
/// peer shard within the same physical tree during an in-flight
/// <see cref="ShardRootState.SplitInProgress"/>, while the latter
/// targets a destination tree during an online resize.
/// <para>
/// Pre-fix bugs (Failure 2 of PR #197 chaos suite):
/// </para>
/// <list type="number">
/// <item><description>The split-shadow forward of a saga prepare-phase
/// write went via <c>target.MergeManyAsync(...)</c>, which lands the
/// value directly in the destination leaf's visible
/// <c>Entries</c> - bypassing <c>BPlusLeafGrain.CommitSetAsync</c>'s
/// prepared-context branch that would otherwise bucket the value into
/// the destination leaf's <c>_pendingTx[txid]</c>. The destination
/// leaf surfaces the prepared value to readers immediately
/// (post-saga visibility before commit) and never receives the saga's
/// terminal mark via the per-shard fan-out, breaking strict atomic
/// reader isolation across the migrating slot.</description></item>
/// <item><description>The terminal-mark shadow-forward at
/// <c>AppendTxTerminalAsync</c> only invoked
/// <c>ForwardShadowAsync</c> (resize-shadow), so during a shard
/// split the destination shard's affected-leaves bucket was never
/// drained - its <c>_pendingTx[txid]</c> orphaned forever once
/// <c>AtomicWriteGrain</c> called
/// <c>ITxRegistryGrain.ForgetAsync</c>.</description></item>
/// </list>
/// </summary>
[TestFixture]
public class ShardRootGrainSplitShadowForwardTests
{
    private const string TreeId = "split-tree";
    private const int SourceShardIndex = 0;
    private const int TargetShardIndex = 1;
    private const int VirtualShardCount = 16;

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required IShardRootGrain ShadowTarget { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
        public required IGrainFactory Factory { get; init; }
    }

    private static Harness CreateHarness(ShardSplitInProgress? splitInProgress = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{SourceShardIndex}"));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = GrainId.Create("leaf", "test-leaf");
        state.State.RootIsLeaf = true;
        state.State.SplitInProgress = splitInProgress;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            shardCount: 2,
            factory: factory);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(null));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        // Leaf raw-entry stub - return a non-tombstone live value so
        // ForwardLocalWriteToShadowIfNeededAsync proceeds past the
        // tombstone short-circuit.
        var hlc = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks, Counter = 0 };
        leaf.GetRawEntryAsync(Arg.Any<string>())
            .Returns(Task.FromResult<LwwEntry?>(new LwwEntry("k", LwwValue<byte[]>.Create([1, 2], hlc))));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var cache = Substitute.For<ILeafCacheGrain>();
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        var shadowTarget = Substitute.For<IShardRootGrain>();
        shadowTarget.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);
        shadowTarget.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>()).Returns(Task.CompletedTask);
        shadowTarget.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shadowTarget);

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new Harness
        {
            Grain = grain,
            Leaf = leaf,
            ShadowTarget = shadowTarget,
            State = state,
            Factory = factory,
        };
    }

    private static ShardSplitInProgress NewSplit(ShardSplitPhase phase = ShardSplitPhase.BeginShadowWrite)
    {
        // Choose moved-slots that cover every 16 virtual slots so any test
        // key forwards regardless of its hash. The split coordinator in
        // production uses contiguous slot ranges - a "cover-all" set is
        // a faithful test fixture.
        var moved = Enumerable.Range(0, VirtualShardCount).ToArray();
        return new ShardSplitInProgress
        {
            Phase = phase,
            ShadowTargetShardIndex = TargetShardIndex,
            MovedSlots = moved,
            VirtualShardCount = VirtualShardCount,
        };
    }

    // ============================================================================
    // Fix 2 - prepared shadow-forward routes via SetAsync (preserves pending-tx)
    // ============================================================================

    [Test]
    public async Task SetAsync_under_prepared_context_forwards_split_shadow_via_SetAsync()
    {
        // Saga prepare-phase write hits source.SetAsync during a shard
        // split's BeginShadowWrite phase. The shadow-forward must route
        // via target.SetAsync so the destination leaf preserves
        // prepared semantics (buckets into _pendingTx[txid]) and
        // registers as an affected leaf for the saga.
        var h = CreateHarness(NewSplit(ShardSplitPhase.BeginShadowWrite));

        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                await h.Grain.SetAsync("k", [1, 2]);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        // The forward must use SetAsync on the destination, NOT MergeManyAsync.
        await h.ShadowTarget.Received().SetAsync("k", Arg.Any<byte[]>());
        await h.ShadowTarget.DidNotReceive().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>());
    }

    [Test]
    public async Task SetAsync_outside_prepared_context_forwards_split_shadow_via_MergeManyAsync()
    {
        // Non-saga writes during a split must continue to use
        // MergeManyAsync - preserves source HLC verbatim for LWW
        // convergence on the destination during drain races.
        var h = CreateHarness(NewSplit(ShardSplitPhase.BeginShadowWrite));

        await h.Grain.SetAsync("k", [1, 2]);

        await h.ShadowTarget.Received().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>());
        // SetAsync on the shadow target is acceptable as part of the
        // shadow-forward observed-task tracker (which calls SetAsync on
        // the resize-shadow destination) - but no resize-shadow is
        // configured here, so it must not have been called.
        await h.ShadowTarget.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetAsync_under_prepared_context_with_empty_transaction_id_falls_back_to_MergeManyAsync()
    {
        // Defensive: if LatticePreparedContext is set but no
        // transaction id is bound, we cannot identify the saga to
        // route through pending-tx semantics. Fall back to the
        // legacy MergeManyAsync path - same as a non-saga write.
        // This guards against a programmer error where prepared
        // scope is opened without first calling
        // LatticeTransactionContext.Set.
        var h = CreateHarness(NewSplit(ShardSplitPhase.BeginShadowWrite));

        // Ensure no inherited transaction id from a sibling test.
        LatticeTransactionContext.Set(Guid.Empty);

        using (LatticePreparedContext.BeginScope())
        {
            await h.Grain.SetAsync("k", [1, 2]);
        }

        await h.ShadowTarget.Received().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>());
    }

    // ============================================================================
    // GetSplitForwardTargetsAsync - saga-side flat fan-out target enumeration
    // ============================================================================
    //
    // Replaces the previous recursive ForwardSplitTerminalAsync hop on
    // the receiving shard root. The saga's BroadcastTerminalsAsync
    // (and the cross-cluster ApplyTxTerminalAsync) now invoke
    // TerminalFanOutResolver.ResolveTransitiveAsync, which walks each
    // shard's GetSplitForwardTargetsAsync to BFS-expand TouchedShards
    // until every transitive split destination is enumerated. The
    // contract under test here: the shard root must report the union
    // of (a) the in-flight split's ShadowTargetShardIndex and (b)
    // every distinct value in MovedAwaySlots, excluding this shard's
    // own index. AppendTxTerminalAsync no longer performs any
    // same-physical-tree forward to split destinations - that
    // responsibility is owned by the saga's flat fan-out.

    [Test]
    public async Task GetSplitForwardTargetsAsync_returns_empty_when_no_split_in_progress_and_no_moved_away_slots()
    {
        var h = CreateHarness(splitInProgress: null);

        var targets = await h.Grain.GetSplitForwardTargetsAsync();

        Assert.That(targets, Is.Empty,
            "no split in progress and no migrated slots → no split-forward destinations");
    }

    [Test]
    public async Task GetSplitForwardTargetsAsync_returns_split_in_progress_target_during_shadow_write_phase()
    {
        var h = CreateHarness(NewSplit(ShardSplitPhase.BeginShadowWrite));

        var targets = await h.Grain.GetSplitForwardTargetsAsync();

        Assert.That(targets, Is.EqualTo(new[] { TargetShardIndex }),
            "an in-flight split exposes its ShadowTargetShardIndex from the BeginShadowWrite phase onward");
    }

    [Test]
    public async Task GetSplitForwardTargetsAsync_returns_split_in_progress_target_during_drain_phase()
    {
        var h = CreateHarness(NewSplit(ShardSplitPhase.Drain));

        var targets = await h.Grain.GetSplitForwardTargetsAsync();

        Assert.That(targets, Is.EqualTo(new[] { TargetShardIndex }));
    }

    [Test]
    public async Task GetSplitForwardTargetsAsync_returns_split_in_progress_target_during_swap_phase()
    {
        var h = CreateHarness(NewSplit(ShardSplitPhase.Swap));

        var targets = await h.Grain.GetSplitForwardTargetsAsync();

        Assert.That(targets, Is.EqualTo(new[] { TargetShardIndex }));
    }

    [Test]
    public async Task GetSplitForwardTargetsAsync_returns_split_in_progress_target_during_reject_phase()
    {
        // Reject is the most load-bearing phase: a saga whose prepare
        // ran during BeginShadowWrite/Drain/Swap shadow-forwarded its
        // prepared writes into the destination's _pendingTx[txid] -
        // the saga's terminal must still reach the destination after
        // the source's split has advanced to Reject. Pre-fix the
        // shard root's recursive forward handled this; post-fix the
        // saga's flat fan-out walks GetSplitForwardTargetsAsync here
        // and reaches the destination directly.
        var h = CreateHarness(NewSplit(ShardSplitPhase.Reject));

        var targets = await h.Grain.GetSplitForwardTargetsAsync();

        Assert.That(targets, Is.EqualTo(new[] { TargetShardIndex }));
    }

    [Test]
    public async Task GetSplitForwardTargetsAsync_returns_moved_away_destinations_after_split_completes()
    {
        // After a split fully completes the coordinator clears
        // SplitInProgress, but MovedAwaySlots persists on the source.
        // A saga whose prepare straddled the split window may have its
        // terminal arrive AFTER SplitInProgress was cleared - without
        // consulting MovedAwaySlots there is no record of the
        // destination shard index and the destination's pending bucket
        // would be orphaned. GetSplitForwardTargetsAsync therefore
        // surfaces every distinct destination ever recorded so the
        // saga's resolver BFS still reaches it.
        var h = CreateHarness(splitInProgress: null);
        h.State.State.MovedAwaySlots = new Dictionary<int, int>
        {
            [0] = TargetShardIndex,
            [1] = TargetShardIndex,
            [2] = TargetShardIndex,
        };
        h.State.State.MovedAwayVirtualShardCount = VirtualShardCount;

        var targets = await h.Grain.GetSplitForwardTargetsAsync();

        Assert.That(targets, Is.EqualTo(new[] { TargetShardIndex }),
            "duplicate destination entries collapse to a single distinct target");
    }

    [Test]
    public async Task GetSplitForwardTargetsAsync_returns_every_distinct_destination_in_moved_away_slots()
    {
        // Two completed splits, two distinct destinations: both must
        // appear in the reported set so the saga's resolver BFS reaches
        // both. Returned list is sorted ascending for deterministic
        // caller iteration.
        var h = CreateHarness(splitInProgress: null);
        const int TargetA = 1;
        const int TargetB = 5;
        h.State.State.MovedAwaySlots = new Dictionary<int, int>
        {
            [0] = TargetA,
            [1] = TargetA,
            [2] = TargetB,
            [3] = TargetB,
        };
        h.State.State.MovedAwayVirtualShardCount = VirtualShardCount;

        var targets = await h.Grain.GetSplitForwardTargetsAsync();

        Assert.That(targets, Is.EqualTo(new[] { TargetA, TargetB }));
    }

    [Test]
    public async Task GetSplitForwardTargetsAsync_excludes_my_shard_index_from_moved_away_targets()
    {
        // Defensive: even if state somehow records the source's own
        // index as a moved-away destination (corruption / partial
        // recovery), the reported set must exclude it - a saga's
        // resolver BFS that re-included the source would create a
        // cycle and stall on the visited-set check, never
        // discovering any further destinations.
        var h = CreateHarness(splitInProgress: null);
        h.State.State.MovedAwaySlots = new Dictionary<int, int>
        {
            [0] = SourceShardIndex,
            [1] = TargetShardIndex,
        };
        h.State.State.MovedAwayVirtualShardCount = VirtualShardCount;

        var targets = await h.Grain.GetSplitForwardTargetsAsync();

        Assert.That(targets, Is.EqualTo(new[] { TargetShardIndex }),
            "this shard's own index is filtered out of moved-away destinations");
    }

    [Test]
    public async Task GetSplitForwardTargetsAsync_unions_split_in_progress_target_and_moved_away_destinations()
    {
        // A shard that completed one earlier split (recorded in
        // MovedAwaySlots) and now started another (recorded in
        // SplitInProgress) must report BOTH destinations so the saga's
        // resolver fan-out reaches both.
        const int PriorTarget = 3;
        var h = CreateHarness(NewSplit(ShardSplitPhase.Drain));
        h.State.State.MovedAwaySlots = new Dictionary<int, int>
        {
            [0] = PriorTarget,
        };
        h.State.State.MovedAwayVirtualShardCount = VirtualShardCount;

        var targets = await h.Grain.GetSplitForwardTargetsAsync();

        Assert.That(targets, Is.EqualTo(new[] { TargetShardIndex, PriorTarget }.OrderBy(i => i).ToArray()));
    }

    [Test]
    public async Task AppendTxTerminalAsync_does_not_forward_to_split_destinations()
    {
        // Post-fix: AppendTxTerminalAsync no longer recurses into split
        // destinations. The saga's BroadcastTerminalsAsync
        // pre-resolves the transitive closure via
        // TerminalFanOutResolver and fans the terminal out flat in
        // parallel from the saga layer, so the receiving shard root
        // performs only its own per-leaf fan-out (and the
        // resize-shadow ForwardShadowAsync for cross-tree resize) -
        // never a same-physical-tree split-destination call. This
        // test pins the bound: a shard with a recorded split must
        // NOT call AppendTxTerminalAsync on its split destination
        // when the saga drives a terminal append.
        var h = CreateHarness(NewSplit(ShardSplitPhase.BeginShadowWrite));

        var txid = Guid.NewGuid();
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        await h.ShadowTarget.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>());
    }

    // ============================================================================
    // Shadow-forward closes Reject phase and post-Complete races
    // ============================================================================
    //
    // Pre-fix, ForwardLocalWriteToShadowIfNeededAsync admitted only
    // BeginShadowWrite / Drain / Swap. Two races could strand a
    // mid-saga prepared write on the source shard while the ShardMap
    // (already swapped at Swap entry) routed readers to the
    // destination:
    //
    //   (A) Swap → Reject mid-write: SetAsync passes
    //       ThrowIfRejectedForKey while phase is Swap, writes to the
    //       leaf, then the coordinator advances phase to Reject
    //       before the shadow-forward call observes it. Pre-fix the
    //       forward early-returned on the phase gate.
    //
    //   (B) Reject → Complete mid-write: same shape, but the
    //       coordinator clears SplitInProgress and populates
    //       MovedAwaySlots in the same write. Pre-fix the forward
    //       early-returned on `sip is null`.
    //
    // The tests below race-simulate both transitions by mutating the
    // persisted state from inside the leaf's SetAsync NSubstitute
    // callback - which fires between ThrowIfRejectedForKey (already
    // passed) and ForwardLocalWriteToShadowIfNeededAsync (the helper
    // under test).

    [Test]
    public async Task ForwardLocalWriteToShadowIfNeededAsync_forwards_when_phase_advances_to_Reject_mid_write()
    {
        // Race (A): SetAsync enters at phase=Swap → passes
        // ThrowIfRejectedForKey → leaf traversal begins. The leaf
        // callback mutates SplitInProgress.Phase to Reject,
        // mimicking the split coordinator advancing the state
        // machine concurrently. ForwardLocalWriteToShadowIfNeededAsync
        // must STILL forward the write so the destination shard
        // observes it before the saga's terminal arrives. Without
        // the Reject-phase admission in the forward helper, the
        // destination never sees the write and a reader routed there
        // post-swap surfaces the pre-saga value.
        var h = CreateHarness(NewSplit(ShardSplitPhase.Swap));
        h.Leaf.When(l => l.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()))
            .Do(_ => h.State.State.SplitInProgress =
                h.State.State.SplitInProgress! with { Phase = ShardSplitPhase.Reject });

        await h.Grain.SetAsync("k", [1, 2]);

        await h.ShadowTarget.Received().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>());
    }

    [Test]
    public async Task ForwardLocalWriteToShadowIfNeededAsync_forwards_via_SetAsync_when_phase_advances_to_Reject_mid_prepared_write()
    {
        // Race (A) + prepared context: a saga prepare write that
        // races the Swap → Reject transition must still forward via
        // target.SetAsync (NOT MergeManyAsync) so the destination
        // leaf buckets the value into its own _pendingTx[txid] and
        // the saga's terminal mark can flip it into Entries
        // atomically. Pre-fix the prepare write was stranded on the
        // source and the destination's pending bucket never received
        // the value, producing a mid-saga visibility split when the
        // reader routed to the destination via the swapped ShardMap.
        var h = CreateHarness(NewSplit(ShardSplitPhase.Swap));
        h.Leaf.When(l => l.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()))
            .Do(_ => h.State.State.SplitInProgress =
                h.State.State.SplitInProgress! with { Phase = ShardSplitPhase.Reject });

        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                await h.Grain.SetAsync("k", [1, 2]);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        await h.ShadowTarget.Received().SetAsync("k", Arg.Any<byte[]>());
        await h.ShadowTarget.DidNotReceive().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>());
    }

    [Test]
    public async Task ForwardLocalWriteToShadowIfNeededAsync_forwards_via_MovedAwaySlots_when_split_completes_mid_write()
    {
        // Race (B): SetAsync enters at phase=Swap → passes
        // ThrowIfRejectedForKey while MovedAwaySlots is still empty
        // → leaf traversal begins. The leaf callback simulates the
        // coordinator completing the split: clears SplitInProgress
        // and populates MovedAwaySlots + MovedAwayVirtualShardCount
        // in the same beat. ForwardLocalWriteToShadowIfNeededAsync
        // sees `sip is null` but must consult the MovedAwaySlots
        // fallback and forward to the recorded post-split owner.
        // Without the fallback the write is stranded on the source
        // while the ShardMap routes the reader to the new owner.
        var h = CreateHarness(NewSplit(ShardSplitPhase.Swap));
        h.Leaf.When(l => l.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()))
            .Do(_ =>
            {
                h.State.State.SplitInProgress = null;
                h.State.State.MovedAwaySlots = Enumerable.Range(0, VirtualShardCount)
                    .ToDictionary(i => i, _ => TargetShardIndex);
                h.State.State.MovedAwayVirtualShardCount = VirtualShardCount;
            });

        await h.Grain.SetAsync("k", [1, 2]);

        await h.ShadowTarget.Received().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>());
    }

    [Test]
    public async Task ForwardLocalWriteToShadowIfNeededAsync_forwards_via_SetAsync_post_complete_when_prepared()
    {
        // Race (B) + prepared context: same post-Complete race but
        // for a saga prepare write. The MovedAwaySlots fallback must
        // also route through target.SetAsync (not MergeManyAsync) so
        // prepared semantics are preserved on the destination.
        var h = CreateHarness(NewSplit(ShardSplitPhase.Swap));
        h.Leaf.When(l => l.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()))
            .Do(_ =>
            {
                h.State.State.SplitInProgress = null;
                h.State.State.MovedAwaySlots = Enumerable.Range(0, VirtualShardCount)
                    .ToDictionary(i => i, _ => TargetShardIndex);
                h.State.State.MovedAwayVirtualShardCount = VirtualShardCount;
            });

        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                await h.Grain.SetAsync("k", [1, 2]);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        await h.ShadowTarget.Received().SetAsync("k", Arg.Any<byte[]>());
        await h.ShadowTarget.DidNotReceive().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>());
    }

    [Test]
    public async Task ForwardLocalWriteToShadowIfNeededAsync_does_not_forward_via_MovedAwaySlots_when_slot_maps_to_self()
    {
        // Defensive: a corrupt/inconsistent state where MovedAwaySlots
        // records the source's own index as the post-split owner must
        // not produce a self-forward (which would recurse onto the
        // same activation and deadlock). The fallback gate requires
        // `newOwner != MyShardIndex` before forwarding.
        var h = CreateHarness(NewSplit(ShardSplitPhase.Swap));
        h.Leaf.When(l => l.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()))
            .Do(_ =>
            {
                h.State.State.SplitInProgress = null;
                h.State.State.MovedAwaySlots = Enumerable.Range(0, VirtualShardCount)
                    .ToDictionary(i => i, _ => SourceShardIndex);  // self!
                h.State.State.MovedAwayVirtualShardCount = VirtualShardCount;
            });

        await h.Grain.SetAsync("k", [1, 2]);

        await h.ShadowTarget.DidNotReceive().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>());
        await h.ShadowTarget.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }
}
