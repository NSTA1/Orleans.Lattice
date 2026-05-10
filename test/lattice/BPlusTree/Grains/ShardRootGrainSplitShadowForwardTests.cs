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
/// <see cref="ShardRootState.ShadowForward"/>) — the former targets a
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
/// <c>Entries</c> — bypassing <c>BPlusLeafGrain.CommitSetAsync</c>'s
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
/// drained — its <c>_pendingTx[txid]</c> orphaned forever once
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
        // Leaf raw-entry stub — return a non-tombstone live value so
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
        shadowTarget.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>()).Returns(Task.CompletedTask);
        shadowTarget.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
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
        };
    }

    private static ShardSplitInProgress NewSplit(ShardSplitPhase phase = ShardSplitPhase.BeginShadowWrite)
    {
        // Choose moved-slots that cover every 16 virtual slots so any test
        // key forwards regardless of its hash. The split coordinator in
        // production uses contiguous slot ranges — a "cover-all" set is
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
    // Fix 2 — prepared shadow-forward routes via SetAsync (preserves pending-tx)
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
        await h.ShadowTarget.DidNotReceive().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
    }

    [Test]
    public async Task SetAsync_outside_prepared_context_forwards_split_shadow_via_MergeManyAsync()
    {
        // Non-saga writes during a split must continue to use
        // MergeManyAsync — preserves source HLC verbatim for LWW
        // convergence on the destination during drain races.
        var h = CreateHarness(NewSplit(ShardSplitPhase.BeginShadowWrite));

        await h.Grain.SetAsync("k", [1, 2]);

        await h.ShadowTarget.Received().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
        // SetAsync on the shadow target is acceptable as part of the
        // shadow-forward observed-task tracker (which calls SetAsync on
        // the resize-shadow destination) — but no resize-shadow is
        // configured here, so it must not have been called.
        await h.ShadowTarget.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetAsync_under_prepared_context_with_empty_transaction_id_falls_back_to_MergeManyAsync()
    {
        // Defensive: if LatticePreparedContext is set but no
        // transaction id is bound, we cannot identify the saga to
        // route through pending-tx semantics. Fall back to the
        // legacy MergeManyAsync path — same as a non-saga write.
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

        await h.ShadowTarget.Received().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
    }

    // ============================================================================
    // Fix 3 — terminal-mark forwards to split shadow target
    // ============================================================================

    [Test]
    public async Task AppendTxTerminalAsync_forwards_to_split_shadow_target_during_shadow_write()
    {
        // The saga's terminal mark must reach the split-shadow target
        // so its pending-tx bucket is drained before the saga's
        // AtomicWriteGrain calls ITxRegistryGrain.ForgetAsync. Pre-fix
        // the terminal only forwarded via ForwardShadowAsync (resize-
        // shadow) which is null during a shard split.
        var h = CreateHarness(NewSplit(ShardSplitPhase.BeginShadowWrite));

        var txid = Guid.NewGuid();
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        await h.ShadowTarget.Received().AppendTxTerminalAsync(
            txid,
            true,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AppendTxTerminalAsync_forwards_to_split_shadow_target_during_drain()
    {
        var h = CreateHarness(NewSplit(ShardSplitPhase.Drain));

        var txid = Guid.NewGuid();
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        await h.ShadowTarget.Received().AppendTxTerminalAsync(
            txid, true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AppendTxTerminalAsync_forwards_to_split_shadow_target_during_swap()
    {
        var h = CreateHarness(NewSplit(ShardSplitPhase.Swap));

        var txid = Guid.NewGuid();
        await h.Grain.AppendTxTerminalAsync(txid, committed: false);

        await h.ShadowTarget.Received().AppendTxTerminalAsync(
            txid, false, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AppendTxTerminalAsync_does_not_forward_to_split_shadow_when_no_split_active()
    {
        // No split in progress → no split-shadow target → no forward.
        var h = CreateHarness(splitInProgress: null);

        var txid = Guid.NewGuid();
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        await h.ShadowTarget.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AppendTxTerminalAsync_does_not_forward_to_split_shadow_in_reject_phase()
    {
        // Reject and Complete phases are post-swap. By then the
        // destination is the authoritative owner and AtomicWriteGrain's
        // stale-routing retry has re-routed the terminal to the
        // destination's own AppendTxTerminalAsync via the public
        // ILattice path; the source-side forward is redundant and is
        // suppressed to avoid double-delivery work (the leaf-side
        // _recentlyTerminal dedup would absorb it, but skipping the
        // RPC entirely is cheaper).
        var h = CreateHarness(NewSplit(ShardSplitPhase.Reject));

        var txid = Guid.NewGuid();
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        await h.ShadowTarget.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AppendTxTerminalAsync_does_not_forward_to_split_shadow_with_empty_transaction_id()
    {
        // A defensive empty-txid call short-circuits before any
        // forwarding — verified by both the source's no-op behaviour
        // and no shadow-target call.
        var h = CreateHarness(NewSplit(ShardSplitPhase.BeginShadowWrite));

        await h.Grain.AppendTxTerminalAsync(Guid.Empty, committed: true);

        await h.ShadowTarget.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }
}
