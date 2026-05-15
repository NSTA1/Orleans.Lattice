using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the retroactive shadow-forward sweep's orphan-window
/// closure: every in-flight prepared mutation captured at split-begin
/// is replayed against the destination, but a saga that has ALREADY
/// terminalised at sweep-time (or terminalises mid-sweep) must reach
/// the destination via a direct terminal application instead of (or
/// in addition to) the prepare replay. Otherwise the destination's
/// pending-tx bucket holds an orphan that no terminal will ever drain.
/// <para>
/// The fixture stubs the source leaf chain to return one moved-slot
/// pending mutation, then drives <c>InitiateSplitStateAsync</c> end-
/// to-end and asserts which combination of
/// <see cref="IShardRootGrain.SetAsync(string, byte[])"/>,
/// <see cref="IShardRootGrain.DeleteAsync(string)"/>, and
/// <see cref="IShardRootGrain.AppendTxTerminalAsync(System.Guid, bool, IReadOnlyDictionary{string, byte[]}?)"/>
/// the destination received.
/// </para>
/// </summary>
public partial class TreeShardSplitGrainTests
{
    /// <summary>
    /// Wires a coordinator whose source shard's leftmost leaf returns
    /// the supplied set of pending-mutation snapshots, and whose
    /// per-tree <c>ITxRegistryGrain</c> returns
    /// <paramref name="preCheckStatus"/> at sweep-time and
    /// <paramref name="postSweepStatus"/> (or the same value when
    /// <c>null</c>) at the cleanup-pass.
    /// </summary>
    private static (TreeShardSplitGrain grain,
                    FakePersistentState<TreeShardSplitState> state,
                    IShardRootGrain sourceShard,
                    IShardRootGrain targetShard,
                    IBPlusLeafGrain leaf,
                    ITxRegistryGrain txRegistry)
        CreateGrainWithSweepWiring(
            PendingMutationSnapshot[] leafSnapshots,
            TxStatus preCheckStatus,
            TxStatus? postSweepStatus = null,
            int sourceShardIndex = 0,
            int virtualShardCount = 16,
            int physicalShardCount = 2)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("split", $"{TreeId}/{sourceShardIndex}"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(ShardMap.CreateDefault(virtualShardCount, physicalShardCount));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = physicalShardCount,
            }));
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        registry.AllocateNextShardIndexAsync(TreeId, Arg.Any<int>())
            .Returns(ci => Task.FromResult(((int)ci[1]) + 1));

        // Source + target shard stubs. The source's leftmost leaf
        // returns a stable grain id; the target captures the sweep's
        // replays and terminal applications.
        var sourceShard = Substitute.For<IShardRootGrain>();
        var targetShard = Substitute.For<IShardRootGrain>();
        var leafGrainId = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        sourceShard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafGrainId));
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(ci =>
        {
            var key = (string)ci[0];
            var idx = int.Parse(key[(key.LastIndexOf('/') + 1)..]);
            return idx == sourceShardIndex ? sourceShard : targetShard;
        });

        // Leaf stub returning the supplied pending-mutation snapshots
        // for any moved-slot query and null next-sibling so the sweep
        // walks exactly one leaf.
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(Task.FromResult(new List<PendingMutationSnapshot>(leafSnapshots)));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        // Per-tree TxRegistry stub: pre-check returns
        // preCheckStatus for every txid the sweep asks about;
        // post-sweep batch returns postSweepStatus (or preCheckStatus
        // when null).
        var effectivePost = postSweepStatus ?? preCheckStatus;
        var txRegistry = Substitute.For<ITxRegistryGrain>();
        txRegistry.GetStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(preCheckStatus));
        txRegistry.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(ci =>
            {
                var ids = (IReadOnlyList<Guid>)ci[0];
                var map = new Dictionary<Guid, TxStatus>(ids.Count);
                foreach (var id in ids) map[id] = effectivePost;
                return Task.FromResult(map);
            });
        grainFactory.GetGrain<ITxRegistryGrain>(TreeId).Returns(txRegistry);

        var state = new FakePersistentState<TreeShardSplitState>();
        var grain = new TreeShardSplitGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, optionsResolver,
            new LoggerFactory().CreateLogger<TreeShardSplitGrain>(), state);
        return (grain, state, sourceShard, targetShard, leaf, txRegistry);
    }

    /// <summary>
    /// Builds a single non-tombstone <see cref="PendingMutationSnapshot"/>
    /// whose key hashes into a slot owned by <paramref name="sourceShardIndex"/>
    /// under the default 16-slot / 2-shard map (slots 0,2,4,…). The
    /// helper picks a probe key the sweep will route into the moved
    /// half so the snapshot is actually returned to the sweep's
    /// per-slot filter.
    /// </summary>
    private static PendingMutationSnapshot BuildSetSnapshot(
        Guid txid,
        out string keyOnMovedSlot,
        int virtualShardCount = 16,
        int sourceShardIndex = 0)
    {
        // The default map for (vsc=16, ps=2) is striped (0,1,0,1,…); the
        // split's MovedSlots is the upper half of slots owned by
        // source, i.e. slots 8/10/12/14 of the source's owned set
        // 0,2,4,6,8,10,12,14. Probe a small set of keys until we
        // find one whose virtual slot is in that moved half.
        var movedSet = new HashSet<int> { 8, 10, 12, 14 };
        string? candidate = null;
        for (int i = 0; i < 4096; i++)
        {
            var k = $"key-{i}";
            var slot = ShardMap.GetVirtualSlot(k, virtualShardCount);
            if (movedSet.Contains(slot)) { candidate = k; break; }
        }
        keyOnMovedSlot = candidate ?? throw new InvalidOperationException("could not synthesise a key on a moved slot");

        return new PendingMutationSnapshot
        {
            TransactionId = txid,
            Key = keyOnMovedSlot,
            Value = [1, 2, 3],
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            IsTombstone = false,
            ExpiresAtTicks = 0,
            OriginClusterId = null,
            VectorClock = null,
        };
    }

    private static PendingMutationSnapshot BuildTombstoneSnapshot(Guid txid, out string keyOnMovedSlot)
    {
        var snap = BuildSetSnapshot(txid, out keyOnMovedSlot);
        return snap with { Value = null, IsTombstone = true };
    }

    // -------- pre-check: already committed at sweep time --------

    [Test]
    public async Task RetroactiveSweep_skips_replay_and_applies_commit_terminal_when_saga_already_committed()
    {
        var txid = Guid.NewGuid();
        var snap = BuildSetSnapshot(txid, out var key);
        var (grain, _, _, target, _, _) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.Committed);

        await grain.InitiateSplitStateAsync(0);

        // Replay path MUST NOT have fired - the snapshot would
        // otherwise install an orphan in the destination's
        // _pendingTx that no terminal could drain.
        await target.DidNotReceive().SetAsync(snap.Key, Arg.Any<byte[]>());
        await target.DidNotReceive().SetAsync(snap.Key, Arg.Any<byte[]>(), Arg.Any<long>());
        await target.DidNotReceive().DeleteAsync(snap.Key);

        // Terminal MUST be applied directly with the snapshot value
        // as the per-key backstop so the destination's
        // ApplyTxTerminalAsync backstop path surfaces the committed
        // value.
        await target.Received(1).AppendTxTerminalAsync(
            txid,
            committed: true,
            Arg.Is<IReadOnlyDictionary<string, byte[]>>(d => d != null && d.Count == 1 && d[key].SequenceEqual(snap.Value!)));
    }

    [Test]
    public async Task RetroactiveSweep_commit_pre_check_omits_committed_values_for_tombstone_snapshot()
    {
        var txid = Guid.NewGuid();
        var snap = BuildTombstoneSnapshot(txid, out _);
        var (grain, _, _, target, _, _) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.Committed);

        await grain.InitiateSplitStateAsync(0);

        // Tombstone snapshots carry Value=null; the terminal still
        // applies committed=true so the destination tombstones the
        // key on its own pending bucket via the standard
        // ApplyTxTerminalAsync path, but no per-key backstop value
        // is supplied.
        await target.Received(1).AppendTxTerminalAsync(
            txid,
            committed: true,
            Arg.Is<IReadOnlyDictionary<string, byte[]>?>(d => d == null));
    }

    // -------- pre-check: already aborted at sweep time --------

    [Test]
    public async Task RetroactiveSweep_skips_replay_and_applies_abort_terminal_when_saga_already_aborted()
    {
        var txid = Guid.NewGuid();
        var snap = BuildSetSnapshot(txid, out _);
        var (grain, _, _, target, _, _) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.Aborted);

        await grain.InitiateSplitStateAsync(0);

        await target.DidNotReceive().SetAsync(snap.Key, Arg.Any<byte[]>());
        await target.DidNotReceive().DeleteAsync(snap.Key);

        // Abort terminal: no committed-values payload (the
        // saga's prepared writes are simply dropped on the
        // destination too).
        await target.Received(1).AppendTxTerminalAsync(
            txid,
            committed: false,
            Arg.Is<IReadOnlyDictionary<string, byte[]>?>(d => d == null));
    }

    // -------- pre-check: in-flight at sweep time --------

    [Test]
    public async Task RetroactiveSweep_replays_prepare_when_saga_in_flight()
    {
        var txid = Guid.NewGuid();
        var snap = BuildSetSnapshot(txid, out var key);
        var (grain, _, _, target, _, _) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.InFlight);

        await grain.InitiateSplitStateAsync(0);

        // In-flight sagas replay normally via target.SetAsync -
        // this is the original retroactive-sweep path. The pre-check
        // only short-circuits when the saga has already terminalised.
        await target.Received(1).SetAsync(key, Arg.Is<byte[]>(b => b.SequenceEqual(snap.Value!)));

        // Post-sweep cleanup queries the registry once with the
        // tracked txid; status is still InFlight in this scenario
        // so no terminal is applied for it.
        await target.DidNotReceive().AppendTxTerminalAsync(
            txid, Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
    }

    [Test]
    public async Task RetroactiveSweep_replays_tombstone_via_DeleteAsync_when_saga_in_flight()
    {
        var txid = Guid.NewGuid();
        var snap = BuildTombstoneSnapshot(txid, out var key);
        var (grain, _, _, target, _, _) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.InFlight);

        await grain.InitiateSplitStateAsync(0);

        await target.Received(1).DeleteAsync(key);
        await target.DidNotReceive().SetAsync(key, Arg.Any<byte[]>());
        await target.DidNotReceive().AppendTxTerminalAsync(
            txid, Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
    }

    // -------- post-sweep cleanup: saga terminalises during sweep --------

    [Test]
    public async Task RetroactiveSweep_applies_commit_terminal_in_cleanup_when_saga_commits_during_sweep()
    {
        var txid = Guid.NewGuid();
        var snap = BuildSetSnapshot(txid, out var key);
        var (grain, _, _, target, _, _) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.InFlight,
            postSweepStatus: TxStatus.Committed);

        await grain.InitiateSplitStateAsync(0);

        // Replay path fired because pre-check was InFlight.
        await target.Received(1).SetAsync(key, Arg.Is<byte[]>(b => b.SequenceEqual(snap.Value!)));

        // Cleanup pass re-checks status; it has flipped to
        // Committed, so the cleanup applies the terminal directly
        // with the snapshot value as the per-key backstop. Without
        // this defence the destination's pending entry would be
        // orphaned because the saga's own broadcast already ran
        // (it queried participants before the sweep registered
        // destination).
        await target.Received(1).AppendTxTerminalAsync(
            txid,
            committed: true,
            Arg.Is<IReadOnlyDictionary<string, byte[]>>(d => d != null && d.Count == 1 && d[key].SequenceEqual(snap.Value!)));
    }

    [Test]
    public async Task RetroactiveSweep_applies_abort_terminal_in_cleanup_when_saga_aborts_during_sweep()
    {
        var txid = Guid.NewGuid();
        var snap = BuildSetSnapshot(txid, out var key);
        var (grain, _, _, target, _, _) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.InFlight,
            postSweepStatus: TxStatus.Aborted);

        await grain.InitiateSplitStateAsync(0);

        // Replay fired (pre-check InFlight); cleanup observes
        // Aborted and applies the abort terminal so the
        // destination drops its pending bucket. No committed-
        // values payload accompanies an abort terminal.
        await target.Received(1).SetAsync(key, Arg.Any<byte[]>());
        await target.Received(1).AppendTxTerminalAsync(
            txid,
            committed: false,
            Arg.Is<IReadOnlyDictionary<string, byte[]>?>(d => d == null));
    }

    [Test]
    public async Task RetroactiveSweep_cleanup_aggregates_per_txid_committed_values_across_multiple_keys()
    {
        // Two snapshots under the SAME txid (e.g. an atomic-write
        // saga that prepared two keys on the source). The cleanup
        // pass must aggregate every Set snapshot's value into the
        // single committed-values payload so the destination's
        // backstop drains every key in one terminal.
        var txid = Guid.NewGuid();
        var snap1 = BuildSetSnapshot(txid, out var key1);
        // Hand-craft a distinct second snapshot for the same txid;
        // use a different key whose slot is also in the moved set
        // by reusing the same probing helper and adjusting the key.
        var snap2 = snap1 with
        {
            Key = key1 + "-sibling",
            Value = [9, 9, 9],
        };
        // Ensure the sibling key also routes to a moved slot - if
        // not, fall through to a structural alternative.
        if (!new HashSet<int> { 8, 10, 12, 14 }.Contains(ShardMap.GetVirtualSlot(snap2.Key, 16)))
        {
            for (int i = 4096; i < 65536; i++)
            {
                var k = $"key-sibling-{i}";
                if (new HashSet<int> { 8, 10, 12, 14 }.Contains(ShardMap.GetVirtualSlot(k, 16)))
                {
                    snap2 = snap2 with { Key = k };
                    break;
                }
            }
        }

        var (grain, _, _, target, _, _) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap1, snap2],
            preCheckStatus: TxStatus.InFlight,
            postSweepStatus: TxStatus.Committed);

        await grain.InitiateSplitStateAsync(0);

        // Cleanup terminal must carry BOTH keys' values in the
        // committedValues payload - per-txid aggregation is the
        // load-bearing invariant for the destination's backstop.
        await target.Received(1).AppendTxTerminalAsync(
            txid,
            committed: true,
            Arg.Is<IReadOnlyDictionary<string, byte[]>>(d =>
                d != null
                && d.Count == 2
                && d[snap1.Key].SequenceEqual(snap1.Value!)
                && d[snap2.Key].SequenceEqual(snap2.Value!)));
    }
}
