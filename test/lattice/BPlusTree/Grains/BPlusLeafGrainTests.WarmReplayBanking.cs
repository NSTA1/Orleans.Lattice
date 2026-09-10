using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the WARM arm of the cancelled-replay livelock: a
/// warm tail replay that is cancelled inside the checkpoint coalescing window
/// discards its entire pending checkpoint advance, so the next activation
/// re-enters replay at the identical persisted offset and the leaf never
/// converges.
/// <para>
/// The cold arm has had a rescue since issue #2280
/// (<c>TryBankColdReplayProgressAsync</c>); the warm arm had none, which is why
/// the field failure is 100% warm. <c>ReplayPartitionAsync</c> calls
/// <c>TryFlushRecoveredCeilingAsync</c> at every slice boundary, but the
/// <c>SetCheckpointOffsetAsync</c> underneath it is NOT a durable write: it
/// records the advance in the in-memory pending map and defers the persist
/// behind <see cref="LatticeOptions.MaterialiserCheckpointInterval"/> (whose
/// clock restarts at every activation) and
/// <see cref="LatticeOptions.MaterialiserCheckpointEntries"/>. An activation
/// cancelled while that window is open - which is what runtime idle collection
/// (<c>DeactivationReasonCode.RuntimeRequested</c>) produces, at volume - loses
/// the whole advance.
/// </para>
/// <para>
/// The graceful hooks cannot cover it: Orleans does not run
/// <c>OnDeactivateAsync</c> when <c>OnActivateAsync</c> throws, and a cancelled
/// replay leaves activation BY throwing. The cancellation catch inside
/// <c>ReplayWalSinceCheckpointAsync</c> is the only reachable banking point.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string WarmBankTreeId = "tree-warm-bank";

    /// <summary>
    /// Persisted checkpoint the warm activation enters replay from. The replay
    /// covers <c>(10, head]</c>, so every offset it absorbs is a genuine forward
    /// advance that a monotonic checkpoint can express - unlike the cold arm,
    /// where the re-read sits below the checkpoint.
    /// </summary>
    private const long WarmBankEntryCheckpoint = 10L;

    /// <summary>Highest offset the cancelled warm replay applies.</summary>
    private const long WarmBankAppliedCeiling = 30L;

    [Test]
    public async Task Cancelled_warm_replay_persists_the_pending_checkpoint_advance()
    {
        // CORE REGRESSION. The warm replay absorbs one slice, advancing its
        // pending checkpoint from 10 to 30, and is then cancelled. The
        // coalescing window is deliberately wide open (a five-minute interval
        // against a run measured in milliseconds, and three entries against a
        // 5000-entry threshold), so nothing has forced a durable write.
        //
        // RED (pre-fix): the pending advance dies with the activation. The
        // persisted checkpoint is still 10, so the NEXT activation re-enters
        // replay at 10 - the stalled replay that
        // orleans.lattice.leaf.activation_stalled_replays counts, and for as
        // long as it repeats the writes routed to this leaf are lost.
        //
        // GREEN (post-fix): the advance is persisted, so the next activation
        // enters at 30 and the replay makes durable forward progress.
        var (state, _) = await RunCancelledWarmReplayAsync();

        Assert.That(state.State.ProjectionCheckpointOffsetsByPartition, Is.Not.Null);
        Assert.That(state.State.ProjectionCheckpointOffsetsByPartition![0], Is.EqualTo(WarmBankAppliedCeiling),
            "a warm replay cancelled mid-flight MUST persist the checkpoint advance it already absorbed. " +
            "Leaving it at the entry offset (10) means the next activation re-enters replay from the " +
            "identical offset, which is precisely the stalled replay the leaf-activation diagnostic " +
            "reports - and while it repeats, the writes routed to this leaf are being lost");
    }

    [Test]
    public async Task Cancelled_warm_replay_advances_the_offset_the_next_activation_enters_from()
    {
        // THE STALL, STATED AS THE FIELD OBSERVES IT. The counter does not
        // measure a checkpoint value; it measures whether consecutive replays
        // enter from the SAME offset. Asserting the inequality directly means
        // this test keeps its meaning even if the ceiling arithmetic changes,
        // and it is the property the fix actually has to deliver.
        var (state, _) = await RunCancelledWarmReplayAsync();

        Assert.That(state.State.ProjectionCheckpointOffsetsByPartition![0],
            Is.GreaterThan(WarmBankEntryCheckpoint),
            "consecutive replays that enter from the same persisted offset are the definition of a stalled " +
            "replay. The cancelled activation absorbed real entries, so the offset the next activation " +
            "enters from MUST be strictly greater than the one this activation entered from");
    }

    [Test]
    public async Task Cancelled_warm_replay_banks_snapshot_coverage_for_the_persisted_advance()
    {
        // THE OTHER HALF OF THE PAIR, AND THE REASON IT IS NOT OPTIONAL.
        // Persisting the checkpoint alone would be safe but self-defeating: the
        // entry cache is per-activation and is never persisted, so a checkpoint
        // advanced past the snapshot that hydrated it makes the NEXT activation
        // elect the -1 cold-rebuild override instead of rehydrating. That trades
        // a warm stall for a cold rebuild of the whole readable window rather
        // than fixing anything. Capturing the covering snapshot is what makes
        // the advance RESUMABLE, and it is exactly the pair OnDeactivateAsync
        // banks on the graceful path.
        var (_, banked) = await RunCancelledWarmReplayAsync();

        Assert.That(banked, Is.Not.Null,
            "the persisted checkpoint advance MUST be paired with snapshot coverage that backs it, or the " +
            "next activation cannot resume warm from it");
        Assert.That(banked!.SnapshotOffsetsByPartition, Is.Not.Null);
        Assert.That(banked.SnapshotOffsetsByPartition![0], Is.EqualTo(WarmBankAppliedCeiling),
            "the banked coverage MUST match the persisted checkpoint. Coverage below it would leave the " +
            "next activation unable to rehydrate; coverage above it would claim a prefix the cache does " +
            "not back, and the durable pin is min(checkpoint, covered), so an over-claim authorises the " +
            "WAL GC to trim data that has no other durable copy");
    }

    /// <summary>
    /// Drives one WARM activation that absorbs a single slice up to
    /// <see cref="WarmBankAppliedCeiling"/> and is then cancelled while the
    /// checkpoint coalescing window is still open, and returns the leaf's
    /// persisted state together with whatever snapshot blob it banked
    /// (<see langword="null"/> when it banked nothing).
    /// </summary>
    private static async Task<(FakePersistentState<LeafNodeState> State, LeafSnapshotBlob? Banked)>
        RunCancelledWarmReplayAsync()
    {
        var snapshotState = new FakePersistentState<LeafSnapshotBlob>();
        var store = new LeafSnapshotStorageGrain(Substitute.For<IGrainContext>(), snapshotState);

        using var cts = new CancellationTokenSource();

        // One slice of forward applies, then a cancelled read. Both offsets sit
        // ABOVE the entry checkpoint, so the ceiling they produce is a genuine
        // monotonic advance - the quantity the pre-fix code holds only in
        // memory and then discards.
        var slice = new List<CommitLogSliceEntry>
        {
            new(20L, BuildCommittedSet("warm-a", Encoding.UTF8.GetBytes("v-a"), treeId: WarmBankTreeId)),
            new(WarmBankAppliedCeiling, BuildCommittedSet("warm-b", Encoding.UTF8.GetBytes("v-b"), treeId: WarmBankTreeId)),
        };

        var reads = 0;
        var partition0 = Substitute.For<ILeafReplayCoordinatorGrain>();
        partition0.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(90L));
        partition0.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        partition0.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (Interlocked.Increment(ref reads) == 1)
                {
                    return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(slice);
                }

                // The teardown the field sees: the runtime cancels the
                // activation as idle while the replay is still walking the
                // window, long before the coalescing window would have forced
                // a durable checkpoint write.
                cts.Cancel();
                throw new OperationCanceledException(cts.Token);
            });

        var leaf = CreateWarmBankLeaf(store, partition0);

        // Seed the entry cache so activation elects the WARM path. Step 0.5
        // takes the -1 cold-rebuild override only when the cache starts empty
        // and unhydrated; a populated cache means the persisted checkpoint is by
        // definition coherent with it, which is the state a snapshot rehydrate
        // produces in the field.
        leaf.Grain.EntriesForTest["warm-seed"] = new LwwValue<byte[]>
        {
            Value = new byte[] { 1 },
            Timestamp = new HybridLogicalClock { WallClockTicks = 1 },
        };

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await ((IGrainBase)leaf.Grain).OnActivateAsync(cts.Token),
            "precondition: the warm activation is torn down by a cancellation, which is the shape the " +
            "field reports for 100% of this leaf's activation failures");

        return (leaf.State, await store.LoadAsync(default));
    }

    /// <summary>
    /// Builds a single-partition leaf whose checkpoint coalescing window is
    /// deliberately wide open, so a cancellation lands while the pending
    /// advance is still held only in memory. Single-partition on purpose: with
    /// no unreached partition there is no partition whose coverage claim the
    /// test would have to reason about separately, so every assertion is about
    /// the offsets this replay actually absorbed.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateWarmBankLeaf(
        ILeafSnapshotStorageGrain snapshotStore,
        ILeafReplayCoordinatorGrain partition0)
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Any<IReadOnlyList<MaterialiserPinReport>>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStore);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(partition0);

        var sc = new ServiceCollection();
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = WarmBankTreeId;
        state.State.ProjectionCheckpointOffset = WarmBankEntryCheckpoint;
        state.State.ProjectionCheckpointOffsetsByPartition = new[] { WarmBankEntryCheckpoint };

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,

                // The defect condition, stated explicitly rather than inherited
                // from the defaults: the interval is far longer than the run and
                // the entry threshold far above the slice, so NOTHING forces a
                // durable checkpoint write before the cancellation lands. The
                // cold-arm suite sets TimeSpan.Zero here, which persists on every
                // entry and would mask this defect entirely.
                MaterialiserCheckpointInterval = TimeSpan.FromMinutes(5),
                MaterialiserCheckpointEntries = 5_000,
                ProjectionRebuildPolicy = ProjectionRebuildPolicy.Fail,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        return (
            new BPlusLeafGrain(
                context, state, grainFactory, optionsResolver,
                TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default()),
            state);
    }
}