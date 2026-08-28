using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the etag race that the real-Azure U9g ladder
/// surfaced. When
/// <see cref="IShardRootGrain.SetManyAsync"/> is marked
/// <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/>, two concurrent batches whose
/// leaf returns a non-null <see cref="SplitResult"/> both enter
/// <c>PromoteRootAsync</c>, both mutate <c>state.State.PendingPromotion</c>,
/// and both call <c>state.WriteStateAsync()</c>. The second writer observes
/// a stale etag and the silo log reports
/// <c>"Etag mismatch during Update for grain shardroot/..."</c>. This
/// fixture pins the contract that interleaved <c>SetManyAsync</c> calls
/// must not produce <see cref="InconsistentStateException"/> on the
/// shard-root persistent state.
/// <para>
/// The test uses the etag-simulation mode on
/// <see cref="FakePersistentState{T}"/> to make the race observable in a
/// single-process unit test: every <c>WriteStateAsync</c> yields once
/// mid-flight, so two concurrent writes that touch the same
/// <c>[PersistentState]</c> instance reproducibly race on the etag.
/// </para>
/// </summary>
public sealed class ShardRootGrainSetManyInterleavedWritesTests
{
    private const string TreeId = "tree";
    private const int ShardIndex = 0;

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required IBPlusInternalGrain Internal { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    private static Harness CreateHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{ShardIndex}"));

        var state = new FakePersistentState<ShardRootState>
        {
            SimulateEtagChecks = true,
        };
        state.State.RootNodeId = GrainId.Create("leaf", "root-leaf");
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        // Every per-leaf batched apply returns a SplitResult so the
        // post-dispatch loop in SetManyLocalOnlyAsync drives
        // PromoteRootAsync, which is the actual hot WriteStateAsync the
        // U9g real-Azure ladder caught racing across interleaved turns.
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(_ => Task.FromResult<SplitResult?>(new SplitResult
            {
                PromotedKey = "split-sep",
                NewSiblingId = GrainId.Create("leaf", "new-sibling"),
            }));
        leaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        // PromoteRootAsync creates a new internal-grain root, calls
        // SetTreeIdAsync + InitializeAsync, then re-writes the shard
        // root state. The substitute does not need to be a fully
        // functional grain reference: the etag race triggers on the
        // first WriteStateAsync inside PromoteRootAsync (line 677),
        // which fires BEFORE CompletePromotionAsync reaches
        // `GetGrainId()` - so a basic NSubstitute proxy is enough for
        // the path we care about.
        var @internal = Substitute.For<IBPlusInternalGrain>();
        @internal.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        @internal.InitializeAsync(Arg.Any<string>(), Arg.Any<GrainId>(), Arg.Any<GrainId>(), Arg.Any<bool>())
            .Returns(Task.CompletedTask);
        @internal.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>())
            .Returns(Task.FromResult<SplitResult?>(null));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(@internal);

        var cache = Substitute.For<ILeafCacheGrain>();
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        var shadowTarget = Substitute.For<IShardRootGrain>();
        shadowTarget.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(Task.CompletedTask);
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shadowTarget);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());

        return new Harness { Grain = grain, Leaf = leaf, Internal = @internal, State = state };
    }

    [Test]
    public async Task Two_concurrent_SetManyAsync_calls_do_not_race_the_shard_root_etag()
    {
        // Drive two SetManyAsync calls in parallel against a single
        // ShardRootGrain activation, both producing a non-null SplitResult
        // from the leaf so each turn enters PromoteRootAsync and writes
        // the shard-root state at line 677 (`state.WriteStateAsync()`
        // for the PendingPromotion intent). Under [AlwaysInterleave] the
        // two turns interleave around the FakePersistentState.WriteStateAsync
        // Task.Yield, so without a per-activation write-serialisation
        // primitive the second writer sees a stale etag and throws
        // InconsistentStateException - exactly the silo-log signal U9g
        // captured on real Azure Tables.
        // <para>
        // The canonical signal is `EtagConflictCount` on the fake state,
        // not the exception shape: `PromoteRootAsync` does not retry the
        // failed write, and the downstream `CompletePromotionAsync` call
        // exercises `GetGrainId()` on the grain-factory substitute, which
        // is not a real Orleans GrainReference and is allowed to throw
        // ArgumentException in the unit harness. Only the conflict count
        // is load-bearing for U9h regression coverage.
        // </para>
        var h = CreateHarness();

        var batchA = new List<KeyValuePair<string, byte[]>>
        {
            new("a0", [0]),
            new("a1", [1]),
        };
        var batchB = new List<KeyValuePair<string, byte[]>>
        {
            new("b0", [0]),
            new("b1", [1]),
        };

        try
        {
            // Use Task.Run so both calls genuinely run on the thread
            // pool. Without this, both `SetManyAsync` invocations
            // execute synchronously up to (and through) the first
            // `state.WriteStateAsync()` because every substitute
            // returns a completed task - so the second call never
            // begins until the first has fully returned. `Task.Run`
            // forces them onto separate continuation chains, allowing
            // the FakePersistentState rendezvous to actually meet two
            // concurrent writers and the etag CAS to fire.
            await Task.WhenAll(
                Task.Run(() => h.Grain.SetManyAsync(batchA)),
                Task.Run(() => h.Grain.SetManyAsync(batchB)));
        }
        catch
        {
            // Swallowed deliberately - see the XML doc above. The
            // FakePersistentState etag throw and the harness-only
            // GetGrainId() ArgumentException are both expected exits
            // during the racing window; the assertion is on the
            // EtagConflictCount counter, which is the real regression
            // signal.
        }

        Assert.That(h.State.EtagConflictCount, Is.EqualTo(0),
            "Interleaved SetManyAsync calls raced WriteStateAsync on the shard root - U9h fix has regressed.");
    }
}
