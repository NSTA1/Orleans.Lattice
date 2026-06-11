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
/// Regression tests for the post-fan-out participant re-fetch loop in
/// <c>AtomicWriteGrain.BroadcastTerminalsAsync</c>. A concurrent
/// <c>TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync</c>
/// can register a destination shard as a participant after the saga's
/// main terminal fan-out and before <c>ForgetAsync</c>, leaving the
/// destination with an orphaned <c>_pendingTx</c> bucket whose
/// <c>Decisions[txid] = Committed</c> in the registry - a reader
/// routed to the destination would resolve pending status to
/// Committed and surface the pre-saga value indefinitely. The
/// post-fan-out loop is the saga's *sole* registry participant
/// discovery path: it re-fetches participants up to a bounded cap and
/// drains any late arrivals before the saga calls <c>ForgetAsync</c>
/// in the cleanup phase. The pre-fan-out registry participant *union*
/// that used to run before the main fan-out was removed - it is
/// subsumed by this loop's round-0 fetch (which runs after fan-out and
/// therefore observes a superset of what the pre-fan-out union saw),
/// saving one <see cref="ITxRegistryGrain.GetParticipantsAsync"/>
/// round-trip per saga.
/// </summary>
public partial class AtomicWriteGrainTests
{
    /// <summary>
    /// Builds an <see cref="AtomicWriteGrain"/> wired to an explicit
    /// <see cref="ITxRegistryGrain"/> substitute so tests can sequence
    /// the registry's participant set across successive
    /// <c>GetParticipantsAsync</c> calls. Mirrors the production
    /// dependency graph closely enough to exercise
    /// <c>BroadcastTerminalsAsync</c>'s post-fan-out late-pickup loop
    /// on a single shard substitute that accepts every
    /// <see cref="IShardRootGrain"/> grain id (so call counts on
    /// <c>AppendTxTerminalAsync</c> measure total terminal fan-out).
    /// </summary>
    private static (AtomicWriteGrain grain,
                     FakePersistentState<AtomicWriteState> state,
                     IShardRootGrain shard,
                     ITxRegistryGrain registry) CreateGrainWithRegistry()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("atomic-write", $"{TreeId}/{OperationId}"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var shard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
        shard.GetRawEntryAsync(Arg.Any<string>())
            .Returns(Task.FromResult<LwwEntry?>(null));
        // Shard-batched pre-saga capture: PrepareAsync fans out one
        // GetRawEntriesAsync per shard bucket. Mirror the single-key
        // mock's "absent" semantics by returning an aligned list of
        // nulls so every key reads as a fresh insert.
        shard.GetRawEntriesAsync(Arg.Any<List<string>>())
            .Returns(call => Task.FromResult(
                Enumerable.Repeat<LwwEntry?>(null, call.Arg<List<string>>().Count).ToList()));
        // TerminalFanOutResolver.ResolveTransitiveAsync expands each
        // seed shard via this RPC; the default empty list keeps the
        // BFS pass trivial (the resolver still returns the seed set).
        shard.GetSplitForwardTargetsAsync()
            .Returns(Task.FromResult(new List<int>()));

        var registry = Substitute.For<ITxRegistryGrain>();
        grainFactory.GetGrain<ITxRegistryGrain>(TreeId).Returns(registry);

        var routing = new RoutingInfo(
            TreeId,
            ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount));
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>()).Returns(routing);
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(routing);

        var reminderRegistry = Substitute.For<IReminderRegistry>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var opts = new LatticeOptions();
        optionsMonitor.CurrentValue.Returns(opts);
        optionsMonitor.Get(Arg.Any<string>()).Returns(opts);

        var state = new FakePersistentState<AtomicWriteState>();
        var grain = new AtomicWriteGrain(
            context,
            grainFactory,
            reminderRegistry,
            optionsMonitor,
            new LoggerFactory().CreateLogger<AtomicWriteGrain>(),
            state);
        return (grain, state, shard, registry);
    }

    [Test]
    public async Task BroadcastTerminals_no_late_arrivals_exits_after_one_refetch()
    {
        // Registry reports a stable empty participant set. The late-
        // pickup loop is the saga's sole registry-discovery path: its
        // round-0 re-fetch (after the main fan-out) sees an empty set
        // and breaks immediately, so GetParticipantsAsync is called
        // exactly once.
        var (grain, state, shard, registry) = CreateGrainWithRegistry();
        registry.GetParticipantsAsync(Arg.Any<Guid>())
            .Returns(Task.FromResult<IReadOnlyList<int>>(new List<int>()));

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        await registry.Received(1).GetParticipantsAsync(Arg.Any<Guid>());
        // One terminal RPC for the single TouchedShard; the late-pickup
        // loop never fires an additional terminal because no new
        // participant arrived.
        await shard.Received(1).AppendTxTerminalAsync(
            Arg.Any<Guid>(),
            true,
            Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>(),
            Arg.Any<bool>());
    }

    [Test]
    public async Task BroadcastTerminals_late_arrival_fires_second_terminal()
    {
        // Registry surfaces a brand-new shard (shard index 99) that
        // wasn't in TouchedShards. The late-pickup loop's round-0
        // fetch (after the main fan-out) sees it, transitively expands
        // it (empty SplitForwardTargets keeps the expansion trivial)
        // and fires a second AppendTxTerminalAsync for it. The follow-
        // up re-fetch returns the same set, finds no new arrivals and
        // the loop breaks.
        var (grain, state, shard, registry) = CreateGrainWithRegistry();
        registry.GetParticipantsAsync(Arg.Any<Guid>())
            .Returns(Task.FromResult<IReadOnlyList<int>>(new List<int> { 99 }));

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        // Two terminal RPCs: one for the original TouchedShard (key's
        // routing target) and one for the late-arrived shard 99.
        await shard.Received(2).AppendTxTerminalAsync(
            Arg.Any<Guid>(),
            true,
            Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>(),
            Arg.Any<bool>());
        // Round-0 fetch (sees late arrival, fans out) + round-1 re-
        // fetch (stable, no new arrivals, breaks) = 2 calls.
        await registry.Received(2).GetParticipantsAsync(Arg.Any<Guid>());
        // TouchedShards is persisted to include shard 99 so a
        // crash-resume picks up the same closure.
        Assert.That(state.State.TouchedShards, Does.Contain(99),
            "Late-arrived shard must be persisted into TouchedShards.");
    }

    [Test]
    public async Task BroadcastTerminals_continuous_late_arrivals_terminate_at_cap()
    {
        // Registry returns a progressively-growing participant set on
        // every call: each re-fetch sees one MORE late shard. The
        // late-pickup loop must terminate at the MaxLateRefetchRounds
        // cap (5) and not retry forever. Total GetParticipantsAsync
        // calls = 5 re-fetches (the loop is the sole discovery path;
        // there is no separate pre-fan-out union). Without the cap the
        // loop would run forever under this stub.
        var (grain, _, shard, registry) = CreateGrainWithRegistry();
        var calls = 0;
        registry.GetParticipantsAsync(Arg.Any<Guid>())
            .Returns(_ =>
            {
                calls++;
                var participants = new List<int>();
                for (var i = 0; i < calls; i++)
                    participants.Add(100 + i);
                return Task.FromResult<IReadOnlyList<int>>(participants);
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        await registry.Received(5).GetParticipantsAsync(Arg.Any<Guid>());
        // Verify the loop made forward progress: more than one terminal
        // RPC fired across the initial fan-out + late-pickup rounds.
        await shard.Received().AppendTxTerminalAsync(
            Arg.Any<Guid>(),
            true,
            Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>(),
            Arg.Any<bool>());
    }

    [Test]
    public async Task BroadcastTerminals_late_pickup_persists_touched_shards_each_round()
    {
        // The loop persists state.State.TouchedShards after each round
        // so a crash-resume picks up the same expanded closure without
        // re-running the participant fetch. Verify TouchedShards
        // contains the union of all rounds' new arrivals after the
        // loop terminates.
        var (grain, state, _, registry) = CreateGrainWithRegistry();
        var calls = 0;
        registry.GetParticipantsAsync(Arg.Any<Guid>())
            .Returns(_ =>
            {
                calls++;
                // Round 0 (call 1): late-pickup sees [55]. Round 1
                // (call 2): sees [55, 56]. Round 2 (call 3): stable
                // [55, 56] -> no new arrivals -> break.
                return Task.FromResult<IReadOnlyList<int>>(calls switch
                {
                    1 => new List<int> { 55 },
                    _ => new List<int> { 55, 56 },
                });
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        Assert.That(state.State.TouchedShards, Does.Contain(55),
            "First late arrival must be persisted into TouchedShards.");
        Assert.That(state.State.TouchedShards, Does.Contain(56),
            "Second late arrival must be persisted into TouchedShards.");
        // Round 0 [55] + round 1 [55,56] + round 2 stable (breaks) = 3 calls.
        await registry.Received(3).GetParticipantsAsync(Arg.Any<Guid>());
    }
}
