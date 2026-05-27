using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Universal cross-cluster atomic-visibility acceptance fixture for the
/// compensation-side reader-isolation invariant: a saga whose execute
/// phase fails mid-batch must (a) emit a
/// <see cref="MutationKind.TxAbort"/> terminal mark to every touched
/// shard so the leaves can drop the matching pending entries from
/// <c>_pendingTx</c>, and (b) leave a continuous reader observing the
/// pre-saga state for every key throughout the rollback.
/// <para>
/// The continuous-reader half of the rollback invariant is structurally
/// subsumed by the topology fixtures (<c>ShardSplitTopologyTests</c>,
/// <c>ResizeTopologyTests</c>, <c>ReshardTopologyTests</c>)
/// because prepared mutations route into <c>_pendingTx</c> rather than
/// <c>Entries</c>: every <c>fullPrePolls</c> count those fixtures emit
/// is itself a witness that readers never see prepared-but-uncommitted
/// state, regardless of whether the saga ultimately commits or rolls
/// back. The TxAbort fan-out is the genuinely novel assertion - without
/// it, a saga rollback would orphan pending entries on every touched
/// shard, breaking convergence on the abandoned writes - so it is
/// covered here as a focused unit test against the saga grain's
/// terminal-broadcast path.
/// </para>
/// </summary>
[TestFixture]
public class CompensationContinuousReaderTests
{
    private const string TreeId = "comp-tree";
    private const string OperationId = "op-comp";

    private static (AtomicWriteGrain grain,
                     FakePersistentState<AtomicWriteState> state,
                     IReminderRegistry reminderRegistry,
                     ILattice lattice,
                     IShardRootGrain shard) CreateGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("atomic-write", $"{TreeId}/{OperationId}"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);

        // Single shared shard substitute - every grainFactory.GetGrain<IShardRootGrain>(...)
        // returns it, so every per-shard fan-out call (one per distinct
        // physical shard the touched-set resolves to) lands on the same
        // mock and is observable via Received().
        var shard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));

        // The production saga now issues a single batched
        // GetRawEntriesAsync per shard rather than one GetRawEntryAsync
        // per key. Stub the batched call to delegate to the existing
        // per-key GetRawEntryAsync mock so this fixture keeps relying
        // on the single-key default (null = pre-saga absent) without
        // having to enumerate per-key responses.
        shard.GetRawEntriesAsync(Arg.Any<List<string>>())
            .Returns(async callInfo =>
            {
                var keys = (List<string>)callInfo[0];
                var results = new List<LwwEntry?>(keys.Count);
                foreach (var key in keys)
                {
                    var entry = await shard.GetRawEntryAsync(key);
                    results.Add(entry);
                }
                return results;
            });

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
        return (grain, state, reminderRegistry, lattice, shard);
    }

    private static List<KeyValuePair<string, byte[]>> MakeEntries(params (string, byte[])[] pairs)
    {
        var list = new List<KeyValuePair<string, byte[]>>();
        foreach (var (k, v) in pairs)
            list.Add(new KeyValuePair<string, byte[]>(k, v));
        return list;
    }

    /// <summary>
    /// On compensation, every distinct physical shard the saga touched
    /// must receive an <see cref="IShardRootGrain.AppendTxTerminalAsync"/>
    /// call with <c>committed: false</c>. Without this fan-out, prepared
    /// pending entries on those shards' leaves would orphan in
    /// <c>_pendingTx</c> and never converge.
    /// </summary>
    [Test]
    public async Task Compensation_broadcasts_TxAbort_to_every_touched_shard()
    {
        var (grain, state, _, lattice, shard) = CreateGrain();
        // Inject a deterministic mid-batch failure on the per-shard
        // bucket containing key "b". Phase D1b (c2-x memo): the saga
        // pre-buckets entries by shard and issues one SetManyAsync per
        // touched shard, so we throw when the call's slice contains
        // "b". This preserves the original test intent: a mid-saga
        // failure must surface to the caller and Compensate must fan
        // an abort terminal to every touched shard.
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(callInfo =>
            {
                var slice = (List<KeyValuePair<string, byte[]>>)callInfo[0];
                foreach (var entry in slice)
                {
                    if (entry.Key == "b")
                    {
                        throw new InvalidOperationException("simulated mid-batch failure");
                    }
                }
                return Task.CompletedTask;
            });

        try
        {
            await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2]), ("c", [3])));
            Assert.Fail("Saga must surface the injected failure.");
        }
        catch (InvalidOperationException ex)
        {
            Assert.That(ex.Message, Does.Contain("simulated mid-batch failure"));
        }

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed),
            "Saga must run to terminal Completed even after compensation.");

        var touchedCount = state.State.TouchedShards.Count;
        Assert.That(touchedCount, Is.GreaterThan(0),
            "Saga must have populated TouchedShards during prepare so the abort can fan out.");

        // The substitute is shared across every per-shard grain id, so
        // one Received call lands per distinct touched shard.
        await shard.Received(touchedCount).AppendTxTerminalAsync(
            Arg.Any<Guid>(),
            committed: false,
            Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>());

        // Defensive: no committed terminal must escape on the rollback
        // path - the abort and commit terminals are mutually exclusive
        // for a single saga.
        await shard.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(),
            committed: true,
            Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// On the happy path, every touched shard receives a
    /// <see cref="MutationKind.TxCommit"/> terminal - included here so
    /// the abort fan-out assertion above is paired with its complement
    /// and a regression on either side fails this fixture.
    /// </summary>
    [Test]
    public async Task Successful_saga_broadcasts_TxCommit_to_every_touched_shard()
    {
        var (grain, state, _, lattice, shard) = CreateGrain();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2]), ("c", [3])));

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));

        var touchedCount = state.State.TouchedShards.Count;
        Assert.That(touchedCount, Is.GreaterThan(0));

        await shard.Received(touchedCount).AppendTxTerminalAsync(
            Arg.Any<Guid>(),
            committed: true,
            Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>());
        await shard.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(),
            committed: false,
            Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Empty-batch sagas don't touch any shard and therefore must emit
    /// no terminal - closing the corner case where a fan-out loop on
    /// <c>TouchedShards</c> with zero entries could still issue a stray
    /// call against the routing's default shard.
    /// </summary>
    [Test]
    public async Task Empty_saga_broadcasts_no_terminal()
    {
        var (grain, _, _, _, shard) = CreateGrain();

        await grain.ExecuteAsync(TreeId, MakeEntries());

        await shard.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(),
            Arg.Any<bool>(),
            Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>());
    }
}
