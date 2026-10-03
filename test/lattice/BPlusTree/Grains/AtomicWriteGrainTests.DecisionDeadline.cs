using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for a batch committed after its caller was answered with
/// a failure. A silo restart failed the first attempt of a saga after most of
/// the caller's response timeout; the routing tier's transient retry re-issued
/// the saga after its backoff, by which time the caller had timed out and
/// written the same keys again, and the re-issued saga committed - so the newer
/// batch read back at the older batch's values. A saga now refuses to record a
/// commit decision past the deadline of the caller that last entered it, and
/// rolls the batch back instead, unless the commit was already recorded.
/// </summary>
public partial class AtomicWriteGrainTests
{
    private static IDisposable CallerDeadline(TimeSpan fromNow) =>
        LatticeSagaDecisionDeadlineContext.With((DateTime.UtcNow + fromNow).Ticks);

    [Test]
    public async Task ExecuteAsync_rolls_back_when_the_caller_deadline_passes_before_the_commit_decision()
    {
        var (grain, state, lattice, shard, registry) = CreateGrainForDeadline();
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(async _ => await Task.Delay(TimeSpan.FromMilliseconds(400)));

        InvalidOperationException? failure;
        using (CallerDeadline(TimeSpan.FromMilliseconds(150)))
        {
            failure = Assert.ThrowsAsync<InvalidOperationException>(
                () => grain.ExecuteAsync(TreeId, MakeEntries(("k", [1]))));
        }

        Assert.That(failure!.Message, Does.Contain("rolled back"));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        await registry.Received(1).MarkAbortedAsync(Arg.Any<Guid>());
        await registry.DidNotReceive().MarkCommittedAsync(Arg.Any<Guid>());
        await AssertOnlyTerminalVerdict(shard, committed: false);
    }

    [Test]
    public async Task ExecuteAsync_commits_when_the_decision_lands_before_the_caller_deadline()
    {
        var (grain, state, _, shard, registry) = CreateGrainForDeadline();

        using (CallerDeadline(TimeSpan.FromMinutes(1)))
        {
            await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));
        }

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        Assert.That(state.State.FailureMessage, Is.Null);
        await registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());
        await AssertOnlyTerminalVerdict(shard, committed: true);
    }

    [Test]
    public async Task ExecuteAsync_persists_the_caller_deadline_with_the_saga()
    {
        var (grain, state, _, _, _) = CreateGrainForDeadline();
        var deadline = DateTime.UtcNow.AddMinutes(1).Ticks;
        long persisted = 0;
        state.OnWriteState = s => persisted = persisted == 0 ? s.DecideByUtcTicks : persisted;

        using (LatticeSagaDecisionDeadlineContext.With(deadline))
        {
            await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));
        }

        Assert.That(persisted, Is.EqualTo(deadline),
            "The deadline must reach storage with the saga's first write, so a resume reads it.");
    }

    [Test]
    public async Task ReceiveReminder_rolls_back_an_undecided_saga_past_its_caller_deadline()
    {
        var (grain, state, lattice, shard, registry) = CreateGrainForDeadline(UndecidedExecuteState());
        registry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(TxStatus.InFlight));

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        Assert.That(state.State.FailureMessage, Does.Contain("was due by"));
        await registry.Received(1).MarkAbortedAsync(Arg.Any<Guid>());
        await registry.DidNotReceive().MarkCommittedAsync(Arg.Any<Guid>());
        await AssertOnlyTerminalVerdict(shard, committed: false);
        await lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task ReceiveReminder_finishes_a_saga_whose_commit_was_already_recorded()
    {
        // A crash between the commit decision and completion: the decision made
        // the batch visible, so the late resume must deliver it, not undo it.
        var (grain, state, _, shard, registry) = CreateGrainForDeadline(UndecidedExecuteState());
        registry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(TxStatus.Committed));

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        Assert.That(state.State.FailureMessage, Is.Null);
        await registry.DidNotReceive().MarkAbortedAsync(Arg.Any<Guid>());
        await AssertOnlyTerminalVerdict(shard, committed: true);
    }

    [Test]
    public async Task ExecuteAsync_reissued_by_a_caller_with_a_fresh_deadline_resumes_and_commits()
    {
        // A caller re-issuing its operation id waits afresh, so the earlier
        // caller's lapsed deadline no longer applies.
        var (grain, state, _, shard, registry) = CreateGrainForDeadline(UndecidedExecuteState());

        using (CallerDeadline(TimeSpan.FromMinutes(1)))
        {
            await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));
        }

        Assert.That(state.State.FailureMessage, Is.Null);
        await registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());
        await AssertOnlyTerminalVerdict(shard, committed: true);
    }

    [Test]
    public async Task ReceiveReminder_does_not_roll_back_a_saga_without_a_caller_deadline()
    {
        // Sagas entered by a caller that supplied no deadline keep the
        // resume-and-commit behaviour.
        var undecided = UndecidedExecuteState();
        undecided.State.DecideByUtcTicks = 0;
        var (grain, state, _, shard, registry) = CreateGrainForDeadline(undecided);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        Assert.That(state.State.FailureMessage, Is.Null);
        await registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());
        await AssertOnlyTerminalVerdict(shard, committed: true);
    }

    /// <summary>
    /// A saga persisted in <see cref="AtomicWritePhase.Execute"/> with every
    /// write staged and no decision recorded in this activation, whose caller's
    /// deadline lapsed a minute ago.
    /// </summary>
    private static FakePersistentState<AtomicWriteState> UndecidedExecuteState()
    {
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Execute;
        state.State.TreeId = TreeId;
        state.State.Entries = MakeEntries(("k", [1]));
        state.State.PreValues = [new AtomicPreValue { Key = "k", Value = null, Existed = false }];
        state.State.NextIndex = 1;
        state.State.TransactionId = Guid.NewGuid();
        state.State.TouchedShards = [0];
        state.State.KeyFingerprint = ComputeKeyFingerprintForTest(["k"]);
        state.State.DecideByUtcTicks = DateTime.UtcNow.AddMinutes(-1).Ticks;
        return state;
    }

    /// <summary>
    /// Asserts the saga broadcast terminals carrying only
    /// <paramref name="committed"/>'s verdict (a resumed saga re-derives its
    /// touched shards, so the number of terminal RPCs is not the point here).
    /// </summary>
    private static async Task AssertOnlyTerminalVerdict(IShardRootGrain shard, bool committed)
    {
        await shard.Received().AppendTxTerminalAsync(
            Arg.Any<Guid>(), Arg.Is(committed), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>(), Arg.Any<bool>());
        await shard.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(), Arg.Is(!committed), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>(), Arg.Any<bool>());
    }

    private static byte[] ComputeKeyFingerprintForTest(string[] keys) =>
        AtomicWriteGrain.ComputeKeyFingerprint(keys.Select(k => new KeyValuePair<string, byte[]>(k, [1])).ToList());

    private static (AtomicWriteGrain Grain,
                    FakePersistentState<AtomicWriteState> State,
                    ILattice Lattice,
                    IShardRootGrain Shard,
                    ITxRegistryGrain Registry) CreateGrainForDeadline(FakePersistentState<AtomicWriteState>? existing = null)
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetParticipantsAsync(Arg.Any<Guid>())
            .Returns(Task.FromResult<IReadOnlyList<int>>(new List<int>()));

        var (grain, state, _, lattice, shard) = CreateGrain(
            existing,
            configureFactory: f => f.GetGrain<ITxRegistryGrain>(TreeId).Returns(registry));
        shard.GetSplitForwardTargetsAsync().Returns(Task.FromResult(new List<int>()));
        return (grain, state, lattice, shard, registry);
    }
}
