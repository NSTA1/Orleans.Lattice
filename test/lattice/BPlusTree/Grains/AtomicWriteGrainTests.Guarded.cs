using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class AtomicWriteGrainTests
{
    // --- ExecuteGuardedAsync (guarded atomic batch) ---

    private sealed record Scored(int Score);

    private static byte[] ScoredJson(int score) => Encoding.UTF8.GetBytes($"{{\"Score\":{score}}}");

    private static LatticePredicateNode ScoreAtLeast(int threshold) =>
        LatticePredicatePushdown.Compile<Scored>(
            s => s.Score >= threshold, JsonLatticeSerializer<Scored>.Default);

    /// <summary>
    /// Stubs the pre-saga snapshot for <paramref name="key"/> to a JSON document
    /// carrying <paramref name="score"/> so the guard predicate can be evaluated
    /// against it. A <see langword="null"/> score stubs an absent key.
    /// </summary>
    private static void StubScored(IShardRootGrain shard, string key, int? score)
    {
        if (score is null)
        {
            shard.GetRawEntryAsync(key).Returns(Task.FromResult<LwwEntry?>(null));
            return;
        }
        var hlc = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks, Counter = 0 };
        shard.GetRawEntryAsync(key).Returns(
            Task.FromResult<LwwEntry?>(new LwwEntry(key, LwwValue<byte[]>.Create(ScoredJson(score.Value), hlc))));
    }

    [Test]
    public async Task ExecuteGuardedAsync_all_keys_match_commits_whole_batch()
    {
        var (grain, state, _, lattice, shard) = CreateGrain();
        StubScored(shard, "a", 1000);
        StubScored(shard, "b", 800);

        List<KeyValuePair<string, byte[]>>? observed = null;
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(callInfo =>
            {
                observed = ((List<KeyValuePair<string, byte[]>>)callInfo[0]).ToList();
                return Task.CompletedTask;
            });

        var entries = MakeEntries(("a", ScoredJson(7777)), ("b", ScoredJson(7777)));
        var outcome = await grain.ExecuteGuardedAsync(TreeId, entries, ScoreAtLeast(500));

        Assert.That(outcome, Is.EqualTo(AtomicWriteOutcome.Committed));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        await lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
        Assert.That(observed, Is.Not.Null);
        Assert.That(observed!.Select(kv => kv.Key).ToList(), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task ExecuteGuardedAsync_one_key_fails_aborts_with_no_writes()
    {
        var (grain, state, _, lattice, shard) = CreateGrain();
        StubScored(shard, "a", 1000);
        StubScored(shard, "b", 100);  // below guard -> whole batch must abort

        var entries = MakeEntries(("a", ScoredJson(7777)), ("b", ScoredJson(7777)));
        var outcome = await grain.ExecuteGuardedAsync(TreeId, entries, ScoreAtLeast(500));

        Assert.That(outcome, Is.EqualTo(AtomicWriteOutcome.PreconditionFailed));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.PreconditionFailed));
        await lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
        await lattice.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public async Task ExecuteGuardedAsync_missing_key_counts_as_non_match()
    {
        var (grain, state, _, lattice, shard) = CreateGrain();
        StubScored(shard, "a", 1000);
        StubScored(shard, "b", null);  // absent -> non-match -> abort

        var entries = MakeEntries(("a", ScoredJson(7777)), ("b", ScoredJson(7777)));
        var outcome = await grain.ExecuteGuardedAsync(TreeId, entries, ScoreAtLeast(500));

        Assert.That(outcome, Is.EqualTo(AtomicWriteOutcome.PreconditionFailed));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.PreconditionFailed));
        await lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task ExecuteGuardedAsync_empty_batch_returns_committed()
    {
        var (grain, state, _, lattice, _) = CreateGrain();

        var outcome = await grain.ExecuteGuardedAsync(TreeId, MakeEntries(), ScoreAtLeast(500));

        Assert.That(outcome, Is.EqualTo(AtomicWriteOutcome.Committed));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.NotStarted));
        await lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public void ExecuteGuardedAsync_throws_on_null_entries()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.ExecuteGuardedAsync(TreeId, null!, ScoreAtLeast(1)));
    }

    [Test]
    public async Task ExecuteGuardedAsync_persists_guard_for_reminder_replay()
    {
        var (grain, state, _, _, shard) = CreateGrain();
        StubScored(shard, "a", 1000);

        await grain.ExecuteGuardedAsync(TreeId, MakeEntries(("a", ScoredJson(7777))), ScoreAtLeast(500));

        Assert.That(state.State.Guard, Is.Not.Null);
    }

    [Test]
    public async Task ExecuteGuardedAsync_reattach_returns_memoized_precondition_failure()
    {
        var (grain, state, _, lattice, shard) = CreateGrain();
        StubScored(shard, "a", 1000);
        StubScored(shard, "b", 100);

        var entries = MakeEntries(("a", ScoredJson(7777)), ("b", ScoredJson(7777)));
        var first = await grain.ExecuteGuardedAsync(TreeId, entries, ScoreAtLeast(500));
        Assert.That(first, Is.EqualTo(AtomicWriteOutcome.PreconditionFailed));

        // Pre-saga values now "move" so the predicate would pass; the memoized
        // terminal outcome must be returned without re-evaluating.
        StubScored(shard, "b", 1000);
        var second = await grain.ExecuteGuardedAsync(TreeId, entries, ScoreAtLeast(500));

        Assert.That(second, Is.EqualTo(AtomicWriteOutcome.PreconditionFailed));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.PreconditionFailed));
        await lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task ExecuteGuardedAsync_unregisters_keepalive_on_precondition_failure()
    {
        var (grain, _, reminder, _, shard) = CreateGrain();
        StubScored(shard, "a", 100);  // below guard

        await grain.ExecuteGuardedAsync(TreeId, MakeEntries(("a", ScoredJson(7777))), ScoreAtLeast(500));

        await reminder.Received().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task ExecuteGuardedAsync_reports_complete_via_IsCompleteAsync_after_precondition_failure()
    {
        var (grain, _, _, _, shard) = CreateGrain();
        StubScored(shard, "a", 100);

        await grain.ExecuteGuardedAsync(TreeId, MakeEntries(("a", ScoredJson(7777))), ScoreAtLeast(500));

        Assert.That(await grain.IsCompleteAsync(), Is.True);
    }
}
