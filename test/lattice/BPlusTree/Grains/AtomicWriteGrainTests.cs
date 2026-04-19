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

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class AtomicWriteGrainTests
{
    private const string TreeId = "atomic-tree";
    private const string OperationId = "op-123";

    private static (AtomicWriteGrain grain,
                     FakePersistentState<AtomicWriteState> state,
                     IReminderRegistry reminderRegistry,
                     ILattice lattice) CreateGrain(
        FakePersistentState<AtomicWriteState>? existingState = null,
        LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("atomic-write", $"{TreeId}/{OperationId}"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var reminderRegistry = Substitute.For<IReminderRegistry>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var opts = options ?? new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(opts);
        optionsMonitor.Get(Arg.Any<string>()).Returns(opts);

        var state = existingState ?? new FakePersistentState<AtomicWriteState>();

        var grain = new AtomicWriteGrain(
            context,
            grainFactory,
            reminderRegistry,
            optionsMonitor,
            new LoggerFactory().CreateLogger<AtomicWriteGrain>(),
            state);
        return (grain, state, reminderRegistry, lattice);
    }

    private static List<KeyValuePair<string, byte[]>> MakeEntries(params (string, byte[])[] pairs)
    {
        var list = new List<KeyValuePair<string, byte[]>>();
        foreach (var (k, v) in pairs)
            list.Add(new KeyValuePair<string, byte[]>(k, v));
        return list;
    }

    // --- Input validation ---

    [Test]
    public void ExecuteAsync_throws_on_null_treeId()
    {
        var (grain, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.ExecuteAsync(null!, MakeEntries(("k", [1]))));
    }

    [Test]
    public void ExecuteAsync_throws_on_null_entries()
    {
        var (grain, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.ExecuteAsync(TreeId, null!));
    }

    [Test]
    public async Task ExecuteAsync_empty_batch_is_noop()
    {
        var (grain, state, _, lattice) = CreateGrain();

        await grain.ExecuteAsync(TreeId, MakeEntries());

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.NotStarted));
        await lattice.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public void ExecuteAsync_throws_on_duplicate_keys()
    {
        var (grain, _, _, _) = CreateGrain();
        var entries = MakeEntries(("a", [1]), ("a", [2]));
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.ExecuteAsync(TreeId, entries));
    }

    [Test]
    public void ExecuteAsync_throws_on_null_value()
    {
        var (grain, _, _, _) = CreateGrain();
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", null!),
        };
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.ExecuteAsync(TreeId, entries));
    }

    // --- Happy path ---

    [Test]
    public async Task ExecuteAsync_commits_all_entries_in_order()
    {
        var (grain, state, _, lattice) = CreateGrain();
        lattice.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        var entries = MakeEntries(("a", [1]), ("b", [2]), ("c", [3]));

        await grain.ExecuteAsync(TreeId, entries);

        Received.InOrder(() =>
        {
            lattice.SetAsync("a", Arg.Any<byte[]>());
            lattice.SetAsync("b", Arg.Any<byte[]>());
            lattice.SetAsync("c", Arg.Any<byte[]>());
        });

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        Assert.That(state.State.NextIndex, Is.EqualTo(3));
    }

    [Test]
    public async Task ExecuteAsync_captures_pre_saga_values()
    {
        var (grain, state, _, lattice) = CreateGrain();
        lattice.GetAsync("a").Returns(Task.FromResult<byte[]?>([9, 9]));
        lattice.GetAsync("b").Returns(Task.FromResult<byte[]?>(null));

        var entries = MakeEntries(("a", [1]), ("b", [2]));
        await grain.ExecuteAsync(TreeId, entries);

        Assert.That(state.State.PreValues, Has.Count.EqualTo(2));
        Assert.That(state.State.PreValues[0].Key, Is.EqualTo("a"));
        Assert.That(state.State.PreValues[0].Existed, Is.True);
        Assert.That(state.State.PreValues[0].Value, Is.EqualTo(new byte[] { 9, 9 }));
        Assert.That(state.State.PreValues[1].Key, Is.EqualTo("b"));
        Assert.That(state.State.PreValues[1].Existed, Is.False);
        Assert.That(state.State.PreValues[1].Value, Is.Null);
    }

    [Test]
    public async Task ExecuteAsync_registers_keepalive_reminder_on_start()
    {
        var (grain, _, reminder, lattice) = CreateGrain();
        lattice.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        await reminder.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "atomic-write-keepalive",
            Arg.Any<TimeSpan>(),
            Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task ExecuteAsync_unregisters_keepalive_on_success()
    {
        var (grain, _, reminder, lattice) = CreateGrain();
        lattice.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        await reminder.Received().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    // --- Compensation ---

    [Test]
    public void ExecuteAsync_throws_and_compensates_on_failure_mid_batch()
    {
        var (grain, state, _, lattice) = CreateGrain();
        lattice.GetAsync("a").Returns(Task.FromResult<byte[]?>([9]));
        lattice.GetAsync("b").Returns(Task.FromResult<byte[]?>(null));
        lattice.GetAsync("c").Returns(Task.FromResult<byte[]?>(null));

        // Fail on the third write (and its retry).
        lattice.SetAsync("c", Arg.Any<byte[]>()).Throws(new InvalidOperationException("shard down"));

        var entries = MakeEntries(("a", [1]), ("b", [2]), ("c", [3]));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ExecuteAsync(TreeId, entries));

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        Assert.That(state.State.NextIndex, Is.EqualTo(0),
            "compensation should have reverted every committed entry");
    }

    [Test]
    public async Task ExecuteAsync_compensation_restores_existing_pre_saga_value()
    {
        var (grain, _, _, lattice) = CreateGrain();
        lattice.GetAsync("a").Returns(Task.FromResult<byte[]?>([9, 9]));
        lattice.GetAsync("b").Returns(Task.FromResult<byte[]?>(null));
        // Fail the 2nd write.
        lattice.SetAsync("b", Arg.Any<byte[]>()).Throws(new InvalidOperationException("boom"));

        var entries = MakeEntries(("a", [1]), ("b", [2]));

        try { await grain.ExecuteAsync(TreeId, entries); } catch { /* expected */ }

        // 'a' was committed then reverted: the rewrite uses the pre-saga value [9,9].
        await lattice.Received().SetAsync("a", Arg.Is<byte[]>(v => v.Length == 2 && v[0] == 9 && v[1] == 9));
    }

    [Test]
    public async Task ExecuteAsync_compensation_deletes_previously_absent_keys()
    {
        var (grain, _, _, lattice) = CreateGrain();
        lattice.GetAsync("a").Returns(Task.FromResult<byte[]?>(null));
        lattice.GetAsync("b").Returns(Task.FromResult<byte[]?>(null));
        lattice.SetAsync("b", Arg.Any<byte[]>()).Throws(new InvalidOperationException("boom"));

        var entries = MakeEntries(("a", [1]), ("b", [2]));

        try { await grain.ExecuteAsync(TreeId, entries); } catch { /* expected */ }

        // 'a' did not exist pre-saga, so compensation tombstones it.
        await lattice.Received().DeleteAsync("a");
    }

    [Test]
    public async Task ExecuteAsync_compensation_preserves_failure_message()
    {
        var (grain, _, _, lattice) = CreateGrain();
        lattice.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        lattice.SetAsync("b", Arg.Any<byte[]>()).Throws(new InvalidOperationException("specific failure"));

        var entries = MakeEntries(("a", [1]), ("b", [2]));

        Exception? caught = null;
        try { await grain.ExecuteAsync(TreeId, entries); } catch (Exception ex) { caught = ex; }

        Assert.That(caught, Is.Not.Null);
        Assert.That(caught!.Message, Does.Contain("specific failure"));
    }

    // --- IsCompleteAsync ---

    [Test]
    public async Task IsCompleteAsync_returns_true_for_fresh_grain()
    {
        var (grain, _, _, _) = CreateGrain();
        Assert.That(await grain.IsCompleteAsync(), Is.True);
    }

    [Test]
    public async Task IsCompleteAsync_returns_false_during_execute()
    {
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Execute;
        var (grain, _, _, _) = CreateGrain(state);

        Assert.That(await grain.IsCompleteAsync(), Is.False);
    }

    [Test]
    public async Task IsCompleteAsync_returns_true_after_completion()
    {
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Completed;
        var (grain, _, _, _) = CreateGrain(state);

        Assert.That(await grain.IsCompleteAsync(), Is.True);
    }

    // --- Reminder-driven resumption ---

    [Test]
    public async Task ReceiveReminder_deactivates_when_already_completed()
    {
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Completed;
        var (grain, _, reminder, _) = CreateGrain(state);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        await reminder.Received().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task ReceiveReminder_ignores_unrelated_reminder_names()
    {
        var (grain, state, _, _) = CreateGrain();
        state.State.Phase = AtomicWritePhase.Execute;

        await grain.ReceiveReminder("other-reminder", new TickStatus());

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Execute));
    }

    [Test]
    public async Task ReceiveReminder_resumes_execute_from_persisted_progress()
    {
        // Simulate a crash after the first write committed but before the second.
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Execute;
        state.State.TreeId = TreeId;
        state.State.Entries = MakeEntries(("a", [1]), ("b", [2]));
        state.State.PreValues = new List<AtomicPreValue>
        {
            new() { Key = "a", Value = null, Existed = false },
            new() { Key = "b", Value = null, Existed = false },
        };
        state.State.NextIndex = 1;

        var (grain, _, _, lattice) = CreateGrain(state);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        // Only the missing entry ("b") should be written on resume.
        await lattice.DidNotReceive().SetAsync("a", Arg.Any<byte[]>());
        await lattice.Received().SetAsync("b", Arg.Any<byte[]>());
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
    }

    [Test]
    public async Task ReceiveReminder_resumes_compensation_from_persisted_progress()
    {
        // Silo crashed mid-compensation: phase=Compensate, NextIndex points
        // at the number of entries still needing rollback. On reminder fire,
        // the grain should restore the remaining pre-saga values.
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Compensate;
        state.State.TreeId = TreeId;
        state.State.Entries = MakeEntries(("a", [1]), ("b", [2]));
        state.State.PreValues = new List<AtomicPreValue>
        {
            new() { Key = "a", Value = [9], Existed = true },
            new() { Key = "b", Value = null, Existed = false },
        };
        // NextIndex=1 => one committed write remains to roll back (index 0 == "a").
        state.State.NextIndex = 1;
        state.State.FailureMessage = "boom";

        var (grain, _, _, lattice) = CreateGrain(state);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        await lattice.Received().SetAsync("a", Arg.Is<byte[]>(v => v.Length == 1 && v[0] == 9));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        Assert.That(state.State.NextIndex, Is.Zero);
    }

    [Test]
    public async Task ReceiveReminder_resets_retry_counter_on_compensate_re_entry()
    {
        // Regression: a previous activation exhausted retries during
        // compensation and crashed. On reminder-driven re-entry the counter
        // must be reset so compensation can retry instead of stalling.
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Compensate;
        state.State.TreeId = TreeId;
        state.State.Entries = MakeEntries(("a", [1]));
        state.State.PreValues = new List<AtomicPreValue>
        {
            new() { Key = "a", Value = [9], Existed = true },
        };
        state.State.NextIndex = 1;
        state.State.RetriesOnCurrentStep = 1; // already at the cap
        state.State.FailureMessage = "boom";

        var (grain, _, _, lattice) = CreateGrain(state);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        await lattice.Received().SetAsync("a", Arg.Is<byte[]>(v => v.Length == 1 && v[0] == 9));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        Assert.That(state.State.RetriesOnCurrentStep, Is.Zero);
    }

    // --- Retention reminder self-cleanup ---

    [Test]
    public async Task ExecuteAsync_success_registers_retention_reminder()
    {
        var (grain, _, registry, _) = CreateGrain();

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        await registry.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "atomic-write-retention",
            Arg.Any<TimeSpan>(),
            Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task ExecuteAsync_skips_retention_when_infinite()
    {
        var opts = new LatticeOptions { AtomicWriteRetention = Timeout.InfiniteTimeSpan };
        var (grain, _, registry, _) = CreateGrain(options: opts);

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        await registry.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "atomic-write-retention",
            Arg.Any<TimeSpan>(),
            Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task ExecuteAsync_clamps_small_retention_to_one_minute_floor()
    {
        var opts = new LatticeOptions { AtomicWriteRetention = TimeSpan.FromSeconds(5) };
        var (grain, _, registry, _) = CreateGrain(options: opts);

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        await registry.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "atomic-write-retention",
            TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    [Test]
    public async Task ReceiveReminder_retention_clears_state_and_unregisters()
    {
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Completed;
        state.State.TreeId = TreeId;
        state.State.Entries = MakeEntries(("k", [1]));

        var reminder = Substitute.For<IGrainReminder>();
        var (grain, persisted, registry, _) = CreateGrain(state);
        registry.GetReminder(Arg.Any<GrainId>(), "atomic-write-retention")
            .Returns(Task.FromResult<IGrainReminder?>(reminder));

        await grain.ReceiveReminder("atomic-write-retention", new TickStatus());

        Assert.That(persisted.State.Phase, Is.EqualTo(AtomicWritePhase.NotStarted),
            "ClearStateAsync resets state to its default (NotStarted).");
        await registry.Received().UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }
}
