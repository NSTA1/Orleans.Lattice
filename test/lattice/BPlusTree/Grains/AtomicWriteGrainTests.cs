using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
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
                     ILattice lattice,
                     IShardRootGrain shard) CreateGrain(
        FakePersistentState<AtomicWriteState>? existingState = null,
        LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("atomic-write", $"{TreeId}/{OperationId}"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);

        // Raw-entry reads now flow saga → IShardRootGrain directly (not through
        // ILattice), so the test harness mocks a single shard substitute and
        // stubs routing to resolve every key to it. This mirrors the production
        // path where AtomicWriteGrain.PrepareAsync calls
        // lattice.GetRoutingAsync() once and then addresses IShardRootGrain
        // via grainFactory.GetGrain<IShardRootGrain>("{physicalTreeId}/{idx}").
        var shard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
        shard.GetRawEntryAsync(Arg.Any<string>())
            .Returns(Task.FromResult<LwwEntry?>(null));

        var opts = options ?? new LatticeOptions();
        var routing = new RoutingInfo(
            TreeId,
            ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount));
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(routing));

        var reminderRegistry = Substitute.For<IReminderRegistry>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

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
        return (grain, state, reminderRegistry, lattice, shard);
    }

    /// <summary>
    /// Stubs <see cref="IShardRootGrain.GetRawEntryAsync"/> for the given key
    /// to return an <see cref="LwwEntry"/> carrying <paramref name="value"/>
    /// with a fresh HLC and no TTL — the non-TTL equivalent of the old
    /// <c>lattice.GetAsync(key).Returns(value)</c> stub.
    /// </summary>
    private static void StubPreValue(IShardRootGrain shard, string key, byte[]? value)
    {
        if (value is null)
        {
            shard.GetRawEntryAsync(key).Returns(Task.FromResult<LwwEntry?>(null));
        }
        else
        {
            var hlc = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks, Counter = 0 };
            shard.GetRawEntryAsync(key).Returns(
                Task.FromResult<LwwEntry?>(new LwwEntry(key, LwwValue<byte[]>.Create(value, hlc))));
        }
    }

    /// <summary>
    /// Stubs <see cref="IShardRootGrain.GetRawEntryAsync"/> for the given key
    /// to return an <see cref="LwwEntry"/> carrying <paramref name="value"/>
    /// with a fresh HLC and an explicit absolute <paramref name="expiresAtTicks"/>
    /// ( TTL). Used to verify compensation preserves TTL metadata.
    /// </summary>
    private static void StubPreValueWithExpiry(IShardRootGrain shard, string key, byte[] value, long expiresAtTicks)
    {
        var hlc = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks, Counter = 0 };
        var lww = LwwValue<byte[]>.CreateWithExpiry(value, hlc, expiresAtTicks);
        shard.GetRawEntryAsync(key).Returns(Task.FromResult<LwwEntry?>(new LwwEntry(key, lww)));
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
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.ExecuteAsync(null!, MakeEntries(("k", [1]))));
    }

    [Test]
    public void ExecuteAsync_throws_on_null_entries()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.ExecuteAsync(TreeId, null!));
    }

    [Test]
    public async Task ExecuteAsync_empty_batch_is_noop()
    {
        var (grain, state, _, lattice, _) = CreateGrain();

        await grain.ExecuteAsync(TreeId, MakeEntries());

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.NotStarted));
        await lattice.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public void ExecuteAsync_throws_on_duplicate_keys()
    {
        var (grain, _, _, _, _) = CreateGrain();
        var entries = MakeEntries(("a", [1]), ("a", [2]));
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.ExecuteAsync(TreeId, entries));
    }

    [Test]
    public void ExecuteAsync_throws_on_null_value()
    {
        var (grain, _, _, _, _) = CreateGrain();
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
        var (grain, state, _, lattice, shard) = CreateGrain();
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));

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
        var (grain, state, _, _, shard) = CreateGrain();
        StubPreValue(shard, "a", [9, 9]);
        StubPreValue(shard, "b", null);

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
        var (grain, _, reminder, _, shard) = CreateGrain();
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));

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
        var (grain, _, reminder, _, shard) = CreateGrain();
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        await reminder.Received().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    // --- Compensation ---

    [Test]
    public void ExecuteAsync_throws_and_compensates_on_failure_mid_batch()
    {
        var (grain, state, _, lattice, shard) = CreateGrain();
        StubPreValue(shard, "a", [9]);
        StubPreValue(shard, "b", null);
        StubPreValue(shard, "c", null);

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
        var (grain, _, _, lattice, shard) = CreateGrain();
        StubPreValue(shard, "a", [9, 9]);
        StubPreValue(shard, "b", null);
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
        var (grain, _, _, lattice, shard) = CreateGrain();
        StubPreValue(shard, "a", null);
        StubPreValue(shard, "b", null);
        lattice.SetAsync("b", Arg.Any<byte[]>()).Throws(new InvalidOperationException("boom"));

        var entries = MakeEntries(("a", [1]), ("b", [2]));

        try { await grain.ExecuteAsync(TreeId, entries); } catch { /* expected */ }

        // 'a' did not exist pre-saga, so compensation tombstones it.
        await lattice.Received().DeleteAsync("a");
    }

    [Test]
    public async Task ExecuteAsync_compensation_preserves_TTL_on_rollback()
    {
        // a pre-saga entry with TTL must survive rollback —
        // compensation re-writes it through the TTL-aware SetAsync overload
        // with the remaining lifetime, not through the plain SetAsync which
        // would drop ExpiresAtTicks.
        var (grain, _, _, lattice, shard) = CreateGrain();
        var expiresAt = DateTimeOffset.UtcNow.UtcTicks + TimeSpan.FromHours(1).Ticks;
        StubPreValueWithExpiry(shard, "a", [9, 9], expiresAt);
        StubPreValue(shard, "b", null);
        lattice.SetAsync("b", Arg.Any<byte[]>()).Throws(new InvalidOperationException("boom"));

        var entries = MakeEntries(("a", [1]), ("b", [2]));
        try { await grain.ExecuteAsync(TreeId, entries); } catch { /* expected */ }

        // Compensation must have restored 'a' via the TTL overload with a
        // positive remaining ttl (< 1 hour because wall-clock has advanced).
        await lattice.Received().SetAsync(
            "a",
            Arg.Is<byte[]>(v => v.Length == 2 && v[0] == 9 && v[1] == 9),
            Arg.Is<TimeSpan>(ttl => ttl > TimeSpan.Zero && ttl <= TimeSpan.FromHours(1)));
    }

    [Test]
    public async Task ExecuteAsync_compensation_deletes_past_due_pre_saga_entry()
    {
        // a pre-saga entry whose TTL has already lapsed by the time
        // compensation runs should be tombstoned, not resurrected.
        var (grain, _, _, lattice, shard) = CreateGrain();
        var pastExpiry = DateTimeOffset.UtcNow.UtcTicks - TimeSpan.FromMinutes(5).Ticks;
        StubPreValueWithExpiry(shard, "a", [9, 9], pastExpiry);
        StubPreValue(shard, "b", null);
        lattice.SetAsync("b", Arg.Any<byte[]>()).Throws(new InvalidOperationException("boom"));

        var entries = MakeEntries(("a", [1]), ("b", [2]));
        try { await grain.ExecuteAsync(TreeId, entries); } catch { /* expected */ }

        // Past-due pre-saga entry is equivalent to "absent" — compensation
        // should tombstone 'a', not restore a stale value.
        await lattice.Received().DeleteAsync("a");
    }

    [Test]
    public async Task ExecuteAsync_compensation_preserves_failure_message()
    {
        var (grain, _, _, lattice, shard) = CreateGrain();
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));
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
        var (grain, _, _, _, _) = CreateGrain();
        Assert.That(await grain.IsCompleteAsync(), Is.True);
    }

    [Test]
    public async Task IsCompleteAsync_returns_false_during_execute()
    {
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Execute;
        var (grain, _, _, _, _) = CreateGrain(state);

        Assert.That(await grain.IsCompleteAsync(), Is.False);
    }

    [Test]
    public async Task IsCompleteAsync_returns_true_after_completion()
    {
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Completed;
        var (grain, _, _, _, _) = CreateGrain(state);

        Assert.That(await grain.IsCompleteAsync(), Is.True);
    }

    // --- Reminder-driven resumption ---

    [Test]
    public async Task ReceiveReminder_deactivates_when_already_completed()
    {
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Completed;
        var (grain, _, reminder, _, _) = CreateGrain(state);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        await reminder.Received().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task ReceiveReminder_ignores_unrelated_reminder_names()
    {
        var (grain, state, _, _, _) = CreateGrain();
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

        var (grain, _, _, lattice, _) = CreateGrain(state);

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

        var (grain, _, _, lattice, _) = CreateGrain(state);

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

        var (grain, _, _, lattice, _) = CreateGrain(state);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        await lattice.Received().SetAsync("a", Arg.Is<byte[]>(v => v.Length == 1 && v[0] == 9));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        Assert.That(state.State.RetriesOnCurrentStep, Is.Zero);
    }

    // --- Retention reminder self-cleanup ---

    [Test]
    public async Task ExecuteAsync_success_registers_retention_reminder()
    {
        var (grain, _, registry, _, _) = CreateGrain();

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
        var (grain, _, registry, _, _) = CreateGrain(options: opts);

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
        var (grain, _, registry, _, _) = CreateGrain(options: opts);

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
        var (grain, persisted, registry, _, _) = CreateGrain(state);
        registry.GetReminder(Arg.Any<GrainId>(), "atomic-write-retention")
            .Returns(Task.FromResult<IGrainReminder?>(reminder));

        await grain.ReceiveReminder("atomic-write-retention", new TickStatus());

        Assert.That(persisted.State.Phase, Is.EqualTo(AtomicWritePhase.NotStarted),
            "ClearStateAsync resets state to its default (NotStarted).");
        await registry.Received().UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    // --- Caller-supplied idempotency key (G-011) ---

    [Test]
    public void ComputeKeyFingerprint_returns_same_digest_for_reordered_keys()
    {
        var a = MakeEntries(("k1", [1]), ("k2", [2]), ("k3", [3]));
        var b = MakeEntries(("k3", [30]), ("k1", [10]), ("k2", [20]));

        var fa = AtomicWriteGrain.ComputeKeyFingerprint(a);
        var fb = AtomicWriteGrain.ComputeKeyFingerprint(b);

        Assert.That(fa, Is.EqualTo(fb),
            "Reordering entries or changing values must not change the fingerprint.");
    }

    [Test]
    public void ComputeKeyFingerprint_differs_when_key_set_differs()
    {
        var a = MakeEntries(("k1", [1]), ("k2", [2]));
        var b = MakeEntries(("k1", [1]), ("k3", [3]));

        Assert.That(
            AtomicWriteGrain.ComputeKeyFingerprint(a),
            Is.Not.EqualTo(AtomicWriteGrain.ComputeKeyFingerprint(b)),
            "Different key sets must produce different fingerprints.");
    }

    [Test]
    public void ComputeKeyFingerprint_differs_when_count_differs()
    {
        var a = MakeEntries(("k1", [1]));
        var b = MakeEntries(("k1", [1]), ("k2", [2]));

        Assert.That(
            AtomicWriteGrain.ComputeKeyFingerprint(a),
            Is.Not.EqualTo(AtomicWriteGrain.ComputeKeyFingerprint(b)));
    }

    [Test]
    public async Task ExecuteAsync_seeds_KeyFingerprint_on_first_Prepare()
    {
        var (grain, state, _, _, _) = CreateGrain();
        var entries = MakeEntries(("a", [1]), ("b", [2]));

        await grain.ExecuteAsync(TreeId, entries);

        Assert.That(state.State.KeyFingerprint, Is.Not.Null,
            "Fresh saga must persist the fingerprint of its key set.");
        Assert.That(
            state.State.KeyFingerprint,
            Is.EqualTo(AtomicWriteGrain.ComputeKeyFingerprint(entries)),
            "Persisted fingerprint must match the caller's key set.");
    }

    [Test]
    public void ExecuteAsync_throws_InvalidOperationException_when_reentered_with_different_key_set()
    {
        // Seed persisted state as if a prior saga is mid-flight with keys k1,k2.
        var original = MakeEntries(("k1", [1]), ("k2", [2]));
        var seeded = new FakePersistentState<AtomicWriteState>
        {
            State =
            {
                Phase = AtomicWritePhase.Execute,
                TreeId = TreeId,
                Entries = original,
                KeyFingerprint = AtomicWriteGrain.ComputeKeyFingerprint(original),
            },
        };

        var (grain, _, _, _, _) = CreateGrain(existingState: seeded);
        var mismatched = MakeEntries(("k1", [1]), ("DIFFERENT", [9]));

        Assert.That(
            async () => await grain.ExecuteAsync(TreeId, mismatched),
            Throws.InvalidOperationException.With.Message.Contains("different key set"));
    }

    [Test]
    public async Task ExecuteAsync_accepts_reentry_with_same_key_set_and_different_values()
    {
        // Seed persisted state as if a prior saga completed keys k1,k2.
        var original = MakeEntries(("k1", [1]), ("k2", [2]));
        var seeded = new FakePersistentState<AtomicWriteState>
        {
            State =
            {
                Phase = AtomicWritePhase.Completed,
                TreeId = TreeId,
                Entries = original,
                KeyFingerprint = AtomicWriteGrain.ComputeKeyFingerprint(original),
            },
        };

        var (grain, _, _, _, _) = CreateGrain(existingState: seeded);
        // Same keys, different values (typical retry scenario where the
        // serialized payload may differ slightly).
        var retry = MakeEntries(("k2", [99]), ("k1", [88]));

        // Must not throw; the completed saga is observed as idempotent success.
        await grain.ExecuteAsync(TreeId, retry);
    }

    [Test]
    public void ExecuteAsync_accepts_reentry_when_legacy_state_has_no_fingerprint()
    {
        // Pre-G-011 persisted state has KeyFingerprint == null. The fingerprint
        // check must be skipped in that case so the grain remains wire-compatible.
        var original = MakeEntries(("k1", [1]));
        var seeded = new FakePersistentState<AtomicWriteState>
        {
            State =
            {
                Phase = AtomicWritePhase.Completed,
                TreeId = TreeId,
                Entries = original,
                KeyFingerprint = null,
            },
        };

        var (grain, _, _, _, _) = CreateGrain(existingState: seeded);
        Assert.That(async () => await grain.ExecuteAsync(TreeId, original), Throws.Nothing);
    }
}
