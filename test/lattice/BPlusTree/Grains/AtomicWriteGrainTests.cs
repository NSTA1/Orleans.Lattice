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
public partial class AtomicWriteGrainTests
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
            .Returns(routing);

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

    // --- OriginClusterId / VectorClock preservation across the saga ---

    [Test]
    public async Task ExecuteAsync_captures_OriginClusterId_and_VectorClock_from_pre_saga_entry()
    {
        // PrepareAsync is private — drive it through a successful ExecuteAsync
        // and inspect the captured pre-value snapshot afterwards.
        var (grain, state, _, lattice, shard) = CreateGrain();
        var hlc = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks, Counter = 0 };
        var vc = new VersionVector();
        vc.Tick("origin-peer");
        var lww = LwwValue<byte[]>.Create(new byte[] { 9 }, hlc)
            with { OriginClusterId = "origin-peer", VectorClock = vc };
        shard.GetRawEntryAsync("a").Returns(Task.FromResult<LwwEntry?>(new LwwEntry("a", lww)));
        shard.GetRawEntryAsync("b").Returns(Task.FromResult<LwwEntry?>(null));
        // All forward writes succeed so we observe the captured PreValues
        // without compensation rewriting them.
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);

        var entries = MakeEntries(("a", [1]), ("b", [2]));
        await grain.ExecuteAsync(TreeId, entries);

        var pre = state.State.PreValues.Single(p => p.Key == "a");
        Assert.That(pre.Existed, Is.True);
        Assert.That(pre.OriginClusterId, Is.EqualTo("origin-peer"));
        Assert.That(pre.VectorClock, Is.SameAs(vc));

        var preB = state.State.PreValues.Single(p => p.Key == "b");
        Assert.That(preB.Existed, Is.False);
        Assert.That(preB.OriginClusterId, Is.Null);
        Assert.That(preB.VectorClock, Is.Null);
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
    public async Task ReceiveReminder_resumes_execute_re_stamps_persisted_delta_context()
    {
        // Crash-replay regression: a saga that captured a DeltaKind/DeltaPayload
        // on its original Prepare call must re-stamp the persisted carry on
        // every resumed per-key write so observers continue to see the
        // author's delta even after a silo restart. Caller-side ambient
        // context is deliberately *unset* here - the value must come from
        // persisted AtomicWriteState alone.
        const string persistedKind = "test.replay.kind";
        var persistedPayload = new byte[] { 7, 7, 7 };

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
        state.State.DeltaKind = persistedKind;
        state.State.DeltaPayload = persistedPayload;

        var (grain, _, _, lattice, _) = CreateGrain(state);

        (string Kind, byte[] Payload)? observedDuringSetB = null;
        lattice.SetAsync("b", Arg.Any<byte[]>())
            .Returns(_ =>
            {
                observedDuringSetB = LatticeDeltaContext.Current;
                return Task.CompletedTask;
            });

        // Sanity: no ambient context outside the resume path.
        Assert.That(LatticeDeltaContext.Current, Is.Null);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        Assert.That(observedDuringSetB, Is.Not.Null);
        Assert.That(observedDuringSetB!.Value.Kind, Is.EqualTo(persistedKind));
        Assert.That(observedDuringSetB.Value.Payload, Is.EqualTo(persistedPayload));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
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

        // Completed saga state must remain Completed with the original entries —
        // a reentry on a finished saga is a pure no-op.
        Assert.That(seeded.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        Assert.That(seeded.State.Entries, Is.EqualTo(original));
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

    // --- Saga-wide vector-clock capture (R-089) ---

    [Test]
    public async Task ExecuteAsync_captures_caller_VectorClock_once_on_first_prepare()
    {
        // Caller wraps the call in LatticeVectorClockContext.With(...);
        // PrepareAsync must persist the frontier on AtomicWriteState so
        // every subsequent emit (and any reminder-driven replay) uses
        // the identical VC.
        var (grain, state, _, lattice, _) = CreateGrain();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);

        var vc = new VersionVector();
        vc.Tick("origin-peer");

        using (LatticeVectorClockContext.With(vc))
        {
            await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2])));
        }

        Assert.That(state.State.VectorClock, Is.SameAs(vc));
    }

    [Test]
    public async Task ExecuteAsync_persists_null_VectorClock_when_caller_unset()
    {
        // Sanity counterpart: no ambient context => persisted VC stays null.
        var (grain, state, _, lattice, _) = CreateGrain();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(state.State.VectorClock, Is.Null);
    }

    [Test]
    public async Task ExecuteAsync_re_stamps_persisted_VectorClock_on_every_per_key_SetAsync()
    {
        // The saga's purpose: every per-key SetAsync the saga issues must
        // observe the saga-wide VC ambient at the time it executes — i.e.
        // before the leaf grain reads LatticeVectorClockContext.Current
        // and stamps LwwValue.VectorClock. We capture the ambient inside
        // each SetAsync callback and assert all observations equal the
        // captured frontier.
        var (grain, _, _, lattice, _) = CreateGrain();
        var vc = new VersionVector();
        vc.Tick("origin-peer");

        var observed = new List<VersionVector?>();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(_ =>
            {
                observed.Add(LatticeVectorClockContext.Current);
                return Task.CompletedTask;
            });

        using (LatticeVectorClockContext.With(vc))
        {
            await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2]), ("c", [3])));
        }

        Assert.That(observed, Has.Count.EqualTo(3));
        Assert.That(observed[0], Is.SameAs(vc));
        Assert.That(observed[1], Is.SameAs(vc));
        Assert.That(observed[2], Is.SameAs(vc));
    }

    [Test]
    public async Task ReceiveReminder_resumes_execute_re_stamps_persisted_VectorClock()
    {
        // Crash-replay regression: a saga that captured a VectorClock on
        // its original Prepare must re-stamp the persisted frontier on
        // every resumed per-key write so observers continue to see the
        // identical batch-wide VC after silo restart. Caller-side ambient
        // context is deliberately *unset* here — the value must come from
        // persisted AtomicWriteState alone.
        var persistedVc = new VersionVector();
        persistedVc.Tick("origin-peer");

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
        state.State.VectorClock = persistedVc;

        var (grain, _, _, lattice, _) = CreateGrain(state);

        VersionVector? observedDuringSetB = null;
        lattice.SetAsync("b", Arg.Any<byte[]>())
            .Returns(_ =>
            {
                observedDuringSetB = LatticeVectorClockContext.Current;
                return Task.CompletedTask;
            });

        // Sanity: no ambient context outside the resume path.
        Assert.That(LatticeVectorClockContext.Current, Is.Null);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        Assert.That(observedDuringSetB, Is.SameAs(persistedVc));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
    }

    [Test]
    public async Task ExecuteAsync_does_not_overwrite_persisted_VectorClock_on_replay()
    {
        // Reminder-driven replay must reuse the persisted frontier even if
        // the activation environment somehow leaks a non-null ambient
        // context — capture-once is honoured.
        var originalVc = new VersionVector();
        originalVc.Tick("origin-peer");
        var contaminatingVc = new VersionVector();
        contaminatingVc.Tick("other-peer");

        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Execute;
        state.State.TreeId = TreeId;
        state.State.Entries = MakeEntries(("a", [1]));
        state.State.PreValues = new List<AtomicPreValue>
        {
            new() { Key = "a", Value = null, Existed = false },
        };
        state.State.NextIndex = 0;
        state.State.VectorClock = originalVc;

        var (grain, _, _, lattice, _) = CreateGrain(state);
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);

        // Persisted VC is already set, so PrepareAsync's capture-once
        // block (guarded on `is null`) must not fire even though
        // ambient context here is non-null. The saga is in Execute
        // phase so PrepareAsync is not re-entered, but we still pin
        // the invariant that the persisted slot is the single source
        // of truth post-capture.
        using (LatticeVectorClockContext.With(contaminatingVc))
        {
            await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());
        }

        Assert.That(state.State.VectorClock, Is.SameAs(originalVc));
    }
}
