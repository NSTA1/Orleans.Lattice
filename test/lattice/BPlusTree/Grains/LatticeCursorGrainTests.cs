using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class LatticeCursorGrainTests
{
    private const string TreeId = "cursor-tree";
    private const string CursorId = "cur-abc";

    private static (LatticeCursorGrain grain,
                     FakePersistentState<LatticeCursorState> state,
                     ILattice lattice) CreateGrain(
        FakePersistentState<LatticeCursorState>? existingState = null,
        LatticeOptions? options = null,
        IReminderRegistry? reminderRegistry = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice-cursor", $"{TreeId}/{CursorId}"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var registry = reminderRegistry ?? Substitute.For<IReminderRegistry>();
        registry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult<IGrainReminder?>(Substitute.For<IGrainReminder>()));

        var opts = options ?? new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(opts);
        optionsMonitor.Get(Arg.Any<string>()).Returns(opts);

        var state = existingState ?? new FakePersistentState<LatticeCursorState>();

        var grain = new LatticeCursorGrain(
            context,
            grainFactory,
            registry,
            optionsMonitor,
            new LoggerFactory().CreateLogger<LatticeCursorGrain>(),
            state);
        return (grain, state, lattice);
    }

    private static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
            await Task.Yield();
        }
    }

    // --- OpenAsync ---

    [Test]
    public void OpenAsync_throws_on_null_treeId()
    {
        var (grain, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.OpenAsync(null!, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys }));
    }

    [Test]
    public void OpenAsync_throws_when_delete_range_missing_bounds()
    {
        var (grain, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.OpenAsync(TreeId, new LatticeCursorSpec
            {
                Kind = LatticeCursorKind.DeleteRange,
                StartInclusive = "a",
                EndExclusive = null,
            }));
    }

    [Test]
    public void OpenAsync_throws_when_delete_range_reverse()
    {
        var (grain, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.OpenAsync(TreeId, new LatticeCursorSpec
            {
                Kind = LatticeCursorKind.DeleteRange,
                StartInclusive = "a",
                EndExclusive = "z",
                Reverse = true,
            }));
    }

    [Test]
    public async Task OpenAsync_persists_spec_and_marks_open()
    {
        var (grain, state, _) = CreateGrain();
        var spec = new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Keys,
            StartInclusive = "a",
            EndExclusive = "m",
            Reverse = false,
        };

        await grain.OpenAsync(TreeId, spec);

        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open));
        Assert.That(state.State.TreeId, Is.EqualTo(TreeId));
        Assert.That(state.State.Spec, Is.EqualTo(spec));
        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task OpenAsync_is_idempotent_on_same_spec()
    {
        var (grain, state, _) = CreateGrain();
        var spec = new LatticeCursorSpec { Kind = LatticeCursorKind.Keys };

        await grain.OpenAsync(TreeId, spec);
        await grain.OpenAsync(TreeId, spec);

        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task OpenAsync_throws_when_reopened_with_different_spec()
    {
        var (grain, _, _) = CreateGrain();
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Entries }));
    }

    // --- NextKeysAsync ---

    [Test]
    public void NextKeysAsync_throws_when_not_opened()
    {
        var (grain, _, _) = CreateGrain();
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.NextKeysAsync(10));
    }

    [Test]
    public async Task NextKeysAsync_throws_when_opened_for_different_kind()
    {
        var (grain, _, _) = CreateGrain();
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Entries });

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.NextKeysAsync(10));
    }

    [Test]
    public async Task NextKeysAsync_throws_on_nonpositive_pageSize()
    {
        var (grain, _, _) = CreateGrain();
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.NextKeysAsync(0));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.NextKeysAsync(-1));
    }

    [Test]
    public async Task NextKeysAsync_returns_page_and_advances_last_yielded_key()
    {
        var (grain, state, lattice) = CreateGrain();
        lattice.KeysAsync(null, null, false, null)
            .Returns(ToAsyncEnumerable(new[] { "a", "b", "c", "d" }));
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        var page = await grain.NextKeysAsync(2);

        Assert.That(page.Keys, Is.EqualTo(new[] { "a", "b" }));
        Assert.That(page.HasMore, Is.True);
        Assert.That(state.State.LastYieldedKey, Is.EqualTo("b"));
    }

    [Test]
    public async Task NextKeysAsync_advances_start_bound_on_subsequent_calls()
    {
        var (grain, state, lattice) = CreateGrain();
        lattice.KeysAsync(null, null, false, null)
            .Returns(ToAsyncEnumerable(new[] { "a", "b" }));
        lattice.KeysAsync("b\0", null, false, null)
            .Returns(ToAsyncEnumerable(new[] { "c", "d" }));
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        var page1 = await grain.NextKeysAsync(2);
        var page2 = await grain.NextKeysAsync(2);

        Assert.That(page1.Keys, Is.EqualTo(new[] { "a", "b" }));
        Assert.That(page2.Keys, Is.EqualTo(new[] { "c", "d" }));
        Assert.That(state.State.LastYieldedKey, Is.EqualTo("d"));
    }

    [Test]
    public async Task NextKeysAsync_marks_exhausted_when_fewer_than_pageSize_returned()
    {
        var (grain, state, lattice) = CreateGrain();
        lattice.KeysAsync(null, null, false, null)
            .Returns(ToAsyncEnumerable(new[] { "a", "b" }));
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        var page = await grain.NextKeysAsync(10);

        Assert.That(page.Keys, Is.EqualTo(new[] { "a", "b" }));
        Assert.That(page.HasMore, Is.False);
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Exhausted));
    }

    [Test]
    public async Task NextKeysAsync_returns_empty_page_when_already_exhausted()
    {
        var (grain, _, lattice) = CreateGrain();
        lattice.KeysAsync(null, null, false, null)
            .Returns(ToAsyncEnumerable(new[] { "a" }));
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });
        await grain.NextKeysAsync(10); // exhausts

        var page = await grain.NextKeysAsync(10);

        Assert.That(page.Keys, Is.Empty);
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task NextKeysAsync_reverse_uses_end_exclusive_continuation()
    {
        var (grain, state, lattice) = CreateGrain();
        lattice.KeysAsync(null, null, true, null)
            .Returns(ToAsyncEnumerable(new[] { "d", "c" }));
        lattice.KeysAsync(null, "c", true, null)
            .Returns(ToAsyncEnumerable(new[] { "b", "a" }));
        await grain.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Keys,
            Reverse = true,
        });

        var page1 = await grain.NextKeysAsync(2);
        var page2 = await grain.NextKeysAsync(2);

        Assert.That(page1.Keys, Is.EqualTo(new[] { "d", "c" }));
        Assert.That(page2.Keys, Is.EqualTo(new[] { "b", "a" }));
        Assert.That(state.State.LastYieldedKey, Is.EqualTo("a"));
    }

    // --- NextEntriesAsync ---

    [Test]
    public async Task NextEntriesAsync_returns_page_and_advances()
    {
        var (grain, state, lattice) = CreateGrain();
        lattice.EntriesAsync(null, null, false)
            .Returns(ToAsyncEnumerable(new[]
            {
                new KeyValuePair<string, byte[]>("a", [1]),
                new KeyValuePair<string, byte[]>("b", [2]),
                new KeyValuePair<string, byte[]>("c", [3]),
            }));
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Entries });

        var page = await grain.NextEntriesAsync(2);

        Assert.That(page.Entries.Count, Is.EqualTo(2));
        Assert.That(page.Entries[0].Key, Is.EqualTo("a"));
        Assert.That(page.Entries[1].Key, Is.EqualTo("b"));
        Assert.That(page.HasMore, Is.True);
        Assert.That(state.State.LastYieldedKey, Is.EqualTo("b"));
    }

    [Test]
    public async Task NextEntriesAsync_throws_when_opened_for_keys()
    {
        var (grain, _, _) = CreateGrain();
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.NextEntriesAsync(10));
    }

    // --- DeleteRangeStepAsync ---

    [Test]
    public async Task DeleteRangeStepAsync_returns_complete_when_range_empty()
    {
        var (grain, state, lattice) = CreateGrain();
        lattice.KeysAsync("a", "z", false, null)
            .Returns(ToAsyncEnumerable(Array.Empty<string>()));
        await grain.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.DeleteRange,
            StartInclusive = "a",
            EndExclusive = "z",
        });

        var progress = await grain.DeleteRangeStepAsync(100);

        Assert.That(progress.IsComplete, Is.True);
        Assert.That(progress.DeletedThisStep, Is.EqualTo(0));
        Assert.That(progress.DeletedTotal, Is.EqualTo(0));
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Exhausted));
    }

    [Test]
    public async Task DeleteRangeStepAsync_final_step_deletes_full_remaining_range()
    {
        var (grain, state, lattice) = CreateGrain();
        lattice.KeysAsync("a", "z", false, null)
            .Returns(ToAsyncEnumerable(new[] { "a", "b", "c" }));
        lattice.DeleteRangeAsync("a", "z").Returns(Task.FromResult(3));
        await grain.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.DeleteRange,
            StartInclusive = "a",
            EndExclusive = "z",
        });

        var progress = await grain.DeleteRangeStepAsync(10);

        Assert.That(progress.DeletedThisStep, Is.EqualTo(3));
        Assert.That(progress.DeletedTotal, Is.EqualTo(3));
        Assert.That(progress.IsComplete, Is.True);
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Exhausted));
    }

    [Test]
    public async Task DeleteRangeStepAsync_partial_step_deletes_up_to_max_and_advances()
    {
        var (grain, state, lattice) = CreateGrain();
        lattice.KeysAsync("a", "z", false, null)
            .Returns(ToAsyncEnumerable(new[] { "a", "b", "c" }));
        lattice.DeleteRangeAsync("a", "b\0").Returns(Task.FromResult(2));
        await grain.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.DeleteRange,
            StartInclusive = "a",
            EndExclusive = "z",
        });

        var progress = await grain.DeleteRangeStepAsync(2);

        Assert.That(progress.DeletedThisStep, Is.EqualTo(2));
        Assert.That(progress.DeletedTotal, Is.EqualTo(2));
        Assert.That(progress.IsComplete, Is.False);
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open));
        Assert.That(state.State.LastYieldedKey, Is.EqualTo("b"));
    }

    [Test]
    public async Task DeleteRangeStepAsync_returns_complete_when_already_exhausted()
    {
        var existing = new FakePersistentState<LatticeCursorState>();
        existing.State.Phase = LatticeCursorPhase.Exhausted;
        existing.State.TreeId = TreeId;
        existing.State.Spec = new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.DeleteRange,
            StartInclusive = "a",
            EndExclusive = "z",
        };
        existing.State.DeletedTotal = 5;
        var (grain, _, _) = CreateGrain(existing);

        var progress = await grain.DeleteRangeStepAsync(10);

        Assert.That(progress.IsComplete, Is.True);
        Assert.That(progress.DeletedThisStep, Is.EqualTo(0));
        Assert.That(progress.DeletedTotal, Is.EqualTo(5));
    }

    [Test]
    public async Task DeleteRangeStepAsync_throws_when_opened_for_keys()
    {
        var (grain, _, _) = CreateGrain();
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DeleteRangeStepAsync(10));
    }

    // --- CloseAsync / IsOpenAsync ---

    [Test]
    public async Task IsOpenAsync_returns_false_before_open()
    {
        var (grain, _, _) = CreateGrain();
        Assert.That(await grain.IsOpenAsync(), Is.False);
    }

    [Test]
    public async Task IsOpenAsync_returns_true_after_open()
    {
        var (grain, _, _) = CreateGrain();
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });
        Assert.That(await grain.IsOpenAsync(), Is.True);
    }

    [Test]
    public async Task CloseAsync_clears_state()
    {
        var (grain, state, _) = CreateGrain();
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        await grain.CloseAsync();

        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.NotStarted));
        Assert.That(state.State.TreeId, Is.Empty);
    }

    [Test]
    public async Task CloseAsync_is_idempotent_on_never_opened_cursor()
    {
        var (grain, _, _) = CreateGrain();
        await grain.CloseAsync();
        await grain.CloseAsync();
        // no exception
    }

    // --- Idle-TTL reminder self-cleanup ---

    [Test]
    public async Task OpenAsync_registers_idle_ttl_reminder()
    {
        var registry = Substitute.For<IReminderRegistry>();
        var (grain, _, _) = CreateGrain(reminderRegistry: registry);

        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        await registry.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "cursor-ttl",
            Arg.Any<TimeSpan>(),
            Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task NextKeysAsync_slides_idle_ttl_reminder()
    {
        var registry = Substitute.For<IReminderRegistry>();
        var (grain, _, lattice) = CreateGrain(reminderRegistry: registry);
        lattice.KeysAsync(null, null, false, null)
            .Returns(ToAsyncEnumerable(new[] { "a" }));
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });
        registry.ClearReceivedCalls();

        await grain.NextKeysAsync(10);

        await registry.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "cursor-ttl", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task OpenAsync_clamps_small_ttl_to_one_minute_floor()
    {
        var registry = Substitute.For<IReminderRegistry>();
        var opts = new LatticeOptions { CursorIdleTtl = TimeSpan.FromSeconds(5) };
        var (grain, _, _) = CreateGrain(reminderRegistry: registry, options: opts);

        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        await registry.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "cursor-ttl",
            TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    [Test]
    public async Task OpenAsync_skips_reminder_when_ttl_is_infinite()
    {
        var registry = Substitute.For<IReminderRegistry>();
        var opts = new LatticeOptions { CursorIdleTtl = Timeout.InfiniteTimeSpan };
        var (grain, _, _) = CreateGrain(reminderRegistry: registry, options: opts);

        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        await registry.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "cursor-ttl", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task CloseAsync_unregisters_idle_ttl_reminder()
    {
        var registry = Substitute.For<IReminderRegistry>();
        var (grain, _, _) = CreateGrain(reminderRegistry: registry);
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });
        registry.ClearReceivedCalls();

        await grain.CloseAsync();

        await registry.Received(1).UnregisterReminder(
            Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task ReceiveReminder_idle_ttl_clears_state_and_unregisters()
    {
        var registry = Substitute.For<IReminderRegistry>();
        var (grain, state, _) = CreateGrain(reminderRegistry: registry);
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open));
        registry.ClearReceivedCalls();

        await grain.ReceiveReminder("cursor-ttl", new TickStatus());

        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.NotStarted),
            "ClearStateAsync resets state to its default (NotStarted).");
        await registry.Received(1).UnregisterReminder(
            Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task ReceiveReminder_ignores_unknown_reminder()
    {
        var registry = Substitute.For<IReminderRegistry>();
        var (grain, state, _) = CreateGrain(reminderRegistry: registry);
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        await grain.ReceiveReminder("some-other-reminder", new TickStatus());

        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open),
            "Unknown reminders must not clear cursor state.");
    }
}
