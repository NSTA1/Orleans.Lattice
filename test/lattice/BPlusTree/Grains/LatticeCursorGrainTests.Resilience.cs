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

/// <summary>
/// Resilience and resume-from-checkpoint coverage for
/// <see cref="LatticeCursorGrain"/>: exception-swallowing TTL paths, failed
/// <c>ClearStateAsync</c>, post-close access, and recovery of an existing
/// persisted cursor on a fresh activation.
/// </summary>
[TestFixture]
public class LatticeCursorGrainResilienceTests
{
    private const string TreeId = "cursor-tree";
    private const string CursorId = "cur-res";

    private static (LatticeCursorGrain grain,
                     FakePersistentState<LatticeCursorState> state,
                     ILattice lattice,
                     IReminderRegistry registry) CreateGrain(
        FakePersistentState<LatticeCursorState>? existingState = null,
        IReminderRegistry? reminderRegistry = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice-cursor", $"{TreeId}/{CursorId}"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var registry = reminderRegistry ?? Substitute.For<IReminderRegistry>();
        if (reminderRegistry is null)
        {
            registry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
                .Returns(Task.FromResult<IGrainReminder?>(Substitute.For<IGrainReminder>()));
        }

        var opts = new LatticeOptions();
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
        return (grain, state, lattice, registry);
    }

    private static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
            await Task.Yield();
        }
    }

    // --- Exception-swallowing TTL paths (G3) ---

    [Test]
    public async Task OpenAsync_succeeds_when_RegisterOrUpdateReminder_throws()
    {
        var registry = Substitute.For<IReminderRegistry>();
        registry.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(),
                Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Throws(new InvalidOperationException("reminder service unavailable"));
        var (grain, state, _, _) = CreateGrain(reminderRegistry: registry);

        // Must not surface the reminder-registry failure to the caller.
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open));
    }

    [Test]
    public async Task CloseAsync_succeeds_when_UnregisterReminder_throws()
    {
        var registry = Substitute.For<IReminderRegistry>();
        registry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult<IGrainReminder?>(Substitute.For<IGrainReminder>()));
        var (grain, state, _, _) = CreateGrain(reminderRegistry: registry);
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });

        // Arm failure only on the close-path unregister call.
        registry.UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>())
            .Throws(new InvalidOperationException("reminder service unavailable"));

        await grain.CloseAsync();

        // State should still be cleared even though the reminder unregister blew up.
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.NotStarted));
    }

    // --- CloseAsync with failing ClearStateAsync (G1) ---

    [Test]
    public async Task CloseAsync_marks_closed_in_memory_when_ClearStateAsync_throws()
    {
        var (grain, state, _, _) = CreateGrain();
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });
        state.ThrowOnClear = new InvalidOperationException("storage unavailable");

        await grain.CloseAsync();

        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Closed),
            "When ClearStateAsync fails, the grain falls back to an in-memory Closed phase.");
    }

    // --- Access after Close (G6) ---

    [Test]
    public async Task NextKeysAsync_throws_after_successful_close()
    {
        var (grain, _, _, _) = CreateGrain();
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });
        await grain.CloseAsync();

        // Successful close resets state to NotStarted (ClearStateAsync → default).
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.NextKeysAsync(10));
    }

    [Test]
    public async Task NextKeysAsync_throws_after_close_with_failed_state_clear()
    {
        var (grain, state, _, _) = CreateGrain();
        await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });
        state.ThrowOnClear = new InvalidOperationException("storage unavailable");
        await grain.CloseAsync();

        // In-memory Closed phase must also reject further work in the same activation.
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.NextKeysAsync(10));
    }

    // --- Resume-from-checkpoint (G5 — unit-level failover simulation) ---

    [Test]
    public async Task Fresh_activation_resumes_from_persisted_LastYieldedKey()
    {
        // Step 1: first activation consumes a page and checkpoints "b".
        var sharedState = new FakePersistentState<LatticeCursorState>();
        var (grain1, _, lattice1, _) = CreateGrain(existingState: sharedState);
        lattice1.KeysAsync(null, null, false, null)
            .Returns(ToAsyncEnumerable(new[] { "a", "b" }));

        await grain1.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Keys });
        var firstPage = await grain1.NextKeysAsync(2);

        Assert.That(firstPage.Keys, Is.EqualTo(new[] { "a", "b" }));
        Assert.That(sharedState.State.LastYieldedKey, Is.EqualTo("b"),
            "First activation must checkpoint the last yielded key.");

        // Step 2: discard grain1 and build a fresh grain over the same
        // persisted state — simulates Orleans reactivating the grain on a
        // different silo after a failover.
        var (grain2, _, lattice2, _) = CreateGrain(existingState: sharedState);
        lattice2.KeysAsync("b\0", null, false, null)
            .Returns(ToAsyncEnumerable(new[] { "c", "d" }));

        var secondPage = await grain2.NextKeysAsync(2);

        Assert.That(secondPage.Keys, Is.EqualTo(new[] { "c", "d" }),
            "Resumed activation must continue from persisted LastYieldedKey + \\0.");
        Assert.That(sharedState.State.LastYieldedKey, Is.EqualTo("d"));
    }
}
