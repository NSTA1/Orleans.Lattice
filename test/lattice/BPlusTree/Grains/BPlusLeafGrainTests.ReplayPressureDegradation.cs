using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2742's replay half: what a leaf does when
/// the commit-log read it needs cannot be afforded.
/// <para>
/// Before this, the answer was "abandon the partition". A read that ran out
/// of memory propagated out of the replay loop, the activation failed, and
/// the next activation re-read the identical 256-entry window from the
/// identical offset - so it failed identically, and banked nothing. That is
/// the measured shape of the incident: 3,080 stalled replays on
/// <c>repo-context-vector-index</c> against 1-27 on every other tree, with
/// the projection checkpoint frozen for the whole census and writes routed to
/// those leaves lost.
/// </para>
/// <para>
/// The important thing about that loop is that it is <b>self-reinforcing</b>,
/// not merely unlucky. A frozen checkpoint holds the whole-tree WAL GC pin, so
/// the WAL cannot trim; an untrimmed WAL makes the next replay's gap longer;
/// a longer gap makes the next read larger; a larger read is less affordable
/// than the one that just failed. Retrying the same width is therefore not a
/// neutral choice that happens not to work - it is the step that feeds the
/// loop. These tests pin the behaviour that breaks it: the loop spends
/// <i>width</i> to keep going, so the same activation gets past the obstacle
/// and the checkpoint moves while memory is still scarce.
/// </para>
/// <para>
/// Every arm is deterministic - pressure is scripted through the injected
/// coordinator, never provoked - because the alternative is a test that has
/// to exhaust a real heap to observe anything.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// A replay coordinator that refuses any read wider than
    /// <see cref="AffordableBudget"/>, the way the storage provider refuses a
    /// page it cannot allocate, and records every width it was asked for.
    /// </summary>
    private sealed class PressuredReplayCoordinator
    {
        private readonly CommitLogSliceEntry[] _entries;
        private readonly int _sliceSize;
        private int _served;

        internal PressuredReplayCoordinator(
            long head,
            int sliceSize,
            int affordableBudget,
            params CommitLogSliceEntry[] entries)
        {
            _entries = entries;
            _sliceSize = sliceSize;
            AffordableBudget = affordableBudget;

            Stub = Substitute.For<ILeafReplayCoordinatorGrain>();
            Stub.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(head));
            Stub.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
            Stub.ReadSliceAsync(
                    Arg.Any<long>(),
                    Arg.Any<long>(),
                    Arg.Any<int>(),
                    Arg.Any<CancellationToken>())
                .Returns(call => Serve(call.ArgAt<long>(0), call.ArgAt<long>(1), call.ArgAt<int>(2)));
        }

        internal ILeafReplayCoordinatorGrain Stub { get; }

        /// <summary>Widest read that can be served; a wider one is refused.</summary>
        internal int AffordableBudget { get; set; }

        /// <summary>Every width requested, in order. This is the whole story.</summary>
        internal List<int> RequestedBudgets { get; } = new();

        /// <summary>Entries served before pressure becomes total, or <c>null</c>.</summary>
        internal int? StarveAfterEntries { get; set; }

        private Task<IReadOnlyList<CommitLogSliceEntry>> Serve(long fromExclusive, long toInclusive, int budget)
        {
            RequestedBudgets.Add(budget);

            if (StarveAfterEntries is { } limit && _served >= limit)
            {
                AffordableBudget = 0;
            }

            if (budget > AffordableBudget)
            {
                throw new WalReadUnderPressureException(
                    ResumableTreeId, 0, fromExclusive, 64L * 1024 * 1024,
                    new OutOfMemoryException("scripted: the page could not be allocated."));
            }

            var cap = Math.Min(_sliceSize, budget);
            var slice = new List<CommitLogSliceEntry>();
            foreach (var entry in _entries)
            {
                if (entry.Offset <= fromExclusive)
                    continue;
                if (entry.Offset > toInclusive)
                    break;
                slice.Add(entry);
                if (slice.Count >= cap)
                    break;
            }

            _served += slice.Count;
            return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(slice);
        }
    }

    private static CommitLogSliceEntry[] PressuredReplayEntries(int count) =>
        Enumerable.Range(1, count)
            .Select(i => new CommitLogSliceEntry(
                i,
                BuildCommittedSet($"p{i:D2}", Encoding.UTF8.GetBytes($"v-{i}"), treeId: ResumableTreeId)))
            .ToArray();

    [Test]
    public async Task Replay_narrows_the_slice_and_continues_instead_of_abandoning_the_partition()
    {
        // The widest read is unaffordable and a narrow one is not, which is
        // the field condition exactly. The activation must complete.
        var entries = PressuredReplayEntries(12);
        var coord = new PressuredReplayCoordinator(head: 12, sliceSize: 4, affordableBudget: 16, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        var (grain, _) = BuildResumableLeaf(state, coord.Stub, store.Stub, reclassifyEveryN: 1);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(12L),
            "Replay must finish the partition despite the first reads being unaffordable.");
        for (var i = 1; i <= 12; i++)
        {
            Assert.That(await grain.GetAsync($"p{i:D2}"), Is.Not.Null, $"key p{i:D2} missing after replay");
        }
    }

    [Test]
    public async Task The_retried_read_is_strictly_narrower_than_the_one_that_failed()
    {
        // Retrying at the SAME width is the step that fed the livelock, and it
        // is indistinguishable from a correct fix by outcome alone on a
        // coordinator whose pressure is transient. Pin the widths.
        var entries = PressuredReplayEntries(12);
        var coord = new PressuredReplayCoordinator(head: 12, sliceSize: 4, affordableBudget: 16, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        var (grain, _) = BuildResumableLeaf(state, coord.Stub, store.Stub, reclassifyEveryN: 1);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(coord.RequestedBudgets, Has.Count.GreaterThan(1),
            "A read that was refused must be retried at all.");
        Assert.That(coord.RequestedBudgets[1], Is.LessThan(coord.RequestedBudgets[0]),
            "The retry after a refusal must ask for less than the read that was refused.");
        Assert.That(coord.RequestedBudgets[0], Is.EqualTo(256),
            "The first read of a healthy replay must still use the full slice budget.");
    }

    [Test]
    public async Task The_slice_budget_recovers_after_the_pressure_passes()
    {
        // Without this the loop is technically correct and practically still
        // stalled: a single blip would pin the partition at one entry per
        // slice for the rest of a multi-million-entry gap.
        var entries = PressuredReplayEntries(12);
        var coord = new PressuredReplayCoordinator(head: 12, sliceSize: 4, affordableBudget: 16, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        var (grain, _) = BuildResumableLeaf(state, coord.Stub, store.Stub, reclassifyEveryN: 1);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        var lowest = coord.RequestedBudgets.Min();
        var afterLowest = coord.RequestedBudgets.Skip(coord.RequestedBudgets.IndexOf(lowest) + 1).ToArray();

        Assert.That(afterLowest, Is.Not.Empty, "The replay must continue past its narrowest read.");
        Assert.That(afterLowest.Any(b => b > lowest), Is.True,
            "Once a narrowed read succeeds the next must ask for more, or the narrowing is permanent.");
    }

    [Test]
    public async Task Progress_is_banked_before_an_unaffordable_replay_gives_up()
    {
        // When even a single-entry read is refused there is no narrower retry
        // to make, so the activation fails - correctly, since it must not
        // serve a partition it did not finish. What makes that an exit rather
        // than the old loop is that the checkpoint MOVED: the next activation
        // faces a strictly shorter, strictly cheaper gap.
        var entries = PressuredReplayEntries(12);
        var coord = new PressuredReplayCoordinator(head: 12, sliceSize: 4, affordableBudget: 16, entries)
        {
            StarveAfterEntries = 4,
        };
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        var (grain, _) = BuildResumableLeaf(state, coord.Stub, store.Stub, reclassifyEveryN: 1);

        Assert.ThrowsAsync<WalReadUnderPressureException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None));

        Assert.That(state.State.ProjectionCheckpointOffset, Is.GreaterThan(0L),
            "A replay that ran out of memory must leave the checkpoint ahead of where it found it, "
            + "or the next activation repeats this one exactly.");
        Assert.That(coord.RequestedBudgets.Min(), Is.EqualTo(1),
            "The loop must exhaust its narrowing before it concedes.");
    }

    [Test]
    public async Task A_pressure_narrowed_replay_still_leaves_the_leaf_eligible_to_register_its_cursor()
    {
        // THE CLAUSE THE PM ASKED FOR, and the one level at which a silent
        // livelock is still reachable after every bound below it is correct.
        //
        // Completing OnActivateAsync is NECESSARY but not SUFFICIENT to
        // release the WAL GC's whole-tree Zero block pin. Registration is
        // gated twice after replay returns
        // (BPlusLeafGrain.Activation.cs:712-734):
        //
        //     if (advanced) return;                  // published during replay
        //     if (clock <= Zero) { SeedDurableMaterialiserFrontierAsync(); return; }
        //     await ReportCursorIfActiveAsync();     // registered
        //
        // So a replay that completes cleanly with advanced == false AND a
        // Zero projection clock re-seeds the Zero pin and leaves the tree
        // blocked - a stall wearing the appearance of a healthy activation.
        //
        // What makes narrowing safe here is that `advanced` is derived from
        // `maxApplied > checkpoint`, and maxApplied is bumped for every
        // SCANNED entry (Activation.cs:3974, deliberately outside the
        // ShouldApplyDuringReplay filter). Narrowing only changes how many
        // entries a slice carries, never whether the loop scans them, and the
        // always-take-one hatch guarantees a slice is never empty while work
        // remains. So a narrowed replay that scans anything above the
        // checkpoint reports advanced == true, exactly as an unnarrowed one
        // does.
        //
        // This test pins that equivalence: the SAME entries, replayed under
        // pressure and without it, must leave the same checkpoint. If
        // narrowing could ever swallow progress, the pressured arm would
        // land short and the leaf would fall through to the Zero branch.
        var entries = PressuredReplayEntries(12);

        var calm = new PressuredReplayCoordinator(head: 12, sliceSize: 4, affordableBudget: 256, entries);
        var calmState = NewResumableState();
        var (calmGrain, _) = BuildResumableLeaf(calmState, calm.Stub, new InMemorySnapshotStore().Stub, reclassifyEveryN: 1);
        await ((IGrainBase)calmGrain).OnActivateAsync(CancellationToken.None);

        var pressured = new PressuredReplayCoordinator(head: 12, sliceSize: 4, affordableBudget: 1, entries);
        var pressuredState = NewResumableState();
        var (pressuredGrain, _) = BuildResumableLeaf(pressuredState, pressured.Stub, new InMemorySnapshotStore().Stub, reclassifyEveryN: 1);
        await ((IGrainBase)pressuredGrain).OnActivateAsync(CancellationToken.None);

        Assert.That(pressured.RequestedBudgets.Min(), Is.LessThan(256),
            "The pressured arm must actually have narrowed, or this proves nothing.");

        Assert.That(
            pressuredState.State.ProjectionCheckpointOffset,
            Is.EqualTo(calmState.State.ProjectionCheckpointOffset),
            "A narrowed replay banked a different checkpoint than an unnarrowed one over the same "
            + "entries. Any shortfall here can leave `advanced` false, which sends the leaf down the "
            + "clock <= Zero branch, re-seeds the Zero block pin, and keeps the whole tree's WAL "
            + "un-trimmable while the activation reports success.");

        Assert.That(pressuredState.State.ProjectionCheckpointOffset, Is.GreaterThan(0L),
            "A checkpoint still at zero means advanced == false and no registration.");

        // The projection clock is the second gate, and it is advanced by
        // APPLYING a mutation rather than scanning one. A leaf that owns the
        // keys it replayed must therefore leave Zero, or ReportCursorIfActive
        // returns early and the tree stays blocked.
        Assert.That(pressuredState.State.Clock, Is.GreaterThan(HybridLogicalClock.Zero),
            "The leaf applied its own entries under pressure but its projection clock is still Zero, "
            + "so ReportCursorIfActiveAsync would decline to register it.");
    }

    [Test]
    public async Task An_unpressured_replay_never_narrows()
    {
        // Negative control. A loop that narrowed unconditionally would pass
        // every arm above while halving read throughput on every healthy
        // deployment in the estate.
        var entries = PressuredReplayEntries(12);
        var coord = new PressuredReplayCoordinator(head: 12, sliceSize: 4, affordableBudget: int.MaxValue, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        var (grain, _) = BuildResumableLeaf(state, coord.Stub, store.Stub, reclassifyEveryN: 1);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(coord.RequestedBudgets, Is.All.EqualTo(256),
            "Nothing refused a read, so nothing should have given up width.");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(12L));
    }

    // ---------------------------------------------------------------------
    // Classification: which failures count as memory pressure.
    // ---------------------------------------------------------------------

    [Test]
    public void Memory_pressure_is_recognised_through_the_wrappers_a_grain_call_adds()
    {
        var verdict = new WalReadUnderPressureException("t", 0, 1, 2, new OutOfMemoryException());

        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.IsReadMemoryPressure(verdict), Is.True);
            Assert.That(BPlusLeafGrain.IsReadMemoryPressure(new OutOfMemoryException()), Is.True,
                "A provider that has not adopted the typed verdict must still be recognised.");
            Assert.That(
                BPlusLeafGrain.IsReadMemoryPressure(new InvalidOperationException("wrapped", verdict)),
                Is.True,
                "A grain call wraps what it propagates; matching only the outermost type would miss every "
                + "real occurrence.");
            Assert.That(
                BPlusLeafGrain.IsReadMemoryPressure(new AggregateException(new Exception("a"), verdict)),
                Is.True);
        });
    }

    [Test]
    public void An_ordinary_replay_failure_is_not_mistaken_for_memory_pressure()
    {
        // This is the arm that matters. Classifying too widely would convert
        // real faults - a corrupt record, a missing shard - into a silent
        // retry loop that narrows to one entry and then rethrows something
        // unrelated, which is strictly worse than the bug being fixed.
        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.IsReadMemoryPressure(null), Is.False);
            Assert.That(BPlusLeafGrain.IsReadMemoryPressure(new InvalidOperationException()), Is.False);
            Assert.That(BPlusLeafGrain.IsReadMemoryPressure(new IOException("disk")), Is.False);
            Assert.That(
                BPlusLeafGrain.IsReadMemoryPressure(new OperationCanceledException()),
                Is.False,
                "Cancellation is the runtime reclaiming an activation, and has its own handling.");
            Assert.That(
                BPlusLeafGrain.IsReadMemoryPressure(
                    new AggregateException(new InvalidOperationException(), new IOException())),
                Is.False);
        });
    }

    [Test]
    public void Classification_terminates_on_a_self_referential_exception_chain()
    {
        // A cycle here would hang an activation that was already failing,
        // which is the least recoverable outcome available.
        var outer = new AggregateException(new OutOfMemoryException());
        var deep = (Exception)outer;
        for (var i = 0; i < 64; i++)
        {
            deep = new InvalidOperationException("layer " + i, deep);
        }

        Assert.That(BPlusLeafGrain.IsReadMemoryPressure(deep), Is.False,
            "Beyond the bounded depth the answer must be a safe 'no', not an unbounded walk.");
    }
}
