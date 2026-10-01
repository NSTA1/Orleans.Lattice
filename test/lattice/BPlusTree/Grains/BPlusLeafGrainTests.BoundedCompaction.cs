using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue 4135:
/// <c>BPlusLeafGrain.CompactTombstonesAsync</c> did an unbounded amount of work
/// in one grain turn - a full synchronous scan of every row, then one
/// sequentially awaited WAL append per condemned entry - so a tombstone-heavy
/// leaf overran the Orleans request timeout. The cost scaled with the number of
/// tombstones, which is exactly the quantity compaction exists to reduce, so
/// the overrun got likelier the more the leaf needed compacting.
/// <para>
/// Two halves are pinned here, and the second is the one that makes the first
/// safe. <b>Bounding the call</b> is tested by
/// <see cref="CompactTombstones_stops_on_its_work_budget_rather_than_draining_the_whole_leaf"/>.
/// <b>Letting the caller learn the result was partial</b> is tested by the
/// stamp and outcome fixtures below: a truncated pass must not stamp
/// <c>LastCompactionVersion</c>, because a bounded call that reported success
/// indistinguishably from a drained one would let the coordinator clear the
/// leaf's dirty mark with condemned entries still in place.
/// </para>
/// <para>
/// <b>The property worth testing is the monotone decrease, not the bound.</b> A
/// bound alone proves only that one call returns; it says nothing about whether
/// the leaf can ever escape. Because each reaped entry is committed to the WAL
/// before it leaves the cache, reaped entries stay reaped across the turn
/// boundary and every re-scan finds strictly less - which is what
/// <see cref="CompactTombstones_reaps_strictly_less_on_every_pass_until_the_leaf_drains"/>
/// pins. Paired with
/// <see cref="CompactTombstones_reaps_at_least_one_entry_even_when_its_budget_is_already_spent"/>,
/// which rules out the zero-progress pass, those two give termination.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// A budget so small it is already spent by the time the removal loop runs,
    /// which makes the truncation branch fire deterministically rather than on
    /// a wall-clock race. Positive, because a non-positive duration disables the
    /// deadline entirely (see
    /// <see cref="CompactTombstones_runs_unbounded_when_the_wall_clock_net_is_disabled"/>).
    /// </summary>
    private static readonly TimeSpan AlreadySpentBudget = TimeSpan.FromTicks(1);

    private static LatticeOptions WithCompactionBudget(TimeSpan budget) =>
        new() { BackgroundDrainMaxDuration = budget };

    /// <summary>
    /// Seeds <paramref name="count"/> tombstones stamped far enough in the past
    /// that any non-negative grace period condemns all of them, and ticks the
    /// version so the leaf's "nothing changed since last compaction"
    /// short-circuit does not fire.
    /// </summary>
    private static void SeedCondemnedTombstones(
        BPlusLeafGrain grain, FakePersistentState<LeafNodeState> state, int count)
    {
        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        for (var i = 0; i < count; i++)
            grain.EntriesForTest[$"dead-{i:D4}"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Version.Tick("test");
    }

    [Test]
    public async Task CompactTombstones_stops_on_its_work_budget_rather_than_draining_the_whole_leaf()
    {
        // A per-append delay is what gives this test its teeth. The unbounded
        // implementation awaited one WAL append per condemned entry with
        // nothing watching the clock, so with 40 entries at 20ms it would run
        // for ~800ms against a 50ms budget and reap all 40. The bounded one
        // stops as soon as the budget is spent.
        var commitLog = new FakeCommitLogWriter { DelayPerAppend = TimeSpan.FromMilliseconds(20) };
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(
            state, options: WithCompactionBudget(TimeSpan.FromMilliseconds(50)), commitLog: commitLog);
        SeedCondemnedTombstones(grain, state, 40);

        var result = await grain.CompactTombstonesAsync(TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(result.Completed, Is.False,
                "a pass that stopped on its work budget with condemned entries left must report "
                + "itself incomplete, or the coordinator will drain the leaf's dirty mark.");
            Assert.That(result.EntriesRemoved, Is.GreaterThanOrEqualTo(1),
                "a bounded pass must still make progress - see the forward-progress fixture.");
            Assert.That(result.EntriesRemoved, Is.LessThan(40),
                "the whole point of the bound is that one turn does not reap every condemned "
                + "entry on a tombstone-heavy leaf.");
            Assert.That(grain.EntriesForTest, Has.Count.GreaterThan(0),
                "entries this turn did not reach must still be in the leaf.");
        });
    }

    [Test]
    public async Task CompactTombstones_reaps_strictly_less_on_every_pass_until_the_leaf_drains()
    {
        // THE EXIT FROM THE ABSORBING STATE. A bound that re-did the same work
        // each turn would return promptly forever and never drain the leaf. Per
        // entry the WAL append is the commit point, so what a truncated pass
        // reaped stays reaped and the next pass starts from a strictly smaller
        // condemned set. Driving the loop to completion is what proves it.
        const int Tombstones = 5;
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: WithCompactionBudget(AlreadySpentBudget));
        SeedCondemnedTombstones(grain, state, Tombstones);

        var remainingPerPass = new List<int>();
        var passes = 0;
        LeafCompactionResult result;
        do
        {
            result = await grain.CompactTombstonesAsync(TimeSpan.Zero);
            remainingPerPass.Add(grain.EntriesForTest.Count);
            passes++;
            Assert.That(passes, Is.LessThanOrEqualTo(Tombstones + 1),
                "the leaf must drain in a bounded number of passes; more passes than condemned "
                + "entries means a pass reaped nothing and the tree cannot escape.");
        }
        while (!result.Completed);

        Assert.Multiple(() =>
        {
            Assert.That(remainingPerPass, Is.Ordered.Descending,
                "every pass must leave strictly fewer entries than the one before it.");
            Assert.That(remainingPerPass, Is.Unique,
                "a pass that left the count unchanged did no work and would loop forever.");
            Assert.That(grain.EntriesForTest, Is.Empty,
                "the leaf must actually drain, not merely return promptly each turn.");
            Assert.That(passes, Is.GreaterThan(1),
                "this fixture is only meaningful if the budget really did truncate; a single "
                + "pass would mean the turn ran unbounded and the test proves nothing.");
        });
    }

    [Test]
    public async Task CompactTombstones_reaps_at_least_one_entry_even_when_its_budget_is_already_spent()
    {
        // Forward progress. The budget is checked AFTER each removal rather than
        // before it, so a turn that arrives with nothing left to spend - a cold
        // activation whose replay barrier consumed the whole net - still reaps
        // one entry. Checking first would reap zero, every pass would re-scan
        // the identical condemned set, and the bound would have converted an
        // overrun into a livelock.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: WithCompactionBudget(AlreadySpentBudget));
        SeedCondemnedTombstones(grain, state, 3);

        var result = await grain.CompactTombstonesAsync(TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(result.EntriesRemoved, Is.EqualTo(1),
                "an exhausted budget must still yield exactly one entry of progress.");
            Assert.That(result.Completed, Is.False);
            Assert.That(grain.EntriesForTest, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public async Task CompactTombstones_does_not_stamp_LastCompactionVersion_on_a_truncated_pass()
    {
        // The fail-closed half, and the reason no durability marker is needed.
        // Stamping is a claim of completeness; NOT stamping requires no write,
        // so a truncated pass cannot fail to record that it was truncated - which
        // matters because the write pressure that causes truncation is the same
        // pressure that fails a coordinator state write.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: WithCompactionBudget(AlreadySpentBudget));
        SeedCondemnedTombstones(grain, state, 3);

        var truncatedPass = await grain.CompactTombstonesAsync(TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(truncatedPass.Completed, Is.False);
            Assert.That(state.State.LastCompactionVersion.DominatesOrEquals(state.State.Version), Is.False,
                "stamping after a truncated pass would short-circuit every later pass until a new "
                + "write ticked the version vector, dead-ending the condemned entries this turn "
                + "never reached.");
            Assert.That(state.WriteCount, Is.Zero,
                "recording the truncation must cost no state write - a marker would be unavailable "
                + "under exactly the write pressure that causes the truncation.");
        });
    }

    [Test]
    public async Task CompactTombstones_stamps_LastCompactionVersion_once_a_bounded_pass_finally_completes()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: WithCompactionBudget(AlreadySpentBudget));
        SeedCondemnedTombstones(grain, state, 3);

        LeafCompactionResult result;
        do
        {
            result = await grain.CompactTombstonesAsync(TimeSpan.Zero);
        }
        while (!result.Completed);

        Assert.That(state.State.LastCompactionVersion.DominatesOrEquals(state.State.Version), Is.True,
            "once the leaf is genuinely drained the completeness claim must land, or the "
            + "short-circuit can never engage and every later pass pays a full re-scan.");
    }

    [Test]
    public async Task CompactTombstones_reports_only_the_entries_it_actually_reaped()
    {
        // The returned count is what left the leaf, not what the scan condemned.
        // Reporting the condemned set would make every truncated pass overstate
        // the reclamation an operator reads, on the exact passes where the
        // figure matters most.
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(
            state, options: WithCompactionBudget(AlreadySpentBudget), commitLog: commitLog);
        SeedCondemnedTombstones(grain, state, 6);

        var result = await grain.CompactTombstonesAsync(TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(result.EntriesRemoved, Is.EqualTo(commitLog.AppendCount),
                "the reported count must equal the number of reap envelopes actually committed.");
            Assert.That(result.EntriesRemoved, Is.EqualTo(6 - grain.EntriesForTest.Count),
                "and must equal the number of rows that actually left the leaf.");
        });
    }

    [Test]
    public async Task CompactTombstones_tags_a_truncated_pass_partial_rather_than_reaped()
    {
        // outcome=partial is what resolves the conflation issue 4135 names: before
        // the bound, an overrun surfaced as a timeout exception and was counted
        // as outcome=skipped alongside a leaf that genuinely declined. A truncated
        // pass did reap, so tagging it `reaped` would read as a drained leaf on
        // the one panel operators use to tell work-done from work-outstanding.
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "partial-outcome-tree";
        var grain = CreateGrain(state, options: WithCompactionBudget(AlreadySpentBudget));
        SeedCondemnedTombstones(grain, state, 4);

        var outcomes = new List<string>();
        using var listener = new System.Diagnostics.Metrics.MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                    && inst.Name == "orleans.lattice.compaction.leaves.visited")
                    l.EnableMeasurementEvents(inst);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == LatticeMetrics.TagOutcome && tag.Value is string outcome)
                    outcomes.Add(outcome);
            }
        });
        listener.Start();

        await grain.CompactTombstonesAsync(TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(outcomes, Is.EqualTo(new[] { "partial" }),
                "a truncated pass contributes exactly one sample, tagged partial.");
            Assert.That(outcomes, Does.Not.Contain("reaped"),
                "reaped must mean the leaf finished, or it cannot be read as work-done.");
        });
    }

    [Test]
    public async Task CompactTombstones_runs_unbounded_when_the_wall_clock_net_is_disabled()
    {
        // Consistent with every other LeafWalkBudget site: a non-positive
        // duration disables the deadline. A misconfigured option must degrade to
        // the pre-4135 unbounded turn rather than to a leaf that reaps one entry
        // per pass forever, which would be a far worse failure than the one the
        // bound removes.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: WithCompactionBudget(TimeSpan.Zero));
        SeedCondemnedTombstones(grain, state, 25);

        var result = await grain.CompactTombstonesAsync(TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(result.Completed, Is.True);
            Assert.That(result.EntriesRemoved, Is.EqualTo(25));
            Assert.That(grain.EntriesForTest, Is.Empty);
        });
    }

    [Test]
    public async Task CompactTombstones_bounded_pass_still_routes_every_reap_through_the_WAL()
    {
        // The bound must not become a way to drop entries from the cache without
        // committing them. If a truncated pass removed a row whose reap envelope
        // never landed, a reactivated leaf would replay the row back and the
        // monotone decrease the exit condition depends on would not hold.
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(
            state, options: WithCompactionBudget(AlreadySpentBudget), commitLog: commitLog);
        SeedCondemnedTombstones(grain, state, 5);

        var reapedKeys = new List<string>();
        LeafCompactionResult result;
        do
        {
            var before = grain.EntriesForTest.Keys.ToHashSet();
            result = await grain.CompactTombstonesAsync(TimeSpan.Zero);
            reapedKeys.AddRange(before.Except(grain.EntriesForTest.Keys));
        }
        while (!result.Completed);

        Assert.Multiple(() =>
        {
            Assert.That(commitLog.Appended.Select(r => r.Key), Is.EquivalentTo(reapedKeys),
                "every row removed across the bounded passes must have a matching reap envelope.");
            Assert.That(commitLog.Appended.All(r => r.Op == MutationKind.Tombstone && r.IsMerge), Is.True,
                "the envelope shape must not change just because the pass was truncated.");
        });
    }

    [Test]
    public async Task CompactTombstones_short_circuit_reports_a_completed_pass()
    {
        // The "nothing changed since last compaction" fast path does no work and
        // has nothing outstanding, so it must claim completeness - otherwise the
        // coordinator would retain a dirty mark for a leaf that is already clean
        // and re-nominate it forever.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: WithCompactionBudget(AlreadySpentBudget));

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        await grain.DeleteAsync("k");
        var drained = await grain.CompactTombstonesAsync(TimeSpan.Zero);

        var shortCircuited = await grain.CompactTombstonesAsync(TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(drained.Completed, Is.True);
            Assert.That(shortCircuited.Completed, Is.True);
            Assert.That(shortCircuited.EntriesRemoved, Is.Zero);
        });
    }

    [Test]
    public async Task CompactTombstones_in_grace_tombstones_still_suppress_the_completeness_stamp()
    {
        // The pre-existing audit-bug-2 condition and the new truncation
        // condition are independent reasons not to claim completeness, and both
        // must gate the stamp. This pins the older one against a regression
        // introduced by the newer one sharing its branch.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: WithCompactionBudget(TimeSpan.Zero));

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        await grain.DeleteAsync("k");

        var result = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.Multiple(() =>
        {
            Assert.That(result.Completed, Is.True,
                "an in-grace tombstone is not outstanding work the caller must re-nominate for - "
                + "it is work that is not yet due, so the pass itself completed.");
            Assert.That(state.State.LastCompactionVersion.DominatesOrEquals(state.State.Version), Is.False,
                "but the completeness stamp must still be withheld, or the eligible-later "
                + "tombstone is dead-ended (audit bug #2).");
        });
    }
}
