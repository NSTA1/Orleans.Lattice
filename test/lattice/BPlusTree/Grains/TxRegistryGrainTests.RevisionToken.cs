using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the registry's effective decisions revision - the
/// token a multi-key reader compares before and after its fan-out.
/// <para>
/// The readable surface is <c>Decisions</c> masked by <c>ForgottenAt</c> at the
/// current instant, so a tombstone crossing its retention boundary removes a row
/// from that surface with no write anywhere to bump a counter. The reader-side
/// fast path short-circuits on revision equality and never consults
/// <c>IsSnapshotStable</c>, so a token that fails to move across that transition
/// hands a reader a stale snapshot it will accept as authoritative. These tests
/// drive the same predicate that fast path uses
/// (<see cref="ReaderStabilityGate.IsRevisionStable(long, long)"/>) with real
/// registry tokens, so they fail if the token stops announcing the transition.
/// </para>
/// </summary>
public partial class TxRegistryGrainTests
{
    [Test]
    public async Task GetDecisionsRevisionAsync_moves_when_a_tombstone_crosses_its_expiry()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        var before = await grain.GetDecisionsRevisionAsync();

        // Nothing writes here. The only thing that changed is the clock, and
        // with it the set of decisions a reader can observe.
        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var after = await grain.GetDecisionsRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.Not.EqualTo(before),
                "A tombstone crossing its retention boundary changes the readable "
                + "surface, so the token readers compare must change with it.");
            Assert.That(after, Is.GreaterThan(before),
                "The token must be non-decreasing so it cannot revisit a value it "
                + "previously carried under a different surface.");
        });
    }

    [Test]
    public async Task IsRevisionStable_rejects_a_snapshot_taken_before_a_tombstone_expired()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var visible = Guid.NewGuid();
        var ageing = Guid.NewGuid();
        await grain.MarkCommittedAsync(visible);
        await grain.MarkCommittedAsync(ageing);
        await grain.ForgetAsync(ageing);

        // snap1: taken while the tombstone is still inside its retention window,
        // so the reader's view contains BOTH sagas.
        var snap1 = await grain.SnapshotWithRevisionAsync();
        Assert.That(snap1.Decisions, Does.ContainKey(ageing),
            "Precondition: the tombstone must still be inside its retention window.");

        clock.Advance(retention + TimeSpan.FromSeconds(1));

        // The reader's post-fan-out probe. This is the exact predicate the
        // LatticeGrain fast path runs before it decides it may skip the
        // IsSnapshotStable rule entirely.
        var revision2 = await grain.GetDecisionsRevisionAsync();

        Assert.That(
            ReaderStabilityGate.IsRevisionStable(snap1.Revision, revision2),
            Is.False,
            "The reader's cheap probe must reject a snapshot whose contents have "
            + "since been masked; it is the only check the fast path performs.");
    }

    [Test]
    public async Task GetDecisionsRevisionAsync_is_unchanged_when_no_tombstone_has_expired()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        var snap = await grain.SnapshotWithRevisionAsync();
        clock.Advance(retention - TimeSpan.FromSeconds(1));
        var revision2 = await grain.GetDecisionsRevisionAsync();

        Assert.That(
            ReaderStabilityGate.IsRevisionStable(snap.Revision, revision2),
            Is.True,
            "The token must not move while the surface is unchanged, or every "
            + "reader pays a snapshot re-fetch it did not need.");
    }

    [Test]
    public async Task SnapshotWithRevisionAsync_stamps_a_revision_consistent_with_its_own_mask()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var snap = await grain.SnapshotWithRevisionAsync();
        var probe = await grain.GetDecisionsRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(snap.Decisions[txid], Is.EqualTo(TxStatus.Indeterminate),
                "Precondition: the snapshot masks the expired tombstone.");
            Assert.That(snap.Revision, Is.EqualTo(probe),
                "The stamp and a probe at the same instant must agree, or a "
                + "snapshot arrives already disagreeing with its own token.");
        });
    }

    [Test]
    public async Task GetDecisionsRevisionAsync_never_decreases_across_a_batch_prune()
    {
        // The motivating case for the retirement epoch. ForgetAsync advances the
        // decisions counter ONCE per batch, but a batch prune of k expired
        // tombstones removes k rows from the live-expired term. Without the
        // epoch the sum would fall by k - 1 and could revisit a value it
        // previously carried under a different surface.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, state) = CreateGrain(retention: retention, timeProvider: clock);
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        var c = Guid.NewGuid();
        foreach (var txid in new[] { a, b, c })
        {
            await grain.MarkCommittedAsync(txid);
            await grain.ForgetAsync(txid);
        }

        clock.Advance(retention + TimeSpan.FromSeconds(1));
        var beforePrune = await grain.GetDecisionsRevisionAsync();

        // A fresh saga's cleanup is what drives the inline prune.
        var trigger = Guid.NewGuid();
        await grain.MarkCommittedAsync(trigger);
        await grain.ForgetAsync(trigger);

        var afterPrune = await grain.GetDecisionsRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ForgottenAt, Does.Not.ContainKey(a),
                "Precondition: the inline prune must have retired the expired tombstones.");
            Assert.That(afterPrune, Is.GreaterThanOrEqualTo(beforePrune),
                "Retiring several expired tombstones at once must not drop the token.");
        });
    }

    [Test]
    public async Task GetDecisionsRevisionAsync_never_revisits_a_value_across_a_staggered_expiry()
    {
        // The concrete aliasing sequence: two tombstones expiring at different
        // instants, then a batch prune. Every observation must carry a strictly
        // larger token than the last, because every one of them is a different
        // readable surface.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var first = Guid.NewGuid();
        await grain.MarkCommittedAsync(first);
        await grain.ForgetAsync(first);

        clock.Advance(TimeSpan.FromSeconds(10));
        var second = Guid.NewGuid();
        await grain.MarkCommittedAsync(second);
        await grain.ForgetAsync(second);

        var observations = new List<long> { await grain.GetDecisionsRevisionAsync() };

        // Only `first` is masked.
        clock.Advance(TimeSpan.FromSeconds(21));
        observations.Add(await grain.GetDecisionsRevisionAsync());

        // Now both are masked.
        clock.Advance(TimeSpan.FromSeconds(10));
        observations.Add(await grain.GetDecisionsRevisionAsync());

        // Retire both.
        var trigger = Guid.NewGuid();
        await grain.MarkCommittedAsync(trigger);
        await grain.ForgetAsync(trigger);
        observations.Add(await grain.GetDecisionsRevisionAsync());

        Assert.That(observations, Is.Unique,
            "Each step is a distinct readable surface, so no two may share a token: "
            + $"observed [{string.Join(", ", observations)}].");
        Assert.That(observations, Is.Ordered.Ascending,
            "The token must be non-decreasing across the whole sequence.");
    }

    [Test]
    public async Task MarkCommittedAsync_same_outcome_repeat_does_not_move_the_revision()
    {
        // The precision half of the defect: the tombstone-clearing prologue used
        // to run before the write-once guard, so a duplicate terminal was
        // classified Record rather than Idempotent and bumped the revision for a
        // surface change that never happened. Every reader mid-fan-out then paid
        // a snapshot re-fetch for nothing.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(
            retention: TimeSpan.FromMinutes(1),
            timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        var before = await grain.GetDecisionsRevisionAsync();
        var writesBefore = state.WriteCount;

        await grain.MarkCommittedAsync(txid);

        var after = await grain.GetDecisionsRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.EqualTo(before));
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
        });
    }

    [Test]
    public async Task MarkAbortedAsync_on_an_expired_tombstone_moves_the_revision()
    {
        // The conflicting-outcome path still clears the tombstone, which pulls a
        // masked saga back into the readable surface. That IS a surface change,
        // so the token has to move - and it only does because clearing an
        // already-expired tombstone raises the retirement epoch to cover the
        // live-expired count it removes.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var before = await grain.GetDecisionsRevisionAsync();
        var snapshotBefore = await grain.SnapshotAsync();

        await grain.MarkAbortedAsync(txid);

        var after = await grain.GetDecisionsRevisionAsync();
        var snapshotAfter = await grain.SnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(snapshotBefore[txid], Is.EqualTo(TxStatus.Indeterminate));
            Assert.That(snapshotAfter[txid], Is.EqualTo(TxStatus.Aborted),
                "Precondition: the clear must have unmasked the saga.");
            Assert.That(after, Is.GreaterThan(before));
        });
    }

    [Test]
    public async Task ClearTombstone_unwound_by_a_failed_persist_restores_the_revision()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, state) = CreateGrain(retention: retention, timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var before = await grain.GetDecisionsRevisionAsync();

        state.ThrowOnWrite = new InvalidOperationException("persist failed");
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.MarkAbortedAsync(txid));

        var after = await grain.GetDecisionsRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.EqualTo(before),
                "An unwound write must restore the token alongside the maps it "
                + "accounts for, or the pair stops being mutually consistent.");
            Assert.That(state.State.TombstoneRetirementEpoch, Is.Zero);
            Assert.That(state.State.ForgottenAt, Does.ContainKey(txid));
        });
    }

    [Test]
    public async Task GetDecisionsRevisionAsync_recomputes_after_a_tombstone_is_added_at_the_same_instant()
    {
        // The expiry scan is memoised on the reader hot path, keyed on the
        // earliest instant at which the answer could change. A mutation of
        // ForgottenAt inside that horizon must invalidate the memo, or the
        // registry keeps serving a count computed against a map that has moved.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var stale = Guid.NewGuid();
        await grain.MarkCommittedAsync(stale);
        await grain.ForgetAsync(stale);
        clock.Advance(retention + TimeSpan.FromSeconds(1));

        // Prime the memo while exactly one row is masked.
        var primed = await grain.GetDecisionsRevisionAsync();

        // Same instant, but the map now holds a second already-expired row: an
        // ancient tombstone loaded from persisted state, which is what a
        // reactivation on a busy tree looks like.
        var reloaded = Guid.NewGuid();
        var (grain2, state2) = CreateGrain(retention: retention, timeProvider: clock);
        state2.State.Decisions[stale] = TxStatus.Committed;
        state2.State.ForgottenAt[stale] = clock.GetUtcNow() - retention - TimeSpan.FromSeconds(1);
        var freshProbe = await grain2.GetDecisionsRevisionAsync();

        state2.State.Decisions[reloaded] = TxStatus.Committed;
        state2.State.ForgottenAt[reloaded] = clock.GetUtcNow() - retention - TimeSpan.FromSeconds(1);
        await grain2.ForgetAsync(Guid.NewGuid());

        var afterMutation = await grain2.GetDecisionsRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(primed, Is.Not.Zero,
                "Precondition: priming must have observed the masked row.");
            Assert.That(afterMutation, Is.Not.EqualTo(freshProbe),
                "A ForgottenAt mutation must invalidate the memoised expiry scan.");
        });
    }

    [Test]
    public async Task GetDecisionsRevisionAsync_is_stable_across_repeated_probes_at_one_instant()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        for (var i = 0; i < 5; i++)
        {
            var txid = Guid.NewGuid();
            await grain.MarkCommittedAsync(txid);
            await grain.ForgetAsync(txid);
        }
        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var first = await grain.GetDecisionsRevisionAsync();
        var second = await grain.GetDecisionsRevisionAsync();
        var third = await grain.GetDecisionsRevisionAsync();

        Assert.That(new[] { second, third }, Is.All.EqualTo(first),
            "The memoised probe must be idempotent at a fixed instant.");
    }

    [Test]
    public async Task GetDecisionsRevisionAsync_tolerates_a_retention_change_between_probes()
    {
        // Retention is re-read from the options monitor on every call, so the
        // memo is keyed on it too. Shrinking it masks rows that were visible a
        // moment ago, which is a surface change like any other.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var options = new LatticeOptions { TxDecisionRetention = TimeSpan.FromMinutes(10) };
        var state = new FakePersistentState<TxRegistryState>();
        var (grain, _) = CreateGrain(state: state, options: options, timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        clock.Advance(TimeSpan.FromMinutes(1));

        var wide = await grain.GetDecisionsRevisionAsync();

        options.TxDecisionRetention = TimeSpan.FromSeconds(30);
        var narrow = await grain.GetDecisionsRevisionAsync();

        Assert.That(narrow, Is.Not.EqualTo(wide),
            "Narrowing retention masks a previously-visible decision, so the "
            + "token must move even though no write occurred.");
    }
}
