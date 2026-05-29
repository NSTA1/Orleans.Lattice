using NSubstitute;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// Phase R1 (revised): cheap-version-probe optimisation. These tests
// pin the contract the LatticeGrain reader-side double-checked retry
// depends on: every observable mutation of the Decisions map bumps a
// monotonic revision counter, the probe and the snapshot are captured
// atomically, and the probe is marked [AlwaysInterleave] so heavy saga
// workloads do not block reader-side probes.
public partial class TxRegistryGrainTests
{
    [Test]
    public async Task DecisionsRevision_starts_at_zero_on_fresh_state()
    {
        var (grain, _) = CreateGrain();

        var revision = await grain.GetDecisionsRevisionAsync();

        Assert.That(revision, Is.EqualTo(0L));
    }

    [Test]
    public async Task MarkCommittedAsync_bumps_revision()
    {
        var (grain, _) = CreateGrain();
        var before = await grain.GetDecisionsRevisionAsync();

        await grain.MarkCommittedAsync(Guid.NewGuid());
        var after = await grain.GetDecisionsRevisionAsync();

        Assert.That(after, Is.GreaterThan(before));
    }

    [Test]
    public async Task MarkAbortedAsync_bumps_revision()
    {
        var (grain, _) = CreateGrain();
        var before = await grain.GetDecisionsRevisionAsync();

        await grain.MarkAbortedAsync(Guid.NewGuid());
        var after = await grain.GetDecisionsRevisionAsync();

        Assert.That(after, Is.GreaterThan(before));
    }

    [Test]
    public async Task Repeated_MarkCommittedAsync_with_same_outcome_does_not_bump_revision()
    {
        // The grain's idempotency short-circuit on a repeat-same-outcome
        // call returns without mutating Decisions, so the revision must
        // not advance either - otherwise a reader's post-fan-out probe
        // would spuriously fall through to the disambiguation path on
        // every retry of an already-committed saga's terminal mark.
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        var afterFirst = await grain.GetDecisionsRevisionAsync();

        await grain.MarkCommittedAsync(txid);
        var afterSecond = await grain.GetDecisionsRevisionAsync();

        Assert.That(afterSecond, Is.EqualTo(afterFirst));
    }

    [Test]
    public async Task Sequential_marks_produce_strictly_monotonic_revisions()
    {
        var (grain, _) = CreateGrain();
        var observed = new List<long>();
        observed.Add(await grain.GetDecisionsRevisionAsync());

        for (var i = 0; i < 5; i++)
        {
            await grain.MarkCommittedAsync(Guid.NewGuid());
            observed.Add(await grain.GetDecisionsRevisionAsync());
        }

        for (var i = 1; i < observed.Count; i++)
        {
            Assert.That(observed[i], Is.GreaterThan(observed[i - 1]),
                $"revision[{i}] = {observed[i]} must be strictly greater than revision[{i - 1}] = {observed[i - 1]}");
        }
    }

    [Test]
    public async Task ForgetAsync_with_zero_retention_bumps_revision()
    {
        // Zero-retention ForgetAsync removes the Decisions entry
        // immediately - that mutation must bump the revision so a
        // reader observing the registry through SnapshotWithRevision
        // before the Forget cannot match its probe afterward.
        var (grain, _) = CreateGrain(retention: TimeSpan.Zero);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        var beforeForget = await grain.GetDecisionsRevisionAsync();

        await grain.ForgetAsync(txid);
        var afterForget = await grain.GetDecisionsRevisionAsync();

        Assert.That(afterForget, Is.GreaterThan(beforeForget));
    }

    [Test]
    public async Task ForgetAsync_with_tombstone_retention_does_not_bump_revision_on_first_call()
    {
        // ForgetAsync under non-zero retention tombstones the decision
        // (it stays in the Decisions map until the tombstone TTL
        // elapses). The observable Decisions surface is unchanged at
        // tombstone time, so the revision must NOT bump - readers
        // continue to observe the same entry until the prune fires.
        // This is a deliberately conservative reading of "what counts
        // as a mutation": a non-bump here means a reader's post-Forget
        // probe still matches its pre-Forget probe, which is safe
        // because the Decisions surface really is the same.
        var (grain, _) = CreateGrain(retention: TimeSpan.FromMinutes(1));
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        var beforeForget = await grain.GetDecisionsRevisionAsync();

        await grain.ForgetAsync(txid);
        var afterForget = await grain.GetDecisionsRevisionAsync();

        Assert.That(afterForget, Is.EqualTo(beforeForget));
    }

    [Test]
    public async Task SnapshotWithRevisionAsync_returns_dict_and_revision_consistently()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        var revisionAfterMark = await grain.GetDecisionsRevisionAsync();

        var pair = await grain.SnapshotWithRevisionAsync();

        Assert.That(pair.Revision, Is.EqualTo(revisionAfterMark),
            "Revision returned by SnapshotWithRevisionAsync must equal the standalone probe captured under the same registry state.");
        Assert.That(pair.Decisions, Has.Count.EqualTo(1));
        Assert.That(pair.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task SnapshotWithRevisionAsync_on_empty_registry_returns_empty_dict_with_zero_revision()
    {
        var (grain, _) = CreateGrain();

        var pair = await grain.SnapshotWithRevisionAsync();

        Assert.That(pair.Decisions, Is.Empty);
        Assert.That(pair.Revision, Is.EqualTo(0L));
    }

    [Test]
    public async Task SnapshotWithRevisionAsync_revision_advances_alongside_dict_after_mark()
    {
        var (grain, _) = CreateGrain();
        var before = await grain.SnapshotWithRevisionAsync();

        await grain.MarkCommittedAsync(Guid.NewGuid());
        var after = await grain.SnapshotWithRevisionAsync();

        Assert.That(after.Revision, Is.GreaterThan(before.Revision));
        Assert.That(after.Decisions, Has.Count.EqualTo(before.Decisions.Count + 1));
    }

    [Test]
    public void GetDecisionsRevisionAsync_is_marked_AlwaysInterleave()
    {
        // Pin the [AlwaysInterleave] attribute on the probe so heavy
        // saga workloads (which would otherwise hold the registry's
        // turn token for the duration of every Mark/Forget call) do
        // not block reader-side probes. Removing the attribute would
        // silently re-introduce the queue-behind-saga-turn pathology
        // the Phase R1 optimisation was designed to remove.
        var method = typeof(ITxRegistryGrain).GetMethod(nameof(ITxRegistryGrain.GetDecisionsRevisionAsync));
        Assert.That(method, Is.Not.Null);
        var attribute = method!.GetCustomAttributes(typeof(AlwaysInterleaveAttribute), inherit: false);
        Assert.That(attribute, Is.Not.Empty,
            "ITxRegistryGrain.GetDecisionsRevisionAsync must carry [AlwaysInterleave] so reader-side probes bypass the registry's turn token under heavy saga load.");
    }

    [Test]
    public async Task DecisionsRevision_persists_across_WriteStateAsync()
    {
        // The revision is part of TxRegistryState and is therefore
        // persisted alongside the Decisions map. After a state write
        // a reactivated grain (simulated by directly inspecting the
        // FakePersistentState) must observe the same revision.
        var (grain, state) = CreateGrain();
        await grain.MarkCommittedAsync(Guid.NewGuid());
        var persistedRevision = state.State.DecisionsRevision;

        var probedRevision = await grain.GetDecisionsRevisionAsync();

        Assert.That(persistedRevision, Is.EqualTo(probedRevision),
            "DecisionsRevision must be persisted via WriteStateAsync so a reactivated grain returns the live value, not the default 0.");
    }

    [Test]
    public async Task Mark_then_state_write_failure_unwinds_revision_bump()
    {
        // The grain captures the prior revision before bumping so a
        // failing WriteStateAsync can restore it in the catch block.
        // Without that unwind, the in-memory revision diverges from
        // the persisted dict - a reader's probe would observe an
        // "advance" that never actually materialised on disk.
        var state = new FakePersistentState<TxRegistryState>();
        state.ThrowOnWrite = new InvalidOperationException("simulated state-write failure");
        var (grain, _) = CreateGrain(state);
        var before = await grain.GetDecisionsRevisionAsync();

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.MarkCommittedAsync(Guid.NewGuid()));
        var afterFailure = await grain.GetDecisionsRevisionAsync();

        Assert.That(afterFailure, Is.EqualTo(before),
            "A failed WriteStateAsync must unwind the in-memory revision bump so the in-memory and persisted views stay in sync.");
    }
}
