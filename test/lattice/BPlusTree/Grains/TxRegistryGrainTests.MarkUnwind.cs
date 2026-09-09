using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the failed-persist unwind on
/// <see cref="TxRegistryGrain.MarkCommittedAsync"/> and
/// <see cref="TxRegistryGrain.MarkAbortedAsync"/>.
/// <para>
/// Both paths mutate four in-memory maps before their persist:
/// <c>ForgottenAt</c> and <c>Decisions</c> (the tombstone-clearing prologue),
/// then <c>ExternalAuthorities</c> and <c>ReceiverDecisionAuthorities</c> (the
/// cross-tree delegation drop), then <c>Decisions</c> and
/// <c>DecisionsRevision</c> again (the decision itself). Their catch arms
/// originally restored only the last pair, leaving the delegation maps ahead of
/// disk for the remainder of the activation.
/// </para>
/// <para>
/// A stranded drop is not a cosmetic divergence. The delegation row is the only
/// pointer <see cref="TxRegistryGrain.GetStatusAsync"/> has to the coordinator,
/// so losing it turns a still-preparing cross-tree saga into a confident
/// <see cref="TxStatus.InFlight"/> by fallthrough, and it is the population the
/// backup post-capture fence counts absolutely, so losing it silently certifies
/// a capture window as quiescent.
/// </para>
/// </summary>
public partial class TxRegistryGrainTests
{
    // ========================================================================
    // The named regression: the coordinator pointer survives a failed persist
    // ========================================================================

    [Test]
    public async Task MarkCommittedAsync_failed_persist_keeps_the_txid_resolvable_against_its_coordinator()
    {
        // Issue #2352. Before the fix, the unconditional delegation drop ran
        // above the persist and was never restored, so this GetStatusAsync
        // returned InFlight by fallthrough (unknown txid) instead of dialling
        // the coordinator - a confident wrong answer, not a degraded one.
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithCoordinator("xop-unwind-a");
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-unwind-a");

        state.ThrowOnWrite = new InvalidOperationException("write boom");
        Assert.That(async () => await grain.MarkCommittedAsync(txid),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));
        state.ThrowOnWrite = null;

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ExternalAuthorities.TryGetValue(txid, out var key), Is.True,
                "the delegation row is the only coordinator pointer the registry holds");
            Assert.That(key, Is.EqualTo("xop-unwind-a"));
            Assert.That(state.State.Decisions.ContainsKey(txid), Is.False,
                "the decision itself must also be unwound");
        });

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.InFlight));
        await coordinator.Received(1).GetDecisionAsync();
    }

    [Test]
    public async Task MarkAbortedAsync_failed_persist_keeps_the_txid_resolvable_against_its_coordinator()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithCoordinator("xop-unwind-b");
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-unwind-b");

        state.ThrowOnWrite = new InvalidOperationException("write boom");
        Assert.That(async () => await grain.MarkAbortedAsync(txid),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));
        state.ThrowOnWrite = null;

        Assert.That(state.State.ExternalAuthorities.ContainsKey(txid), Is.True);
        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.InFlight));
        await coordinator.Received(1).GetDecisionAsync();
    }

    [Test]
    public async Task MarkCommittedAsync_failed_persist_restores_a_receiver_delegation()
    {
        // The receiver map is dropped by the same statement pair and was
        // restored by neither catch. Cover it explicitly: a remedy applied to
        // only one of the two maps leaves the receiving cluster's rows exposed.
        var txid = Guid.NewGuid();
        var (grain, state) = CreateGrain();
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "xop-recv");

        state.ThrowOnWrite = new InvalidOperationException("write boom");
        Assert.That(async () => await grain.MarkCommittedAsync(txid),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.That(state.State.ReceiverDecisionAuthorities.TryGetValue(txid, out var key), Is.True);
        Assert.That(key, Is.EqualTo("xop-recv"));
    }

    [Test]
    public async Task MarkAbortedAsync_failed_persist_restores_a_receiver_delegation()
    {
        var txid = Guid.NewGuid();
        var (grain, state) = CreateGrain();
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "xop-recv-b");

        state.ThrowOnWrite = new InvalidOperationException("write boom");
        Assert.That(async () => await grain.MarkAbortedAsync(txid),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.That(state.State.ReceiverDecisionAuthorities.TryGetValue(txid, out var key), Is.True);
        Assert.That(key, Is.EqualTo("xop-recv-b"));
    }

    // ========================================================================
    // The backup fence's absolute in-flight clause is unmoved by a failed Mark
    // ========================================================================

    [Test]
    public async Task ObserveCrossTreeInFlightAsync_count_is_unchanged_by_a_failed_MarkCommitted()
    {
        // The post-capture fence tests the epoch as a DELTA across the capture
        // window but the in-flight count ABSOLUTELY. A failed Mark that leaked
        // the drop would therefore drive the count to zero and let the fence
        // certify the window as quiescent while the saga was still preparing.
        var txid = Guid.NewGuid();
        var (grain, state) = CreateGrain();
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "xop-fence");

        var before = await grain.ObserveCrossTreeInFlightAsync();
        Assert.That(before.InFlightCount, Is.EqualTo(1),
            "the registration must be observable for this test to mean anything");

        state.ThrowOnWrite = new InvalidOperationException("write boom");
        Assert.That(async () => await grain.MarkCommittedAsync(txid),
            Throws.TypeOf<InvalidOperationException>());
        state.ThrowOnWrite = null;

        var after = await grain.ObserveCrossTreeInFlightAsync();
        Assert.Multiple(() =>
        {
            Assert.That(after.InFlightCount, Is.EqualTo(before.InFlightCount),
                "a failed persist must not drain the fence's in-flight population");
            Assert.That(after.RegistrationEpoch, Is.EqualTo(before.RegistrationEpoch),
                "the unwind restores rows rather than bumping the epoch: the fence "
                + "compares the epoch as a delta, so a bump would be absorbed into the baseline");
        });
    }

    [Test]
    public async Task ObserveCrossTreeInFlightAsync_count_is_unchanged_by_a_failed_MarkAborted()
    {
        var txid = Guid.NewGuid();
        var (grain, state) = CreateGrain();
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "xop-fence-b");
        var before = await grain.ObserveCrossTreeInFlightAsync();

        state.ThrowOnWrite = new InvalidOperationException("write boom");
        Assert.That(async () => await grain.MarkAbortedAsync(txid),
            Throws.TypeOf<InvalidOperationException>());
        state.ThrowOnWrite = null;

        var after = await grain.ObserveCrossTreeInFlightAsync();
        Assert.That(after.InFlightCount, Is.EqualTo(before.InFlightCount));
        Assert.That(after.RegistrationEpoch, Is.EqualTo(before.RegistrationEpoch));
    }

    // ========================================================================
    // The tombstone-clearing prologue is unwound too
    // ========================================================================

    [Test]
    public async Task MarkCommittedAsync_failed_persist_restores_a_cleared_tombstone()
    {
        // The prologue clears ForgottenAt AND the retained Decisions row before
        // the write-once guard. Leaving that clear in place after a failed
        // persist silently retires a tombstone that disk still carries.
        var (grain, state) = CreateGrain(retention: TimeSpan.FromMinutes(5));
        var txid = Guid.NewGuid();
        await grain.MarkAbortedAsync(txid);
        await grain.ForgetAsync(txid);
        var forgottenAt = state.State.ForgottenAt[txid];

        state.ThrowOnWrite = new InvalidOperationException("write boom");
        Assert.That(async () => await grain.MarkCommittedAsync(txid),
            Throws.TypeOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ForgottenAt.TryGetValue(txid, out var restored), Is.True,
                "a failed persist must not retire a tombstone disk still carries");
            Assert.That(restored, Is.EqualTo(forgottenAt));
            Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Aborted),
                "the retained decision under the tombstone must come back as it was");
        });
    }

    // ========================================================================
    // Ordering: the drop now sits below the write-once guard
    // ========================================================================

    [Test]
    public async Task MarkCommittedAsync_idempotent_repeat_performs_no_write_and_no_drop()
    {
        // The Idempotent and Conflict exits return without ever reaching a
        // WriteStateAsync. With the drop above the guard they still mutated the
        // delegation maps - an unconditional mutation on a path that persists
        // nothing at all. Below the guard they cannot.
        var txid = Guid.NewGuid();
        var (grain, state) = CreateGrain();
        await grain.MarkCommittedAsync(txid);
        // Re-register after the decision so a row coexists with it; only
        // reachable defensively, but it is exactly the state the old ordering
        // silently repaired and the new ordering must not depend on.
        state.State.ReceiverDecisionAuthorities[txid] = "xop-idem";
        var writesBefore = state.WriteCount;

        await grain.MarkCommittedAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore),
                "an idempotent repeat must not persist");
            Assert.That(state.State.ReceiverDecisionAuthorities.ContainsKey(txid), Is.True,
                "and must therefore not mutate a map either");
        });
    }

    [Test]
    public async Task MarkAbortedAsync_conflicting_outcome_performs_no_write_and_no_drop()
    {
        var txid = Guid.NewGuid();
        var (grain, state) = CreateGrain();
        await grain.MarkCommittedAsync(txid);
        state.State.ReceiverDecisionAuthorities[txid] = "xop-conflict";
        var writesBefore = state.WriteCount;

        Assert.That(async () => await grain.MarkAbortedAsync(txid),
            Throws.TypeOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
            Assert.That(state.State.ReceiverDecisionAuthorities.ContainsKey(txid), Is.True);
        });
    }
}
