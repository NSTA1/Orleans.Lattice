using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for issue #2318: the registry must not report a saga whose outcome
/// it cannot determine as <see cref="TxStatus.InFlight"/>.
/// <para>
/// Two routes reach that state - a decision whose tombstone has outlived
/// <see cref="LatticeOptions.TxDecisionRetention"/>, and a delegated saga whose
/// coordinator could not be dialled - and both used to answer
/// <see cref="TxStatus.InFlight"/>. That is an affirmative claim the saga has
/// not decided, which the visibility gate acts on by serving each prepared key's
/// pre-saga value, so a retention boundary or one unreachable grain silently
/// disclosed superseded data. The correct answer is
/// <see cref="TxStatus.Indeterminate"/>, which hides.
/// </para>
/// </summary>
public partial class TxRegistryGrainTests
{
    // ========================================================================
    // Retention route: a recorded row we may no longer report
    // ========================================================================

    [Test]
    public async Task GetStatusAsync_reports_an_aged_out_decision_as_indeterminate_not_in_flight()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero));
        var (grain, _) = CreateGrain(retention: TimeSpan.FromMinutes(1), timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed),
            "inside the retention window the decision is still reportable");

        clock.Advance(TimeSpan.FromMinutes(5));

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Indeterminate),
            "past the retention window the registry knows a decision exists but may not report it");
    }

    [Test]
    public async Task GetStatusAsync_reports_a_txid_it_has_never_seen_as_in_flight()
    {
        var (grain, _) = CreateGrain(retention: TimeSpan.FromMinutes(1));

        Assert.That(await grain.GetStatusAsync(Guid.NewGuid()), Is.EqualTo(TxStatus.InFlight),
            "genuine absence is still InFlight - only a recorded-but-unreportable row is Indeterminate");
    }

    [Test]
    public async Task GetStatusManyAsync_separates_aged_out_rows_from_never_seen_ones()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero));
        var (grain, _) = CreateGrain(retention: TimeSpan.FromMinutes(1), timeProvider: clock);
        var aged = Guid.NewGuid();
        var unknown = Guid.NewGuid();
        await grain.MarkAbortedAsync(aged);
        await grain.ForgetAsync(aged);
        clock.Advance(TimeSpan.FromMinutes(5));

        var result = await grain.GetStatusManyAsync([aged, unknown]);

        Assert.Multiple(() =>
        {
            Assert.That(result[aged], Is.EqualTo(TxStatus.Indeterminate));
            Assert.That(result[unknown], Is.EqualTo(TxStatus.InFlight));
        });
    }

    // ========================================================================
    // GetRecordedStatusAsync: the narrow, deliberate retention-mask bypass
    // ========================================================================

    [Test]
    public async Task GetRecordedStatusAsync_returns_the_stored_verdict_behind_an_indeterminate_answer()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero));
        var (grain, _) = CreateGrain(retention: TimeSpan.FromMinutes(1), timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        clock.Advance(TimeSpan.FromMinutes(5));

        var masked = await grain.GetStatusAsync(txid);
        var recorded = await grain.GetRecordedStatusAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(masked, Is.EqualTo(TxStatus.Indeterminate),
                "the read path stays masked");
            Assert.That(recorded, Is.EqualTo(TxStatus.Committed),
                "the sweep path sees the row it needs to finish its own prepare");
        });
    }

    [Test]
    public async Task GetRecordedStatusAsync_returns_in_flight_when_no_row_is_stored()
    {
        var (grain, _) = CreateGrain();

        Assert.That(await grain.GetRecordedStatusAsync(Guid.NewGuid()), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task GetRecordedStatusAsync_does_not_mutate_state()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero));
        var (grain, state) = CreateGrain(retention: TimeSpan.FromMinutes(1), timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        clock.Advance(TimeSpan.FromMinutes(5));
        var writesBefore = state.WriteCount;

        await grain.GetRecordedStatusAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore),
                "a status read must stay side-effect free");
            Assert.That(state.State.ForgottenAt.ContainsKey(txid), Is.True,
                "the bypass must not retire the tombstone it read past");
        });
    }

    // ========================================================================
    // Dial-failure route: authoring side and receiver side
    // ========================================================================

    [Test]
    public async Task GetStatusAsync_reports_indeterminate_when_the_cross_tree_coordinator_cannot_be_dialled()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xt-unreach-a");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-unreach-a");
        coordinator.GetDecisionAsync().Throws(new TimeoutException("coordinator unreachable"));

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Indeterminate),
            "an unreachable coordinator is not evidence the saga is still preparing");
    }

    [Test]
    public async Task GetStatusAsync_reports_indeterminate_when_the_receiver_coordinator_cannot_be_dialled()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithReceiverCoordinator("rt-unreach-a");
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rt-unreach-a");
        coordinator.GetDecisionAsync().Throws(new TimeoutException("coordinator unreachable"));

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Indeterminate),
            "the receiver-side route must be fixed alongside the authoring-side one");
    }

    [Test]
    public async Task GetStatusAsync_leaves_the_delegation_in_place_after_a_failed_dial()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithCoordinator("xt-unreach-b");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-unreach-b");
        coordinator.GetDecisionAsync().Throws(new TimeoutException("coordinator unreachable"));

        await grain.GetStatusAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ExternalAuthorities.ContainsKey(txid), Is.True,
                "the delegation must survive so a later read can retry it");
            Assert.That(state.State.Decisions.ContainsKey(txid), Is.False,
                "an unreachable coordinator must not cache a fabricated verdict");
        });
    }

    [Test]
    public async Task GetStatusAsync_recovers_the_real_verdict_once_the_coordinator_is_reachable_again()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xt-unreach-c");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-unreach-c");
        var reachable = false;
        coordinator.GetDecisionAsync().Returns(_ => reachable
            ? Task.FromResult(TxStatus.Committed)
            : Task.FromException<TxStatus>(new TimeoutException("coordinator unreachable")));

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Indeterminate));

        reachable = true;

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed),
            "Indeterminate is a transient refusal to answer, not a terminal state");
    }

    [Test]
    public async Task ResolveDelegated_keeps_returning_the_true_verdict_when_only_the_cache_write_fails()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithCoordinator("xt-writefail-a");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-writefail-a");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        var status = await grain.GetStatusAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(TxStatus.Committed),
                "we HAVE the answer and merely failed to cache it - suppressing it would discard knowledge we hold");
            Assert.That(state.State.ExternalAuthorities.ContainsKey(txid), Is.True,
                "the failed cache write is unwound so the next read re-dials");
        });
    }

    // ========================================================================
    // ObserveCrossTreeInFlightAsync.UnresolvableCount
    // ========================================================================

    [Test]
    public async Task ObserveCrossTreeInFlightAsync_reports_zero_unresolvable_when_every_coordinator_answers()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xt-unres-a");
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-unres-a");

        var observation = await grain.ObserveCrossTreeInFlightAsync();

        Assert.Multiple(() =>
        {
            Assert.That(observation.InFlightCount, Is.EqualTo(1));
            Assert.That(observation.UnresolvableCount, Is.Zero,
                "a coordinator that answered 'still preparing' is pending, not unreachable");
        });
    }

    [Test]
    public async Task ObserveCrossTreeInFlightAsync_counts_an_unreachable_coordinator_as_unresolvable()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xt-unres-b");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-unres-b");
        coordinator.GetDecisionAsync().Throws(new TimeoutException("coordinator unreachable"));

        var observation = await grain.ObserveCrossTreeInFlightAsync();

        Assert.Multiple(() =>
        {
            Assert.That(observation.InFlightCount, Is.EqualTo(1),
                "still counted - an unreachable coordinator is not evidence of a decision");
            Assert.That(observation.UnresolvableCount, Is.EqualTo(1),
                "but a fence can now tell a connectivity fault from healthy pipelining");
        });
    }

    [Test]
    public async Task ObserveCrossTreeInFlightAsync_counts_an_unreachable_receiver_coordinator_as_unresolvable()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithReceiverCoordinator("rt-unres-c");
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rt-unres-c");
        coordinator.GetDecisionAsync().Throws(new TimeoutException("coordinator unreachable"));

        var observation = await grain.ObserveCrossTreeInFlightAsync();

        Assert.That(observation.UnresolvableCount, Is.EqualTo(1));
    }

    [Test]
    public void CrossTreeInFlightObservation_two_argument_constructor_still_reports_zero_unresolvable()
    {
        var observation = new CrossTreeInFlightObservation(3, 7);

        Assert.Multiple(() =>
        {
            Assert.That(observation.InFlightCount, Is.EqualTo(3));
            Assert.That(observation.RegistrationEpoch, Is.EqualTo(7));
            Assert.That(observation.UnresolvableCount, Is.Zero,
                "the pre-existing overload must stay source- and wire-compatible");
        });
    }

    [Test]
    public void CrossTreeInFlightObservation_rejects_a_negative_unresolvable_count()
    {
        Assert.That(
            () => new CrossTreeInFlightObservation(1, 1, -1),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    // ========================================================================
    // Pin-aware retention mask
    // ========================================================================

    [Test]
    public async Task GetStatusAsync_still_reports_a_pinned_decision_after_its_retention_window_lapses()
    {
        var (grain, _, clock) = CreateGrainForPins(
            retention: TimeSpan.FromMinutes(1),
            maxPinTtl: TimeSpan.FromMinutes(30));
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        await grain.PinSnapshotAsync(Guid.NewGuid(), [txid], TimeSpan.FromMinutes(20));

        clock.Advance(TimeSpan.FromMinutes(5));

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed),
            "the prune pass has always spared a pinned row; the read mask must agree, or the pin "
            + "protects a row the reader that took it is then refused");
    }

    [Test]
    public async Task GetStatusAsync_masks_a_pinned_decision_once_the_pin_itself_lapses()
    {
        var (grain, _, clock) = CreateGrainForPins(
            retention: TimeSpan.FromMinutes(1),
            maxPinTtl: TimeSpan.FromMinutes(10));
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        await grain.PinSnapshotAsync(Guid.NewGuid(), [txid], TimeSpan.FromMinutes(5));

        clock.Advance(TimeSpan.FromMinutes(3));
        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));

        clock.Advance(TimeSpan.FromMinutes(4));

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Indeterminate),
            "the pin's own expiry must invalidate the memoised union without any state mutation to trigger it");
    }

    [Test]
    public async Task Unpinning_immediately_re_masks_a_decision_past_its_retention_window()
    {
        var (grain, _, clock) = CreateGrainForPins(
            retention: TimeSpan.FromMinutes(1),
            maxPinTtl: TimeSpan.FromMinutes(30));
        var pinId = Guid.NewGuid();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        await grain.PinSnapshotAsync(pinId, [txid], TimeSpan.FromMinutes(20));
        clock.Advance(TimeSpan.FromMinutes(5));
        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));

        await grain.UnpinSnapshotAsync(pinId);

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Indeterminate),
            "the pin memo must be invalidated by every SnapshotPins mutation");
    }

    [Test]
    public async Task GetStatusAsync_is_unaffected_by_a_pin_that_does_not_hold_the_txid()
    {
        var (grain, _, clock) = CreateGrainForPins(
            retention: TimeSpan.FromMinutes(1),
            maxPinTtl: TimeSpan.FromMinutes(30));
        var pinned = Guid.NewGuid();
        var unpinned = Guid.NewGuid();
        await grain.MarkCommittedAsync(pinned);
        await grain.MarkCommittedAsync(unpinned);
        await grain.ForgetAsync(pinned);
        await grain.ForgetAsync(unpinned);
        await grain.PinSnapshotAsync(Guid.NewGuid(), [pinned], TimeSpan.FromMinutes(20));

        clock.Advance(TimeSpan.FromMinutes(5));

        var pinnedStatus = await grain.GetStatusAsync(pinned);
        var unpinnedStatus = await grain.GetStatusAsync(unpinned);

        Assert.Multiple(() =>
        {
            Assert.That(pinnedStatus, Is.EqualTo(TxStatus.Committed));
            Assert.That(unpinnedStatus, Is.EqualTo(TxStatus.Indeterminate));
        });
    }

    // ========================================================================
    // The revision fast path must observe the Committed -> Indeterminate flip
    // ========================================================================

    [Test]
    public async Task IsRevisionStable_rejects_a_snapshot_taken_before_a_pin_lapsed_and_re_masked_a_decision()
    {
        var (grain, _, clock) = CreateGrainForPins(
            retention: TimeSpan.FromMinutes(1),
            maxPinTtl: TimeSpan.FromMinutes(10));
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        await grain.PinSnapshotAsync(Guid.NewGuid(), [txid], TimeSpan.FromMinutes(5));
        clock.Advance(TimeSpan.FromMinutes(3));

        var captured = await grain.GetDecisionsRevisionAsync();
        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));

        // The pin lapses. The saga's surface flips Committed -> Indeterminate
        // with no decision-map mutation to bump the raw revision, so the
        // composite token's live-expired term is the only thing that can carry
        // it - and it has to, because the reader fast path never consults
        // IsSnapshotStable.
        clock.Advance(TimeSpan.FromMinutes(4));
        var after = await grain.GetDecisionsRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.Not.EqualTo(captured),
                "the composite revision must move when a pin lapse re-masks a decision");
            Assert.That(ReaderStabilityGate.IsRevisionStable(captured, after), Is.False,
                "the fast path is the only gate a multi-key reader runs, so it must reject here");
        });
    }
}
