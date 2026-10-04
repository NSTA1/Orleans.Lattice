using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the snapshot-capture decision gate on <see cref="TxRegistryGrain"/>
/// (issue #4485): a gate refuses new decisions and freezes the decision snapshot
/// (D0) a capture resolves against, a fence refuses new cross-tree delegations,
/// a lapsed or released hold re-admits decisions and fails the capture closed,
/// delegated verdicts are not cached while gated, and the terminal-intent reads
/// the sweeps use only report a decision recorded on the registry.
/// </summary>
public partial class TxRegistryGrainTests
{
    private static readonly TimeSpan GateLease = TimeSpan.FromSeconds(30);

    [Test]
    public async Task Gate_refuses_a_new_commit_decision_as_retryable()
    {
        var (grain, _) = CreateGrain();
        await grain.AcquireCaptureGateAsync(Guid.NewGuid(), TxRegistryCaptureGateMode.Gate, GateLease);

        var ex = Assert.ThrowsAsync<TxDecisionGateRefusedException>(() => grain.MarkCommittedAsync(Guid.NewGuid()));

        Assert.That(ex!.Refusal, Is.EqualTo(TxDecisionGateRefusal.DecisionGated));
        Assert.That(ex.RetryAfterMilliseconds, Is.GreaterThan(0));
    }

    [Test]
    public async Task Gate_refuses_a_new_abort_decision()
    {
        var (grain, _) = CreateGrain();
        await grain.AcquireCaptureGateAsync(Guid.NewGuid(), TxRegistryCaptureGateMode.Gate, GateLease);

        var ex = Assert.ThrowsAsync<TxDecisionGateRefusedException>(() => grain.MarkAbortedAsync(Guid.NewGuid()));

        Assert.That(ex!.Refusal, Is.EqualTo(TxDecisionGateRefusal.DecisionGated));
    }

    [Test]
    public async Task Gate_admits_a_repeat_of_a_decision_recorded_before_it()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.AcquireCaptureGateAsync(Guid.NewGuid(), TxRegistryCaptureGateMode.Gate, GateLease);

        await grain.MarkCommittedAsync(txid);

        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task Fence_admits_decisions_but_refuses_a_new_external_delegation()
    {
        var (grain, _) = CreateGrain();
        await grain.AcquireCaptureGateAsync(Guid.NewGuid(), TxRegistryCaptureGateMode.Fence, GateLease);

        await grain.MarkCommittedAsync(Guid.NewGuid());
        var ex = Assert.ThrowsAsync<TxDecisionGateRefusedException>(
            () => grain.RegisterExternalDecisionAuthorityAsync(Guid.NewGuid(), "xop-fenced"));

        Assert.That(ex!.Refusal, Is.EqualTo(TxDecisionGateRefusal.RegistrationFenced));
    }

    [Test]
    public async Task Fence_refuses_a_new_receiver_delegation()
    {
        var (grain, _) = CreateGrain();
        await grain.AcquireCaptureGateAsync(Guid.NewGuid(), TxRegistryCaptureGateMode.Fence, GateLease);

        var ex = Assert.ThrowsAsync<TxDecisionGateRefusedException>(
            () => grain.RegisterReceiverDecisionAuthorityAsync(Guid.NewGuid(), "rcv-fenced"));

        Assert.That(ex!.Refusal, Is.EqualTo(TxDecisionGateRefusal.RegistrationFenced));
    }

    [Test]
    public async Task Fence_admits_a_repeat_of_an_existing_delegation()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-existing");
        await grain.AcquireCaptureGateAsync(Guid.NewGuid(), TxRegistryCaptureGateMode.Fence, GateLease);

        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-existing");

        Assert.That(state.State.ExternalAuthorities[txid], Is.EqualTo("xop-existing"));
    }

    [Test]
    public async Task Gate_status_lookup_answers_from_the_decisions_at_acquisition()
    {
        var (grain, _) = CreateGrain();
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        var unknown = Guid.NewGuid();
        await grain.MarkCommittedAsync(committed);
        await grain.MarkAbortedAsync(aborted);
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, GateLease);

        var statuses = await grain.GetCaptureGateStatusManyAsync(token, [committed, aborted, unknown]);

        Assert.Multiple(() =>
        {
            Assert.That(statuses[committed], Is.EqualTo(TxStatus.Committed));
            Assert.That(statuses[aborted], Is.EqualTo(TxStatus.Aborted));
            Assert.That(statuses[unknown], Is.EqualTo(TxStatus.InFlight));
        });
    }

    [Test]
    public async Task Upgrading_a_fence_to_a_gate_captures_the_decisions_at_the_upgrade()
    {
        var (grain, _) = CreateGrain();
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Fence, GateLease);
        var decidedUnderFence = Guid.NewGuid();
        await grain.MarkCommittedAsync(decidedUnderFence);

        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, GateLease);
        var statuses = await grain.GetCaptureGateStatusManyAsync(token, [decidedUnderFence]);

        Assert.That(statuses[decidedUnderFence], Is.EqualTo(TxStatus.Committed));
        Assert.ThrowsAsync<TxDecisionGateRefusedException>(() => grain.MarkCommittedAsync(Guid.NewGuid()));
    }

    [Test]
    public async Task Gate_status_lookup_fails_closed_for_an_unknown_token()
    {
        var (grain, _) = CreateGrain();

        var ex = Assert.ThrowsAsync<TxDecisionGateRefusedException>(
            () => grain.GetCaptureGateStatusManyAsync(Guid.NewGuid(), [Guid.NewGuid()]));

        Assert.That(ex!.Refusal, Is.EqualTo(TxDecisionGateRefusal.GateLapsed));
    }

    [Test]
    public async Task Released_gate_reports_valid_and_readmits_decisions()
    {
        var (grain, state) = CreateGrain();
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, GateLease);

        var valid = await grain.ReleaseCaptureGateAsync(token);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        Assert.That(valid, Is.True);
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task Lapsed_gate_readmits_decisions_and_reports_invalid_everywhere()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(timeProvider: clock);
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, TimeSpan.FromSeconds(5));

        clock.Advance(TimeSpan.FromSeconds(6));
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed), "a crashed capture's gate must not wedge sagas");
            Assert.ThrowsAsync<TxDecisionGateRefusedException>(
                () => grain.GetCaptureGateStatusManyAsync(token, [txid]));
            Assert.ThrowsAsync<TxDecisionGateRefusedException>(
                () => grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, GateLease));
        });
        Assert.That(await grain.RenewCaptureGateAsync(token, GateLease), Is.False, "a lapsed gate is never revived");
        Assert.That(await grain.ReleaseCaptureGateAsync(token), Is.False, "the capture must fail closed");
    }

    [Test]
    public async Task Renewed_gate_outlives_its_original_lease()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, _) = CreateGrain(timeProvider: clock);
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, TimeSpan.FromSeconds(5));

        clock.Advance(TimeSpan.FromSeconds(4));
        Assert.That(await grain.RenewCaptureGateAsync(token, TimeSpan.FromSeconds(5)), Is.True);
        clock.Advance(TimeSpan.FromSeconds(4));

        Assert.ThrowsAsync<TxDecisionGateRefusedException>(() => grain.MarkCommittedAsync(Guid.NewGuid()));
        Assert.That(await grain.ReleaseCaptureGateAsync(token), Is.True);
    }

    [Test]
    public async Task Release_of_a_token_never_acquired_reports_invalid()
    {
        var (grain, _) = CreateGrain();

        Assert.That(await grain.ReleaseCaptureGateAsync(Guid.NewGuid()), Is.False);
    }

    [Test]
    public async Task Gate_holds_while_any_concurrent_capture_holds_it()
    {
        var (grain, _) = CreateGrain();
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(first, TxRegistryCaptureGateMode.Gate, GateLease);
        await grain.AcquireCaptureGateAsync(second, TxRegistryCaptureGateMode.Gate, GateLease);

        Assert.That(await grain.ReleaseCaptureGateAsync(first), Is.True);

        Assert.ThrowsAsync<TxDecisionGateRefusedException>(() => grain.MarkCommittedAsync(Guid.NewGuid()));
        Assert.That(await grain.ReleaseCaptureGateAsync(second), Is.True);
        await grain.MarkCommittedAsync(Guid.NewGuid());
    }

    [Test]
    public void AcquireCaptureGateAsync_rejects_an_empty_token_and_a_non_positive_lease()
    {
        var (grain, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.AcquireCaptureGateAsync(Guid.Empty, TxRegistryCaptureGateMode.Gate, GateLease));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => grain.AcquireCaptureGateAsync(Guid.NewGuid(), TxRegistryCaptureGateMode.Gate, TimeSpan.Zero));
    }

    [Test]
    public async Task Gated_registry_serves_a_delegated_verdict_to_readers_without_caching_it()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithCoordinator("xop-gated");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-gated");
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, GateLease);

        var live = await grain.GetStatusAsync(txid);
        var snapshot = await grain.SnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(live, Is.EqualTo(TxStatus.Committed), "a reader still sees the coordinator's decision");
            Assert.That(snapshot[txid], Is.EqualTo(TxStatus.Committed), "a reader's snapshot agrees with the per-txid read");
            Assert.That(state.State.Decisions.ContainsKey(txid), Is.False, "no new local decision is recorded under the gate");
            Assert.That(state.State.ExternalAuthorities.ContainsKey(txid), Is.True, "the delegation row stays, so the set re-check still sees it");
        });
    }

    [Test]
    public async Task Gated_registry_answers_a_terminal_intent_read_for_a_delegated_txid_as_InFlight()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xop-terminal");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-terminal");
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, GateLease);

        var single = await grain.GetStatusForTerminalAsync(txid);
        var many = await grain.GetStatusManyForTerminalAsync([txid]);
        var d0 = await grain.GetCaptureGateStatusManyAsync(token, [txid]);

        Assert.Multiple(() =>
        {
            Assert.That(single, Is.EqualTo(TxStatus.InFlight));
            Assert.That(many[txid], Is.EqualTo(TxStatus.InFlight));
            Assert.That(d0[txid], Is.EqualTo(TxStatus.InFlight), "D0 holds local decisions only");
        });
    }

    [Test]
    public async Task Ungated_terminal_intent_read_caches_a_delegated_verdict_before_reporting_it()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithCoordinator("xop-ungated");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-ungated");

        var status = await grain.GetStatusForTerminalAsync(txid);

        Assert.That(status, Is.EqualTo(TxStatus.Committed));
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed), "a terminal is only ever applied after a local decision");
    }

    [Test]
    public async Task Terminal_intent_read_reports_local_decisions_and_InFlight_for_unknown()
    {
        var (grain, _) = CreateGrain();
        var committed = Guid.NewGuid();
        await grain.MarkCommittedAsync(committed);

        Assert.That(await grain.GetStatusForTerminalAsync(committed), Is.EqualTo(TxStatus.Committed));
        Assert.That(await grain.GetStatusForTerminalAsync(Guid.NewGuid()), Is.EqualTo(TxStatus.InFlight));
    }
}
