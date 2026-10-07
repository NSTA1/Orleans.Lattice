using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TxRegistryGrainTests
{
    [Test]
    public void Read_gate_writer_window_exceeds_the_actual_maximum_writer_retry_delay()
    {
        var delay = TxRegistryWriteRetry.GatedBaseDelay;
        while (delay < TxRegistryWriteRetry.GatedMaxDelay) delay *= 2;
        Assert.That(TxRegistryGrain.ReadGateWriterWindow, Is.GreaterThan(delay));
    }

    [Test]
    public async Task AcquireReadCaptureGateAsync_back_to_back_readers_allow_the_writer_arriving_mid_hold_to_finish()
    {
        var clock = new Orleans.Lattice.Testing.ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(timeProvider: clock);
        var token = Guid.NewGuid();
        await grain.AcquireReadCaptureGateAsync(token, GateLease);
        for (var round = 0; round < 10; round++)
        {
            var txid = Guid.NewGuid();
            // Drive the real writer retry path: this writer arrives while the
            // current read gate is live, and a competing reader is already queued.
            var writer = TxRegistryWriteRetry.MarkDecisionAsync(grain, txid, committed: true);
            Assert.That(writer.IsCompleted, Is.False);
            var nextToken = Guid.NewGuid();
            var reader = grain.AcquireReadCaptureGateAsync(nextToken, GateLease);
            Assert.That(reader.IsCompleted, Is.False, "read holds must not overlap");
            Assert.That(await grain.ReleaseCaptureGateAsync(token), Is.True);

            await writer.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.That(reader.IsCompleted, Is.False, "the reader must leave the writer-open window");
            Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
            clock.Advance(TxRegistryGrain.ReadGateWriterWindow);
            await reader;
            token = nextToken;
        }
        await grain.ReleaseCaptureGateAsync(token);
        Assert.That(state.State.Decisions.Count, Is.EqualTo(10));
    }

    [Test]
    public async Task AcquireReadCaptureGateAsync_crashed_reader_expires_without_renewal_and_leaves_a_writer_window()
    {
        var clock = new Orleans.Lattice.Testing.ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, _) = CreateGrain(timeProvider: clock);
        var token = Guid.NewGuid();
        await grain.AcquireReadCaptureGateAsync(token, GateLease);
        Assert.That(await grain.RenewCaptureGateAsync(token, GateLease), Is.False);
        clock.Advance(GateLease / 2);
        await grain.AcquireReadCaptureGateAsync(token, GateLease);
        clock.Advance(GateLease / 2 + TimeSpan.FromMilliseconds(1));
        var reader = grain.AcquireReadCaptureGateAsync(Guid.NewGuid(), GateLease);
        Assert.That(reader.IsCompleted, Is.False, "expiry does not erase the writer window");
        await grain.MarkCommittedAsync(Guid.NewGuid());
        clock.Advance(TxRegistryGrain.ReadGateWriterWindow);
        await reader;
    }

    [Test]
    public async Task AcquireReadCaptureGateAsync_waiting_reader_can_cancel_without_installing_a_hold()
    {
        var clock = new Orleans.Lattice.Testing.ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, _) = CreateGrain(timeProvider: clock);
        var current = Guid.NewGuid();
        await grain.AcquireReadCaptureGateAsync(current, GateLease);
        using var cancellation = new CancellationTokenSource();
        var waitingToken = Guid.NewGuid();
        var waiting = grain.AcquireReadCaptureGateAsync(waitingToken, GateLease, cancellation.Token);
        cancellation.Cancel();
        Assert.ThrowsAsync<OperationCanceledException>(async () => await waiting);
        Assert.That(await grain.ReleaseCaptureGateAsync(waitingToken), Is.False);
        Assert.That(await grain.ReleaseCaptureGateAsync(current), Is.True);
    }

    [Test]
    public async Task AcquireReadCaptureGateAsync_contended_admission_has_a_finite_deadline()
    {
        var clock = new Orleans.Lattice.Testing.ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, _) = CreateGrain(timeProvider: clock);
        await grain.AcquireReadCaptureGateAsync(Guid.NewGuid(), GateLease);
        var waitingToken = Guid.NewGuid();
        var waiting = grain.AcquireReadCaptureGateAsync(waitingToken, TimeSpan.FromSeconds(1));
        clock.Advance(TimeSpan.FromSeconds(1));
        var refusal = Assert.ThrowsAsync<TxDecisionGateRefusedException>(async () => await waiting);
        Assert.That(refusal!.Refusal, Is.EqualTo(TxDecisionGateRefusal.GateLapsed));
        Assert.That(await grain.ReleaseCaptureGateAsync(waitingToken), Is.False);
    }

    [Test]
    public async Task GetCaptureGateSnapshotAsync_preserves_forgotten_decisions_and_returns_a_defensive_copy()
    {
        var (grain, _) = CreateGrain(retention: TimeSpan.Zero);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, GateLease);
        await grain.ForgetAsync(txid);

        var snapshot = await grain.GetCaptureGateSnapshotAsync(token);
        Assert.That(snapshot[txid], Is.EqualTo(TxStatus.Committed));
        snapshot.Clear();
        Assert.That((await grain.GetCaptureGateSnapshotAsync(token))[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task GetCaptureGateSnapshotAsync_never_dials_a_receiver_delegation_outside_D0()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "receiver");
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, GateLease);

        Assert.That(await grain.GetCaptureGateSnapshotAsync(token), Is.Empty);
        Assert.That(await grain.GetStatusForTerminalAsync(txid), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task GetCaptureGateSnapshotAsync_fails_closed_after_expiry_or_reactivation()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(timeProvider: clock);
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, GateLease);
        clock.Advance(GateLease + TimeSpan.FromSeconds(1));

        Assert.ThrowsAsync<TxDecisionGateRefusedException>(() => grain.GetCaptureGateSnapshotAsync(token));
        var (reactivated, _) = CreateGrain(state);
        Assert.ThrowsAsync<TxDecisionGateRefusedException>(() => reactivated.GetCaptureGateSnapshotAsync(token));
        Assert.That(await grain.ReleaseCaptureGateAsync(token), Is.False);
        await grain.MarkCommittedAsync(Guid.NewGuid());
    }
}
