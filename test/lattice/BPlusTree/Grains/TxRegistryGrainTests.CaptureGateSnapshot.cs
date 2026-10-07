using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TxRegistryGrainTests
{
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
