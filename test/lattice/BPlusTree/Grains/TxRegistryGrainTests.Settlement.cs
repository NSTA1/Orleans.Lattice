using NSubstitute;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TxRegistryGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Settlement_expired_stored_verdict_matches_the_separate_reads(bool prepareDedup)
    {
        var clock = new Orleans.Lattice.Testing.ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        await grain.MarkCommittedAsync(committed);
        await grain.MarkAbortedAsync(aborted);
        await grain.ForgetAsync(committed);
        await grain.ForgetAsync(aborted);
        clock.Advance(retention + TimeSpan.FromSeconds(1));
        await grain.AcquireCaptureGateAsync(Guid.NewGuid(), TxRegistryCaptureGateMode.Gate, GateLease);

        foreach (var (txid, expected) in new[] { (committed, TxStatus.Committed), (aborted, TxStatus.Aborted) })
        {
            Assert.That(await grain.GetStatusForTerminalAsync(txid), Is.EqualTo(TxStatus.Indeterminate));
            var answer = prepareDedup
                ? await grain.GetStatusForPrepareDedupAsync(txid)
                : await grain.GetStatusForSettlementAsync(txid);
            Assert.That(answer, Is.EqualTo(expected));
        }
    }

    [Test]
    public async Task Settlement_gate_does_not_dial_or_cache_a_delegated_verdict_but_prepare_dedup_keeps_reader_intent()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithCoordinator("settlement-gated");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "settlement-gated");
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, GateLease);

        Assert.That(await grain.GetStatusForSettlementAsync(txid), Is.EqualTo(TxStatus.InFlight));
        await coordinator.DidNotReceive().GetDecisionAsync();
        Assert.That(await grain.GetStatusForPrepareDedupAsync(txid), Is.EqualTo(TxStatus.Committed));
        Assert.That(state.State.Decisions.ContainsKey(txid), Is.False);
        Assert.That((await grain.GetCaptureGateStatusManyAsync(token, [txid]))[txid], Is.EqualTo(TxStatus.InFlight));

        await grain.ReleaseCaptureGateAsync(token);
        Assert.That(await grain.GetStatusForSettlementAsync(txid), Is.EqualTo(TxStatus.Committed));
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Settlement_unknown_or_pruned_verdict_remains_in_flight(bool prepareDedup)
    {
        var (grain, _) = CreateGrain(retention: TimeSpan.Zero);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        foreach (var id in new[] { txid, Guid.NewGuid() })
        {
            Assert.That(prepareDedup
                ? await grain.GetStatusForPrepareDedupAsync(id)
                : await grain.GetStatusForSettlementAsync(id), Is.EqualTo(TxStatus.InFlight));
        }
    }

    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    [TestCase(true, true)]
    public async Task Settlement_waits_for_durability_and_recomputes_after_rollback(bool prepareDedup, bool failWrite)
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState(failWrite ? new InvalidOperationException("storage unavailable") : null);
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();
        var mark = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        var read = OnTurnAsync(turn, () => prepareDedup
            ? grain.GetStatusForPrepareDedupAsync(txid)
            : grain.GetStatusForSettlementAsync(txid));
        await DrainAsync(turn);
        Assert.That(read.IsCompleted, Is.False);
        gate.SetResult();
        if (failWrite)
            Assert.ThrowsAsync<TxRegistryWriteFailedException>(async () => await mark);
        else
            await mark;
        Assert.That(await read, Is.EqualTo(failWrite ? TxStatus.InFlight : TxStatus.Committed));
    }

    [Test]
    public void Settlement_dispatches_retain_always_interleave()
    {
        foreach (var name in new[] { nameof(ITxRegistryGrain.GetStatusForSettlementAsync), nameof(ITxRegistryGrain.GetStatusForPrepareDedupAsync) })
        {
            var method = typeof(ITxRegistryGrain).GetMethod(name);
            Assert.That(method, Is.Not.Null, "Update the settlement dispatch contract if renamed.");
            Assert.That(method!.IsDefined(typeof(AlwaysInterleaveAttribute), false), Is.True);
        }
    }
}
