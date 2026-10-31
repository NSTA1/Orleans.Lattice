using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4473: the prepared-bucket sweep resolves a masked registry answer
/// (<see cref="TxStatus.Indeterminate"/>) to the decision the registry still
/// records, in its pre-check and in its post-sweep cleanup. Treated as in flight,
/// a masked decided saga was replayed as a prepare the destination refuses
/// (#4445), leaving only an activation-scoped shadow marker.
/// </summary>
public partial class TreeShardSplitGrainTests
{
    [Test]
    public async Task RetroactiveSweep_cleanup_fallback_failure_preserves_replays_and_whole_sweep_retry_settles()
    {
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        var factory = Substitute.For<IGrainFactory>();
        var (_, _, source, target, _, registry) = CreateGrainWithSweepWiring(
            leafSnapshots: [BuildSetSnapshot(committed, out _), BuildSetSnapshot(aborted, out _)],
            preCheckStatus: TxStatus.InFlight,
            postSweepStatus: TxStatus.Indeterminate,
            factory: factory);
        registry.GetRecordedStatusAsync(committed).Returns(TxStatus.Committed);
        registry.GetRecordedStatusAsync(aborted)
            .Returns(Task.FromException<TxStatus>(new InvalidOperationException("recorded status unavailable")));
        var firstLeaf = (await source.GetLeftmostLeafIdAsync())!.Value;

        Assert.ThrowsAsync<InvalidOperationException>(() => PreparedBucketSweep.RunAsync(
            factory, TreeId, firstLeaf, target, [8, 10, 12, 14], 16, new PreparedBucketSweepProgress()));

        await target.Received(2).SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
        await target.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>());

        registry.GetRecordedStatusAsync(aborted).Returns(TxStatus.Aborted);
        await PreparedBucketSweep.RunAsync(
            factory, TreeId, firstLeaf, target, [8, 10, 12, 14], 16, new PreparedBucketSweepProgress());

        await target.Received(4).SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
        await target.Received(1).AppendTxTerminalAsync(committed, true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
        await target.Received(1).AppendTxTerminalAsync(aborted, false, Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
    }

    [Test]
    public async Task RetroactiveSweep_cleanup_settles_masked_transactions_in_one_registry_dispatch()
    {
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        var pending = Guid.NewGuid();
        var (grain, _, _, target, _, registry) = CreateGrainWithSweepWiring(
            leafSnapshots: [BuildSetSnapshot(committed, out _), BuildSetSnapshot(aborted, out _), BuildSetSnapshot(pending, out _)],
            preCheckStatus: TxStatus.InFlight);
        registry.GetStatusManyForSettlementAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(new Dictionary<Guid, TxStatus>
        {
            [committed] = TxStatus.Committed,
            [aborted] = TxStatus.Aborted,
            [pending] = TxStatus.InFlight,
        });
        registry.ClearReceivedCalls();

        await grain.InitiateSplitStateAsync(0);

        await registry.Received(1).GetStatusManyForSettlementAsync(
            Arg.Is<IReadOnlyList<Guid>>(ids => ids.Count == 3 && ids.Contains(committed) && ids.Contains(aborted) && ids.Contains(pending)));
        await registry.DidNotReceive().GetStatusManyForTerminalAsync(Arg.Any<IReadOnlyList<Guid>>());
        await registry.DidNotReceive().GetRecordedStatusAsync(Arg.Any<Guid>());
        await target.Received(1).AppendTxTerminalAsync(committed, true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
        await target.Received(1).AppendTxTerminalAsync(aborted, false, Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
        await target.DidNotReceive().AppendTxTerminalAsync(pending, Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
    }

    [Test]
    public async Task RetroactiveSweep_applies_the_recorded_commit_when_the_pre_check_is_masked()
    {
        var txid = Guid.NewGuid();
        var snap = BuildSetSnapshot(txid, out var key);
        var (grain, _, _, target, _, registry) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.Indeterminate);
        registry.GetRecordedStatusAsync(txid).Returns(Task.FromResult(TxStatus.Committed));
        registry.GetStatusForSettlementAsync(txid).Returns(TxStatus.Committed);

        await grain.InitiateSplitStateAsync(0);

        await target.DidNotReceive().SetAsync(key, Arg.Any<byte[]>());
        await target.DidNotReceive().MarkSagaShadowAsync(txid, Arg.Any<IReadOnlyList<string>>());
        await target.Received(1).AppendTxTerminalAsync(
            txid,
            committed: true,
            Arg.Is<IReadOnlyDictionary<string, byte[]>>(d => d != null && d.Count == 1 && d[key].SequenceEqual(snap.Value!)));
        await registry.Received(1).GetStatusForSettlementAsync(txid);
        await registry.DidNotReceive().GetStatusForTerminalAsync(txid);
        await registry.DidNotReceive().GetRecordedStatusAsync(txid);
    }

    [Test]
    public async Task RetroactiveSweep_applies_the_recorded_abort_when_the_pre_check_is_masked()
    {
        var txid = Guid.NewGuid();
        var snap = BuildSetSnapshot(txid, out var key);
        var (grain, _, _, target, _, registry) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.Indeterminate);
        registry.GetRecordedStatusAsync(txid).Returns(Task.FromResult(TxStatus.Aborted));
        registry.GetStatusForSettlementAsync(txid).Returns(TxStatus.Aborted);

        await grain.InitiateSplitStateAsync(0);

        await target.DidNotReceive().SetAsync(key, Arg.Any<byte[]>());
        await target.Received(1).AppendTxTerminalAsync(
            txid,
            committed: false,
            Arg.Is<IReadOnlyDictionary<string, byte[]>?>(d => d == null));
    }

    [Test]
    public async Task RetroactiveSweep_replays_a_masked_prepare_with_no_recorded_decision()
    {
        // No stored row behind the mask (a delegated txid with no local decision,
        // say): nothing is decided, so the prepare is carried as before.
        var txid = Guid.NewGuid();
        var snap = BuildSetSnapshot(txid, out var key);
        var (grain, _, _, target, _, registry) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.Indeterminate);
        registry.GetRecordedStatusAsync(txid).Returns(Task.FromResult(TxStatus.InFlight));
        registry.GetStatusForSettlementAsync(txid).Returns(TxStatus.InFlight);

        await grain.InitiateSplitStateAsync(0);

        await target.Received(1).SetAsync(key, Arg.Is<byte[]>(b => b.SequenceEqual(snap.Value!)));
        await target.Received(1).MarkSagaShadowAsync(txid, Arg.Any<IReadOnlyList<string>>());
        await target.DidNotReceive().AppendTxTerminalAsync(
            txid, Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
    }

    [Test]
    public async Task RetroactiveSweep_cleanup_applies_the_recorded_commit_of_a_saga_masked_during_the_sweep()
    {
        var txid = Guid.NewGuid();
        var snap = BuildSetSnapshot(txid, out var key);
        var (grain, _, _, target, _, registry) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.InFlight,
            postSweepStatus: TxStatus.Indeterminate);
        registry.GetRecordedStatusAsync(txid).Returns(Task.FromResult(TxStatus.Committed));

        await grain.InitiateSplitStateAsync(0);

        await target.Received(1).SetAsync(key, Arg.Any<byte[]>());
        await target.Received(1).AppendTxTerminalAsync(
            txid,
            committed: true,
            Arg.Is<IReadOnlyDictionary<string, byte[]>>(d => d != null && d.Count == 1 && d[key].SequenceEqual(snap.Value!)));
    }
}
