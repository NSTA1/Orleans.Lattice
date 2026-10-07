using NSubstitute;
using Orleans.Lattice.BPlusTree;

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
    public async Task RetroactiveSweep_applies_the_recorded_commit_when_the_pre_check_is_masked()
    {
        var txid = Guid.NewGuid();
        var snap = BuildSetSnapshot(txid, out var key);
        var (grain, _, _, target, _, registry) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.Indeterminate);
        registry.GetRecordedStatusAsync(txid).Returns(Task.FromResult(TxStatus.Committed));

        await grain.InitiateSplitStateAsync(0);

        await target.DidNotReceive().SetAsync(key, Arg.Any<byte[]>());
        await target.DidNotReceive().MarkSagaShadowAsync(txid, Arg.Any<IReadOnlyList<string>>());
        await target.Received(1).AppendTxTerminalAsync(
            txid,
            committed: true,
            Arg.Is<IReadOnlyDictionary<string, byte[]>>(d => d != null && d.Count == 1 && d[key].SequenceEqual(snap.Value!)));
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
