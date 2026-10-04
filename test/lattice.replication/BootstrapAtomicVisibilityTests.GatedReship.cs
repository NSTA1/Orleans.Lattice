using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4590: a re-shipped replicated prepare is settled against the
/// receiver's registry only through the terminal-intent read. While a snapshot
/// capture holds the registry's decision gate (#4485), a reader is still served
/// a delegated saga's coordinator verdict, uncached and outside the gate's
/// decision snapshot; settling on it would land the saga's value on one key
/// while a sibling key's bucket resolves against that snapshot as pre-saga, so
/// the capture would hold the batch torn. The prepare must be staged instead.
/// </summary>
public partial class BootstrapAtomicVisibilityTests
{
    private static readonly TimeSpan CaptureGateLease = TimeSpan.FromMinutes(2);

    [Test]
    public async Task Reshipped_prepare_of_a_saga_its_decided_receiver_coordinator_owns_is_staged_not_written_under_a_capture_gate()
    {
        const string receiverTree = "snap-gated-reship-receiver";
        var key = $"gated-reship-{Guid.NewGuid():N}";
        var txid = Guid.NewGuid();
        var registry = TxRegistryRouting.GetRegistry(_cluster.Client, receiverTree, txid);

        // A cross-tree saga's terminal reached this receiver: the tree's registry
        // delegates the sub-saga to the receiver coordinator, which decides it.
        var receiverKey = LatticeCrossTreeReceiverGrain.ComputeKey(PreCutOrigin, $"xop-{txid:N}");
        await registry.RegisterReceiverDecisionAuthorityAsync(txid, receiverKey);
        var decision = await _cluster.Client.GetGrain<ILatticeCrossTreeReceiverGrain>(receiverKey)
            .NotifyTerminalAsync(new CrossTreeReceiverTerminal
            {
                OriginClusterId = PreCutOrigin,
                OperationId = $"xop-{txid:N}",
                TreeId = receiverTree,
                TransactionId = txid,
                Committed = true,
                WaitSet = [receiverTree],
                ObservedSourceShards = [],
                TerminalHlc = Hlc(6_100),
            });
        Assert.That(decision is { Decided: true, Committed: true }, Is.True, "precondition: the receiver coordinator decided the saga committed");

        var token = Guid.NewGuid();
        await registry.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, CaptureGateLease);
        try
        {
            Assert.That(await registry.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed),
                "precondition: under the gate a reader is still served the coordinator's verdict");

            await ReshipPreCutPrepareAsync(receiverTree, key, 2, txid, index: 1, Hlc(6_000));

            var shard = _cluster.Client.GetGrain<IShardRootGrain>(
                $"{receiverTree}/{LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount)}");
            var d0 = await registry.GetCaptureGateStatusManyAsync(token, [txid]);
            await Assert.MultipleAsync(async () =>
            {
                Assert.That(d0[txid], Is.EqualTo(TxStatus.InFlight), "the gate's decision snapshot holds the saga in flight");
                Assert.That(await PendingKeysForAsync(receiverTree, txid), Is.EqualTo(new[] { key }),
                    "the re-shipped prepare must be staged, so the capture resolves it against its decision snapshot");
                Assert.That(await shard.GetRawEntryAsync(key), Is.Null,
                    "the re-shipped prepare must not be written as committed from a verdict outside the decision snapshot");
            });
        }
        finally
        {
            await registry.ReleaseCaptureGateAsync(token);
        }

        // Released, the terminal-intent read caches the verdict and reports it,
        // so the leaf's sweep or the saga's terminal now settles the staged bucket.
        Assert.That(await registry.GetStatusForTerminalAsync(txid), Is.EqualTo(TxStatus.Committed));
    }
}
