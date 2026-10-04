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

    [Test]
    public async Task Snapshot_drain_row_of_a_delegated_saga_under_a_capture_gate_is_staged_not_deferred()
    {
        // Issue #4604: the bootstrap drain stops at any row the applier defers.
        // A held decision gate (#4485) must not be one: a drained prepared row of
        // a saga whose decision the gate keeps out of the snapshot is staged in a
        // pending bucket, as live delivery stages it, so the drain completes.
        const string receiverTree = "snap-gated-drain-receiver";
        var key = $"gated-drain-{Guid.NewGuid():N}";
        var txid = Guid.NewGuid();
        var registry = TxRegistryRouting.GetRegistry(_cluster.Client, receiverTree, txid);
        var receiverKey = LatticeCrossTreeReceiverGrain.ComputeKey(PreCutOrigin, $"xop-{txid:N}");
        await registry.RegisterReceiverDecisionAuthorityAsync(txid, receiverKey);
        await _cluster.Client.GetGrain<ILatticeCrossTreeReceiverGrain>(receiverKey)
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

        var token = Guid.NewGuid();
        await registry.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, CaptureGateLease);
        ApplyResult result;
        try
        {
            using (LatticeBootstrapApplyContext.BeginScope())
            {
                result = await ReceiverApplier.ApplyAsync(new WalRecord
                {
                    TreeId = receiverTree,
                    Op = MutationKind.Set,
                    Key = key,
                    Value = new byte[] { 2 },
                    Timestamp = Hlc(6_000),
                    OriginClusterId = PreCutOrigin,
                    TransactionId = txid,
                    IsPrepared = true,
                    AtomicBatchSize = 2,
                    AtomicBatchIndex = 1,
                });
            }
        }
        finally
        {
            await registry.ReleaseCaptureGateAsync(token);
        }

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(result.Deferred, Is.False, "a held decision gate must not defer a drained snapshot row");
            Assert.That(await PendingKeysForAsync(receiverTree, txid), Is.EqualTo(new[] { key }),
                "the drained prepared row is staged for its saga's terminal");
        });
    }
}
