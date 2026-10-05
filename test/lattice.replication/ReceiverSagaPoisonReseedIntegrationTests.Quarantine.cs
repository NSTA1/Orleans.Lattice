using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4692: a saga record whose failure survives the re-seed - a malformed
/// terminal, a terminal that contradicts the decision the re-seed recorded -
/// must not cycle poison and re-seed for ever. Once a re-seed has settled the
/// saga, a record of it that fails past the bound again quarantines the saga:
/// the record is parked without being applied, the stream moves past it, the
/// saga is not poisoned or re-seeded again, and the quarantine is counted. A
/// real re-seed runs between the two failures.
/// </summary>
public partial class ReceiverSagaPoisonReseedIntegrationTests
{
    private static System.Diagnostics.Metrics.MeterListener ListenForSagaPoison(string tree, ConcurrentBag<string> outcomes) =>
        MeterListening.StartForInstrument(LatticeReplicationMetrics.ReceiverSagaPoisoned, listener =>
            listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string? treeTag = null;
                string? outcome = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeReplicationMetrics.TagTree) treeTag = tag.Value as string;
                    if (tag.Key == LatticeReplicationMetrics.TagOutcome) outcome = tag.Value as string;
                }

                if (treeTag == tree && outcome is not null)
                {
                    outcomes.Add(outcome);
                }
            }));

    private static WalRecord Terminal(string tree, Guid txid, MutationKind op, long ticks, int shardIndex, string key) => new()
    {
        TreeId = tree,
        Op = op,
        Key = key,
        Timestamp = Hlc(ticks),
        OriginClusterId = SiteAClusterId,
        TransactionId = txid,
        ShardIndex = shardIndex,
        AtomicShardCount = 2,
    };

    private async Task AssertQuarantinedAfterTheReseedSettledTheSagaAsync(string tree, string keyA, string keyB, Guid txid, WalRecord failing)
    {
        // First failure: past the bound the saga is poisoned and a real re-seed settles it from the export.
        Assert.That(await DeliverAsync(failing), Is.False, "precondition: the failing terminal is deferred");
        await Task.Delay(TimeSpan.FromMilliseconds(450));
        Assert.That(await PushAsync(failing), Is.False, "precondition: past the bound the terminal is withheld");
        var poison = _siteB.Client.GetGrain<IReceiverSagaPoisonGrain>(tree);
        Assert.That(await poison.GetPoisonedAsync(SiteAClusterId), Does.Contain(txid), "precondition: the saga is poisoned");

        await WaitForLiveIncrementalAsync(_siteB, tree);
        var receiver = _siteB.Client.GetGrain<ILattice>(tree);
        Assert.That(await receiver.GetAsync(keyA), Is.EqualTo(new byte[] { 1 }), "precondition: the re-seed settled the saga");
        Assert.That(await poison.GetPoisonedAsync(SiteAClusterId), Does.Not.Contain(txid), "precondition: the re-seed retired the poison");

        // The sender re-ships the withheld terminal; it fails again for the same cause.
        Assert.That(await DeliverAsync(failing), Is.False, "the re-shipped terminal is deferred again within the bound");
        await Task.Delay(TimeSpan.FromMilliseconds(450));

        var outcomes = new ConcurrentBag<string>();
        bool acked;
        using (ListenForSagaPoison(tree, outcomes))
        {
            acked = await PushAsync(failing);
        }

        var parked = await _siteB.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services
            .GetRequiredService<ILatticeReplicationDeadLetters>().ListAsync(tree);
        var after = new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "after-quarantine",
            Value = new byte[] { 9 },
            Timestamp = Hlc(49_000),
            OriginClusterId = SiteAClusterId,
        };
        var streamMoved = await DeliverAsync(after);

        Assert.Multiple(async () =>
        {
            Assert.That(acked, Is.True, "past the bound again the saga is quarantined and its record parked, so the stream moves past it");
            Assert.That(await poison.GetQuarantinedAsync(SiteAClusterId), Does.Contain(txid));
            Assert.That(await poison.GetPoisonedAsync(SiteAClusterId), Does.Not.Contain(txid), "a quarantined saga is not poisoned again");
            Assert.That(await poison.GetReseedOwedOriginsAsync(), Does.Not.Contain(SiteAClusterId), "nor re-seeded again");
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.OutcomeReceiverSagaQuarantined));
            Assert.That(parked.Count(e => e.Entry.TransactionId == txid && e.Entry.Op == failing.Op), Is.EqualTo(1),
                "the quarantined record is parked, never silently dropped");
            Assert.That(await receiver.GetAsync(keyA), Is.EqualTo(new byte[] { 1 }), "the settled saga is untouched");
            Assert.That(await receiver.GetAsync(keyB), Is.EqualTo(new byte[] { 2 }));
            Assert.That(streamMoved, Is.True);
            Assert.That(await receiver.GetAsync("after-quarantine"), Is.EqualTo(new byte[] { 9 }));
        });

        // A later copy of the quarantined saga's terminal is parked at once.
        Assert.That(await PushAsync(failing), Is.True, "a quarantined saga's records are parked without waiting out the bound");
    }

    private async Task<(string KeyA, string KeyB, Guid TxId)> StageCommittedSagaAsync(string tree, string prefix, long ticks)
    {
        var (keyA, keyB) = KeysOnDistinctShards(prefix);
        var txid = Guid.NewGuid();
        await _siteA.Client.GetGrain<ILattice>(tree).SetManyAtomicAsync(
        [
            new KeyValuePair<string, byte[]>(keyA, [1]),
            new KeyValuePair<string, byte[]>(keyB, [2]),
        ]);

        Assert.That(await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: ticks)), Is.True);
        Assert.That(await DeliverAsync(Prepare(tree, keyB, 2, txid, index: 1, ticks: ticks + 1)), Is.True);
        return (keyA, keyB, txid);
    }

    [Test]
    public async Task A_malformed_terminal_that_fails_again_after_its_re_seed_quarantines_the_saga()
    {
        const string tree = "rspr-quarantine-malformed";
        var (keyA, keyB, txid) = await StageCommittedSagaAsync(tree, "rspq-m", 40_000);

        var malformed = Terminal(tree, txid, MutationKind.TxCommit, 40_100, shardIndex: 0, key: "not-a-shard");
        await AssertQuarantinedAfterTheReseedSettledTheSagaAsync(tree, keyA, keyB, txid, malformed);
    }

    [Test]
    public async Task A_terminal_contradicting_the_decision_its_re_seed_recorded_quarantines_the_saga()
    {
        const string tree = "rspr-quarantine-conflict";
        var (keyA, keyB, txid) = await StageCommittedSagaAsync(tree, "rspq-c", 41_000);

        // The receiver registry records the source's commit; this terminal says
        // abort, so it conflicts before the re-seed and after it alike.
        await TxRegistryRouting.GetRegistry(_siteB.Client, tree, txid).MarkCommittedAsync(txid);
        var shard = LatticeSharding.GetShardIndex(keyA, LatticeConstants.DefaultShardCount);
        var abort = Terminal(tree, txid, MutationKind.TxAbort, 41_100, shardIndex: shard,
            key: shard.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await AssertQuarantinedAfterTheReseedSettledTheSagaAsync(tree, keyA, keyB, txid, abort);
    }
}
