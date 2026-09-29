using NUnit.Framework;
using Orleans.Lattice.BPlusTree.Grains;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2823. Pins the production half of the transactional clause in the
/// scan-page reuse gate: a range read that resolved uncommitted transactional
/// writes must record the cookie it ran at, and a read that resolved none must
/// record nothing.
/// <para>
/// Testing the gate alone cannot catch a leaf that never publishes the signal,
/// because the gate would then be behaving correctly on a wrong input - which is
/// the same reason the expiry-horizon publication has its own arms in
/// <c>BPlusLeafGrainTests.RevisionCookieCoverage.cs</c>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static long? TransactionalReadRevision(string replicaId) =>
        BPlusLeafGrain.TryGetLeafTransactionalReadRevision(GrainId.Create("leaf", replicaId), out var revision)
            ? revision
            : null;

    [Test]
    public async Task A_key_range_read_over_pending_transactions_records_the_cookie_it_ran_at()
    {
        var replicaId = FreshReplicaId("tx-read-keys");
        var grain = CreateGrain(replicaId: replicaId);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "k2", Encoding.UTF8.GetBytes("v2"));

        using (InFlight(txid))
        {
            await grain.GetKeysAsync();
        }

        var cookie = RevisionCookie(replicaId);
        Assert.That(cookie, Is.Not.Null, "precondition: the leaf must have published a cookie");
        Assert.That(
            TransactionalReadRevision(replicaId),
            Is.EqualTo(cookie),
            "the read resolved a pending transaction against the registry, so its answer can change "
            + "with nothing written; it must record the cookie it ran at so a settled copy of it is "
            + "never reused");
    }

    [Test]
    public async Task An_entry_range_read_over_pending_transactions_records_the_cookie_it_ran_at()
    {
        var replicaId = FreshReplicaId("tx-read-entries");
        var grain = CreateGrain(replicaId: replicaId);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "k2", Encoding.UTF8.GetBytes("v2"));

        using (InFlight(txid))
        {
            await grain.GetEntriesAsync();
        }

        var cookie = RevisionCookie(replicaId);
        Assert.That(cookie, Is.Not.Null, "precondition: the leaf must have published a cookie");
        Assert.That(
            TransactionalReadRevision(replicaId),
            Is.EqualTo(cookie),
            "the entries read resolved a pending transaction and must record the cookie it ran at, "
            + "exactly as the keys read does: both feed the scan-page reuse map");
    }

    /// <summary>
    /// The control: a read over a leaf with no pending transactions records
    /// nothing. Without it, a publication that fired on every read would
    /// satisfy the arms above while disabling scan-page reuse outright.
    /// </summary>
    [Test]
    public async Task A_range_read_with_no_pending_transactions_records_no_transactional_read()
    {
        var replicaId = FreshReplicaId("tx-read-none");
        var grain = CreateGrain(replicaId: replicaId);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var keys = await grain.GetKeysAsync();
        var entries = await grain.GetEntriesAsync();
        Assert.That(keys, Has.Count.EqualTo(1), "precondition: the read must surface the row");
        Assert.That(entries, Has.Count.EqualTo(1), "precondition: the read must surface the row");

        Assert.That(
            TransactionalReadRevision(replicaId),
            Is.Null,
            "a read that consulted no pending transaction is a pure function of the leaf's rows and "
            + "the clock, both of which the cookie and horizon already cover");
    }

    /// <summary>
    /// Once the pending set drains, the recorded cookie must fall behind the
    /// leaf's current one, so a settled read issued after the drain is reusable
    /// again. This is what the cookie-valued signal buys over a flag.
    /// </summary>
    [Test]
    public async Task A_drained_pending_set_leaves_the_transactional_read_behind_the_current_cookie()
    {
        var replicaId = FreshReplicaId("tx-read-drained");
        var grain = CreateGrain(replicaId: replicaId);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "k2", Encoding.UTF8.GetBytes("v2"));

        using (InFlight(txid))
        {
            await grain.GetKeysAsync();
        }

        var recorded = TransactionalReadRevision(replicaId);
        Assert.That(recorded, Is.EqualTo(RevisionCookie(replicaId)),
            "precondition: the read over the pending set must have recorded the cookie");

        await grain.ApplyTxTerminalAsync(txid, committed: false, committedValues: null);
        await grain.GetKeysAsync();

        Assert.Multiple(() =>
        {
            Assert.That(RevisionCookie(replicaId), Is.GreaterThan(recorded!.Value),
                "the abort removed a pending bucket and must have advanced the cookie");
            Assert.That(TransactionalReadRevision(replicaId), Is.EqualTo(recorded),
                "a read over the drained leaf consulted no registry and must not move the signal "
                + "up to the current cookie");
        });
    }

    [Test]
    public async Task Deactivation_drops_the_transactional_read_signal()
    {
        var replicaId = FreshReplicaId("tx-read-deactivate");
        var grain = CreateGrain(replicaId: replicaId);
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "k2", Encoding.UTF8.GetBytes("v2"));

        using (InFlight(txid))
        {
            await grain.GetKeysAsync();
        }

        Assert.That(TransactionalReadRevision(replicaId), Is.Not.Null,
            "precondition: the leaf must be holding a signal to be deprived of");

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        Assert.That(TransactionalReadRevision(replicaId), Is.Null,
            "the activation that recorded this signal is gone, so the signal must go with it or the "
            + "map grows once per activation for the life of the silo");
    }
}
