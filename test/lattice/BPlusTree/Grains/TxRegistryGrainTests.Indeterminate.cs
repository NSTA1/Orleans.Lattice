using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the registry snapshot surface's explicit
/// "cannot currently determine" case.
/// <para>
/// A decision whose tombstone outlives <c>TxDecisionRetention</c> used to be
/// dropped from the snapshot dictionary entirely, which made it indistinguishable
/// from a saga the registry had never heard of. That collapse is harmless for a
/// same-process reader (both readings hide the prepared row) but not for a
/// consumer that treats the dictionary as a transferable payload: a cross-cluster
/// snapshot export read the absence of an aged-out <c>Committed</c> saga as
/// "still preparing", and the terminal that would have corrected it was already
/// behind the incremental stream the receiver drains next, so nothing on either
/// side could repair the divergence. These tests pin the row being carried
/// explicitly rather than omitted.
/// </para>
/// </summary>
public partial class TxRegistryGrainTests
{
    [Test]
    public async Task SnapshotAsync_reports_an_expired_tombstone_as_indeterminate_not_absent()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        var live = await grain.SnapshotAsync();

        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var aged = await grain.SnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(live[txid], Is.EqualTo(TxStatus.Committed),
                "Inside the retention window the recorded outcome is still readable.");
            Assert.That(aged.ContainsKey(txid), Is.True,
                "An aged-out row must stay in the dictionary. Dropping it makes it "
                + "indistinguishable from a saga the registry never recorded, which "
                + "a snapshot consumer reads as 'still preparing'.");
            Assert.That(aged[txid], Is.EqualTo(TxStatus.Indeterminate),
                "The row must say that its outcome is not currently determinable, "
                + "not report an outcome it is no longer entitled to report.");
        });
    }

    [Test]
    public async Task SnapshotAsync_reports_an_expired_abort_as_indeterminate_too()
    {
        // The masking rule is about the retention boundary, not about which
        // outcome was recorded. An aborted saga aged out of the window is just as
        // undeterminable as a committed one, and collapsing it onto InFlight would
        // be the same category error even though it happens to hide the same keys.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkAbortedAsync(txid);
        await grain.ForgetAsync(txid);

        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var aged = await grain.SnapshotAsync();

        Assert.That(aged[txid], Is.EqualTo(TxStatus.Indeterminate));
    }

    [Test]
    public async Task SnapshotAsync_omits_a_txid_it_never_recorded()
    {
        // The complement of the rule above: absence must keep meaning exactly one
        // thing. If an unrecorded txid also appeared as Indeterminate the new
        // status would carry no information.
        var (grain, _) = CreateGrain();

        var snapshot = await grain.SnapshotAsync();

        Assert.That(snapshot.ContainsKey(Guid.NewGuid()), Is.False);
    }

    [Test]
    public async Task SnapshotWithRevisionAsync_reports_an_expired_tombstone_as_indeterminate()
    {
        // The revision-carrying overload builds its own dictionary, so it needs
        // its own guard - a fix applied to only one of the two would leave the
        // reader fast path (which uses this overload) on the old behaviour.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var snapshot = await grain.SnapshotWithRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Decisions.ContainsKey(txid), Is.True);
            Assert.That(snapshot.Decisions[txid], Is.EqualTo(TxStatus.Indeterminate));
        });
    }

    [Test]
    public async Task SnapshotWithRevisionAsync_pairs_the_indeterminate_row_with_a_moved_revision()
    {
        // The two halves of the snapshot must agree. The revision token counts
        // rows currently past their retention boundary, so the same crossing that
        // turns a row Indeterminate must also move the token - otherwise a reader
        // holding the pre-crossing snapshot passes the revision fast path and
        // keeps reading the recorded outcome for a row the registry has stopped
        // vouching for.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        var before = await grain.SnapshotWithRevisionAsync();

        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var after = await grain.SnapshotWithRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(before.Decisions[txid], Is.EqualTo(TxStatus.Committed));
            Assert.That(after.Decisions[txid], Is.EqualTo(TxStatus.Indeterminate));
            Assert.That(
                ReaderStabilityGate.IsRevisionStable(before.Revision, after.Revision),
                Is.False,
                "The reader fast path short-circuits on revision equality and never "
                + "consults the dictionary, so the transition has to be visible in "
                + "the token as well as in the row.");
        });
    }

    [Test]
    public async Task SnapshotAsync_leaves_unexpired_rows_at_their_recorded_outcome()
    {
        // Guard against an over-broad mask: only rows past the boundary change
        // what they say, and a row with no tombstone at all is never masked.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var neverForgotten = Guid.NewGuid();
        var forgottenRecently = Guid.NewGuid();
        await grain.MarkCommittedAsync(neverForgotten);
        await grain.MarkAbortedAsync(forgottenRecently);
        await grain.ForgetAsync(forgottenRecently);

        clock.Advance(retention - TimeSpan.FromSeconds(1));

        var snapshot = await grain.SnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot[neverForgotten], Is.EqualTo(TxStatus.Committed));
            Assert.That(snapshot[forgottenRecently], Is.EqualTo(TxStatus.Aborted));
        });
    }
}
