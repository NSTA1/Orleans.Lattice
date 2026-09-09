using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the interaction between snapshot pins and the
/// effective decisions revision.
/// <para>
/// The read mask is pin-aware - that is the whole point of a pin, so a
/// point-in-time cursor keeps reading the decisions its snapshot captured even
/// after they age out of retention - which makes <c>SnapshotPins</c> a second
/// input to the token's live-expired term alongside <c>ForgottenAt</c>. It is an
/// input the clock-driven validity horizon cannot see: an explicit pin, unpin,
/// or refresh moves the readable surface with no clock advance at all. Two
/// distinct failures follow from ignoring it, and both are covered here: a stale
/// memo serving a pre-mutation count (so the token does not move across a real
/// surface change), and a falling token (so it revisits a value it previously
/// carried under a different surface). Both defeat the reader-side fast path in
/// <see cref="ReaderStabilityGate.IsRevisionStable(long, long)"/>, which
/// short-circuits on token equality and never consults <c>IsSnapshotStable</c>.
/// </para>
/// </summary>
public partial class TxRegistryGrainTests
{
    [Test]
    public async Task GetDecisionsRevisionAsync_moves_when_an_unpin_re_masks_an_expired_tombstone()
    {
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _, clock) = CreateGrainForPins(retention: retention);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        // A cursor pins the decision while it is still inside its retention
        // window, then the tombstone ages out underneath the pin. The pin holds
        // it readable, so the surface still shows the recorded outcome.
        var pinId = Guid.NewGuid();
        await grain.PinSnapshotAsync(pinId, [txid], TimeSpan.FromMinutes(10));
        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var snap = await grain.SnapshotWithRevisionAsync();
        Assert.That(snap.Decisions[txid], Is.EqualTo(TxStatus.Committed),
            "Precondition: the live pin must keep the aged-out decision readable.");
        var before = snap.Revision;

        // The cursor closes. The row is now past retention AND unpinned, so it
        // drops out of the readable surface - a real change, driven by a
        // mutation the clock-keyed memo horizon cannot observe.
        await grain.UnpinSnapshotAsync(pinId);

        var after = await grain.GetDecisionsRevisionAsync();
        var reread = await grain.SnapshotWithRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(reread.Decisions[txid], Is.EqualTo(TxStatus.Indeterminate),
                "Unpinning re-masks the aged-out decision.");
            Assert.That(after, Is.Not.EqualTo(before),
                "Unpinning changed what a reader can observe, so the token must move.");
            Assert.That(ReaderStabilityGate.IsRevisionStable(before, after), Is.False,
                "The reader fast path must reject a snapshot taken before the unpin.");
        });
    }

    [Test]
    public async Task GetDecisionsRevisionAsync_does_not_fall_when_a_pin_covers_an_expired_tombstone()
    {
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _, clock) = CreateGrainForPins(retention: retention);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var before = await grain.GetDecisionsRevisionAsync();

        // Pinning an ALREADY-masked decision restores it to the readable
        // surface, which drops the live-expired term. Without compensation the
        // token falls, and a falling token can revisit a value it carried under
        // a different surface.
        await grain.PinSnapshotAsync(Guid.NewGuid(), [txid], TimeSpan.FromMinutes(10));

        var after = await grain.GetDecisionsRevisionAsync();

        Assert.That(after, Is.GreaterThan(before),
            "The token must strictly increase across a pin that un-masks a "
            + "decision, both because the surface changed and because it must "
            + "never fall.");
    }

    [Test]
    public async Task GetDecisionsRevisionAsync_never_revisits_a_value_across_a_staggered_pin()
    {
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _, clock) = CreateGrainForPins(retention: retention);
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        await grain.MarkCommittedAsync(first);
        await grain.MarkCommittedAsync(second);
        await grain.ForgetAsync(first);

        // t1: `first` is tombstoned and ages out; `second` is still live.
        clock.Advance(retention + TimeSpan.FromSeconds(1));
        var t1 = await grain.SnapshotWithRevisionAsync();
        Assert.Multiple(() =>
        {
            Assert.That(t1.Decisions[first], Is.EqualTo(TxStatus.Indeterminate));
            Assert.That(t1.Decisions[second], Is.EqualTo(TxStatus.Committed));
        });

        // t2: `second` is tombstoned and ages out too, while a pin takes cover
        // of `first`. The masked count is 1 at both instants - but it is a
        // DIFFERENT row that is masked, so the surfaces differ and the token
        // must not repeat.
        await grain.PinSnapshotAsync(Guid.NewGuid(), [first], TimeSpan.FromHours(1));
        await grain.ForgetAsync(second);
        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var t2 = await grain.SnapshotWithRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(t2.Decisions[first], Is.EqualTo(TxStatus.Committed),
                "Precondition: the pin un-masked the first decision.");
            Assert.That(t2.Decisions[second], Is.EqualTo(TxStatus.Indeterminate),
                "Precondition: the second decision aged out unpinned.");
            Assert.That(t2.Revision, Is.Not.EqualTo(t1.Revision),
                "The two surfaces differ, so the token must differ. Equal tokens "
                + "here would let the reader fast path serve the t1 snapshot as "
                + "though nothing had changed.");
            Assert.That(t2.Revision, Is.GreaterThan(t1.Revision),
                "The token must be non-decreasing across every sequence.");
        });
    }

    [Test]
    public async Task GetDecisionsRevisionAsync_is_unmoved_by_a_pin_that_masks_nothing()
    {
        var retention = TimeSpan.FromMinutes(30);
        var (grain, _, _) = CreateGrainForPins(retention: retention);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        // The tombstone is well inside its retention window, so the decision is
        // already readable and pinning it changes nothing a reader can see. The
        // compensation must not fire, or every cursor open would spuriously
        // invalidate every concurrent reader's fan-out.
        var before = await grain.GetDecisionsRevisionAsync();
        await grain.PinSnapshotAsync(Guid.NewGuid(), [txid], TimeSpan.FromMinutes(10));
        var after = await grain.GetDecisionsRevisionAsync();

        Assert.That(after, Is.EqualTo(before),
            "A pin that un-masks nothing changes no readable surface, so the "
            + "token must stay put.");
    }

    [Test]
    public async Task PinSnapshotAsync_unwinds_the_unmask_epoch_when_the_write_fails()
    {
        var retention = TimeSpan.FromSeconds(30);
        var (grain, state, clock) = CreateGrainForPins(retention: retention);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var before = await grain.GetDecisionsRevisionAsync();

        state.ThrowOnWrite = new InvalidOperationException("persist failed");
        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.PinSnapshotAsync(Guid.NewGuid(), [txid], TimeSpan.FromMinutes(10)));
        state.ThrowOnWrite = null;

        var after = await grain.GetDecisionsRevisionAsync();

        Assert.That(after, Is.EqualTo(before),
            "A failed pin persist leaves the readable surface untouched, so its "
            + "compensation must unwind with the pin it accounts for - otherwise "
            + "the token and the state it describes drift apart.");
    }

    [Test]
    public async Task GetDecisionsRevisionAsync_moves_when_a_pin_lapses_over_an_expired_tombstone()
    {
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _, clock) = CreateGrainForPins(retention: retention, maxPinTtl: TimeSpan.FromMinutes(5));
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        await grain.PinSnapshotAsync(Guid.NewGuid(), [txid], TimeSpan.FromMinutes(2));

        clock.Advance(retention + TimeSpan.FromSeconds(1));
        var before = await grain.GetDecisionsRevisionAsync();

        // The pin lapses on the clock alone - no mutation anywhere to hang a
        // bump on, which is why the live-expired term has to be pin-aware and
        // its memo horizon clamped to the first lapse.
        clock.Advance(TimeSpan.FromMinutes(3));

        var after = await grain.GetDecisionsRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.GreaterThan(before),
                "A lapsing pin re-masks its rows, which the token must announce.");
            Assert.That(ReaderStabilityGate.IsRevisionStable(before, after), Is.False,
                "The reader fast path must reject a snapshot taken before the lapse.");
        });
    }
}
