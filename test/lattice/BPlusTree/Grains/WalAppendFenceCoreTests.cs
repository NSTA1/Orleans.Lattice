using Orleans.Lattice.BPlusTree.Grains;
using static Orleans.Lattice.BPlusTree.Grains.WalAppendFenceCore;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The pure guards of the WAL append fence (epic #5605), each pinned against the
/// model guard it implements in <c>spec/wal/design/WalLiveAdvance.tla</c>.
/// </summary>
[TestFixture]
public sealed class WalAppendFenceCoreTests
{
    [Test]
    public void ClaimWrite_writes_only_strictly_above_the_loaded_and_durable_entries()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DecideClaimWrite(value: 3, loaded: Unloaded, durable: 0, floor: 0), Is.EqualTo(ClaimWriteDecision.Write));
            Assert.That(DecideClaimWrite(value: 3, loaded: 2, durable: 2, floor: 0), Is.EqualTo(ClaimWriteDecision.Write));
            Assert.That(DecideClaimWrite(value: 2, loaded: 3, durable: 3, floor: 0), Is.EqualTo(ClaimWriteDecision.Refuse),
                "StaleEpochNeverAppendsClaimEntryOverwrite: a lower claim must never overwrite the durable entry");
            Assert.That(DecideClaimWrite(value: 2, loaded: Unloaded, durable: 3, floor: 0), Is.EqualTo(ClaimWriteDecision.Refuse),
                "the durable entry outranks a claim even when the shard has not loaded it");
            Assert.That(DecideClaimWrite(value: 2, loaded: 3, durable: 1, floor: 0), Is.EqualTo(ClaimWriteDecision.Refuse),
                "the loaded entry outranks a claim even when the row read lags it");
        });
    }

    [Test]
    public void ClaimWrite_is_refused_at_or_below_the_retired_floor()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DecideClaimWrite(value: 4, loaded: Unloaded, durable: 0, floor: 4), Is.EqualTo(ClaimWriteDecision.Refuse),
                "StaleEpochNeverAppendsIgnoresRetiredFloor: the floor is checked at the claim write");
            Assert.That(DecideClaimWrite(value: 5, loaded: Unloaded, durable: 0, floor: 4), Is.EqualTo(ClaimWriteDecision.Write));
            Assert.That(DecideClaimWrite(value: 4, loaded: Unloaded, durable: 4, floor: 4), Is.EqualTo(ClaimWriteDecision.Refuse),
                "a value already durable is still refused once a retirement has raised the floor to it");
        });
    }

    [Test]
    public void A_re_presented_claim_already_durable_proceeds_without_a_write()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DecideClaimWrite(value: 3, loaded: Unloaded, durable: 3, floor: 0), Is.EqualTo(ClaimWriteDecision.AlreadyDurable),
                "the re-presented claim after a restart reloads the entry");
            Assert.That(DecideClaimWrite(value: 3, loaded: 3, durable: 3, floor: 0), Is.EqualTo(ClaimWriteDecision.AlreadyDurable));
        });
    }

    [Test]
    public void ClaimRaise_is_refused_when_another_claim_or_a_retirement_overtook_the_write()
    {
        Assert.Multiple(() =>
        {
            Assert.That(IsRaiseAdmitted(value: 3, loaded: 2, durableSeen: 3, floor: 0), Is.True);
            Assert.That(IsRaiseAdmitted(value: 3, loaded: 4, durableSeen: 3, floor: 0), Is.False,
                "another claim raised the entry above this one since its write");
            Assert.That(IsRaiseAdmitted(value: 3, loaded: 2, durableSeen: 4, floor: 0), Is.False,
                "another claim wrote the entry above this one since its write");
            Assert.That(IsRaiseAdmitted(value: 3, loaded: 2, durableSeen: 3, floor: 3), Is.False,
                "StaleEpochNeverAppendsRaiseIgnoresRetiredFloor: a retirement since the write raised the floor to it");
        });
    }

    [Test]
    public void A_stamped_append_is_admitted_only_from_a_loaded_current_epoch()
    {
        // The retired floor is not an append-time guard (issue #5635): it is one
        // value per partition while epochs are per leaf, so checking it here would
        // supersede every live leaf below a retired one. The claim write and raise
        // check it, and a retired leaf's own appends meet its removed entry.
        Assert.Multiple(() =>
        {
            Assert.That(IsEpochAdmitted(epoch: 3, loaded: 3), Is.True);
            Assert.That(IsEpochAdmitted(epoch: 4, loaded: 3), Is.True);
            Assert.That(IsEpochAdmitted(epoch: 2, loaded: 3), Is.False, "a superseded epoch is rejected");
            Assert.That(IsEpochAdmitted(epoch: 3, loaded: Unloaded), Is.False,
                "StaleEpochNeverAppendsUnloadedEntryAccepted: nothing is admitted from an unloaded entry");
        });
    }

    [Test]
    public void A_routing_generation_is_admitted_only_from_a_loaded_current_entry()
    {
        Assert.Multiple(() =>
        {
            Assert.That(IsGenerationAdmitted(generation: 2, loaded: 2), Is.True);
            Assert.That(IsGenerationAdmitted(generation: 1, loaded: 2), Is.False);
            Assert.That(IsGenerationAdmitted(generation: 2, loaded: Unloaded), Is.False);
        });
    }

    [Test]
    public void ClaimThreshold_is_the_highest_of_the_entries_and_the_floor()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClaimThreshold(loaded: Unloaded, durable: 2, floor: 5), Is.EqualTo(5));
            Assert.That(ClaimThreshold(loaded: 7, durable: 2, floor: 5), Is.EqualTo(7));
            Assert.That(ClaimThreshold(loaded: 1, durable: 9, floor: 5), Is.EqualTo(9));
        });
    }
}
