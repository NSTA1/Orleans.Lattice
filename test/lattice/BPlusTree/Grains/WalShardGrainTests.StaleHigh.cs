using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the WAL shard's per-donor stale high (#5837): the highest offset of a
/// donor's records a receiver holding the donor's close pending must replay before it
/// advances live. It counts every activation but the closing one, at generations no later
/// than the close's (epoch-only records as generation 0, another shard root's generations
/// always), every displaced generation and activation, and every forward but those tagged
/// with the split that created the receiver; it is reported on the receiver's append as
/// the maximum over the closes the stamp carries, and never falls below the restart or
/// eviction floor.
/// </summary>
public partial class WalShardGrainTests
{
    private static readonly Guid Donor = new("d0000000-0000-0000-0000-0000000000d0");
    private static readonly Guid ClosingToken = new("c0000000-0000-0000-0000-0000000000c1");
    private static readonly Guid OtherToken = new("c0000000-0000-0000-0000-0000000000c2");
    private static readonly Guid ThirdToken = new("c0000000-0000-0000-0000-0000000000c3");

    private static WalShardGrain.LeafAuthors Authored(params (Guid Token, long Offset, int Shard, long Generation)[] records)
    {
        var authors = WalShardGrain.LeafAuthors.Empty;
        foreach (var (token, offset, shard, generation) in records)
        {
            authors = authors.With(token, offset, shard, generation);
        }

        return authors;
    }

    [Test]
    public void The_stale_high_excludes_the_closing_activation_and_counts_every_other_at_or_below_the_close()
    {
        var authors = Authored((OtherToken, 10, 0, 3), (ClosingToken, 20, 0, 3), (OtherToken, 30, 0, 4));

        Assert.Multiple(() =>
        {
            Assert.That(authors.StaleHigh(ClosingToken, 0, 4, Guid.Empty), Is.EqualTo(30),
                "another activation's record at the close's generation was not counted");
            Assert.That(authors.StaleHigh(ClosingToken, 0, 3, Guid.Empty), Is.EqualTo(10),
                "a record admitted above the close's generation was counted, or the closing activation's was");
            Assert.That(authors.StaleHigh(OtherToken, 0, 4, Guid.Empty), Is.EqualTo(20),
                "the closing activation is not compared by value");
        });
    }

    [Test]
    public void A_later_generation_of_another_shard_root_is_always_counted()
    {
        // A leaf that moved shard roots: shard 1's generation 50 is not later than shard 0's generation 3.
        var authors = Authored((OtherToken, 10, 1, 50), (OtherToken, 20, 0, 9));

        Assert.That(authors.StaleHigh(ClosingToken, 0, 3, Guid.Empty), Is.EqualTo(10),
            "a record admitted under another shard root's generation was compared against the close's");
    }

    [Test]
    public void An_epoch_only_record_counts_as_generation_zero()
    {
        var authors = Authored((OtherToken, 10, -1, 0), (OtherToken, 20, 0, 5));

        Assert.That(authors.StaleHigh(ClosingToken, 0, 1, Guid.Empty), Is.EqualTo(10),
            "an epoch-only record was not counted at every close");
    }

    [Test]
    public void A_displaced_generation_is_always_counted()
    {
        var authors = Authored((OtherToken, 10, 0, 1), (OtherToken, 20, 0, 2), (OtherToken, 30, 0, 3));

        Assert.Multiple(() =>
        {
            Assert.That(authors.StaleHigh(ClosingToken, 0, 0, Guid.Empty), Is.EqualTo(10),
                "the displaced generation's high was dropped");
            Assert.That(authors.StaleHigh(ClosingToken, 0, 2, Guid.Empty), Is.EqualTo(20));
        });
    }

    [Test]
    public void A_displaced_activation_is_always_counted_even_as_the_closing_one()
    {
        var authors = Authored((ThirdToken, 10, 0, 9), (OtherToken, 20, 0, 9), (ClosingToken, 30, 0, 9));

        Assert.Multiple(() =>
        {
            Assert.That(authors.StaleHigh(ClosingToken, 0, 1, Guid.Empty), Is.EqualTo(10),
                "a displaced activation's high was dropped");
            Assert.That(authors.StaleHigh(ThirdToken, 0, 1, Guid.Empty), Is.EqualTo(10),
                "a displaced activation was excluded as the closing one");
            Assert.That(authors.AllHigh, Is.EqualTo(30));
        });
    }

    [Test]
    public void The_stale_high_excludes_only_the_forwards_of_the_split_that_created_the_receiver()
    {
        // StaleHighExcludesOtherTag.
        var authors = Authored((OtherToken, 5, 0, 1))
            .WithForward(NarrowTagOne, 10)
            .WithForward(NarrowTagTwo, 20)
            .WithForward(NarrowTagOne, 30);

        Assert.Multiple(() =>
        {
            Assert.That(authors.StaleHigh(ClosingToken, 0, 1, NarrowTagOne), Is.EqualTo(20),
                "a forward of another split was excluded, or the receiver's own was not");
            Assert.That(authors.StaleHigh(ClosingToken, 0, 1, NarrowTagTwo), Is.EqualTo(30));
            Assert.That(authors.StaleHigh(ClosingToken, 0, 1, NarrowTagThree), Is.EqualTo(30));
        });
    }

    [Test]
    public void Folding_an_activations_generations_keeps_its_forwards()
    {
        var authors = Authored((OtherToken, 5, 0, 1)).WithForward(NarrowTagOne, 10);

        authors = authors.With(ThirdToken, 20, 0, 1).With(ClosingToken, 30, 0, 1);

        Assert.That(authors.StaleHigh(ClosingToken, 0, 1, NarrowTagTwo), Is.EqualTo(20),
            "a record of an activation or a forward was lost when the activations were displaced");
    }

    [Test]
    public async Task An_append_reports_the_highest_stale_high_of_the_closes_it_carries()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var secondDonor = new Guid("d0000000-0000-0000-0000-0000000000d2");
        var low = await grain.AppendStampedAsync(MakeEntry("d1"), WalAppendStamp.PreArm(Donor, OtherToken) with { Initiator = PreArmRoot, Generation = 1 }, CancellationToken.None);
        var high = await grain.AppendStampedAsync(MakeEntry("d2"), WalAppendStamp.PreArm(secondDonor, OtherToken) with { Initiator = PreArmRoot, Generation = 1 }, CancellationToken.None);

        var close1 = new WalPendingClose(Donor, ClosingToken, PreArmRoot.Index, 1);
        var close2 = new WalPendingClose(secondDonor, ClosingToken, PreArmRoot.Index, 1);
        var withNone = await grain.AppendStampedAsync(MakeEntry("r0"), PreArmStamp(ActivationA, 1), CancellationToken.None);
        var withFirst = await grain.AppendStampedAsync(MakeEntry("r1"), PreArmStamp(ActivationA, 1) with { Close1 = close1 }, CancellationToken.None);
        var withBoth = await grain.AppendStampedAsync(MakeEntry("r2"), PreArmStamp(ActivationA, 1) with { Close1 = close1, Close2 = close2 }, CancellationToken.None);
        var secondOnly = await grain.AppendStampedAsync(MakeEntry("r3"), PreArmStamp(ActivationA, 1) with { Close2 = close2 }, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(withNone.UnstampedWatermark, Is.LessThan(low.Offset), "precondition: nothing foreign to the receiver");
            Assert.That(withFirst.UnstampedWatermark, Is.EqualTo(low.Offset), "the first close's stale high was not reported");
            Assert.That(withBoth.UnstampedWatermark, Is.EqualTo(high.Offset), "the reported high is not the maximum over the closes");
            Assert.That(secondOnly.UnstampedWatermark, Is.EqualTo(high.Offset), "the second close's stale high was not reported");
        });
    }

    [Test]
    public async Task The_stale_high_never_falls_below_the_restart_floor()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var provider = new InMemoryWalStorageProvider();
        var grain = await CreateFencedGrainAsync(storage, provider);
        var record = await grain.AppendStampedAsync(MakeEntry("d"), WalAppendStamp.PreArm(Donor, OtherToken) with { Initiator = PreArmRoot, Generation = 1 }, CancellationToken.None);

        var restarted = await CreateFencedGrainAsync(storage, provider);

        Assert.That(
            restarted.StaleHighForTesting(new WalPendingClose(Donor, ClosingToken, PreArmRoot.Index, 1), FenceLeaf),
            Is.GreaterThanOrEqualTo(record.Offset),
            "a restart forgot a donor record the receiver had not replayed");
    }

    [Test]
    public async Task The_stale_high_never_falls_below_the_eviction_floor()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var record = await grain.AppendStampedAsync(MakeEntry("d"), WalAppendStamp.PreArm(Donor, OtherToken) with { Initiator = PreArmRoot, Generation = 1 }, CancellationToken.None);

        grain.FoldLeafAuthorsForTesting();

        Assert.That(
            grain.StaleHighForTesting(new WalPendingClose(Donor, ClosingToken, PreArmRoot.Index, 1), FenceLeaf),
            Is.GreaterThanOrEqualTo(record.Offset),
            "an evicted donor's records fell out of the stale high");
    }

    [Test]
    public async Task A_split_cut_reads_the_head_and_every_bound_in_one_turn()
    {
        // SplitHintBoundsReadBeforeHeads (D4): the head and the bounds come from one turn.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var foreign = await grain.AppendStampedAsync(MakeEntry("f"), PreArmStamp(ActivationB, 1), CancellationToken.None);
        var donorRecord = await grain.AppendStampedAsync(MakeEntry("d"), WalAppendStamp.PreArm(Donor, OtherToken) with { Initiator = PreArmRoot, Generation = 1 }, CancellationToken.None);
        var own = await grain.AppendStampedAsync(MakeEntry("o"), PreArmStamp(ActivationA, 1), CancellationToken.None);

        var plain = await grain.GetSplitCutAsync(WalAppendStamp.PreArm(FenceLeaf, ActivationA), CancellationToken.None);
        var withClose = await grain.GetSplitCutAsync(
            WalAppendStamp.PreArm(FenceLeaf, ActivationA) with { Close1 = new WalPendingClose(Donor, ClosingToken, PreArmRoot.Index, 1) },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(plain.Head, Is.EqualTo(own.Offset + 1), "the cut's head is not the readable head");
            Assert.That(plain.ForeignHigh, Is.EqualTo(foreign.Offset), "another activation's record was not bounded, or the reader's own was");
            Assert.That(withClose.ForeignHigh, Is.EqualTo(donorRecord.Offset), "a carried close's stale high was not in the cut");
        });
    }

    [Test]
    public async Task A_resumed_close_reads_every_activation_s_records_never_below_the_floor()
    {
        // ResumedCloseUsesOnlyOwnHigh and ResumedCloseOwnHighOldTokenNotStale (rule R).
        var storage = new EnumerableMemoryGrainStorage();
        var provider = new InMemoryWalStorageProvider();
        var grain = await CreateFencedGrainAsync(storage, provider);
        var stale = await grain.AppendStampedAsync(MakeEntry("s"), WalAppendStamp.PreArm(Donor, OtherToken) with { Initiator = PreArmRoot, Generation = 1 }, CancellationToken.None);
        var live = await grain.AppendStampedAsync(MakeEntry("l"), WalAppendStamp.PreArm(Donor, ClosingToken) with { Initiator = PreArmRoot, Generation = 1 }, CancellationToken.None);
        var everyActivation = new WalAppendStamp(Donor, 0, WalRoutingInitiator.None, 0);

        var cut = await grain.GetSplitCutAsync(everyActivation, CancellationToken.None);
        var restarted = await CreateFencedGrainAsync(storage, provider);
        var afterRestart = await restarted.GetSplitCutAsync(everyActivation, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(cut.ForeignHigh, Is.EqualTo(Math.Max(stale.Offset, live.Offset)), "a resumed close's bound left out an activation's records");
            Assert.That(afterRestart.ForeignHigh, Is.GreaterThanOrEqualTo(Math.Max(stale.Offset, live.Offset)), "a restart lowered a resumed close's bound below the recovered head");
        });
    }

    [Test]
    public async Task StaleHighOmitsEpochOnlyNonClosingToken()
    {
        // a5e2e2ef's H2, StaleEpochOnlyAlloc: a stale duplicate of the donor appends under
        // its claimed epoch with no routing generation (WalRoutingStamp.None). The record
        // is in the donor's stale high whatever generation the close names.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var claim = await grain.ClaimAppendEpochAsync(Donor, 1, CancellationToken.None);
        var epochOnly = await grain.AppendStampedAsync(MakeEntry("d"), WalAppendStamp.ForLeaf(Donor, 1, OtherToken), CancellationToken.None);
        var closing = await grain.AppendStampedAsync(MakeEntry("c"), WalAppendStamp.ForLeaf(Donor, 1, ClosingToken), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(claim.Accepted, Is.True, "precondition: the donor's epoch was claimed");
            Assert.That(epochOnly.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted), "precondition: the epoch-only write was admitted");
            Assert.That(
                grain.StaleHighForTesting(new WalPendingClose(Donor, ClosingToken, PreArmRoot.Index, 1), FenceLeaf),
                Is.GreaterThanOrEqualTo(epochOnly.Offset),
                "a non-closing token's epoch-only record was left out of the donor's stale high");
            Assert.That(
                grain.StaleHighForTesting(new WalPendingClose(Donor, ClosingToken, PreArmRoot.Index, 1), FenceLeaf),
                Is.LessThan(closing.Offset),
                "the closing token's own record was counted");
        });
    }
}
