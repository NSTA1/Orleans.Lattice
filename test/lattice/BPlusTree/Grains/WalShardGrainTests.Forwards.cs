using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the WAL shard's attribution of forwarded records (#5837, condition 4 and
/// the forward-exclusion rule X1-X8): a record a donor forwarded into its unlinked sibling
/// is attributed to the sibling's activation and to the first forwarder under no token, by
/// the narrow tag that moved its key; the forwarder's own narrow tag is left out of the
/// foreign high reported to it, every other tag is not; a displaced tag folds into a
/// never-excluded high; eviction floors to an entry's overall high, forwards included; and
/// a restart floors every forward below the recovered head.
/// </summary>
public partial class WalShardGrainTests
{
    private static readonly Guid Forwarder = new("dddddddd-0000-0000-0000-00000000000d");
    private static readonly Guid NarrowTagOne = new("11111111-0000-0000-0000-000000000001");
    private static readonly Guid NarrowTagTwo = new("22222222-0000-0000-0000-000000000002");
    private static readonly Guid NarrowTagThree = new("33333333-0000-0000-0000-000000000003");

    private static WalAppendStamp ForwardedStamp(Guid receiverActivation, Guid tag) =>
        PreArmStamp(receiverActivation, 1) with { Forwarder = Forwarder, ForwarderTag = tag };

    [Test]
    public async Task A_forwarded_record_is_foreign_to_every_activation_of_its_forwarder()
    {
        // ForwardNotAttributedToForwarder and ForwardAttributedToForwarderToken.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());

        var forwarded = await grain.AppendStampedAsync(MakeEntry("moved"), ForwardedStamp(ActivationA, NarrowTagOne), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(forwarded.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
            Assert.That(grain.ForeignHighForTesting(Forwarder, ActivationA), Is.EqualTo(forwarded.Offset),
                "a forward was not attributed to its forwarder");
            Assert.That(grain.ForeignHighForTesting(Forwarder, ActivationB), Is.EqualTo(forwarded.Offset),
                "a forward was attributed under one of the forwarder's activation tokens");
            Assert.That(grain.ForeignHighForTesting(FenceLeaf, ActivationA), Is.LessThan(forwarded.Offset),
                "the receiver's own forwarded write was counted as foreign to it");
        });
    }

    [Test]
    public async Task A_forwarder_excludes_only_the_forwards_of_its_own_persisted_narrow()
    {
        // ForwardExclusionByForwarderSpan and ForwardExclusionTagCollision (X1-X4).
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var mine = await grain.AppendStampedAsync(MakeEntry("a"), ForwardedStamp(ActivationA, NarrowTagOne), CancellationToken.None);
        var other = await grain.AppendStampedAsync(MakeEntry("b"), ForwardedStamp(ActivationA, NarrowTagTwo), CancellationToken.None);
        var mineAgain = await grain.AppendStampedAsync(MakeEntry("c"), ForwardedStamp(ActivationA, NarrowTagOne), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.ForeignHighForTesting(Forwarder, ActivationB, NarrowTagOne), Is.EqualTo(other.Offset),
                "a forward of another narrow was excluded, or the reader's own narrow was not");
            Assert.That(grain.ForeignHighForTesting(Forwarder, ActivationB, NarrowTagTwo), Is.EqualTo(mineAgain.Offset));
            Assert.That(grain.ForeignHighForTesting(Forwarder, ActivationB), Is.EqualTo(mineAgain.Offset),
                "a reader with no persisted narrow excluded a forward");
            Assert.That(mine.Offset, Is.LessThan(other.Offset), "precondition: offsets ascend");
        });
    }

    [Test]
    public async Task A_stamped_append_excludes_its_own_narrow_from_the_foreign_high_it_reports()
    {
        // ForwardExclusionByForwarderSpan, through the append response.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var forwarded = await grain.AppendStampedAsync(MakeEntry("moved"), ForwardedStamp(ActivationA, NarrowTagOne), CancellationToken.None);

        var withTag = await grain.AppendStampedAsync(
            MakeEntry("own-1"),
            WalAppendStamp.PreArm(Forwarder, ActivationB) with { Initiator = PreArmRoot, Generation = 1, ReaderTag = NarrowTagOne },
            CancellationToken.None);
        var withoutTag = await grain.AppendStampedAsync(
            MakeEntry("own-2"),
            WalAppendStamp.PreArm(Forwarder, ActivationB) with { Initiator = PreArmRoot, Generation = 1 },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(withTag.UnstampedWatermark, Is.LessThan(forwarded.Offset),
                "the forwarder's own narrow's forward was reported to it");
            Assert.That(withoutTag.UnstampedWatermark, Is.GreaterThanOrEqualTo(forwarded.Offset),
                "a forwarder with no persisted narrow was not reported its forward");
        });
    }

    [Test]
    public async Task A_re_forward_is_foreign_to_every_forwarder_on_its_chain()
    {
        // ReforwardOmitsImmediateForwarder and ReforwardOmitsIntermediateForwarder (F2').
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var second = new Guid("eeeeeeee-0000-0000-0000-00000000000e");
        var third = new Guid("ffffffff-0000-0000-0000-00000000000f");

        var reforwarded = await grain.AppendStampedAsync(
            MakeEntry("moved"),
            ForwardedStamp(ActivationA, NarrowTagOne) with
            {
                Forwarder2 = second,
                ForwarderTag2 = NarrowTagTwo,
                Forwarder3 = third,
                ForwarderTag3 = NarrowTagThree,
            },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.ForeignHighForTesting(Forwarder, ActivationB), Is.EqualTo(reforwarded.Offset), "the first forwarder was not attributed");
            Assert.That(grain.ForeignHighForTesting(second, ActivationB), Is.EqualTo(reforwarded.Offset), "an intermediate forwarder was not attributed");
            Assert.That(grain.ForeignHighForTesting(third, ActivationB), Is.EqualTo(reforwarded.Offset), "the immediate forwarder was not attributed");
            Assert.That(grain.ForeignHighForTesting(second, ActivationB, NarrowTagTwo), Is.LessThan(reforwarded.Offset),
                "a hop's own narrow was not excluded for that hop (X1 per hop)");
            Assert.That(grain.ForeignHighForTesting(second, ActivationB, NarrowTagOne), Is.EqualTo(reforwarded.Offset),
                "a hop excluded another hop's narrow");
        });
    }

    [Test]
    public async Task A_displaced_narrow_tag_folds_into_a_high_no_reader_excludes()
    {
        // ForwardTagEvictionDropsHigh (X6).
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var first = await grain.AppendStampedAsync(MakeEntry("a"), ForwardedStamp(ActivationA, NarrowTagOne), CancellationToken.None);
        await grain.AppendStampedAsync(MakeEntry("b"), ForwardedStamp(ActivationA, NarrowTagTwo), CancellationToken.None);
        await grain.AppendStampedAsync(MakeEntry("c"), ForwardedStamp(ActivationA, NarrowTagThree), CancellationToken.None);

        Assert.That(grain.ForeignHighForTesting(Forwarder, ActivationB, NarrowTagOne), Is.GreaterThanOrEqualTo(first.Offset),
            "a displaced tag's forward was dropped rather than counted for every reader");
    }

    [Test]
    public async Task An_evicted_forwarder_floors_to_its_highest_forward()
    {
        // EvictionDropsForwardHigh: the floor takes an evicted entry's forwards, not only its activations.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        await grain.AppendStampedAsync(MakeEntry("own"), WalAppendStamp.PreArm(Forwarder, ActivationB) with { Initiator = PreArmRoot, Generation = 1 }, CancellationToken.None);
        var forwarded = await grain.AppendStampedAsync(MakeEntry("moved"), ForwardedStamp(ActivationA, NarrowTagOne), CancellationToken.None);

        grain.FoldLeafAuthorsForTesting();

        Assert.That(grain.ForeignHighForTesting(Forwarder, ActivationB, NarrowTagOne), Is.GreaterThanOrEqualTo(forwarded.Offset),
            "an eviction dropped the forwarder's forward high");
    }

    [Test]
    public async Task A_restart_floors_every_forward_below_the_recovered_head()
    {
        // ForwardTableLostOnRestart (X8).
        var storage = new EnumerableMemoryGrainStorage();
        var provider = new InMemoryWalStorageProvider();
        var grain = await CreateFencedGrainAsync(storage, provider);
        var forwarded = await grain.AppendStampedAsync(MakeEntry("moved"), ForwardedStamp(ActivationA, NarrowTagOne), CancellationToken.None);

        var restarted = await CreateFencedGrainAsync(storage, provider);

        Assert.That(restarted.ForeignHighForTesting(Forwarder, ActivationB, NarrowTagOne), Is.GreaterThanOrEqualTo(forwarded.Offset),
            "a restart forgot a forward its reader's tag would have excluded");
    }
}
