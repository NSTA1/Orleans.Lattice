using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The read-only report fetch (#5851 (2), a5e2e2ef's R1 and F1-F3): a shard returns the
/// bound a stamped append by the reader would carry back in the same turn - the shared
/// watermark, the reader's foreign high and the stale high of each close it carries -
/// through the same code, and attributes nothing.
/// </summary>
public partial class WalShardGrainTests
{
    [Test]
    public async Task ReportFetch_equals_the_report_an_append_by_the_same_reader_would_carry()
    {
        // R1: foreign, stale-for-a-carried-close and unstamped records all feed the report;
        // the fetch and an append immediately after it agree.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        await grain.AppendAsync(MakeEntry("u"), CancellationToken.None);
        await grain.AppendStampedAsync(MakeEntry("f"), PreArmStamp(ActivationB, 1), CancellationToken.None);
        await grain.AppendStampedAsync(MakeEntry("d"), WalAppendStamp.PreArm(Donor, OtherToken) with { Initiator = PreArmRoot, Generation = 1 }, CancellationToken.None);
        await grain.AppendStampedAsync(MakeEntry("o"), PreArmStamp(ActivationA, 1), CancellationToken.None);
        var close = new WalPendingClose(Donor, ClosingToken, PreArmRoot.Index, 1);

        var results = new List<(long Fetched, long Appended)>();
        foreach (var reader in new[]
                 {
                     PreArmStamp(ActivationA, 1),
                     PreArmStamp(ActivationA, 1) with { Close1 = close },
                     PreArmStamp(ActivationB, 1) with { Close2 = close },
                 })
        {
            var fetched = await grain.ReadLeafReportAsync(reader, CancellationToken.None);
            var appended = await grain.AppendStampedAsync(MakeEntry("r"), reader, CancellationToken.None);
            results.Add((fetched, appended.UnstampedWatermark));
        }

        Assert.Multiple(() =>
        {
            for (var i = 0; i < results.Count; i++)
            {
                Assert.That(results[i].Fetched, Is.EqualTo(results[i].Appended), $"reader {i}: the fetched report is not the append's");
            }

            Assert.That(results[1].Fetched, Is.GreaterThan(results[0].Fetched), "precondition: the carried close's stale high is in the report");
        });
    }

    [Test]
    public async Task ReportFetch_reports_the_shared_unstamped_watermark()
    {
        // R1: the shared watermark, not the key-filtered high, is in the report, as it is in
        // an append's response.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        await grain.AppendStampedAsync(MakeEntry("o"), PreArmStamp(ActivationA, 1), CancellationToken.None);
        var unstamped = await grain.AppendAsync(MakeEntry("u"), CancellationToken.None);

        var fetched = await grain.ReadLeafReportAsync(PreArmStamp(ActivationA, 1), CancellationToken.None);

        Assert.That(fetched, Is.EqualTo(unstamped), "the fetched report left out the shared unstamped watermark");
    }

    [Test]
    public async Task ReportFetch_reports_the_eviction_floor_of_a_folded_leaf()
    {
        // R1: a fold drops the leaf's authors; the fetched report keeps the floor.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var foreign = await grain.AppendStampedAsync(MakeEntry("f"), PreArmStamp(ActivationB, 1), CancellationToken.None);
        grain.FoldLeafAuthorsForTesting();

        var fetched = await grain.ReadLeafReportAsync(PreArmStamp(ActivationA, 1), CancellationToken.None);

        Assert.That(fetched, Is.GreaterThanOrEqualTo(foreign.Offset), "a folded leaf's foreign record fell out of the fetched report");
    }

    [Test]
    public async Task ReportFetch_attributes_nothing()
    {
        // F2: a fetch is a read. It records no author, so another activation's report is
        // unchanged by it, and it allocates no offset.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        await grain.AppendStampedAsync(MakeEntry("o"), PreArmStamp(ActivationA, 1), CancellationToken.None);
        var before = await grain.ReadLeafReportAsync(PreArmStamp(ActivationB, 1), CancellationToken.None);
        var headBefore = await grain.GetNextSequenceAsync(CancellationToken.None);

        await grain.ReadLeafReportAsync(PreArmStamp(ActivationA, 1), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.ReadLeafReportAsync(PreArmStamp(ActivationB, 1), CancellationToken.None), Is.EqualTo(before), "a fetch was attributed as a record");
            Assert.That(await grain.GetNextSequenceAsync(CancellationToken.None), Is.EqualTo(headBefore), "a fetch allocated an offset");
        });
    }

    [Test]
    public async Task ReportFetch_for_a_reader_naming_no_leaf_or_activation_reports_nothing()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        await grain.AppendStampedAsync(MakeEntry("f"), PreArmStamp(ActivationB, 1), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.ReadLeafReportAsync(default, CancellationToken.None), Is.EqualTo(-1L));
            Assert.That(await grain.ReadLeafReportAsync(new WalAppendStamp(FenceLeaf, 0, WalRoutingInitiator.None, 0), CancellationToken.None), Is.EqualTo(-1L));
        });
    }
}
