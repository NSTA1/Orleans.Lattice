using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Covers the <b>tree-level driver</b> half of the orphaned-leaf verbs -
/// <c>LatticeGrain.DriveOrphanedLeafPassAsync</c> in
/// <c>LatticeGrain.OrphanRepair.cs</c> - which pumps each shard's work-bounded
/// pages and reduces them into one report.
/// <para>
/// The shard-root half is pinned by <c>ShardRootGrainOrphanAuditGapTests</c>.
/// What had no fixture was the reduction: a shard that cannot establish a
/// verdict over part of its keyspace reports that on the page, and if the
/// driver drops it the whole defect of issue 3301 reappears one layer up -
/// an operator reading a report with no findings and no gaps concludes the
/// tree is clean when in fact no verdict was ever reached.
/// </para>
/// </summary>
[TestFixture]
public class LatticeGrainOrphanedLeafDriverTests
{
    private const string TreeId = "orphaned-leaf-driver-tree";

    private static (LatticeGrain Grain, IShardRootGrain Shard) CreateGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>())
            .Returns(Task.FromResult<ShardMap?>(new ShardMap { Slots = new int[8], Version = 1 }));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }));

        var shard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shard);

        var grain = new LatticeGrain(
            context,
            grainFactory,
            optionsMonitor,
            TestOptionsResolver.ForFactory(grainFactory),
            Substitute.For<IServiceProvider>(),
            NullLogger<LatticeGrain>.Instance);

        return (grain, shard);
    }

    private static OrphanedLeafAuditGap Gap(OrphanedLeafAuditGapReason reason, string leafId) => new()
    {
        ShardIndex = 0,
        Reason = reason,
        LeafId = leafId,
        KeyHint = leafId,
    };

    /// <summary>
    /// The reduction must carry every page's gaps out to the caller. Dropping
    /// them turns a shard that declined to judge into a shard that reported
    /// nothing, which is the exact false-green issue 3301 is about.
    /// </summary>
    [Test]
    public async Task InspectOrphanedLeavesAsync_carries_every_page_gap_into_the_report()
    {
        var (grain, shard) = CreateGrain();

        var pages = 0;
        shard.RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                pages++;
                return new OrphanedLeafRepairPage
                {
                    LeavesWalked = 3,
                    Gaps = [Gap(OrphanedLeafAuditGapReason.ChainTruncated, $"leaf-{pages}")],
                    ResumeFromInclusive = pages < 2 ? "k1" : null,
                };
            });

        var report = await grain.InspectOrphanedLeavesAsync(null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Gaps, Has.Count.EqualTo(2),
                "a gap raised on any page must survive the reduction; dropping one silently "
                + "narrows the report back to the finding list that issue 3301 showed is empty "
                + "on precisely the trees that are broken");
            Assert.That(report.Gaps.Select(g => g.LeafId), Is.EquivalentTo(new[] { "leaf-1", "leaf-2" }));
            Assert.That(report.VerdictComplete, Is.False,
                "a report carrying a gap is not a complete verdict, whatever its finding count");
            Assert.That(report.Findings, Is.Empty);
        });
    }

    /// <summary>
    /// The negative control for the test above: a pass that establishes a
    /// verdict everywhere must report a complete one, so the fixture cannot be
    /// passing by asserting gaps that are always present.
    /// </summary>
    [Test]
    public async Task InspectOrphanedLeavesAsync_reports_a_complete_verdict_when_no_shard_declined()
    {
        var (grain, shard) = CreateGrain();

        shard.RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => new OrphanedLeafRepairPage { LeavesWalked = 7 });

        var report = await grain.InspectOrphanedLeavesAsync(null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Gaps, Is.Empty);
            Assert.That(report.VerdictComplete, Is.True,
                "a walk that reached every leaf and declined nowhere is the one case in which "
                + "an empty finding list really is a clean bill of health");
            Assert.That(report.LeavesWalked, Is.EqualTo(7));
            Assert.That(report.DryRun, Is.True);
        });
    }

    /// <summary>
    /// A shard that declines the whole pass returns a page with no resume
    /// position, which is indistinguishable on the cursor alone from a shard
    /// that finished. Only the gap tells the two apart, so the repair verb has
    /// to carry it too - an operator who runs the repair and gets a clean
    /// report would otherwise believe the shard was reached.
    /// </summary>
    [Test]
    public async Task RepairOrphanedLeavesAsync_carries_a_whole_shard_decline_out_as_a_gap()
    {
        var (grain, shard) = CreateGrain();

        shard.RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => OrphanedLeafRepairPage.Declined(
                0, OrphanedLeafAuditGapReason.ShardSplitInProgress));

        var report = await grain.RepairOrphanedLeavesAsync(null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.VerdictComplete, Is.False);
            Assert.That(report.Gaps.Select(g => g.Reason),
                Is.EquivalentTo(new[] { OrphanedLeafAuditGapReason.ShardSplitInProgress }));
            Assert.That(report.DryRun, Is.False);
        });
    }
}
