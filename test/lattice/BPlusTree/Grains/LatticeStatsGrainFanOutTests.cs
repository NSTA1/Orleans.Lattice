using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the three arms of <c>LatticeStatsGrain</c>'s per-shard
/// diagnostics fan-out that the single-page happy path never enters: the
/// multi-batch drain loop, the cancellation rethrow, and the per-shard fault
/// containment arm.
/// <para>
/// The shard-side leaf walk is work-bounded (issue 1972), so a wide shard
/// answers <c>GetDiagnosticsBoundedAsync</c> with a partial page plus a resume
/// cursor rather than head-of-line-blocking the shard for the whole leaf chain.
/// The stats grain is what stitches those batches back together, and its sums
/// are the numbers an operator reads - so a drain that stopped after the first
/// batch would silently under-report every wide shard rather than fail.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeStatsGrainFanOutTests
{
    private const string TreeId = "stats-fanout-tree";

    private static (LatticeStatsGrain Grain, Dictionary<int, IShardRootGrain> Shards) CreateGrain(
        int physicalShardCount = 1,
        LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("stats", TreeId));

        var factory = Substitute.For<IGrainFactory>();

        var lattice = Substitute.For<ILattice>();
        var map = ShardMap.CreateDefault(16, physicalShardCount);
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>()).Returns(new RoutingInfo(TreeId, map));
        factory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var shards = new Dictionary<int, IShardRootGrain>();
        for (var i = 0; i < physicalShardCount; i++)
        {
            var shard = Substitute.For<IShardRootGrain>();
            shards[i] = shard;
            factory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Returns(shard);
        }

        var resolver = TestOptionsResolver.ForFactory(factory, options ?? new LatticeOptions());
        var grain = new LatticeStatsGrain(context, factory, resolver, NullLogger<LatticeStatsGrain>.Instance);
        return (grain, shards);
    }

    private static ShardDiagnosticsPage Page(long liveKeys, long tombstones, string? resumeFrom) => new()
    {
        Report = new ShardDiagnosticReport
        {
            Depth = 2,
            RootIsLeaf = false,
            LiveKeys = liveKeys,
            Tombstones = tombstones,
        },
        ResumeFromInclusive = resumeFrom,
    };

    [Test]
    public async Task A_multi_batch_shard_walk_sums_every_batch_not_just_the_first()
    {
        var (grain, shards) = CreateGrain();
        shards[0].GetDiagnosticsBoundedAsync(false, null).Returns(Page(10, 1, "k100"));
        shards[0].GetDiagnosticsBoundedAsync(false, "k100").Returns(Page(20, 2, "k200"));
        shards[0].GetDiagnosticsBoundedAsync(false, "k200").Returns(Page(30, 3, null));

        var report = await grain.GetReportAsync(deep: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            // 10+20+30 and 1+2+3. Stopping after the first batch would report 10/1,
            // which is exactly the silent under-count this drain exists to prevent.
            Assert.That(report.TotalLiveKeys, Is.EqualTo(60));
            Assert.That(report.TotalTombstones, Is.EqualTo(6));
        });

        // Every batch must actually have been requested, so the sums above cannot
        // be satisfied by a single fat page.
        await shards[0].Received(1).GetDiagnosticsBoundedAsync(false, null);
        await shards[0].Received(1).GetDiagnosticsBoundedAsync(false, "k100");
        await shards[0].Received(1).GetDiagnosticsBoundedAsync(false, "k200");
    }

    [Test]
    public async Task A_multi_batch_shard_walk_recomputes_the_tombstone_ratio_over_the_whole_chain()
    {
        var (grain, shards) = CreateGrain();
        shards[0].GetDiagnosticsBoundedAsync(true, null).Returns(Page(30, 10, "k1"));
        shards[0].GetDiagnosticsBoundedAsync(true, "k1").Returns(Page(50, 10, null));

        var report = await grain.GetReportAsync(deep: true, CancellationToken.None);
        var shard = report.Shards.Single();

        Assert.Multiple(() =>
        {
            Assert.That(shard.LiveKeys, Is.EqualTo(80));
            Assert.That(shard.Tombstones, Is.EqualTo(20));
            // 20 / (80 + 20). The first page's own ratio was 10/40 = 0.25, so a
            // ratio carried over from the first batch would read 0.25 here.
            Assert.That(shard.TombstoneRatio, Is.EqualTo(0.2).Within(1e-9));
            Assert.That(shard.Depth, Is.EqualTo(2), "the first page's structural fields are retained");
        });
    }

    [Test]
    public async Task An_empty_shard_reports_a_zero_tombstone_ratio_rather_than_dividing_by_zero()
    {
        var (grain, shards) = CreateGrain();
        shards[0].GetDiagnosticsBoundedAsync(false, null).Returns(Page(0, 0, null));

        var report = await grain.GetReportAsync(deep: false, CancellationToken.None);

        Assert.That(report.Shards.Single().TombstoneRatio, Is.EqualTo(0.0));
    }

    [Test]
    public void Cancellation_between_batches_propagates_rather_than_being_reported_as_an_empty_shard()
    {
        var (grain, shards) = CreateGrain();
        using var cts = new CancellationTokenSource();

        // Cancel as the first batch is served, so the drain loop's own
        // ThrowIfCancellationRequested is what observes it.
        shards[0].GetDiagnosticsBoundedAsync(false, null).Returns(_ =>
        {
            cts.Cancel();
            return Page(10, 0, "k100");
        });

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await grain.GetReportAsync(deep: false, cts.Token));
    }

    [Test]
    public async Task Cancellation_between_batches_is_not_swallowed_into_a_zeroed_report()
    {
        var (grain, shards) = CreateGrain();
        using var cts = new CancellationTokenSource();
        shards[0].GetDiagnosticsBoundedAsync(false, null).Returns(_ =>
        {
            cts.Cancel();
            return Page(10, 0, "k100");
        });

        try
        {
            await grain.GetReportAsync(deep: false, cts.Token);
            Assert.Fail("a cancelled diagnostics drain must not return a report");
        }
        catch (OperationCanceledException)
        {
            // The control that makes the assertion above meaningful: the drain
            // really did start, so the throw is the cancellation arm and not a
            // pre-flight guard that never reached the fan-out.
            await shards[0].Received(1).GetDiagnosticsBoundedAsync(false, null);
        }
    }

    [Test]
    public async Task A_failing_shard_is_contained_and_reported_as_an_empty_shard()
    {
        var (grain, shards) = CreateGrain(physicalShardCount: 2);
        shards[0].GetDiagnosticsBoundedAsync(Arg.Any<bool>(), Arg.Any<string?>())
            .Returns<ShardDiagnosticsPage>(_ => throw new InvalidOperationException("shard is offline"));
        shards[1].GetDiagnosticsBoundedAsync(false, null).Returns(Page(42, 7, null));

        var report = await grain.GetReportAsync(deep: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.ShardCount, Is.EqualTo(2), "a failed shard still occupies a slot in the report");
            Assert.That(report.Shards[0].ShardIndex, Is.EqualTo(0));
            Assert.That(report.Shards[0].LiveKeys, Is.Zero);
            Assert.That(report.Shards[0].Tombstones, Is.Zero);

            // The healthy shard must still be reported in full: a fan-out that
            // failed whole rather than per-shard would lose these.
            Assert.That(report.Shards[1].LiveKeys, Is.EqualTo(42));
            Assert.That(report.TotalLiveKeys, Is.EqualTo(42));
            Assert.That(report.TotalTombstones, Is.EqualTo(7));
        });
    }

    [Test]
    public async Task A_shard_that_fails_midway_through_its_drain_is_contained()
    {
        var (grain, shards) = CreateGrain();
        shards[0].GetDiagnosticsBoundedAsync(false, null).Returns(Page(10, 0, "k100"));
        shards[0].GetDiagnosticsBoundedAsync(false, "k100")
            .Returns<ShardDiagnosticsPage>(_ => throw new TimeoutException("batch timed out"));

        var report = await grain.GetReportAsync(deep: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Partial batch totals are discarded rather than reported as a
            // complete answer, so a truncated walk cannot masquerade as a shrunk
            // shard.
            Assert.That(report.Shards.Single().LiveKeys, Is.Zero);
            Assert.That(report.TotalLiveKeys, Is.Zero);
        });
    }
}
