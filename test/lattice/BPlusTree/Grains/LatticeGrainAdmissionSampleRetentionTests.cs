using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6017, write admission: a partial storage-usage report is a lower
/// bound, so it must never displace a complete sample the activation already
/// holds - otherwise a refused WAL scan lowers the figure the per-tree caps are
/// compared against and admits writes past them.
/// </summary>
[TestFixture]
public sealed class LatticeGrainAdmissionSampleRetentionTests
{
    private const string TreeId = "adm-retain";

    [SetUp]
    public void Reset() => LatticeIdempotencyContext.Current = null;

    [TearDown]
    public void Clear() => LatticeIdempotencyContext.Current = null;

    [Test]
    public void SetAsync_after_a_partial_report_keeps_enforcing_the_previous_complete_sample()
    {
        var reports = new Queue<TreeStorageUsageReport>(
        [
            new TreeStorageUsageReport { TreeId = TreeId, LiveKeys = 10, TotalBytes = 1_000 },
            new TreeStorageUsageReport { TreeId = TreeId, LiveKeys = 0, TotalBytes = 0, Partial = true },
        ]);
        var grain = CreateGrain(reports);

        Assert.ThrowsAsync<LatticeQuotaExceededException>(() => grain.SetAsync("k1", [1]),
            "the complete sample is over the cap");
        Assert.ThrowsAsync<LatticeQuotaExceededException>(() => grain.SetAsync("k2", [1]),
            "a partial report must not lower the sample the cap is compared against");
        Assert.That(reports, Is.Empty, "both reports were read");
    }

    [Test]
    public async Task SetAsync_with_only_a_partial_report_uses_it_as_before()
    {
        var reports = new Queue<TreeStorageUsageReport>(
        [
            new TreeStorageUsageReport { TreeId = TreeId, LiveKeys = 0, Partial = true },
        ]);
        var grain = CreateGrain(reports);

        await grain.SetAsync("k1", [1]);

        Assert.That(reports, Is.Empty);
    }

    private static LatticeGrain CreateGrain(Queue<TreeStorageUsageReport> reports)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var options = new LatticeOptions { MaxLiveKeys = 5, StorageUsageCacheTtl = TimeSpan.Zero };
        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.RouteEntriesThroughAliasAndMapStubs(_ => new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 });

        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        var usage = Substitute.For<ILatticeStorageUsage>();
        usage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(reports.Dequeue()));
        grainFactory.GetGrain<ILatticeStorageUsage>(Arg.Is(TreeId), Arg.Any<string>()).Returns(usage);

        var services = Substitute.For<IServiceProvider>();
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, options);
        return new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
    }
}
