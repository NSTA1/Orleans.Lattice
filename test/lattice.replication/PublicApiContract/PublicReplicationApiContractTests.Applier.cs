using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the <see cref="IReplicationApplier"/> public contract:
/// applying a single <see cref="WalRecord"/> returns
/// <see cref="ApplyResult.Applied"/> = true with the entry's HLC as
/// the high-water-mark, re-delivering the same entry returns
/// <see cref="ApplyResult.Applied"/> = false with the same HWM, and
/// <see cref="IReplicationApplier.ApplyBatchAsync"/> aggregates the
/// HWM across a batch.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    private static WalRecord BuildSetRecord(string treeId, string key, string value, HybridLogicalClock hlc, string originClusterId) =>
        new()
        {
            TreeId = treeId,
            Op = MutationKind.Set,
            Key = key,
            Value = System.Text.Encoding.UTF8.GetBytes(value),
            Timestamp = hlc,
            OriginClusterId = originClusterId,
        };

    [Test]
    public async Task IReplicationApplier_applyAsync_first_delivery_returns_applied_with_entry_hlc()
    {
        var treeId = NextTreeId("applier-first");
        await CreateReplicatedTreeAsync(treeId);
        var applier = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<IReplicationApplier>();

        var hlc = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 7 };
        var record = BuildSetRecord(treeId, "k", "v", hlc, PublicReplicationApiClusterFixture.SiteAClusterId);

        var result = await applier.ApplyAsync(record);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(hlc));
        });
    }

    [Test]
    public async Task IReplicationApplier_applyAsync_re_delivery_returns_not_applied_with_existing_hwm()
    {
        var treeId = NextTreeId("applier-re-deliver");
        await CreateReplicatedTreeAsync(treeId);
        var applier = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<IReplicationApplier>();

        var hlc = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 11 };
        var first = BuildSetRecord(treeId, "k", "v1", hlc, PublicReplicationApiClusterFixture.SiteAClusterId);

        var firstResult = await applier.ApplyAsync(first);
        Assert.That(firstResult.Applied, Is.True);

        var redeliver = BuildSetRecord(treeId, "k", "v1-again", hlc, PublicReplicationApiClusterFixture.SiteAClusterId);
        var secondResult = await applier.ApplyAsync(redeliver);

        Assert.Multiple(() =>
        {
            Assert.That(secondResult.Applied, Is.False, "Re-delivery at-or-below the HWM is filtered.");
            Assert.That(secondResult.HighWaterMark, Is.EqualTo(hlc));
        });
    }

    [Test]
    public async Task IReplicationApplier_applyBatchAsync_returns_pointwise_max_high_water_mark()
    {
        var treeId = NextTreeId("applier-batch");
        await CreateReplicatedTreeAsync(treeId);
        var applier = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<IReplicationApplier>();

        var origin = PublicReplicationApiClusterFixture.SiteAClusterId;
        var baseTicks = DateTime.UtcNow.Ticks;
        var batch = new[]
        {
            BuildSetRecord(treeId, "a", "1", new HybridLogicalClock { WallClockTicks = baseTicks, Counter = 1 }, origin),
            BuildSetRecord(treeId, "b", "2", new HybridLogicalClock { WallClockTicks = baseTicks, Counter = 2 }, origin),
            BuildSetRecord(treeId, "c", "3", new HybridLogicalClock { WallClockTicks = baseTicks, Counter = 5 }, origin),
        };
        var expectedMax = batch[^1].Timestamp;

        var result = await applier.ApplyBatchAsync(batch);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(expectedMax));
        });
    }
}
