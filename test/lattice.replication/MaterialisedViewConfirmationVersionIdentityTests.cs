using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Source confirmation observes a completed batch's prepared revision through a
/// coherent source row and version read (issue #4882). Equal bytes at a newer
/// version are not the same revision: a pure projection may depend on timestamp.
/// These integration cases exercise an identity projection with newer equal-byte
/// sets and tombstones, followed by an older ordinary write. The timestamp-sensitive
/// reader-snapshot discriminator lives in ViewMaintainerReplayFlushTests.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewConfirmationVersionIdentityTests
{
    private const string Origin = "remote-origin";
    private const string OtherOrigin = "remote-origin-b";

    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    public enum Newer
    {
        SameBytesSet,
        DeleteBeforePreparedDelete,
        DeleteBeforePreparedSet,
    }

    private static byte[] Bytes(string value) => System.Text.Encoding.UTF8.GetBytes(value);

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static int ShardOf(string key) =>
        LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

    private async Task DrainToZeroAsync(string view)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail($"View '{view}' did not catch up to the source head.");
    }

    [Test]
    public async Task Content_confirmation_agrees_with_the_source_version([Values] Newer newer)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var tree = $"mv-cvi-{(int)newer}-src";
        var view = $"mv-cvi-{(int)newer}-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        _ = factory.Create(source, view, MaterialisedViewRuntimeProjectionProvider.DescriptorFor(
            new LatticeViewDefinition(view, new PredicateLatticeViewProjection())));
        await _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view).EnsureActiveAsync();

        const string key = "cvi-k";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        await apply.ApplySetAsync(key, Bytes("seed"), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        await DrainToZeroAsync(view);

        // The newer write is logged, and drained, before the older prepare.
        if (newer == Newer.SameBytesSet)
        {
            await apply.ApplySetAsync(key, Bytes("x"), Hlc(400), OtherOrigin, sourceVectorClock: null, expiresAtTicks: 0);
        }
        else
        {
            await apply.ApplyDeleteAsync(key, Hlc(400), OtherOrigin, sourceVectorClock: null);
        }

        await DrainToZeroAsync(view);

        var tx = Guid.NewGuid();
        if (newer == Newer.DeleteBeforePreparedDelete)
        {
            await apply.ApplyPreparedDeleteAsync(key, Hlc(100), Origin, sourceVectorClock: null, tx, atomicBatchSize: 1, atomicBatchIndex: 0);
        }
        else
        {
            await apply.ApplyPreparedSetAsync(key, Bytes("x"), Hlc(100), Origin, sourceVectorClock: null, expiresAtTicks: 0, tx, atomicBatchSize: 1, atomicBatchIndex: 0);
        }

        await apply.ApplyTxTerminalAsync(tx, committed: true, ShardOf(key), Hlc(101), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(view);

        // A later, older ordinary write: the source keeps its newer version, so the
        // view must too, whatever version the flushed prepare carried.
        await apply.ApplySetAsync(key, Bytes("older"), Hlc(150), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        await DrainToZeroAsync(view);

        var expected = newer == Newer.SameBytesSet ? Bytes("x") : null;
        var sourceValue = await source.GetAsync(key);
        var viewValue = await (await _fixture.ActiveViewTreeAsync(view)).GetAsync(key);
        await Assert.MultipleAsync(() =>
        {
            Assert.That(sourceValue, Is.EqualTo(expected), "The source keeps the newer write by last-writer-wins.");
            Assert.That(viewValue, Is.EqualTo(expected), "The view must agree with the source's newer version.");
            return Task.CompletedTask;
        });
    }
}