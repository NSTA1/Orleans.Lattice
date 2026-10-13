using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Source admission must resolve a prepared value, version and deletion kind from
/// the same logical decision observation. A recorded commit can precede terminal
/// application at every leaf; materialised metadata is then older than the logical
/// row and must not be combined with its bytes. These cases hold terminal delivery,
/// inspect the logical and raw seams directly, and require whole reader snapshots.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewDecisionBeforeTerminalTests
{
    private const string Origin = "remote-origin";

    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

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

    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    public async Task Committed_batch_logged_before_its_leaves_drain_confirms_against_the_decision(
        bool deleted, bool olderOrdinary)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var caseId = (deleted ? 1 : 0) + (olderOrdinary ? 2 : 0);
        var tree = $"mv-dbt-{caseId}-src";
        var view = $"mv-dbt-{caseId}-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var serverGrains = _fixture.SiloServices.GetRequiredService<IGrainFactory>();
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        _ = factory.Create(source, view, MaterialisedViewRuntimeProjectionProvider.DescriptorFor(
            new LatticeViewDefinition(view, new PredicateLatticeViewProjection())));
        await _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view).EnsureActiveAsync();

        const string a = "dbt-a";
        var b = Enumerable.Range(0, 64).Select(i => $"dbt-b{i}").First(k => ShardOf(k) != ShardOf(a));
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        await apply.ApplySetAsync(a, Bytes("a0"), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        await apply.ApplySetAsync(b, Bytes("b0"), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        await DrainToZeroAsync(view);

        var tx = Guid.NewGuid();
        await apply.ApplyPreparedSetAsync(a, Bytes("a1"), Hlc(200), Origin, sourceVectorClock: null, expiresAtTicks: 0, tx, atomicBatchSize: 2, atomicBatchIndex: 0);
        if (deleted)
        {
            await apply.ApplyPreparedDeleteAsync(b, Hlc(200), Origin, sourceVectorClock: null, tx, atomicBatchSize: 2, atomicBatchIndex: 1);
        }
        else
        {
            await apply.ApplyPreparedSetAsync(b, Bytes("b1"), Hlc(200), Origin, sourceVectorClock: null, expiresAtTicks: 0, tx, atomicBatchSize: 2, atomicBatchIndex: 1);
        }

        // Every leaf refuses the terminal, so it reaches the commit log while the
        // prepared entries stay undrained on both leaves.
        LeafTerminalFaultFilter.Arm(tx);
        try
        {
            await apply.ApplyTxTerminalAsync(tx, committed: true, ShardOf(a), Hlc(201), Origin, atomicShardCount: 2);
            Assert.ThrowsAsync(Is.InstanceOf<Exception>(), () =>
                apply.ApplyTxTerminalAsync(tx, committed: true, ShardOf(b), Hlc(201), Origin, atomicShardCount: 2));
            Assert.That(LeafTerminalFaultFilter.Failures(tx), Is.GreaterThan(0), "No leaf terminal apply was attempted.");

            if (olderOrdinary)
            {
                await apply.ApplySetAsync(a, Bytes("a-older"), Hlc(100), Origin, sourceVectorClock: null, expiresAtTicks: 0);
            }

            var routing = await source.GetRoutingAsync();
            var sourceA = serverGrains.GetGrain<IShardRootGrain>(
                $"{routing.PhysicalTreeId}/{routing.Map.Resolve(a)}");
            var sourceB = serverGrains.GetGrain<IShardRootGrain>(
                $"{routing.PhysicalTreeId}/{routing.Map.Resolve(b)}");
            var logicalA = (await sourceA.GetVisibleRawEntriesAsync([a]))[0];
            var logicalB = (await sourceB.GetVisibleRawEntriesAsync([b]))[0];
            var materialisedA = (await sourceA.GetRawEntriesIncludingTombstonesAsync([a]))[0];
            var materialisedB = (await sourceB.GetRawEntriesIncludingTombstonesAsync([b]))[0];
            Assert.Multiple(() =>
            {
                Assert.That(logicalA?.Value, Is.EqualTo(Bytes("a1")));
                Assert.That(logicalA?.Timestamp, Is.EqualTo(Hlc(200)));
                Assert.That(logicalA?.IsTombstone, Is.False);
                Assert.That(logicalB?.Value, Is.EqualTo(deleted ? null : Bytes("b1")));
                Assert.That(logicalB?.Timestamp, Is.EqualTo(Hlc(200)));
                Assert.That(logicalB?.IsTombstone, Is.EqualTo(deleted));
                Assert.That(materialisedA?.Timestamp, Is.EqualTo(Hlc(olderOrdinary ? 100 : 50)),
                    "The terminal must genuinely remain unapplied, not merely have one failed delivery.");
                Assert.That(materialisedB?.Timestamp, Is.EqualTo(Hlc(50)));
            });

            var sourceDuring = await source.GetManyAsync([a, b]);
            var viewTree = await _fixture.ActiveViewTreeAsync(view);
            var snapshots = new List<Dictionary<string, byte[]>> { await viewTree.GetManyAsync([a, b]) };
            var drain = DrainToZeroAsync(view);
            while (!drain.IsCompleted)
            {
                snapshots.Add(await viewTree.GetManyAsync([a, b]));
                await Task.Delay(1);
            }
            await drain;
            var viewDuring = await viewTree.GetManyAsync([a, b]);
            snapshots.Add(viewDuring);
            foreach (var snapshot in snapshots)
            {
                var old = snapshot.GetValueOrDefault(a)?.SequenceEqual(Bytes("a0")) == true
                    && snapshot.GetValueOrDefault(b)?.SequenceEqual(Bytes("b0")) == true;
                var current = snapshot.GetValueOrDefault(a)?.SequenceEqual(Bytes("a1")) == true
                    && (deleted ? !snapshot.ContainsKey(b) : snapshot.GetValueOrDefault(b)?.SequenceEqual(Bytes("b1")) == true);
                Assert.That(old || current, Is.True, "A held-terminal batch and any older ordinary write must never expose a partial slice.");
            }
            await Assert.MultipleAsync(() =>
            {
                Assert.That(sourceDuring.GetValueOrDefault(a), Is.EqualTo(Bytes("a1")), "The source resolves the undrained prepare by its recorded decision.");
                Assert.That(sourceDuring.GetValueOrDefault(b), Is.EqualTo(deleted ? null : Bytes("b1")), "The source resolves the undrained prepare by its recorded decision.");
                Assert.That(viewDuring.GetValueOrDefault(a), Is.EqualTo(Bytes("a1")), "The view must not treat the committed batch as superseded.");
                Assert.That(viewDuring.GetValueOrDefault(b), Is.EqualTo(deleted ? null : Bytes("b1")), "The view must not treat the committed batch as superseded.");
                return Task.CompletedTask;
            });
        }
        finally
        {
            LeafTerminalFaultFilter.Disarm(tx);
        }

        // Redelivery lets the leaves drain; the view stays on the committed batch.
        await apply.ApplyTxTerminalAsync(tx, committed: true, ShardOf(b), Hlc(201), Origin, atomicShardCount: 2);
        await DrainToZeroAsync(view);
        var viewAfter = await (await _fixture.ActiveViewTreeAsync(view)).GetManyAsync([a, b]);
        await Assert.MultipleAsync(() =>
        {
            Assert.That(viewAfter.GetValueOrDefault(a), Is.EqualTo(Bytes("a1")));
            Assert.That(viewAfter.GetValueOrDefault(b), Is.EqualTo(deleted ? null : Bytes("b1")));
            return Task.CompletedTask;
        });
    }
}