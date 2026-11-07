using System.Collections.Concurrent;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// The shadow build stamps every shard bulk load with the registration it runs
/// under (issue #5813), so a load still in flight after its restore gave up is
/// refused by a purged shard re-created under a later registration of the same
/// deterministic shadow id. The stamp is recorded where the shard root reads it,
/// by an incoming call filter on the silo.
/// </summary>
public sealed partial class LatticeBackupCoordinatedRestoreEngineTests
{
    [Test]
    public async Task BuildShadowAsync_stamps_every_shard_bulk_load_with_the_shadow_registration_lineage()
    {
        BulkLoadStampRecorder.Observed.Clear();
        await _fixture.InitializeAsync(builder => builder.AddSiloBuilderConfigurator<BulkLoadStampRecorder>());
        var backupId = await SeedAndCaptureAsync("stamp", ("k1", "v1"), ("k2", "v2"), ("k3", "v3"));

        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backupId, "orders-stamp", scope: null, mode: LatticeRestoreMode.ShadowCutover));

        var shadowTreeId = shadow.ShadowPhysicalTreeId!;
        var registered = await _fixture.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetEntryAsync(shadowTreeId);
        var loads = BulkLoadStampRecorder.Observed.Where(o => o.TreeId == shadowTreeId).ToList();

        Assert.That(registered?.Lineage, Is.Not.Null);
        Assert.That(loads, Is.Not.Empty, "a single full backup restores through the shard bulk-load path");
        Assert.That(loads.Select(o => o.Lineage), Is.All.EqualTo(registered!.Lineage),
            "every shard bulk load must carry the shadow registration's lineage for its own tree");
    }

    /// <summary>
    /// Records, for every <c>IShardRootGrain.BulkLoadRawAsync</c> a silo receives,
    /// the shard's tree and the registration lineage the call is stamped with for it.
    /// </summary>
    internal sealed class BulkLoadStampRecorder : ISiloConfigurator, IIncomingGrainCallFilter
    {
        public static readonly ConcurrentQueue<(string TreeId, Guid? Lineage)> Observed = new();

        public void Configure(ISiloBuilder siloBuilder) =>
            siloBuilder.AddIncomingGrainCallFilter<BulkLoadStampRecorder>();

        public Task Invoke(IIncomingGrainCallContext context)
        {
            if (context.InterfaceMethod?.DeclaringType == typeof(IShardRootGrain)
                && context.InterfaceMethod.Name == nameof(IShardRootGrain.BulkLoadRawAsync))
            {
                var key = context.TargetId.Key.ToString()!;
                var treeId = key[..key.LastIndexOf('/')];
                Observed.Enqueue((treeId, BulkWriteRegistrationLineage.For(treeId)));
            }

            return context.Invoke();
        }
    }
}
