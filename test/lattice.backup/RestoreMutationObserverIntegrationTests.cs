using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

[TestFixture]
[Category("Integration")]
public sealed class RestoreMutationObserverIntegrationTests
{
    [Test]
    public async Task Writes_after_shadow_restore_publish_logical_identity()
    {
        var fixture = new RestoreClusterFixture();
        await fixture.InitializeAsync(builder => builder.AddSiloBuilderConfigurator<ObserverConfigurator>());
        try
        {
            const string treeId = "restore-observer";
            var tree = fixture.GrainFactory.GetGrain<ILattice>(treeId);
            await tree.SetAsync("seed", [1]);
            var backup = await fixture.Capture.CaptureAsync(
                new LatticeBackupCaptureRequest("observer", BackupScopeSelector.WholeTree(treeId)));
            await fixture.Restore.RestoreAsync(
                new LatticeRestoreRequest(backup.BackupId, treeId, mode: LatticeRestoreMode.ShadowCutover));
            Assert.That(await fixture.GrainFactory.GetLatticeRegistry().ResolveAsync(treeId), Is.Not.EqualTo(treeId));
            var observer = fixture.SiloServices.GetRequiredService<Observer>();
            observer.Mutations.Clear();

            await tree.SetAsync("point", [2]);
            await tree.PnCounter("counter").IncrementAsync("local");
            await tree.SetManyAsync([new("many1", [3]), new("many2", [4])]);
            await tree.SetManyAtomicAsync([new("atomic1", [5]), new("atomic2", [6])]);
            await tree.DeleteAsync("point");
            await tree.DeleteRangeAsync("many", "manz");
            var apply = fixture.GrainFactory.GetGrain<IReplicationApplyGrain>(treeId);
            await apply.ApplySetAsync("remote", [7],
                new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks }, "peer", null, 0);

            var mutations = observer.Mutations.ToArray();
            Assert.That(mutations.Where(m => m.Kind == MutationKind.Set).Select(m => m.Key),
                Is.EquivalentTo(new[] { "point", "counter", "many1", "many2", "atomic1", "atomic2", "remote" }));
            Assert.That(mutations.Any(m => m.Kind == MutationKind.Delete && m.Key == "point"), Is.True);
            Assert.That(mutations.Any(m => m.Kind == MutationKind.DeleteRange), Is.True);
            Assert.That(mutations.Select(m => m.TreeId), Is.All.EqualTo(treeId));
        }
        finally
        {
            await fixture.DisposeAsync();
        }
    }

    private sealed class ObserverConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.Services.AddSingleton<Observer>();
            siloBuilder.Services.AddSingleton<IMutationObserver>(services => services.GetRequiredService<Observer>());
        }
    }

    private sealed class Observer : IMutationObserver
    {
        public ConcurrentQueue<LatticeMutation> Mutations { get; } = new();

        public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
        {
            Mutations.Enqueue(mutation);
            return Task.CompletedTask;
        }
    }
}
