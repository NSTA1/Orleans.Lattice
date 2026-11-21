using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5548: every pass of one export reads the physical copy the export
/// pinned when it opened. The tombstone and prepared passes used to re-resolve
/// the tree through the registry, so an alias moved after the key pass made
/// them read the new copy while the key pass had read the old one. A move met
/// in any pass must defer the export transiently instead.
/// </summary>
public partial class BootstrapAtomicVisibilityTests
{
    [Test]
    public async Task Export_whose_alias_moves_before_the_tombstone_pass_defers_instead_of_reading_the_new_copy()
    {
        const string tree = "snap-redirect-before-tombstones";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        for (var i = 0; i < 8; i++)
        {
            await lattice.SetAsync($"k{i:D3}", new byte[] { (byte)i });
        }

        await lattice.DeleteAsync("k000");

        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var resize = _cluster.Client.GetGrain<ITreeResizeGrain>(tree);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();
        var pinned = await registry.ResolveAsync(tree);
        Assert.That(pinned, Is.Not.EqualTo(tree), "precondition: the tree is aliased to a copy");

        var service = new LatticeRemoteSnapshotService(
            _provider,
            new StubReplicationContext(ClusterId, LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        // The key pass has finished on the pinned copy; the alias moves before
        // the tombstone pass reads anything.
        var moved = false;
        _provider.BeforeTombstonePassForTesting = async () =>
        {
            moved = true;
            await resize.ResizeAsync(32, 32);
            await resize.RunResizePassAsync();
        };

        Exception? fault = null;
        try
        {
            await foreach (var _ in service.RequestSnapshotItemsAsync(tree, ClusterId, HybridLogicalClock.Zero))
            {
            }
        }
        catch (Exception ex)
        {
            fault = ex;
        }
        finally
        {
            _provider.BeforeTombstonePassForTesting = null;
        }

        var current = await registry.ResolveAsync(tree);
        Assert.Multiple(() =>
        {
            Assert.That(moved, Is.True, "precondition: the alias moved between the key and tombstone passes");
            Assert.That(current, Is.Not.EqualTo(pinned), "precondition: the alias moved off the pinned copy");
            Assert.That(fault, Is.InstanceOf<LatticeSnapshotExportDeferredException>(),
                $"a move before the tombstone pass must defer the export, not read the new copy: {fault}");
            Assert.That(fault is not null && LatticeBootstrapTransientFaultClassifier.IsTransient(fault), Is.True,
                "the receiver's bootstrap must classify the deferral as retryable");
        });
    }
}
