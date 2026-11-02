using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5537: a peer bootstrap's export pins the physical copy the tree
/// resolved to when it opened. A restore leaves the tree aliased to a copy, and
/// a later resize fences that copy and moves the alias while the export is
/// still streaming. The export must end with the transient deferral the
/// receiver's bootstrap retries, never a permanent fault, and the retried
/// export must read every key from the tree's current copy. Runs the real
/// export, resize coordinator and shard fences.
/// </summary>
public partial class BootstrapAtomicVisibilityTests
{
    [Test]
    public async Task Export_redirected_mid_stream_defers_transiently_and_a_fresh_export_reads_the_current_copy()
    {
        const string tree = "snap-redirect-mid-export";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        var expected = new SortedSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < 24; i++)
        {
            var key = $"k{i:D3}";
            await lattice.SetAsync(key, new byte[] { (byte)i });
            expected.Add(key);
        }

        // The tree is aliased to a copy first, as a shadow-cutover restore leaves it.
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

        // Once the export is streaming from the pinned copy, a resize fences that
        // copy and moves the alias, exactly as A8's resize did after C1's restore.
        var redirected = false;
        _provider.BeforeCommittedEntryForTesting = async _ =>
        {
            if (redirected)
            {
                return;
            }

            redirected = true;
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
            _provider.BeforeCommittedEntryForTesting = null;
        }

        var current = await registry.ResolveAsync(tree);
        Assert.Multiple(() =>
        {
            Assert.That(redirected, Is.True, "precondition: the resize ran while the export streamed");
            Assert.That(current, Is.Not.EqualTo(pinned), "precondition: the alias moved off the pinned copy");
            Assert.That(fault, Is.InstanceOf<LatticeSnapshotExportDeferredException>(),
                $"a redirect mid-export must defer the export, not fail it: {fault}");
            Assert.That(fault is not null && LatticeBootstrapTransientFaultClassifier.IsTransient(fault), Is.True,
                "the receiver's bootstrap must classify the redirect as retryable");
        });

        // The retry the receiver makes: a fresh export re-resolves the alias.
        var retried = new SortedSet<string>(StringComparer.Ordinal);
        await foreach (var item in service.RequestSnapshotItemsAsync(tree, ClusterId, HybridLogicalClock.Zero))
        {
            if (item.Entry.Key is { } key && !item.Entry.IsTombstone && !item.Entry.IsPrepared && !item.Entry.IsDecision)
            {
                retried.Add(key);
            }
        }

        Assert.That(retried, Is.EqualTo(expected), "the retried export reads every key from the current copy");
    }
}
