using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4534: every full export takes a fresh, durable, strictly greater
/// export epoch, which a receiver echoes so a sender knows the re-seed it
/// waited for came from an export taken after it took the receiver off the log.
/// </summary>
public partial class BootstrapAtomicVisibilityTests
{
    [Test]
    public async Task Every_full_export_takes_a_strictly_greater_export_epoch()
    {
        const string tree = "snap-export-epoch";
        await _cluster.Client.GetGrain<ILattice>(tree).SetAsync("k", new byte[] { 1 });

        var first = await _provider.ExportAsync(tree, HybridLogicalClock.Zero);
        var second = await _provider.ExportAsync(tree, HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(first.ExportEpoch, Is.GreaterThan(0));
            Assert.That(second.ExportEpoch, Is.GreaterThan(first.ExportEpoch));
        });
    }
}
