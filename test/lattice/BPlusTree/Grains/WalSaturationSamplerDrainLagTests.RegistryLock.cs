using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// The drain-lag pass must not hold the cursor registry's lock while it measures
// lag (issue #6166). Every leaf's cursor report takes that lock, and measuring a
// consumer reads the partition's append history under its own lock, contended with
// every WAL append; measured under the registry's lock during a bulk seed, the pass
// held it for most of each tick and blocked the reporting leaves' threads behind it.
public partial class WalSaturationSamplerDrainLagTests
{
    [Test]
    [NonParallelizable]
    public async Task The_drain_lag_pass_measures_lag_outside_the_cursor_registry_lock()
    {
        var registry = new InMemoryWalCursorRegistry();
        var now = DateTimeOffset.UtcNow;
        for (var partition = 0; partition < 4; partition++)
        {
            await registry.ReportCheckpointAsync(_treeId, LeafConsumerId($"leaf-{partition}"), At(now - TimeSpan.FromHours(1)), partition, 0);
            WalCommitLogWriter.RecordAppend(_treeId, partition, 1, now.UtcTicks - TimeSpan.FromSeconds(1).Ticks);
        }

        var sampler = CreateRegistrySampler(registry, now, NewSignal(), NullLogger<WalSaturationSampler>.Instance);
        var heldWhileMeasuring = new List<bool>();
        WalAppendTimeline.LagQueriedForTest = () => heldWhileMeasuring.Add(registry.IsLockHeldByCurrentThreadForTest);
        try
        {
            await sampler.SampleOnceAsync(CancellationToken.None);
        }
        finally
        {
            WalAppendTimeline.LagQueriedForTest = null;
        }

        Assert.That(heldWhileMeasuring, Has.Count.EqualTo(4), "precondition: every consumer's lag was measured");
        Assert.That(heldWhileMeasuring, Has.None.True, "no consumer's lag was measured under the registry's lock");
    }
}
