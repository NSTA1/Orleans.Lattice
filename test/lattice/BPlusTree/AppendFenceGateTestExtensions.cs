using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Waits for the append-fence gate (epic #5605) to open on every silo of a test
/// cluster. A bulk append reads the right edge and narrows the rightmost leaf
/// only while the gate is open, since those are leaf methods an older silo lacks;
/// a fixture that asserts on that behaviour waits for it first, so it never tests
/// the older path by accident while the silos' manifests are still arriving.
/// </summary>
internal static class AppendFenceGateTestExtensions
{
    /// <summary>
    /// Returns once every silo's <see cref="IWalAppendFenceGate"/> is open, failing
    /// the fixture if that takes longer than <paramref name="timeout"/>.
    /// </summary>
    public static async Task WaitForAppendFenceGateAsync(this TestCluster cluster, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        while (!cluster.Silos.All(s => ((InProcessSiloHandle)s).SiloHost.Services
                   .GetRequiredService<IWalAppendFenceGate>().IsOpen))
        {
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "the append-fence gate never opened on every silo");
            await Task.Delay(100);
        }
    }
}
