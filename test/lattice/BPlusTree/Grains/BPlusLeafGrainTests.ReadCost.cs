using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    [Category("Performance")]
    [NonParallelizable]
    [Explicit("Local cost attribution; no timing threshold in CI.")]
    public async Task Measure_raw_and_versioned_read_costs()
    {
        var grain = CreateGrain();
        await grain.SetAsync("key", new byte[128]);
        var mutationCount = typeof(BPlusLeafGrain).GetField("_leafRoutingMutationsInFlight",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var bytesSerializer = services.GetRequiredService<Serializer<byte[]>>();
        var versionSerializer = services.GetRequiredService<Serializer<VersionedValue>>();
        var raw = (await grain.GetAsync("key"))!;
        var proved = await grain.GetWithVersionAsync("key");
        var unproved = proved with { LeafRoutingEpoch = default, LeafRoutingGeneration = 0 };
        TestContext.Out.WriteLine($"COST bytes raw={bytesSerializer.SerializeToArray(raw).Length} " +
            $"versioned={versionSerializer.SerializeToArray(unproved).Length} " +
            $"proved={versionSerializer.SerializeToArray(proved).Length}");
        var id = GrainId.Create("leaf", "cost");
        var stamp = (proved.LeafRoutingEpoch, proved.LeafRoutingGeneration);
        var stamps = new Dictionary<GrainId, (Guid, long)> { [id] = stamp };
        long matches = 0;
        for (var repeat = 0; repeat < 3; repeat++)
        {
            MeasureReadCost("leaf-raw", () => GC.KeepAlive(grain.GetAsync("key").GetAwaiter().GetResult()));
            mutationCount.SetValue(grain, 1);
            try
            {
                MeasureReadCost("leaf-versioned-proof-suppressed",
                    () => GC.KeepAlive(grain.GetWithVersionAsync("key").GetAwaiter().GetResult()));
            }
            finally
            {
                mutationCount.SetValue(grain, 0);
            }
            MeasureReadCost("leaf-versioned-proved",
                () => GC.KeepAlive(grain.GetWithVersionAsync("key").GetAwaiter().GetResult()));
            MeasureReadCost("serialize-raw", () => GC.KeepAlive(bytesSerializer.SerializeToArray(raw)));
            MeasureReadCost("serialize-versioned", () => GC.KeepAlive(versionSerializer.SerializeToArray(unproved)));
            MeasureReadCost("serialize-proved", () => GC.KeepAlive(versionSerializer.SerializeToArray(proved)));
            MeasureReadCost("root-stamp-lookup-compare", () =>
            {
                if (stamps.TryGetValue(id, out var expected) && expected == stamp) matches++;
            });
        }
        Assert.That(matches, Is.GreaterThan(0));
    }

    private static void MeasureReadCost(string label, Action action)
    {
        const int iterations = 100_000;
        for (var i = 0; i < 10_000; i++) action();
        var allocated = GC.GetAllocatedBytesForCurrentThread();
        var timer = Stopwatch.StartNew();
        for (var i = 0; i < iterations; i++) action();
        timer.Stop();
        var bytes = GC.GetAllocatedBytesForCurrentThread() - allocated;
        TestContext.Out.WriteLine($"COST {label} ns={timer.Elapsed.TotalNanoseconds / iterations:F1} " +
            $"allocatedBytes={bytes / (double)iterations:F1}");
    }
}
