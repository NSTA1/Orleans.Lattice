using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins <see cref="LeafSnapshotHydrationAdmission.HydrationHeapAmplification"/>
/// against a measured read of a real <see cref="LeafSnapshotBlob"/> through both
/// paths that are live today (issue #2858): the binary <c>LGB1</c> path every
/// current build writes, and the legacy JSON path a blob written before issue
/// #2516 still takes, because <see cref="LatticeGrainStorageSerializer"/> routes
/// reads on the payload rather than on the type.
/// <para>
/// Every ratio is expressed against the <b>frame</b> length, which is the stored
/// figure the gate multiplies (<c>BPlusLeafGrain.MeasureSnapshotLoadBytes</c>),
/// not against the persisted document. The legacy JSON document is itself about
/// 4/3 of the frame, because the frame is base64-encoded inside it.
/// </para>
/// <para>
/// Two measurements, chosen so that each assertion can only err in the safe
/// direction. Total allocation on the reading thread
/// (<see cref="GC.GetAllocatedBytesForCurrentThread"/>) is deterministic and is
/// an <b>upper</b> bound on the peak live heap, so it backs every "is covered"
/// and "stays within" claim. The peak live heap is sampled with forced
/// collections while the read runs, which can only miss a peak and so is a
/// <b>lower</b> bound, so it backs the one "exceeds" claim.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class LeafSnapshotHydrationAmplificationTests
{
    private const int FrameBytes = 2 * 1024 * 1024;

    // Roughly how long one peak-heap sampling pass may take, however large the
    // process heap is. See SamplePeakLiveBytes.
    private static readonly TimeSpan SamplingBudget = TimeSpan.FromSeconds(10);

    private ServiceProvider provider = null!;
    private IGrainStorageSerializer json = null!;
    private LatticeGrainStorageSerializer lattice = null!;
    private byte[] binaryPayload = null!;
    private byte[] legacyJsonPayload = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddOptions();
        services.AddSingleton<OrleansJsonSerializer>();
        this.provider = services.BuildServiceProvider();

        // The real JSON serializer, which is what LatticeGrainStorageSerializer
        // falls back to on a silo: Orleans registers it before Lattice does.
        this.json = ActivatorUtilities.CreateInstance<JsonGrainStorageSerializer>(this.provider);
        this.lattice = new LatticeGrainStorageSerializer(
            this.provider.GetRequiredService<Serializer>(), this.json);

        var blob = BuildBlob();
        this.binaryPayload = this.lattice.Serialize(blob).ToArray();
        this.legacyJsonPayload = this.json.Serialize(blob).ToArray();

        Assert.That(
            this.binaryPayload.AsSpan(0, LatticeGrainStorageSerializer.BinaryMagic.Length).ToArray(),
            Is.EqualTo(LatticeGrainStorageSerializer.BinaryMagic.ToArray()),
            "sanity: the binary payload must carry the LGB1 magic, or it does not take the binary read path");
        Assert.That(
            this.legacyJsonPayload.AsSpan(0, LatticeGrainStorageSerializer.BinaryMagic.Length).ToArray(),
            Is.Not.EqualTo(LatticeGrainStorageSerializer.BinaryMagic.ToArray()),
            "sanity: the legacy payload must not carry the LGB1 magic, or it does not take the JSON read path");
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => this.provider?.Dispose();

    [Test]
    public void Binary_read_total_allocation_is_covered_by_the_amplification()
    {
        var ratio = MeasureTotalAllocationRatio(this.binaryPayload);

        TestContext.Out.WriteLine($"binary read: total allocation {ratio:F3}x the frame");

        // The column the provider hands over (about 1x) plus the deserialized
        // EncodedRows (about 1x), and nothing else of the frame's order.
        Assert.That(ratio, Is.LessThanOrEqualTo(2.5),
            "the binary read path is documented as about 2x the frame; a larger figure means the documented cost model is stale");
        Assert.That(ratio, Is.LessThanOrEqualTo(LeafSnapshotHydrationAdmission.HydrationHeapAmplification),
            "every payload a current build writes must be covered by the per-claim amplification");
    }

    [Test]
    public void Legacy_json_read_peak_heap_exceeds_the_amplification()
    {
        var ratio = MeasurePeakLiveRatio(this.legacyJsonPayload);

        TestContext.Out.WriteLine($"legacy JSON read: sampled peak live heap at least {ratio:F3}x the frame");

        // The reason this is pinned at all: the constant was once justified as a
        // conservative floor on the JSON read, and it is not one. A reader who
        // believes it is will "correct" the factor to the binary figure and
        // strip the only headroom the legacy corpus has, or will restate the
        // floor claim in an operator document. If the factor is ever raised past
        // the legacy cost, this goes red so the documentation is updated with it.
        Assert.That(ratio, Is.GreaterThan(LeafSnapshotHydrationAdmission.HydrationHeapAmplification),
            "the legacy JSON read is documented as costing more than the per-claim amplification; " +
            "if that is no longer true, the constant's documentation and docs/lattice/metrics.md are stale");
    }

    [Test]
    public void Worst_read_path_fully_admitted_stays_within_half_the_heap_limit()
    {
        var binary = MeasureTotalAllocationRatio(this.binaryPayload);
        var legacy = MeasureTotalAllocationRatio(this.legacyJsonPayload);

        TestContext.Out.WriteLine($"total allocation: binary {binary:F3}x, legacy JSON {legacy:F3}x the frame");

        Assert.That(legacy, Is.GreaterThan(binary),
            "the legacy JSON read is documented as the worse of the two live paths");

        // The gate admits claims until their modelled cost fills the budget,
        // which is heap limit / HeapBudgetDivisor. A claim modelled at
        // `amplification` that really costs `legacy` therefore lets a storm made
        // entirely of legacy blobs reach legacy / amplification / divisor of the
        // limit. Total allocation over-states the live peak, so this is the
        // conservative reading of that bound.
        var fullyAdmittedFractionOfLimit =
            legacy
            / LeafSnapshotHydrationAdmission.HydrationHeapAmplification
            / LeafSnapshotHydrationAdmission.HeapBudgetDivisor;

        TestContext.Out.WriteLine(
            $"an all-legacy storm fully admitted reaches at most {fullyAdmittedFractionOfLimit:F3} of the heap limit");

        Assert.That(fullyAdmittedFractionOfLimit, Is.LessThanOrEqualTo(0.5),
            "lowering the amplification toward the binary figure lets a cold start over the legacy JSON corpus " +
            "admit more than half the heap limit; the factor is held above the binary cost for exactly this reason");
    }

    private static LeafSnapshotBlob BuildBlob()
    {
        // Random bytes, so the frame is incompressible and base64 inflates it by
        // exactly 4/3, as it does for a real encoded leaf.
        var frame = new byte[FrameBytes];
        new Random(2858).NextBytes(frame);
        return new LeafSnapshotBlob
        {
            SnapshotOffset = 42L,
            EncodedRows = frame,
            CapturedAtTicks = 1_234_567L,
            SnapshotBytes = FrameBytes,
            SnapshotOffsetsByPartition = [42L],
        };
    }

    private double MeasureTotalAllocationRatio(byte[] stored)
    {
        Warm(stored);

        var before = GC.GetAllocatedBytesForCurrentThread();

        // The column copy is inside the window deliberately: the provider's read
        // buffer is live for the whole deserialize and is part of what one
        // hydration costs.
        var column = new BinaryData(stored.AsSpan().ToArray());
        var blob = this.lattice.Deserialize<LeafSnapshotBlob>(column);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        AssertRoundTripped(blob);
        GC.KeepAlive(column);
        return (double)allocated / FrameBytes;
    }

    private double MeasurePeakLiveRatio(byte[] stored)
    {
        Warm(stored);

        // The unhindered read time, which paces the sampler below.
        var clock = Stopwatch.StartNew();
        Warm(stored);
        var readTime = clock.Elapsed;

        // A sampler can only miss a peak, never invent one, so repeating and
        // keeping the largest reading only ever tightens the lower bound - and a
        // lower bound that already clears the factor needs no further attempts.
        long best = 0;
        for (var attempt = 0; attempt < 3; attempt++)
        {
            best = Math.Max(best, SamplePeakLiveBytes(stored, readTime));
            if (best > (long)FrameBytes * LeafSnapshotHydrationAdmission.HydrationHeapAmplification)
            {
                break;
            }
        }

        return (double)best / FrameBytes;
    }

    private long SamplePeakLiveBytes(byte[] stored, TimeSpan readTime)
    {
        var column = new BinaryData(stored.AsSpan().ToArray());
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        var baseline = GC.GetTotalMemory(forceFullCollection: false);

        long peak = baseline;
        var samples = 0;
        var done = 0;
        using var sampling = new ManualResetEventSlim();
        var sampler = new Thread(() =>
        {
            var clock = Stopwatch.StartNew();
            while (Volatile.Read(ref done) == 0)
            {
                var started = clock.Elapsed;
                GC.Collect(2, GCCollectionMode.Forced, blocking: true);
                peak = Math.Max(peak, GC.GetTotalMemory(forceFullCollection: false));
                samples++;
                sampling.Set();

                // Every forced collection suspends the reader, and sampling back to
                // back is what makes the sampler dense: the read advances only a
                // sliver between samples. But the whole measurement then costs
                // (read time / sliver) collections, and a collection's cost grows
                // with the process heap. In the coverage lane the whole suite
                // shares one process, and this ran past the ten-minute hang
                // timeout, so the host was killed and the core library's coverage
                // was lost. Letting the read advance by readTime * (this
                // collection's cost / budget) caps the measurement at about
                // SamplingBudget at any heap size. On a small heap the pause
                // rounds to nothing and the sampler stays as dense as ever.
                var resume = clock.Elapsed + readTime * ((clock.Elapsed - started) / SamplingBudget);
                while (clock.Elapsed < resume && Volatile.Read(ref done) == 0)
                {
                    Thread.Yield();
                }
            }
        });

        sampler.Start();
        sampling.Wait();
        var blob = this.lattice.Deserialize<LeafSnapshotBlob>(column);
        Volatile.Write(ref done, 1);
        sampler.Join();

        AssertRoundTripped(blob);
        Assert.That(samples, Is.GreaterThan(1), "sanity: the sampler must have observed the read in flight");

        // The column was live at the baseline, so it is added back: it is part
        // of what the hydration costs, exactly as in the total-allocation arm.
        GC.KeepAlive(column);
        return peak - baseline + stored.Length;
    }

    private void Warm(byte[] stored)
        => AssertRoundTripped(this.lattice.Deserialize<LeafSnapshotBlob>(new BinaryData(stored.AsSpan().ToArray())));

    private static void AssertRoundTripped(LeafSnapshotBlob blob)
        => Assert.That(blob.EncodedRows, Has.Length.EqualTo(FrameBytes),
            "sanity: the read must reproduce the whole frame, or the ratio measures something else");
}
