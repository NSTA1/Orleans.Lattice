using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Measures the bounded per-partition WAL floor-holder candidate samples
/// against one tree-wide sample for the same pin population.
/// </summary>
[MemoryDiagnoser]
public class WalGcPartitionFloorSamplingBenchmarks
{
    private const int PartitionCount = 8;
    private const int TreeBudget = 32;
    private (int Partition, WalGcFloorHolderCandidate Candidate)[] _pins = [];

    /// <summary>Builds an even eight-partition population outside the measured path.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _pins = new (int, WalGcFloorHolderCandidate)[PartitionCount * 512];
        var index = 0;
        for (var leaf = 0; leaf < 512; leaf++)
        {
            for (var partition = 0; partition < PartitionCount; partition++)
            {
                var consumerId = $"leaf-{leaf}-partition-{partition}";
                _pins[index++] = (
                    partition,
                    new WalGcFloorHolderCandidate(
                        consumerId,
                        GrainId.Create("leaf", leaf.ToString()),
                        true,
                        leaf * PartitionCount + partition,
                        HybridLogicalClock.Zero));
            }
        }
    }

    /// <summary>Builds one ascending candidate sample over all pins.</summary>
    [Benchmark(Baseline = true)]
    public int OneGlobalSample()
    {
        var sample = new List<WalGcFloorHolderCandidate>();
        foreach (var pin in _pins)
        {
            LatticeWalGcScheduler.OfferFloorHolderCandidate(sample, TreeBudget, pin.Candidate);
        }

        return sample.Count;
    }

    /// <summary>Builds independent samples with the production partition budget helper.</summary>
    [Benchmark]
    public int PartitionLocalSamples()
    {
        var sampleBudget = LatticeWalGcScheduler.ScaleUnmeasuredFloorHolderPartitionBudget(
            TreeBudget,
            PartitionCount);
        var samples = new SortedDictionary<int, List<WalGcFloorHolderCandidate>>();
        foreach (var pin in _pins)
        {
            if (!samples.TryGetValue(pin.Partition, out var sample))
            {
                sample = new List<WalGcFloorHolderCandidate>();
                samples.Add(pin.Partition, sample);
            }

            LatticeWalGcScheduler.OfferFloorHolderCandidate(sample, sampleBudget, pin.Candidate);
        }

        var count = 0;
        foreach (var sample in samples.Values)
        {
            count += sample.Count;
        }

        return count;
    }
}
