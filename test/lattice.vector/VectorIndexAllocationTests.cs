namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Allocation contracts, measured rather than asserted. The steady-state query
/// path must allocate nothing at all, and the build path must confine its scratch
/// to pooled buffers so a rebuild does not churn the heap.
/// <para>
/// Each measurement warms the code path first, so what is measured is the steady
/// state rather than one-off JIT and array-pool priming.
/// </para>
/// </summary>
[TestFixture]
public sealed class VectorIndexAllocationTests
{
    private const int Dimensions = 32;

    private static float[][] BuildCorpus(int count, ulong seed = 41) =>
        VectorCorpus.Clustered(count, Dimensions, clusters: 64, seed: seed);

    private static VectorIndex Build(float[][] corpus, int partitionCount, int probes, bool train)
    {
        var index = new VectorIndex(new VectorIndexOptions
        {
            Dimensions = Dimensions,
            PartitionCount = partitionCount,
            Probes = probes,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 4_096,
        });

        index.EnsureCapacity(corpus.Length);
        for (var i = 0; i < corpus.Length; i++)
        {
            index.Add(i, corpus[i]);
        }

        if (train)
        {
            index.Train();
        }

        return index;
    }

    private static long Measure(Action action, int warmup, int iterations)
    {
        for (var i = 0; i < warmup; i++)
        {
            action();
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
        {
            action();
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    [Test]
    public void The_approximate_query_path_allocates_nothing_in_steady_state()
    {
        var corpus = BuildCorpus(8_000);
        var index = Build(corpus, partitionCount: 64, probes: 8, train: true);
        var results = new VectorSearchResult[10];
        var query = 0;

        var allocated = Measure(
            () => index.Search(corpus[query++ % corpus.Length], results),
            warmup: 200,
            iterations: 2_000);

        Assert.That(allocated, Is.EqualTo(0L),
            $"The approximate query path allocated {allocated} bytes over 2000 searches.");
    }

    [Test]
    public void The_exhaustive_query_path_allocates_nothing_in_steady_state()
    {
        var corpus = BuildCorpus(2_000);
        var index = Build(corpus, partitionCount: 0, probes: 0, train: false);
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Building));

        var results = new VectorSearchResult[10];
        var query = 0;

        var allocated = Measure(
            () => index.Search(corpus[query++ % corpus.Length], results),
            warmup: 100,
            iterations: 500);

        Assert.That(allocated, Is.EqualTo(0L),
            $"The exhaustive query path allocated {allocated} bytes over 500 searches.");
    }

    [Test]
    public void The_query_path_allocates_nothing_beyond_the_stack_allocated_probe_bound()
    {
        // 200 probes is past the stack-allocation limit, so the probe scratch is
        // rented instead. Once the pool is warm that must still cost nothing.
        var corpus = BuildCorpus(20_000, seed: 43);
        var index = Build(corpus, partitionCount: 256, probes: 200, train: true);
        Assert.That(index.Probes, Is.EqualTo(200));

        var results = new VectorSearchResult[10];
        var query = 0;

        var allocated = Measure(
            () => index.Search(corpus[query++ % corpus.Length], results),
            warmup: 50,
            iterations: 200);

        Assert.That(allocated, Is.EqualTo(0L),
            $"The pooled-scratch query path allocated {allocated} bytes over 200 searches.");
    }

    [Test]
    public void SelectPartitions_allocates_nothing_in_steady_state()
    {
        var corpus = BuildCorpus(8_000);
        var index = Build(corpus, partitionCount: 64, probes: 8, train: true);
        var partitions = new int[8];
        var query = 0;

        var allocated = Measure(
            () => index.SelectPartitions(corpus[query++ % corpus.Length], partitions),
            warmup: 200,
            iterations: 2_000);

        Assert.That(allocated, Is.EqualTo(0L),
            $"Partition selection allocated {allocated} bytes over 2000 calls.");
    }

    [Test]
    public void Inserting_into_a_reserved_index_allocates_nothing()
    {
        var corpus = BuildCorpus(4_000);
        var index = new VectorIndex(new VectorIndexOptions { Dimensions = Dimensions });
        index.EnsureCapacity(corpus.Length);

        // Warm the insert path on a separate, equally reserved index so the JIT
        // work is not attributed to the measurement.
        var warm = new VectorIndex(new VectorIndexOptions { Dimensions = Dimensions });
        warm.EnsureCapacity(corpus.Length);
        for (var i = 0; i < corpus.Length; i++)
        {
            warm.Add(i, corpus[i]);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < corpus.Length; i++)
        {
            index.Add(i, corpus[i]);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(index.Count, Is.EqualTo(corpus.Length));
        Assert.That(allocated, Is.EqualTo(0L),
            $"Inserting {corpus.Length} vectors into a reserved index allocated {allocated} bytes.");
    }

    [Test]
    public void Lookup_and_removal_allocate_nothing()
    {
        var corpus = BuildCorpus(2_000);
        var index = Build(corpus, partitionCount: 0, probes: 0, train: false);
        var destination = new float[Dimensions];

        var probe = 0;
        var allocated = Measure(
            () =>
            {
                index.Contains(probe % corpus.Length);
                index.TryGetVector(probe++ % corpus.Length, destination);
            },
            warmup: 200,
            iterations: 2_000);

        Assert.That(allocated, Is.EqualTo(0L), $"Lookup allocated {allocated} bytes over 2000 calls.");

        var removeAllocated = Measure(() => index.Remove(-1), warmup: 100, iterations: 1_000);
        Assert.That(removeAllocated, Is.EqualTo(0L),
            $"Removing an absent key allocated {removeAllocated} bytes over 1000 calls.");
    }

    [Test]
    public void Training_confines_its_scratch_to_pooled_buffers()
    {
        const int Count = 20_000;
        const int Partitions = 64;

        // A small dimensionality on purpose: it shrinks what training must retain
        // without shrinking the scratch it needs, so an unpooled buffer would show
        // up as a large multiple rather than hide inside the vector blocks.
        const int SmallDimensions = 4;

        var corpus = VectorCorpus.Clustered(Count, SmallDimensions, clusters: Partitions, seed: 47);
        var index = new VectorIndex(new VectorIndexOptions
        {
            Dimensions = SmallDimensions,
            PartitionCount = Partitions,
            Probes = 8,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 4_096,
        });

        index.EnsureCapacity(Count);
        for (var i = 0; i < Count; i++)
        {
            index.Add(i, corpus[i]);
        }

        index.Train();

        // The second train is the measured one: the pools are warm, so anything
        // left is the partitioning the index must genuinely keep.
        var before = GC.GetAllocatedBytesForCurrentThread();
        index.Train();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        var retained = VectorIndexMemory.Bytes(index.Capacity, SmallDimensions, Partitions);

        TestContext.Out.WriteLine(
            $"training allocated {allocated} bytes; the cells it must retain are {retained} bytes "
            + $"({(double)allocated / retained:F2}x)");

        Assert.That(allocated, Is.LessThan(retained * 3 / 2),
            $"Training allocated {allocated} bytes against {retained} bytes of retained cells, so scratch is "
            + "escaping the pool rather than being rented.");
    }
}
