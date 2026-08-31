namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Allocation contracts, measured rather than asserted.
/// <para>
/// Every figure here is a <b>differential</b> measurement: the same path is run
/// at two loop sizes after a warm-up and the assertion is on the
/// <i>difference</i>. A one-off runtime cost - tiered JIT, on-stack replacement
/// landing inside the window, an array pool's first rent - appears in both
/// measurements and cancels, while a genuine per-iteration allocation scales
/// with the loop and survives. An absolute "allocated zero bytes" assertion
/// cannot tell those apart, so it passes when the fixture runs alone and fails
/// in a larger batch where the shared test host has already compiled a different
/// set of methods. That failure mode is real and has cost this repository a
/// round of rework, so it is designed out here rather than tuned around.
/// </para>
/// <para>
/// The query and mutation paths are fully synchronous and never await, so they
/// are measured with the per-thread counter, which excludes unrelated threads'
/// noise. Training is measured with the process-wide precise counter instead,
/// because it may hand its assignment pass to the thread pool and a per-thread
/// figure would silently under-count that work.
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

    /// <summary>
    /// Runs <paramref name="action"/> at <paramref name="iterations"/> and at
    /// twice that, and returns the difference - the bytes attributable to the
    /// extra <paramref name="iterations"/> runs, with any one-off cost cancelled.
    /// </summary>
    private static long PerIterationDelta(Action action, int warmup, int iterations)
    {
        for (var i = 0; i < warmup; i++)
        {
            action();
        }

        var single = AllocatedOverLoop(action, iterations);
        var doubled = AllocatedOverLoop(action, iterations * 2);
        return doubled - single;
    }

    private static long AllocatedOverLoop(Action action, int iterations)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
        {
            action();
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static void AssertNoPerIterationAllocation(long delta, int iterations, string what)
    {
        Assert.That(delta, Is.Zero,
            $"{what} allocated {delta} bytes across an extra {iterations} runs "
            + $"({(double)delta / iterations:F3} bytes per run), so it allocates in steady state.");
    }

    [Test]
    public void The_approximate_query_path_allocates_nothing_per_query()
    {
        const int Iterations = 2_000;
        var corpus = BuildCorpus(8_000);
        var index = Build(corpus, partitionCount: 64, probes: 8, train: true);
        var results = new VectorSearchResult[10];
        var query = 0;

        var delta = PerIterationDelta(
            () => index.Search(corpus[query++ % corpus.Length], results),
            warmup: 200,
            iterations: Iterations);

        AssertNoPerIterationAllocation(delta, Iterations, "The approximate query path");
    }

    [Test]
    public void The_exhaustive_query_path_allocates_nothing_per_query()
    {
        const int Iterations = 500;
        var corpus = BuildCorpus(2_000);
        var index = Build(corpus, partitionCount: 0, probes: 0, train: false);
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Building));

        var results = new VectorSearchResult[10];
        var query = 0;

        var delta = PerIterationDelta(
            () => index.Search(corpus[query++ % corpus.Length], results),
            warmup: 100,
            iterations: Iterations);

        AssertNoPerIterationAllocation(delta, Iterations, "The exhaustive query path");
    }

    [Test]
    public void The_query_path_allocates_nothing_per_query_beyond_the_stack_allocated_probe_bound()
    {
        // 200 probes is past the stack-allocation limit, so the probe scratch is
        // rented from the array pool instead. Once the pool is warm that must
        // still cost nothing per query.
        const int Iterations = 200;
        var corpus = BuildCorpus(20_000, seed: 43);
        var index = Build(corpus, partitionCount: 256, probes: 200, train: true);
        Assert.That(index.Probes, Is.EqualTo(200));

        var results = new VectorSearchResult[10];
        var query = 0;

        var delta = PerIterationDelta(
            () => index.Search(corpus[query++ % corpus.Length], results),
            warmup: 50,
            iterations: Iterations);

        AssertNoPerIterationAllocation(delta, Iterations, "The pooled-scratch query path");
    }

    [Test]
    public void SelectPartitions_allocates_nothing_per_call()
    {
        const int Iterations = 2_000;
        var corpus = BuildCorpus(8_000);
        var index = Build(corpus, partitionCount: 64, probes: 8, train: true);
        var partitions = new int[8];
        var query = 0;

        var delta = PerIterationDelta(
            () => index.SelectPartitions(corpus[query++ % corpus.Length], partitions),
            warmup: 200,
            iterations: Iterations);

        AssertNoPerIterationAllocation(delta, Iterations, "Partition selection");
    }

    [Test]
    public void Lookup_allocates_nothing_per_call()
    {
        const int Iterations = 2_000;
        var corpus = BuildCorpus(2_000);
        var index = Build(corpus, partitionCount: 0, probes: 0, train: false);
        var destination = new float[Dimensions];
        var probe = 0;

        var delta = PerIterationDelta(
            () =>
            {
                index.Contains(probe % corpus.Length);
                index.TryGetVector(probe++ % corpus.Length, destination);
            },
            warmup: 200,
            iterations: Iterations);

        AssertNoPerIterationAllocation(delta, Iterations, "Lookup");
    }

    [Test]
    public void Removing_an_absent_key_allocates_nothing_per_call()
    {
        const int Iterations = 1_000;
        var corpus = BuildCorpus(2_000);
        var index = Build(corpus, partitionCount: 0, probes: 0, train: false);

        var delta = PerIterationDelta(() => index.Remove(-1), warmup: 100, iterations: Iterations);

        AssertNoPerIterationAllocation(delta, Iterations, "Removing an absent key");
    }

    [Test]
    public void Inserting_into_a_reserved_index_allocates_nothing_per_vector()
    {
        // The differential here is across two reserved indexes rather than two
        // loop sizes on one: filling an index is not repeatable in place. The
        // reservation itself is excluded from the measured window, so what is
        // left is exactly the per-insert cost.
        const int Vectors = 4_000;
        var corpus = BuildCorpus(Vectors * 2);

        // Warm the insert path on a separate, equally reserved index so its JIT
        // cost is not attributed to either measurement.
        FillReserved(corpus, Vectors);

        var single = FillReserved(corpus, Vectors);
        var doubled = FillReserved(corpus, Vectors * 2);

        AssertNoPerIterationAllocation(doubled - single, Vectors, "Inserting into a reserved index");
    }

    private static long FillReserved(float[][] corpus, int count)
    {
        var index = new VectorIndex(new VectorIndexOptions { Dimensions = Dimensions });
        index.EnsureCapacity(count);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < count; i++)
        {
            index.Add(i, corpus[i]);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.That(index.Count, Is.EqualTo(count));
        return allocated;
    }

    [Test]
    public void Training_confines_its_scratch_to_pooled_buffers()
    {
        const int Count = 20_000;
        const int Partitions = 64;

        // A small dimensionality on purpose: it shrinks what training must retain
        // without shrinking the scratch it needs, so an unpooled buffer shows up
        // as a large multiple rather than hiding inside the vector blocks.
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

        // The second train is the measured one: the pools are warm, so what is
        // left is the partitioning the index genuinely keeps. This is a ratio
        // against that retained size rather than an absolute figure, so a stray
        // one-off cost of a few kilobytes cannot flip it. The process-wide
        // precise counter is used because training may hand its assignment pass
        // to the thread pool.
        var before = GC.GetTotalAllocatedBytes(precise: true);
        index.Train();
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

        var retained = VectorIndexMemory.Bytes(index.Capacity, SmallDimensions, Partitions);

        TestContext.Out.WriteLine(
            $"training allocated {allocated} bytes; the cells it must retain are {retained} bytes "
            + $"({(double)allocated / retained:F2}x)");

        Assert.That(allocated, Is.LessThan(retained * 3 / 2),
            $"Training allocated {allocated} bytes against {retained} bytes of retained cells, so scratch is "
            + "escaping the pool rather than being rented.");
    }
}
