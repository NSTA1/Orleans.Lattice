namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Allocation contracts, measured rather than asserted.
/// <para>
/// This fixture is the worked example the repository points at for allocation
/// probes, so it follows all four parts of the rule rather than only the first.
/// Every part exists because a probe that violates it produces a <b>false
/// negative that looks exactly like a passing test</b>: you cannot tell a
/// correct probe from a broken one by reading the result, only by deliberately
/// making it fail and checking that it does.
/// </para>
/// <list type="number">
/// <item><description>
/// <b>Differential, never absolute.</b> The same path runs at two loop sizes and
/// the assertion is on the <i>difference</i>. A one-off runtime cost - tiered
/// JIT, on-stack replacement landing inside the window, an array pool's first
/// rent - appears in both measurements and cancels, while a genuine
/// per-iteration allocation scales with the loop and survives. An absolute
/// "allocated zero bytes" assertion cannot tell those apart, so it passes when
/// the fixture runs alone and fails in a larger batch where the shared test host
/// has already compiled a different set of methods.
/// </description></item>
/// <item><description>
/// <b>The warm-up is full-size</b> - the largest window that will be measured,
/// not a small constant. This is load-bearing rather than hygiene: a warm-up too
/// small to promote the method to tier-1 leaves the measurement straddling the
/// promotion, and it also hides JIT elision, which is what makes an
/// unescaped battery-test allocation vanish. Sizing the warm-up to the
/// measurement is what turns that class of defect into something an ordinary
/// test lane catches instead of only a run with tiered compilation disabled.
/// </description></item>
/// <item><description>
/// <b>The minimum is kept across repeats</b>, never a single sample and never a
/// short circuit on the first non-positive difference. On a loop that genuinely
/// allocates, one noisy attempt where the small window absorbed more noise than
/// the large one would otherwise be reported as allocation-free.
/// </description></item>
/// <item><description>
/// <b>Set-up stays outside the measured window.</b> In particular no NUnit
/// assertion is ever made inside a measured loop: a constraint assertion
/// allocates a few hundred bytes of its own, so a probe that asserts per
/// iteration measures the probe rather than the path.
/// </description></item>
/// </list>
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

    /// <summary>
    /// How many times each measurement is repeated before the minimum is taken.
    /// </summary>
    private const int Attempts = 5;

    /// <summary>
    /// The battery test's sink. <b>Load-bearing: do not simplify.</b> A reference
    /// stored to a static field is a definite escape at every JIT tier and has no
    /// constant-folding surface, so the allocation cannot be elided. A sink that
    /// does not escape - a local, or <c>new long[1].Length</c>, whose length
    /// folds to a constant - is removed outright by escape analysis, and the
    /// battery test then truthfully reports zero and becomes the false negative
    /// it was written to rule out.
    /// </summary>
    private static object? _escapeSink;

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
    /// twice that, and returns the bytes attributable to the extra
    /// <paramref name="iterations"/> runs with any one-off cost cancelled.
    /// <para>
    /// The warm-up is the full doubled window, so tiering and on-stack
    /// replacement have settled before either sample is taken rather than
    /// landing inside one of them, and the minimum is kept across
    /// <see cref="Attempts"/> repeats so a single noisy attempt cannot report a
    /// genuinely allocating loop as clean.
    /// </para>
    /// </summary>
    private static long PerIterationDelta(Action action, int iterations)
    {
        RunLoop(action, iterations * 2);

        var best = long.MaxValue;
        for (var attempt = 0; attempt < Attempts; attempt++)
        {
            var single = AllocatedOverLoop(action, iterations);
            var doubled = AllocatedOverLoop(action, iterations * 2);
            best = Math.Min(best, doubled - single);
        }

        return Math.Max(0, best);
    }

    private static void RunLoop(Action action, int iterations)
    {
        for (var i = 0; i < iterations; i++)
        {
            action();
        }
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
    public void The_allocation_probe_detects_a_loop_that_does_allocate()
    {
        // The smoke detector's own battery. A probe that cannot see a deliberate
        // allocation silently approves the regression it exists to catch, so the
        // allocation here must PROVABLY escape - see the note on _escapeSink.
        var delta = PerIterationDelta(() => _escapeSink = new object(), iterations: 1_000);

        Assert.That(delta, Is.GreaterThan(0),
            "The differential probe failed to detect a loop that allocates on every iteration. "
            + "Either the probe is broken, or the sink stopped escaping and the JIT elided the allocation.");
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
            iterations: Iterations);

        AssertNoPerIterationAllocation(delta, Iterations, "Lookup");
    }

    [Test]
    public void Removing_an_absent_key_allocates_nothing_per_call()
    {
        const int Iterations = 1_000;
        var corpus = BuildCorpus(2_000);
        var index = Build(corpus, partitionCount: 0, probes: 0, train: false);

        var delta = PerIterationDelta(() => index.Remove(-1), iterations: Iterations);

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

        // Warm at the FULL doubled size on a separate, equally reserved index, so
        // the insert path is already promoted before either sample is taken and
        // its one-off cost is attributed to neither.
        FillReserved(corpus, Vectors * 2);

        var best = long.MaxValue;
        for (var attempt = 0; attempt < Attempts; attempt++)
        {
            var single = FillReserved(corpus, Vectors);
            var doubled = FillReserved(corpus, Vectors * 2);
            best = Math.Min(best, doubled - single);
        }

        AssertNoPerIterationAllocation(Math.Max(0, best), Vectors, "Inserting into a reserved index");
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

        // The measured trains come after that warm-up one: the pools are warm, so
        // what is left is the partitioning the index genuinely keeps. The minimum
        // across repeats is taken for the same reason every differential here
        // does, and this is a ratio against the retained size rather than an
        // absolute figure, so a stray one-off cost of a few kilobytes cannot flip
        // it. The process-wide precise counter is used because training may hand
        // its assignment pass to the thread pool.
        var allocated = long.MaxValue;
        for (var attempt = 0; attempt < Attempts; attempt++)
        {
            var before = GC.GetTotalAllocatedBytes(precise: true);
            index.Train();
            allocated = Math.Min(allocated, GC.GetTotalAllocatedBytes(precise: true) - before);
        }

        var retained = VectorIndexMemory.Bytes(index.Capacity, SmallDimensions, Partitions);

        TestContext.Out.WriteLine(
            $"training allocated {allocated} bytes; the cells it must retain are {retained} bytes "
            + $"({(double)allocated / retained:F2}x)");

        Assert.That(allocated, Is.LessThan(retained * 3 / 2),
            $"Training allocated {allocated} bytes against {retained} bytes of retained cells, so scratch is "
            + "escaping the pool rather than being rented.");
    }
}
