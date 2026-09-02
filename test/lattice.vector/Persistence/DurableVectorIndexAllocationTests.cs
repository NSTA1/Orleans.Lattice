using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Allocation contracts for the durable index's load, query, and update paths.
/// <para>
/// Every figure is a <b>differential</b> measurement: the same path runs at two
/// loop sizes after a warm-up and the assertion is on the difference. A one-off
/// runtime cost - tiered JIT, on-stack replacement landing inside the window, a
/// pool's first rent - appears in both measurements and cancels, while a genuine
/// per-iteration allocation scales with the loop and survives. An absolute
/// "allocated zero bytes" assertion cannot tell those apart, so it passes in
/// isolation and fails in a larger batch where the shared test host has already
/// compiled a different set of methods. That failure mode has cost this
/// repository real rework twice, so it is designed out here rather than tuned
/// around.
/// </para>
/// <para>
/// The synchronous query and update paths are measured with the per-thread
/// counter, which excludes unrelated threads' noise. Anything that awaits is
/// measured with the process-wide precise counter instead, because a
/// continuation may resume on a different thread and a per-thread figure would
/// then be meaningless rather than merely noisy.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexAllocationTests
{
    private const int Corpus = 2_000;

    private static DurableVectorIndexOptions Options() => new()
    {
        KeyPrefix = "alloc/",
        MaxItemsPerChunk = 128,
        IngestBatchSize = 1_024,
        Index = new VectorIndexOptions
        {
            Dimensions = DurableIndexHarness.Dimensions,
            PartitionCount = 32,
            Probes = 4,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 2_048,
        },
    };

    /// <summary>
    /// The number of times each differential measurement is repeated. The
    /// <b>minimum</b> across attempts is kept, never the first sample and never a
    /// short circuit on the first non-positive one: on a loop that genuinely
    /// allocates, a single noisy attempt where the small window absorbed more
    /// noise than the large one reports allocation-free, which is the exact false
    /// negative this fixture exists to prevent. A loop that truly allocates every
    /// iteration cannot have a non-positive minimum, and a clean loop's minimum
    /// picks the least noisy attempt.
    /// </summary>
    private const int Attempts = 5;

    /// <summary>
    /// The battery test's sink. <b>Load-bearing: do not simplify.</b> A reference
    /// stored to a static field is a definite escape at every JIT tier and has no
    /// constant-folding surface, so the allocation cannot be elided. A sink that
    /// does not escape - a local, or <c>new long[1].Length</c>, whose length folds
    /// to a constant - is removed outright by escape analysis, and the battery
    /// test then truthfully reports zero and becomes the false negative it was
    /// written to rule out. Verified by substituting the non-escaping form and
    /// watching this fixture's battery test fail.
    /// </summary>
    private static object? _escapeSink;

    private static long PerIterationDelta(Action action, int iterations)
    {
        // Full-size warm-up: the largest window that will be measured, so tiering
        // and on-stack replacement have already settled before either sample is
        // taken rather than landing inside one of them.
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
        // The per-thread counter, used only on paths that never await: it
        // excludes unrelated threads' noise, which makes the differential
        // tighter. It returns nonsense across an await, because continuations
        // migrate threads, so anything asynchronous uses the process-wide
        // counter below instead.
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
        {
            action();
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static async Task<long> PerIterationDeltaAsync(Func<ValueTask> action, int iterations)
    {
        for (var i = 0; i < iterations * 2; i++)
        {
            await action();
        }

        var best = long.MaxValue;
        for (var attempt = 0; attempt < Attempts; attempt++)
        {
            var single = await AllocatedOverLoopAsync(action, iterations);
            var doubled = await AllocatedOverLoopAsync(action, iterations * 2);
            best = Math.Min(best, doubled - single);
        }

        return Math.Max(0, best);
    }

    private static async Task<long> AllocatedOverLoopAsync(Func<ValueTask> action, int iterations)
    {
        // The process-wide precise counter, because an awaited path may resume on
        // a different thread and the per-thread counter would then report a
        // figure that is not merely noisy but wrong.
        var before = GC.GetTotalAllocatedBytes(precise: true);
        for (var i = 0; i < iterations; i++)
        {
            await action();
        }

        return GC.GetTotalAllocatedBytes(precise: true) - before;
    }

    private static void AssertNoPerIterationAllocation(long delta, int iterations, string what)
    {
        Assert.That(delta, Is.Zero,
            $"{what} allocated {delta} bytes across an extra {iterations} runs "
            + $"({(double)delta / iterations:F3} bytes per run), so it allocates in steady state.");
    }

    private static void AssertBoundedPerIterationAllocation(
        long delta, int iterations, long budget, string what)
    {
        var perRun = (double)delta / iterations;
        TestContext.Out.WriteLine($"{what}: {perRun:F1} bytes per run across {iterations} extra runs");
        Assert.That(perRun, Is.LessThanOrEqualTo(budget),
            $"{what} allocated {perRun:F1} bytes per run, above the {budget} byte budget.");
    }

    private static async Task<DurableVectorIndex> BuiltAsync()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        return await DurableIndexHarness.BuiltAsync(store, source, Options());
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
    public async Task The_query_path_allocates_nothing_per_query()
    {
        const int Iterations = 2_000;
        var index = await BuiltAsync();
        var results = new VectorSearchResult[10];
        var query = new float[DurableIndexHarness.Dimensions];
        index.Search(query, results, out _);

        var probe = 0;
        var delta = PerIterationDelta(
            () =>
            {
                query[probe++ % query.Length] = probe * 0.001f;
                index.Search(query, results, out _);
            },
            iterations: Iterations);

        AssertNoPerIterationAllocation(delta, Iterations, "The durable index query path");
    }

    [Test]
    public async Task Resolving_a_result_identifier_allocates_nothing_per_call()
    {
        const int Iterations = 2_000;
        var index = await BuiltAsync();
        var results = new VectorSearchResult[10];
        var found = index.Search(new float[DurableIndexHarness.Dimensions], results, out _);
        Assert.That(found, Is.GreaterThan(0));

        var probe = 0;
        var delta = PerIterationDelta(
            () => index.TryGetId(results[probe++ % found].Key, out _),
            iterations: Iterations);

        AssertNoPerIterationAllocation(delta, Iterations, "Resolving a result identifier");
    }

    [Test]
    public async Task Looking_up_a_key_by_identifier_allocates_nothing_per_call()
    {
        const int Iterations = 2_000;
        var index = await BuiltAsync();
        var ids = new string[64];
        for (var i = 0; i < ids.Length; i++)
        {
            ids[i] = DurableIndexHarness.Id(i);
        }

        var probe = 0;
        var delta = PerIterationDelta(
            () => index.TryGetKey(ids[probe++ % ids.Length], out _),
            iterations: Iterations);

        AssertNoPerIterationAllocation(delta, Iterations, "Looking up a key by identifier");
    }

    [Test]
    public async Task Re_embedding_a_known_identifier_allocates_nothing_per_update()
    {
        // The maintenance loop's hot path: a source is re-embedded, so the
        // identifier is already mapped and the update touches neither the store
        // nor the key dictionary. It is written as a synchronous fast path
        // precisely so this costs nothing.
        //
        // Each identifier is re-embedded to its own vector so it stays in its own
        // cell. A re-embed that moves a vector to a different cell can amortise a
        // growth of the destination cell's block, which is the index's own array
        // growth and is bounded by the cell's high-water mark; measuring that
        // here would be measuring the wrong layer.
        const int Iterations = 1_000;
        var source = DurableIndexHarness.Source(Corpus);
        var index = await DurableIndexHarness.BuiltAsync(
            new InMemoryVectorIndexStore(), source, Options());

        var ids = new string[64];
        var vectors = new float[64][];
        for (var i = 0; i < ids.Length; i++)
        {
            ids[i] = DurableIndexHarness.Id(i);
            vectors[i] = source[ids[i]];
        }

        var probe = 0;

        // The contract check happens once, outside the measured window: an
        // NUnit constraint assertion allocates a few hundred bytes of its own,
        // and measuring the probe rather than the path is exactly how an
        // allocation test comes to assert nothing useful.
        var sample = index.UpsertAsync(ids[0], vectors[0]);
        Assert.Multiple(() =>
        {
            Assert.That(sample.IsCompletedSuccessfully, Is.True,
                "A known identifier must not need a round trip.");
            Assert.That(sample.GetAwaiter().GetResult(), Is.True,
                "Re-embedding replaces rather than adds.");
        });

        var delta = PerIterationDelta(
            () =>
            {
                var slot = probe++ % ids.Length;
                _ = index.UpsertAsync(ids[slot], vectors[slot]).GetAwaiter().GetResult();
            },
            iterations: Iterations);

        AssertNoPerIterationAllocation(delta, Iterations, "Re-embedding a known identifier");
    }

    [Test]
    public async Task An_unchanged_flush_allocates_a_bounded_amount()
    {
        const int Iterations = 200;
        var index = await BuiltAsync();
        await index.FlushAsync();

        var delta = await PerIterationDeltaAsync(
            async () => await index.FlushAsync(),
            iterations: Iterations);

        // A flush with nothing dirty writes one manifest record: a fixed handful
        // of small objects, independent of the corpus.
        AssertBoundedPerIterationAllocation(delta, Iterations, budget: 2_048, "An unchanged flush");
    }

    [Test]
    public async Task Loading_allocates_in_proportion_to_the_corpus_rather_than_to_the_records()
    {
        // Load has to allocate - it materialises the index - so the contract is
        // that it stays within a small multiple of the vectors it retains, not
        // that it allocates nothing. A per-record copy or a buffering layer would
        // show up as a large multiple.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();
        await DurableIndexHarness.BuiltAsync(store, source, options);

        await DurableIndexHarness.OpenAsync(store, source, options);

        // The minimum across attempts, for the same reason every differential
        // measurement here takes one: a single sample can absorb unrelated
        // noise, and the cheapest load is the one that reflects the path rather
        // than the machine.
        var allocated = long.MaxValue;
        var loaded = await DurableIndexHarness.OpenAsync(store, source, options);
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var before = GC.GetTotalAllocatedBytes(precise: true);
            loaded = await DurableIndexHarness.OpenAsync(store, source, options);
            allocated = Math.Min(allocated, GC.GetTotalAllocatedBytes(precise: true) - before);
        }

        var retained = VectorIndexMemory.Bytes(
            loaded.Status.Capacity, loaded.Status.Dimensions, loaded.Status.PartitionCount);

        TestContext.Out.WriteLine(
            $"loading {loaded.Count} vectors allocated {allocated} bytes against {retained} bytes retained "
            + $"({(double)allocated / retained:F2}x)");

        Assert.That(allocated, Is.LessThan(retained * 6),
            "Loading must not copy the corpus several times over on its way into the index.");
    }

    [Test]
    public async Task A_lazy_search_over_resident_cells_allocates_a_bounded_amount()
    {
        const int Iterations = 500;
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();
        await DurableIndexHarness.BuiltAsync(store, source, options);

        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);
        var results = new VectorSearchResult[10];
        var query = source[DurableIndexHarness.Id(1)];

        // Warm every cell this query touches, so the measurement is of the
        // steady-state path rather than of the fetch.
        await lazy.SearchAsync(query, results);

        var delta = await PerIterationDeltaAsync(
            async () => await lazy.SearchAsync(query, results),
            iterations: Iterations);

        // With a full-size warm-up and the minimum taken across attempts this
        // measures zero: the probe scratch is pooled, and an asynchronous method
        // that completes without ever suspending does not box its state machine.
        // The budget is kept small rather than zero only because the
        // process-wide counter this path must use can see unrelated threads.
        AssertBoundedPerIterationAllocation(delta, Iterations, budget: 64, "A warm lazy search");
    }
}
