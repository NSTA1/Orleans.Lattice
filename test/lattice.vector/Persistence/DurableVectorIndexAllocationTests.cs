using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Allocation contracts for the durable index's load, query, and update paths.
/// <para>
/// Every <b>synchronous</b> figure is a <b>differential</b> measurement: the
/// same path runs at two loop sizes after a warm-up and the assertion is on the
/// difference. A one-off runtime cost - tiered JIT, on-stack replacement landing
/// inside the window, a pool's first rent - appears in both measurements and
/// cancels, while a genuine per-iteration allocation scales with the loop and
/// survives. An absolute "allocated zero bytes" assertion cannot tell those
/// apart, so it passes in isolation and fails in a larger batch where the shared
/// test host has already compiled a different set of methods. That failure mode
/// has cost this repository real rework twice, so it is designed out here rather
/// than tuned around.
/// </para>
/// <para>
/// The synchronous query and update paths are measured with the per-thread
/// counter, which excludes unrelated threads' noise. Anything that awaits is
/// measured with the process-wide precise counter instead, because a
/// continuation may resume on a different thread and a per-thread figure would
/// then be meaningless rather than merely noisy.
/// </para>
/// <para>
/// The process-wide counter changes which aggregation is sound, so the
/// asynchronous probe is <b>not</b> differential. Other threads' traffic is a
/// noise floor that can only ever <i>add</i> to a sample, and a difference of
/// two such samples inherits that noise with either sign: a spike in the
/// narrower window drives the difference below the truth, and a minimum over
/// attempts then selects exactly that attempt. The asynchronous probe instead
/// reports the least an absolute window of the loop allocated. The counter is
/// monotonic and every byte the loop allocates lands inside the window, so each
/// sample - and therefore their minimum - is an upper bound on what the loop
/// allocated that noise cannot pull below the truth. It suits the upper-bound
/// budgets it serves: it can over-report, which a budget turns into a visible
/// red, and it cannot under-report, which would turn a budget into a silent
/// pass. See issue #3419.
/// </para>
/// <para>
/// Every assertion here about an <b>asynchronous</b> path presupposes an
/// <b>optimized</b> build. Roslyn emits an async state machine as a struct
/// under <c>&lt;Optimize&gt;</c> and as a class without it, so a Debug build
/// heap-allocates one per call regardless of whether the method suspends. That
/// cost is deterministic and per-iteration, so it defeats every defence above:
/// it scales with the loop and so survives the differential, it occurs on every
/// attempt and so survives the minimum, and it is a compilation decision and so
/// survives the warm-up. It then presents as a clean bimodal split - one
/// developer measuring zero, another a large constant, CI green throughout -
/// which reads as a hardware difference and invites a hunt through the SIMD
/// paths for an allocating fallback that does not exist. That hunt is what
/// issue #2540 actually was.
/// </para>
/// <para>
/// Only the flush and load measurements are exposed to that cost, and both
/// carry a budget large enough to absorb it. The warm lazy search does not:
/// rather than asserting the precondition and skipping the test in every build
/// that cannot meet it - which removes the red but leaves the path unmeasured
/// in the configuration most contributors run - the production path answers a
/// fully resident query before entering an asynchronous frame, so there is no
/// state machine to allocate in either configuration. That test therefore uses
/// the synchronous probe, asserts zero rather than a tolerance, and binds
/// identically in Debug and Release. See issue #2450.
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
    /// The number of times each measurement is repeated. The <b>minimum</b>
    /// across attempts is kept, never the first sample and never a short circuit
    /// on the first non-positive one. For the synchronous differential probe the
    /// per-thread counter has no other-thread noise floor, so a clean loop's
    /// minimum picks the least disturbed attempt. For the asynchronous probe
    /// every sample is an upper bound on the loop's own allocation, so the
    /// minimum is the tightest upper bound on offer and still cannot fall below
    /// the truth.
    /// </summary>
    private const int Attempts = 5;

    /// <summary>
    /// The smallest heap object the runtime can allocate: a header, a method
    /// table pointer, and one pointer-sized payload slot. It is a lower bound on
    /// what a planted <c>new object()</c> costs on any platform, so a probe that
    /// reports less than this per planted allocation has provably under-reported.
    /// </summary>
    private static readonly long MinimumObjectBytes = 3L * IntPtr.Size;

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

    private static Task<long> AllocatedOverLoopUpperBoundAsync(Func<ValueTask> action, int iterations)
        => AllocatedOverLoopUpperBoundAsync(action, iterations, ProcessWideAllocatedBytes);

    /// <summary>
    /// The asynchronous probe: the least any of <see cref="Attempts"/> windows of
    /// <paramref name="iterations"/> runs allocated, as read from
    /// <paramref name="allocatedBytes"/>. Each window is an absolute reading of a
    /// monotonic counter, so it contains every byte the loop allocated plus
    /// whatever other threads allocated meanwhile, and the minimum is therefore
    /// an upper bound that noise can only raise. It deliberately does not
    /// subtract one noisy window from another, and it does not clamp: a
    /// negative figure can only come from a counter that ran backwards, which
    /// is a broken instrument and is refused rather than reported as zero.
    /// </summary>
    private static async Task<long> AllocatedOverLoopUpperBoundAsync(
        Func<ValueTask> action, int iterations, Func<long> allocatedBytes)
    {
        // Full-size warm-up, as in the synchronous probe, so tiering and
        // on-stack replacement have settled before any window is read. A
        // one-off that still lands in a window only raises that window, and the
        // minimum across attempts discards it.
        for (var i = 0; i < iterations * 2; i++)
        {
            await action();
        }

        var least = long.MaxValue;
        for (var attempt = 0; attempt < Attempts; attempt++)
        {
            least = Math.Min(least, await AllocatedOverLoopAsync(action, iterations, allocatedBytes));
        }

        if (least < 0)
        {
            throw new InvalidOperationException(
                $"The allocation counter ran backwards ({least} bytes over a window), so it cannot "
                + "bound anything. Refusing to report a figure rather than clamping it to zero.");
        }

        return least;
    }

    private static async Task<long> AllocatedOverLoopAsync(
        Func<ValueTask> action, int iterations, Func<long> allocatedBytes)
    {
        var before = allocatedBytes();
        for (var i = 0; i < iterations; i++)
        {
            await action();
        }

        return allocatedBytes() - before;
    }

    // The process-wide precise counter, because an awaited path may resume on a
    // different thread and the per-thread counter would then report a figure
    // that is not merely noisy but wrong.
    private static long ProcessWideAllocatedBytes() => GC.GetTotalAllocatedBytes(precise: true);

    /// <summary>
    /// The aggregation issue #3419 replaced, retained only as the known-positive
    /// control that proves the adversarial noise schedules below have teeth: the
    /// minimum of (doubled window - single window) differences, clamped at zero.
    /// Never use it to measure anything.
    /// </summary>
    private static async Task<long> MinimumOfClampedDifferencesAsync(
        Func<ValueTask> action, int iterations, Func<long> allocatedBytes)
    {
        for (var i = 0; i < iterations * 2; i++)
        {
            await action();
        }

        var best = long.MaxValue;
        for (var attempt = 0; attempt < Attempts; attempt++)
        {
            var single = await AllocatedOverLoopAsync(action, iterations, allocatedBytes);
            var doubled = await AllocatedOverLoopAsync(action, iterations * 2, allocatedBytes);
            best = Math.Min(best, doubled - single);
        }

        return Math.Max(0, best);
    }

    /// <summary>
    /// A process-wide allocation counter with scripted additive noise, standing
    /// in for other threads allocating while a window is open. Noise is only
    /// ever added and the running total never decreases, which is the one
    /// property real other-thread traffic is guaranteed to have, so any probe
    /// it defeats is defeated by a noise pattern the real counter can produce.
    /// </summary>
    private sealed class NoisyAllocationCounter(int period, int phase, long spikeBytes)
    {
        private long _reads;
        private long _noise;

        /// <summary>The number of reads that carried a noise spike.</summary>
        public int Spikes { get; private set; }

        /// <summary>The number of times the counter was read.</summary>
        public long Reads => _reads;

        /// <summary>
        /// Reads the real process-wide counter, first adding a spike of noise
        /// when this read falls on the schedule, so the spike lands in whichever
        /// window this read closes.
        /// </summary>
        public long Read()
        {
            if (_reads++ % period == phase)
            {
                _noise += spikeBytes;
                Spikes++;
            }

            return ProcessWideAllocatedBytes() + _noise;
        }
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
        TestContext.Out.WriteLine($"{what}: at most {perRun:F1} bytes per run across {iterations} runs");
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
    public async Task The_asynchronous_allocation_probe_detects_a_loop_that_does_allocate()
    {
        // The same battery, for the other probe. AllocatedOverLoopUpperBoundAsync
        // is what the flush budget rests on, and until this existed nothing
        // checked that it could see an allocation at all: had it been broken,
        // that budget would have passed vacuously and reported nothing, which is
        // strictly worse than failing because a vacuous gate is invisible.
        //
        // A bare object, exactly as in the synchronous battery. The superseded
        // differential probe needed a four-kilobyte allocation here to stay
        // clear of the process-wide noise floor, because a spike in its narrower
        // window pulled its minimum to zero (#3419). The absolute probe cannot be
        // pulled below what the loop allocated, so the smallest possible object
        // is enough, and the assertion is the lower bound rather than merely
        // non-zero.
        const int Iterations = 1_000;
        var allocated = await AllocatedOverLoopUpperBoundAsync(
            () =>
            {
                _escapeSink = new object();
                return ValueTask.CompletedTask;
            },
            Iterations);

        Assert.That(allocated, Is.GreaterThanOrEqualTo(Iterations * MinimumObjectBytes),
            "The asynchronous probe reported less than the loop provably allocated. Either the "
            + "probe under-reports, or the sink stopped escaping and the JIT elided the allocation.");
    }

    [Test]
    public async Task The_asynchronous_probe_cannot_be_pulled_to_zero_by_noise_that_zeroes_the_superseded_probe()
    {
        // The regression test for #3419. A known allocation is planted - one
        // escaping object per iteration, about 24 kB across the loop - and the
        // counter is fed additive noise: a spike far larger than the signal
        // landing in the single-width window of every attempt. That is a pattern
        // real other-thread traffic can produce, and it is exactly the one the
        // superseded probe was blind to.
        const int Iterations = 1_000;
        const long Spike = 16L * 1024 * 1024;
        Func<ValueTask> planted = () =>
        {
            _escapeSink = new object();
            return ValueTask.CompletedTask;
        };

        // Known-positive control: the schedule has teeth. The superseded
        // aggregation reads four times per attempt (single before/after, doubled
        // before/after), so period 4, phase 1 lands every spike inside the
        // single-width window. Its minimum difference goes negative and the clamp
        // reports a flat zero for a loop that allocates on every iteration.
        var superseded = new NoisyAllocationCounter(period: 4, phase: 1, Spike);
        var supersededReading = await MinimumOfClampedDifferencesAsync(planted, Iterations, superseded.Read);

        // The probe the budgets actually use, under the same schedule.
        var current = new NoisyAllocationCounter(period: 4, phase: 1, Spike);
        var currentReading = await AllocatedOverLoopUpperBoundAsync(planted, Iterations, current.Read);

        Assert.Multiple(() =>
        {
            Assert.That(superseded.Spikes, Is.EqualTo(Attempts),
                "The control must have injected one spike per attempt, or it measured nothing.");
            Assert.That(supersededReading, Is.Zero,
                "The noise schedule no longer defeats the superseded probe, so this test no longer "
                + "demonstrates anything. Re-derive the schedule before trusting the assertion below.");
            Assert.That(current.Spikes, Is.GreaterThan(0),
                "No noise reached the probe under test, so the assertion below is unexercised.");
            Assert.That(currentReading, Is.GreaterThanOrEqualTo(Iterations * MinimumObjectBytes),
                "The asynchronous probe reported less than the loop provably allocated under additive "
                + "noise, so an allocation budget resting on it can pass vacuously.");
        });
    }

    [TestCase(1, 0)]
    [TestCase(2, 0)]
    [TestCase(2, 1)]
    [TestCase(3, 1)]
    public async Task The_asynchronous_probe_never_reports_below_a_planted_allocation_under_additive_noise(
        int period, int phase)
    {
        // Noise on every read, on only the reads that open a window, on only the
        // reads that close one, and out of step with the window altogether. The
        // probe must stay at or above the planted allocation in every case,
        // because additive noise on a monotonic counter can only raise an
        // absolute window.
        const int Iterations = 1_000;
        var counter = new NoisyAllocationCounter(period, phase, spikeBytes: 16L * 1024 * 1024);

        var allocated = await AllocatedOverLoopUpperBoundAsync(
            () =>
            {
                _escapeSink = new object();
                return ValueTask.CompletedTask;
            },
            Iterations,
            counter.Read);

        Assert.Multiple(() =>
        {
            Assert.That(counter.Reads, Is.EqualTo(2L * Attempts),
                "The probe must read the supplied counter twice per window, or it measured something else.");
            Assert.That(counter.Spikes, Is.GreaterThan(0),
                "No noise reached the probe, so the lower-bound assertion is unexercised.");
            Assert.That(allocated, Is.GreaterThanOrEqualTo(Iterations * MinimumObjectBytes),
                $"The probe under-reported under a period {period}, phase {phase} noise schedule.");
        });
    }

    [Test]
    public void The_asynchronous_probe_refuses_a_counter_that_runs_backwards()
    {
        // Fails closed rather than clamping: a figure below zero cannot bound
        // anything, and reporting it as zero is how a budget goes vacuous.
        var reading = 0L;

        Assert.That(
            async () => await AllocatedOverLoopUpperBoundAsync(
                () => ValueTask.CompletedTask, iterations: 10, () => reading -= 1_000),
            Throws.InvalidOperationException.With.Message.Contains("ran backwards"));
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

        var allocated = await AllocatedOverLoopUpperBoundAsync(
            async () => await index.FlushAsync(),
            iterations: Iterations);

        // A flush with nothing dirty writes one manifest record: a fixed handful
        // of small objects, independent of the corpus. The figure is an upper
        // bound (see AllocatedOverLoopUpperBoundAsync), so a regression cannot
        // hide beneath the process-wide noise floor.
        AssertBoundedPerIterationAllocation(allocated, Iterations, budget: 2_048, "An unchanged flush");
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

        // The minimum across attempts, for the same reason every measurement here
        // takes one: a single sample can absorb unrelated noise, and the cheapest
        // load is the one that reflects the path rather than the machine. Like
        // the asynchronous probe this is an absolute window of a monotonic
        // counter, so noise can only raise it and the minimum cannot fall below
        // what the load allocated (#3419).
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

        // Asserted once, outside the measured window, because an NUnit
        // constraint allocates a few hundred bytes of its own. It is also what
        // licenses the synchronous probe below: a call that never enters an
        // asynchronous frame cannot migrate threads, so the per-thread counter
        // is exact rather than merely approximate, and the budget can be zero.
        Assert.That(lazy.SearchAsync(query, results).IsCompletedSuccessfully, Is.True,
            "A fully resident lazy search must answer without entering an asynchronous frame, "
            + "or it heap-allocates a state machine per call in every unoptimized build.");

        var delta = PerIterationDelta(
            () =>
            {
                _ = lazy.SearchAsync(query, results).GetAwaiter().GetResult();
            },
            iterations: Iterations);

        // Zero, and identically so in Debug and Release. The probe scratch is
        // stack-allocated, the search writes into the caller's buffer, and the
        // fast path in DurableVectorIndex.SearchAsync answers before any async
        // frame exists - so there is no state machine whose emitted shape could
        // differ by configuration. That is the whole of issue #2450: the old
        // budget of 64 was not loose, it was measuring a frame that the
        // production path no longer creates.
        AssertNoPerIterationAllocation(delta, Iterations, "A warm lazy search");
    }
}
