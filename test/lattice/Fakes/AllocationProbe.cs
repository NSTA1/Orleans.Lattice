namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Shared harness for allocation probes. Every failure mode of an allocation
/// test is a <b>false negative that looks like a passing test</b>, so the
/// discipline is encoded here once rather than re-derived per probe.
/// <para>
/// Three properties are load-bearing, and all three exist because each has
/// already produced a green test over a defect in this repository:
/// </para>
/// <list type="number">
/// <item><description>
/// <b>Differential, never absolute.</b> The same work is measured at two sizes
/// and only the <em>growth</em> between them is reported. A one-off tiered-JIT
/// or on-stack-replacement cost lands in both samples and cancels, whereas a
/// genuine per-iteration allocation scales with the size and survives. An
/// absolute assertion against a GC counter passes alone and fails in a larger
/// batch, because whether the runtime recompiles inside the measured window
/// depends on what the shared test host already compiled.
/// </description></item>
/// <item><description>
/// <b>Warm-up at full size.</b> The measured delegates are exercised at the
/// <em>large</em> size before any sample is taken, so both samples observe
/// fully-tiered code. Warming at a smaller size leaves the first real sample
/// paying a compilation cost the second one does not.
/// </description></item>
/// <item><description>
/// <b>Minimum across attempts, never a short-circuit.</b> The growth is
/// sampled repeatedly and the <em>minimum</em> is kept (clamped at zero).
/// Returning early on the first non-positive difference is correct for an
/// allocation-free loop but reports a genuinely allocating loop as clean the
/// moment one noisy sample lets the small window absorb more than the large
/// one.
/// </description></item>
/// </list>
/// <para>
/// Counter choice is a fourth rule and is exposed as
/// <c>crossesThreads</c>: <see cref="GC.GetAllocatedBytesForCurrentThread"/>
/// excludes other threads' noise and gives the tightest differential, but it
/// only means anything on a path that never awaits, because a continuation can
/// resume on another thread and leave the per-thread counter reporting
/// nonsense. Anything that awaits or touches the thread pool must measure with
/// <see cref="GC.GetTotalAllocatedBytes(bool)"/>.
/// </para>
/// </summary>
internal static class AllocationProbe
{
    /// <summary>
    /// Escape hatch for the battery tests that prove this harness can actually
    /// fail. Storing a reference here is a <b>definite escape at every JIT
    /// tier</b>, which is the point: an allocation that does not escape is
    /// removed outright by escape analysis, and a battery test whose allocation
    /// is elided truthfully reports zero and becomes the exact false negative it
    /// exists to prevent.
    /// <para>
    /// This field is load-bearing. Do not "simplify" a battery test to
    /// something like <c>sink += new long[1].Length</c>: a freshly allocated
    /// constant-size array never escapes and its length folds to a constant, so
    /// the allocation disappears entirely.
    /// </para>
    /// </summary>
    internal static object? EscapeSink;

    /// <summary>
    /// Non-allocating sink for a battery test that proves the harness does not
    /// simply always report growth.
    /// </summary>
    internal static long ScalarSink;

    /// <summary>
    /// Measures how much the allocation of <paramref name="measure"/> grows
    /// between <paramref name="smallSize"/> and <paramref name="largeSize"/>,
    /// returning the minimum growth observed across
    /// <paramref name="attempts"/> samples, clamped at zero.
    /// <para>
    /// <paramref name="prepare"/> builds whatever state the work needs and runs
    /// <em>outside</em> the measured window, so fixture setup is never charged
    /// to the result. <paramref name="measure"/> performs the work for the given
    /// size and is the only thing measured. The caller is responsible for
    /// defeating dead-code elimination inside <paramref name="measure"/> (store
    /// to <see cref="ScalarSink"/>, or accumulate into a value the loop reads
    /// back).
    /// </para>
    /// <para>
    /// A zero return means "no growth was observed at any attempt", which is the
    /// evidence that the work allocates nothing per unit of size. It does not
    /// mean the work allocates nothing at all: a fixed set-up cost inside the
    /// measured window appears in both samples and cancels by design.
    /// </para>
    /// </summary>
    /// <typeparam name="TState">State the measured work runs against.</typeparam>
    /// <param name="prepare">Builds the state for a given size, outside the measured window.</param>
    /// <param name="measure">Performs the work for a given size, inside the measured window.</param>
    /// <param name="smallSize">The smaller of the two sizes to compare.</param>
    /// <param name="largeSize">The larger of the two sizes to compare. Must exceed <paramref name="smallSize"/>.</param>
    /// <param name="attempts">How many times to sample the pair. Defaults to 5.</param>
    /// <param name="crossesThreads">
    /// <see langword="true"/> when the work awaits or touches the thread pool, selecting the
    /// process-wide counter. Defaults to <see langword="false"/>, the per-thread counter.
    /// </param>
    internal static long Growth<TState>(
        Func<int, TState> prepare,
        Action<TState, int> measure,
        int smallSize,
        int largeSize,
        int attempts = 5,
        bool crossesThreads = false)
    {
        ArgumentNullException.ThrowIfNull(prepare);
        ArgumentNullException.ThrowIfNull(measure);
        ArgumentOutOfRangeException.ThrowIfLessThan(smallSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(largeSize, smallSize);
        ArgumentOutOfRangeException.ThrowIfLessThan(attempts, 1);

        // Warm up at the LARGE size, and warm the small path too, so both
        // samples below observe fully-tiered code and neither pays a
        // compilation cost the other does not.
        for (var i = 0; i < 3; i++)
        {
            measure(prepare(largeSize), largeSize);
            measure(prepare(smallSize), smallSize);
        }

        var minimum = long.MaxValue;
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            var small = Sample(prepare, measure, smallSize, crossesThreads);
            var large = Sample(prepare, measure, largeSize, crossesThreads);

            // Keep the minimum rather than returning on the first non-positive
            // difference: a single noisy sample must not be able to certify a
            // genuinely allocating loop as clean.
            minimum = Math.Min(minimum, large - small);
        }

        return Math.Max(0L, minimum);
    }

    private static long Sample<TState>(
        Func<int, TState> prepare, Action<TState, int> measure, int size, bool crossesThreads)
    {
        var state = prepare(size);
        var before = crossesThreads ? GC.GetTotalAllocatedBytes(precise: true) : GC.GetAllocatedBytesForCurrentThread();
        measure(state, size);
        var after = crossesThreads ? GC.GetTotalAllocatedBytes(precise: true) : GC.GetAllocatedBytesForCurrentThread();
        GC.KeepAlive(state);
        return after - before;
    }
}
