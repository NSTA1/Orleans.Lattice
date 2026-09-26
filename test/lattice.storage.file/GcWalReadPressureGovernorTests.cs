namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Pins the one property the read-budget governor must never violate: the
/// budget it hands back is always a number of bytes a read can actually be
/// attempted with.
/// <para>
/// <see cref="FileWalReadPressureTests"/> covers the narrowing curve itself -
/// unchanged below the relaxed occupancy, collapsed to the floor at the
/// critical one, monotone in between. What it never exercises is a
/// <em>configured ceiling</em> that is already degenerate, because every
/// caller in that fixture supplies a sane one. The ceiling is operator
/// configuration, so zero and negative values are reachable from a host that
/// binds the option from configuration and gets it wrong, and they reach the
/// governor before any validation the storage provider applies.
/// </para>
/// <para>
/// A non-positive budget is the specific failure the type's own documentation
/// rules out: "refusing to read at all would be stable but dead", because a
/// replay that reads nothing never advances a checkpoint, and a checkpoint
/// that never advances is the pin that froze the log. A zero ceiling would
/// produce exactly that dead loop rather than a loud failure, so the clamp is
/// load-bearing and is asserted here on every arm of the narrowing rule.
/// </para>
/// </summary>
[TestFixture]
public sealed class GcWalReadPressureGovernorTests
{
    private const long OneGibibyte = 1024L * 1024L * 1024L;

    /// <summary>An occupancy below the relaxed threshold, where the ceiling is returned unchanged.</summary>
    private static long RelaxedLoad => (long)(OneGibibyte * 0.10d);

    /// <summary>An occupancy inside the interpolation band between relaxed and critical.</summary>
    private static long InterpolatedLoad => (long)(OneGibibyte * 0.80d);

    /// <summary>An occupancy at or above the critical threshold, where the floor is used.</summary>
    private static long CriticalLoad => (long)(OneGibibyte * 0.95d);

    [Test]
    public void A_non_positive_ceiling_is_clamped_before_any_narrowing_decision()
    {
        // The clamp runs first, so it holds on the no-signal arm that returns
        // the ceiling straight back without consulting occupancy at all.
        Assert.Multiple(() =>
        {
            Assert.That(
                GcWalReadPressureGovernor.NarrowCore(0L, totalAvailableBytes: 0L, memoryLoadBytes: 0L),
                Is.EqualTo(1L),
                "a zero ceiling must not be handed back as a zero budget");
            Assert.That(
                GcWalReadPressureGovernor.NarrowCore(-4096L, totalAvailableBytes: 0L, memoryLoadBytes: 0L),
                Is.EqualTo(1L),
                "a negative ceiling must not be handed back as a negative budget");
        });
    }

    [Test]
    public void A_non_positive_ceiling_stays_positive_on_every_arm_of_the_narrowing_rule()
    {
        // Each arm computes its result differently: the relaxed arm returns the
        // ceiling, the critical arm returns the floor, and the band interpolates
        // between them. A clamp that only protected one of them would leave the
        // other two able to return zero.
        Assert.Multiple(() =>
        {
            Assert.That(
                GcWalReadPressureGovernor.NarrowCore(0L, OneGibibyte, RelaxedLoad),
                Is.EqualTo(1L),
                "the relaxed arm returns the ceiling, which must already be clamped");
            Assert.That(
                GcWalReadPressureGovernor.NarrowCore(0L, OneGibibyte, InterpolatedLoad),
                Is.EqualTo(1L),
                "the interpolated arm must not narrow a clamped ceiling below one byte");
            Assert.That(
                GcWalReadPressureGovernor.NarrowCore(0L, OneGibibyte, CriticalLoad),
                Is.EqualTo(1L),
                "the critical arm returns the floor, which is bounded by the clamped ceiling");
        });
    }

    [Test]
    public void A_negative_memory_load_is_treated_as_no_signal_rather_than_as_pressure()
    {
        // Reading a negative load would make occupancy negative, which compares
        // below the relaxed threshold and so happens to return the ceiling
        // anyway. That is the right answer by accident, not by construction, so
        // the explicit no-signal arm is what is pinned here.
        const long Configured = 16L * 1024L * 1024L;

        Assert.That(
            GcWalReadPressureGovernor.NarrowCore(Configured, OneGibibyte, memoryLoadBytes: -1L),
            Is.EqualTo(Configured),
            "an unusable reading must never invent pressure");
    }

    [Test]
    public void The_budget_is_never_zero_or_negative_for_any_ceiling_or_occupancy()
    {
        // The invariant the clamp exists to guarantee, swept rather than spot
        // checked: a dead budget is the one outcome that cannot be recovered
        // from inside the replay loop.
        long[] ceilings = [long.MinValue, -1L, 0L, 1L, 4096L, 16L * 1024L * 1024L];

        Assert.Multiple(() =>
        {
            foreach (var ceiling in ceilings)
            {
                for (var percent = 0; percent <= 100; percent += 5)
                {
                    var load = (long)(OneGibibyte * (percent / 100d));
                    var narrowed = GcWalReadPressureGovernor.NarrowCore(ceiling, OneGibibyte, load);

                    Assert.That(
                        narrowed,
                        Is.GreaterThan(0L),
                        $"ceiling {ceiling} at {percent}% occupancy produced a budget no read can use");
                }
            }
        });
    }

    [Test]
    public void The_live_reading_stays_within_the_bounds_the_pure_core_guarantees()
    {
        // NarrowBudget is the only member that reads the real GC, so it is the
        // only place the pure core can be wired to the wrong fields. Asserting
        // the bounds rather than a fixed value keeps it deterministic on any
        // host: whatever this machine's occupancy is, the answer must be a
        // usable budget that never exceeds what was asked for.
        const long Configured = 16L * 1024L * 1024L;
        var narrowed = GcWalReadPressureGovernor.Instance.NarrowBudget(Configured);

        Assert.Multiple(() =>
        {
            Assert.That(narrowed, Is.GreaterThan(0L), "the live reading must never produce a dead budget");
            Assert.That(narrowed, Is.LessThanOrEqualTo(Configured), "narrowing must never widen the configured ceiling");
            Assert.That(
                narrowed,
                Is.GreaterThanOrEqualTo(GcWalReadPressureGovernor.MinimumBudgetBytes),
                "a ceiling above the floor can never narrow below it");
        });
    }

    [Test]
    public void Allocate_returns_a_buffer_of_exactly_the_requested_size()
    {
        // The allocation seam exists so a test can script a failure; the
        // production implementation must still be an ordinary exact-size array,
        // because the read path slices it by the count it asked for.
        var buffer = GcWalReadPressureGovernor.Instance.Allocate(4096);

        Assert.That(buffer, Has.Length.EqualTo(4096));
    }
}
