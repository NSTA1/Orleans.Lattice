using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Battery tests for <see cref="AllocationProbe"/> - the smoke-detector's own
/// test battery.
/// <para>
/// An allocation probe that cannot fail silently approves the regression it
/// exists to catch, and every failure mode of one looks exactly like a passing
/// test. So the harness is proved from both sides here: it must report growth
/// for a loop that provably allocates, and must report none for one that does
/// not. Neither assertion is meaningful without the other - a harness wired to
/// return zero passes the first half's negation, and one wired to return a
/// large number passes the second's.
/// </para>
/// </summary>
[TestFixture]
public sealed class AllocationProbeTests
{
    [Test]
    public void The_probe_detects_an_allocation_that_provably_escapes()
    {
        var growth = AllocationProbe.Growth(
            static _ => 0,
            static (_, iterations) =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    // LOAD-BEARING: the reference is stored to a static field,
                    // which is a definite escape at every JIT tier. Do not
                    // "simplify" this to a local, or to something with a
                    // constant-folding surface such as `new long[1].Length` -
                    // escape analysis removes such an allocation outright and
                    // this battery test becomes the very false negative it
                    // exists to prevent. Verified: swapping this body for
                    // `sink += new long[1].Length` and running under
                    // DOTNET_TieredCompilation=0 reports "Expected: greater
                    // than 0, But was: 0".
                    AllocationProbe.EscapeSink = new object();
                }
            },
            smallSize: 1_000,
            largeSize: 2_000);

        Assert.That(growth, Is.GreaterThan(0L),
            "a probe that cannot observe a provably-escaping allocation silently approves every regression "
            + "it exists to catch");
        Assert.That(growth, Is.GreaterThanOrEqualTo(1_000 * 8L),
            "the growth must track the extra 1,000 object headers, not merely be non-zero noise");
    }

    [Test]
    public void The_probe_reports_no_growth_for_a_loop_that_does_not_allocate()
    {
        var growth = AllocationProbe.Growth(
            static _ => 0,
            static (_, iterations) =>
            {
                long sum = 0;
                for (var i = 0; i < iterations; i++)
                {
                    sum += i;
                }

                // Stored so the loop cannot be elided; a long written to a
                // static field allocates nothing.
                AllocationProbe.ScalarSink = sum;
            },
            smallSize: 1_000,
            largeSize: 2_000);

        Assert.That(growth, Is.Zero,
            "a harness that always reports growth would make every probe vacuously pass its own battery");
    }

    [Test]
    public void The_probe_cancels_a_fixed_cost_that_does_not_scale_with_size()
    {
        // A set-up allocation of the same size in both samples is not a
        // per-iteration cost and must cancel; only growth is reported.
        var growth = AllocationProbe.Growth(
            static _ => 0,
            static (_, iterations) =>
            {
                AllocationProbe.EscapeSink = new byte[4096];
                long sum = 0;
                for (var i = 0; i < iterations; i++)
                {
                    sum += i;
                }

                AllocationProbe.ScalarSink = sum;
            },
            smallSize: 1_000,
            largeSize: 2_000);

        Assert.That(growth, Is.Zero,
            "a one-off cost lands in both samples and must cancel, which is exactly why the assertion is "
            + "differential rather than absolute");
    }

    [Test]
    public void The_probe_does_not_charge_set_up_performed_outside_the_measured_window()
    {
        var growth = AllocationProbe.Growth(
            static size =>
            {
                // Scales with size, but happens in `prepare` - outside the
                // window - so it must not appear in the result.
                AllocationProbe.EscapeSink = new byte[size];
                return size;
            },
            static (_, iterations) =>
            {
                long sum = 0;
                for (var i = 0; i < iterations; i++)
                {
                    sum += i;
                }

                AllocationProbe.ScalarSink = sum;
            },
            smallSize: 1_000,
            largeSize: 100_000);

        Assert.That(growth, Is.Zero);
    }

    [Test]
    public void Growth_validates_its_arguments()
    {
        Assert.Throws<ArgumentNullException>(
            () => AllocationProbe.Growth<int>(null!, static (_, _) => { }, 1, 2));
        Assert.Throws<ArgumentNullException>(
            () => AllocationProbe.Growth<int>(static _ => 0, null!, 1, 2));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => AllocationProbe.Growth(static _ => 0, static (_, _) => { }, 0, 2));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => AllocationProbe.Growth(static _ => 0, static (_, _) => { }, 10, 10),
            "the two sizes must differ, or the difference carries no information");
        Assert.Throws<ArgumentOutOfRangeException>(
            () => AllocationProbe.Growth(static _ => 0, static (_, _) => { }, 1, 2, attempts: 0));
    }
}
