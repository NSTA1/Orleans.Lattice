using System.Diagnostics;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2685: a batched write whose stages are each
/// individually inside the caller's deadline, and which breaches it only by
/// <em>summing</em>.
/// <para>
/// <b>The point of this fixture is the thing it refuses to assert.</b> The
/// incident measured a <c>gate</c> of 4,108.96 ms and a <c>fanout</c> of
/// 26,709.17 ms against a 30,000 ms Orleans response timeout. Every
/// single-stage assertion anyone could write against that deadline
/// <em>passes</em>: 4.1 s is inside 30 s and 26.7 s is inside 30 s. A guard
/// built on one stage is therefore vacuous by construction - it passes before
/// the regression and it passes after it, because no single stage is ever the
/// thing that breaches. Only the running total moves, so the total is what
/// these tests assert against, and
/// <see cref="Envelope_breaches_on_the_sum_while_neither_stage_breaches_alone"/>
/// pins both halves of that claim together so the second cannot quietly be
/// dropped.
/// </para>
/// <para>
/// Every test here is deterministic and sleeps for nothing. The budget takes
/// its start stamp as a parameter (production passes the one the caller-visible
/// envelope metric already starts at), so elapsed time is <em>synthesised</em>
/// by handing it a <see cref="Stopwatch"/> stamp from the past. That keeps the
/// assertions exact and immune to how loaded the host happens to be, which
/// matters for a fixture about timing above all others.
/// </para>
/// </summary>
[TestFixture]
public sealed class WriteEnvelopeBudgetTests
{
    /// <summary>
    /// A <see cref="Stopwatch"/> timestamp <paramref name="elapsed"/> in the
    /// past, so a budget constructed from it behaves exactly as one that had
    /// genuinely been running that long.
    /// </summary>
    private static long StampAgo(TimeSpan elapsed)
        => Stopwatch.GetTimestamp() - (long)(elapsed.TotalSeconds * Stopwatch.Frequency);

    /// <summary>Tolerance for the microseconds the test itself takes to run.</summary>
    private static readonly TimeSpan Slack = TimeSpan.FromMilliseconds(250);

    [Test]
    public void Start_returns_null_for_an_infinite_budget()
    {
        Assert.That(WriteEnvelopeBudget.Start(Timeout.InfiniteTimeSpan), Is.Null,
            "An unbounded budget must arm nothing and allocate nothing. It is the default, so "
            + "an existing deployment has to keep its historical behaviour exactly.");
    }

    [Test]
    public void Start_returns_null_for_a_non_positive_budget()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WriteEnvelopeBudget.Start(TimeSpan.Zero), Is.Null);
            Assert.That(WriteEnvelopeBudget.Start(TimeSpan.FromSeconds(-1)), Is.Null);
        });

        // A misconfigured option must degrade to unbounded rather than to a
        // budget that refuses every write on arrival.
    }

    [Test]
    public void Remaining_is_floored_at_zero_once_the_budget_is_spent()
    {
        var budget = WriteEnvelopeBudget.Start(
            TimeSpan.FromSeconds(1), StampAgo(TimeSpan.FromSeconds(5)))!;

        Assert.Multiple(() =>
        {
            Assert.That(budget.Remaining, Is.EqualTo(TimeSpan.Zero),
                "An overrun must not present as a negative remaining budget. Task.WaitAsync "
                + "rejects a negative timeout, so a negative here would turn the overrun into "
                + "an ArgumentOutOfRangeException instead of the attributed refusal.");
            Assert.That(budget.IsSpent, Is.True);
        });
    }

    [Test]
    public void Envelope_breaches_on_the_sum_while_neither_stage_breaches_alone()
    {
        // The #2685 shape, scaled down but in the same proportions: a gate at
        // roughly 13% of the envelope and a fan-out at roughly 89%, summing
        // past it. The two single-stage assertions are the control, and they
        // are the whole reason this test exists - they are what a per-stage
        // guard would have asserted, and they PASS, which is precisely why such
        // a guard cannot detect this defect.
        var envelopeBudget = TimeSpan.FromSeconds(30);
        var gate = TimeSpan.FromSeconds(4.1);
        var fanOut = TimeSpan.FromSeconds(26.7);

        Assert.Multiple(() =>
        {
            Assert.That(gate, Is.LessThan(envelopeBudget),
                "Control: the gate alone is inside the budget, so a gate-only guard passes.");
            Assert.That(fanOut, Is.LessThan(envelopeBudget),
                "Control: the fan-out alone is inside the budget, so a fan-out-only guard "
                + "passes. This is the assertion LatticeOptions.SetManyFanOutBudget makes, and "
                + "it is why that option alone cannot see this breach.");
            Assert.That(gate + fanOut, Is.GreaterThan(envelopeBudget),
                "...and yet the sum breaches. Only a bound on the total observes it.");
        });

        // Now drive the budget through those two stages and assert it agrees.
        var budget = WriteEnvelopeBudget.Start(envelopeBudget, StampAgo(gate))!;
        budget.RecordGate(gate.TotalMilliseconds);

        var afterGate = WriteEnvelopeBudget.ResolveFanOutWait(budget, Timeout.InfiniteTimeSpan);
        Assert.That(afterGate.Duration, Is.LessThan(fanOut).And.GreaterThan(TimeSpan.Zero),
            "The fan-out must be granted only what the gate left, not a fresh full window. "
            + "Granting it the whole budget again is the additive breach in one line.");
        Assert.That(afterGate.BoundByEnvelope, Is.True);
    }

    [Test]
    public void ResolveFanOutWait_narrows_the_fan_out_budget_by_what_earlier_stages_spent()
    {
        // Envelope of 30 s with 25 s already spent upstream, against a fan-out
        // budget of 30 s. The fan-out budget on its own would hand this call a
        // further 30 s - 55 s in total against a 30 s deadline.
        var budget = WriteEnvelopeBudget.Start(
            TimeSpan.FromSeconds(30), StampAgo(TimeSpan.FromSeconds(25)))!;

        var wait = WriteEnvelopeBudget.ResolveFanOutWait(budget, TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(wait.Duration, Is.EqualTo(TimeSpan.FromSeconds(5)).Within(Slack),
                "The effective wait must be the envelope's remainder, not the fan-out's own "
                + "budget, whenever the remainder is narrower.");
            Assert.That(wait.BoundByEnvelope, Is.True,
                "A refusal under this wait is an envelope breach, and must be attributed to "
                + "the envelope so the operator is not sent after a fan-out that was healthy.");
            Assert.That(wait.IsArmed, Is.True);
        });
    }

    [Test]
    public void ResolveFanOutWait_prefers_the_fan_out_budget_when_it_is_narrower()
    {
        var budget = WriteEnvelopeBudget.Start(
            TimeSpan.FromSeconds(60), StampAgo(TimeSpan.FromSeconds(1)))!;

        var wait = WriteEnvelopeBudget.ResolveFanOutWait(budget, TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(wait.Duration, Is.EqualTo(TimeSpan.FromSeconds(5)),
                "The narrower of the two bounds wins, so adding an envelope budget never "
                + "relaxes a fan-out ceiling an operator had already set.");
            Assert.That(wait.BoundByEnvelope, Is.False,
                "The fan-out's own budget fired, so the pre-existing SetManyFanOut contract "
                + "must be preserved unchanged.");
        });
    }

    [Test]
    public void ResolveFanOutWait_is_unbounded_when_neither_bound_is_armed()
    {
        var wait = WriteEnvelopeBudget.ResolveFanOutWait(null, Timeout.InfiniteTimeSpan);

        Assert.Multiple(() =>
        {
            Assert.That(wait.Duration, Is.EqualTo(Timeout.InfiniteTimeSpan));
            Assert.That(wait.IsArmed, Is.False,
                "With neither budget configured the fan-out must be awaited exactly as it was "
                + "before #2685, so the default deployment is untouched.");
        });
    }

    [Test]
    public void ResolveFanOutWait_passes_the_fan_out_budget_through_when_no_envelope_is_armed()
    {
        var wait = WriteEnvelopeBudget.ResolveFanOutWait(null, TimeSpan.FromSeconds(7));

        Assert.Multiple(() =>
        {
            Assert.That(wait.Duration, Is.EqualTo(TimeSpan.FromSeconds(7)));
            Assert.That(wait.BoundByEnvelope, Is.False);
        });
    }

    [Test]
    public void Describe_names_every_stage_so_a_refusal_is_self_diagnosing()
    {
        var budget = WriteEnvelopeBudget.Start(
            TimeSpan.FromSeconds(30), StampAgo(TimeSpan.FromSeconds(10)))!;
        budget.RecordGate(4108.96);
        budget.RecordRoute(0.64);
        budget.RecordBucket(0.02);
        budget.RecordFanOut(26709.17);

        var description = budget.Describe();

        Assert.Multiple(() =>
        {
            Assert.That(description, Does.Contain("gate=4109.0ms"),
                "The gate is the stage that degraded 12,085x in #2685 while carrying only 13% "
                + "of the absolute time. A breakdown that omits it ranks by magnitude, finds "
                + "the fan-out, and sends the investigation to the stage that did not move.");
            Assert.That(description, Does.Contain("route=0.6ms"));
            Assert.That(description, Does.Contain("bucket=0.0ms"));
            Assert.That(description, Does.Contain("fan-out=26709.2ms"));
            Assert.That(description, Does.Contain("30000ms envelope budget"),
                "The budget the stages are being measured against has to appear alongside "
                + "them, or the breakdown cannot be read as a breach.");
        });
    }

    [Test]
    public void Stage_contributions_accumulate_across_stale_routing_retries()
    {
        // SetManyAsyncCore is re-entered by RetryOnStaleRoutingAsync, so a
        // retried call records route/bucket/fan-out more than once. The
        // breakdown must report the work actually done rather than only the
        // last attempt's, or a retried breach under-reports its own cost.
        var budget = WriteEnvelopeBudget.Start(TimeSpan.FromSeconds(30))!;
        budget.RecordFanOut(1000);
        budget.RecordFanOut(2500);

        Assert.That(budget.Describe(), Does.Contain("fan-out=3500.0ms"));
    }
}
