namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Deterministic coverage for <see cref="ScaleInGate"/>: eligibility must persist
/// continuously for the whole window before scale-in is permitted, and any break
/// resets the window.
/// </summary>
[TestFixture]
public sealed class ScaleInGateTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(120);

    [Test]
    public void Ineligible_tick_never_permits_scale_in()
    {
        var gate = new ScaleInGate();

        Assert.That(gate.Evaluate(eligible: false, T0, Window), Is.False);
    }

    [Test]
    public void Eligible_but_before_window_elapses_does_not_permit_scale_in()
    {
        var gate = new ScaleInGate();
        gate.Evaluate(eligible: true, T0, Window);

        var result = gate.Evaluate(eligible: true, T0.AddSeconds(119), Window);

        Assert.That(result, Is.False);
    }

    [Test]
    public void Eligible_for_the_full_window_permits_scale_in()
    {
        var gate = new ScaleInGate();
        gate.Evaluate(eligible: true, T0, Window);

        var result = gate.Evaluate(eligible: true, T0.AddSeconds(120), Window);

        Assert.That(result, Is.True);
    }

    [Test]
    public void A_break_in_eligibility_resets_the_window()
    {
        var gate = new ScaleInGate();
        gate.Evaluate(eligible: true, T0, Window);
        // Break eligibility at t+60 -> window resets.
        gate.Evaluate(eligible: false, T0.AddSeconds(60), Window);
        // Re-eligible at t+61; only 119s later (t+180) is still short of the window.
        gate.Evaluate(eligible: true, T0.AddSeconds(61), Window);

        var result = gate.Evaluate(eligible: true, T0.AddSeconds(180), Window);

        Assert.That(result, Is.False);
    }

    [Test]
    public void Eligible_since_tracks_the_start_of_the_current_streak()
    {
        var gate = new ScaleInGate();
        gate.Evaluate(eligible: true, T0, Window);

        Assert.That(gate.EligibleSince, Is.EqualTo(T0));

        gate.Evaluate(eligible: false, T0.AddSeconds(10), Window);

        Assert.That(gate.EligibleSince, Is.Null);
    }
}
