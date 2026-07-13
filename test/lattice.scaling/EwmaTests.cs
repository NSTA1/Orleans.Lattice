namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Deterministic coverage for <see cref="Ewma"/>: seeding, the half-life
/// smoothing factor, timestamp-driven decay, and the fast-attack
/// <see cref="Ewma.Set"/> reset. All timing is supplied as explicit
/// <see cref="DateTimeOffset"/> values so there is no wall-clock dependence.
/// </summary>
[TestFixture]
public sealed class EwmaTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Test]
    public void First_update_seeds_to_the_sample_exactly()
    {
        var ewma = new Ewma();

        var result = ewma.Update(1.0, T0, TimeSpan.FromSeconds(10));

        Assert.That(result, Is.EqualTo(1.0));
    }

    [Test]
    public void Current_is_zero_before_first_update()
    {
        Assert.That(new Ewma().Current, Is.Zero);
    }

    [Test]
    public void One_half_life_step_moves_halfway_to_the_new_sample()
    {
        var ewma = new Ewma();
        var halfLife = TimeSpan.FromSeconds(10);
        ewma.Update(1.0, T0, halfLife);

        var result = ewma.Update(0.0, T0.AddSeconds(10), halfLife);

        Assert.That(result, Is.EqualTo(0.5).Within(1e-9));
    }

    [Test]
    public void Two_half_life_steps_decay_geometrically()
    {
        var ewma = new Ewma();
        var halfLife = TimeSpan.FromSeconds(10);
        ewma.Update(1.0, T0, halfLife);
        ewma.Update(0.0, T0.AddSeconds(10), halfLife);

        var result = ewma.Update(0.0, T0.AddSeconds(20), halfLife);

        Assert.That(result, Is.EqualTo(0.25).Within(1e-9));
    }

    [Test]
    public void Non_positive_half_life_adopts_the_sample_directly()
    {
        var ewma = new Ewma();
        ewma.Update(1.0, T0, TimeSpan.FromSeconds(10));

        var result = ewma.Update(0.0, T0.AddSeconds(10), TimeSpan.Zero);

        Assert.That(result, Is.Zero);
    }

    [Test]
    public void Non_positive_elapsed_step_adopts_the_sample_directly()
    {
        var ewma = new Ewma();
        ewma.Update(1.0, T0, TimeSpan.FromSeconds(10));

        // Same timestamp -> zero elapsed -> no smoothing this step.
        var result = ewma.Update(0.4, T0, TimeSpan.FromSeconds(10));

        Assert.That(result, Is.EqualTo(0.4));
    }

    [Test]
    public void Set_snaps_value_and_resets_decay_baseline()
    {
        var ewma = new Ewma();
        var halfLife = TimeSpan.FromSeconds(10);
        ewma.Update(0.2, T0, halfLife);

        ewma.Set(1.0, T0.AddSeconds(5));
        // From the reset baseline, one half-life later a 0.0 sample lands at 0.5.
        var result = ewma.Update(0.0, T0.AddSeconds(15), halfLife);

        Assert.Multiple(() =>
        {
            Assert.That(ewma.Current, Is.EqualTo(0.5).Within(1e-9));
            Assert.That(result, Is.EqualTo(0.5).Within(1e-9));
        });
    }
}
