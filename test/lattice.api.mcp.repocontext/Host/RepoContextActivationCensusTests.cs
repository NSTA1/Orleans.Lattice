using System.Diagnostics.Metrics;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextActivationCensus"/>, the reader that makes the
/// resident activation set visible while the container is running.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="RepoContextDrainSignal"/> has always stated that "drain duration tracks
/// the resident activation set, which nothing here bounds", and until issue #2598
/// nothing in the host could read that set. These tests drive the listener from a
/// probe instrument the fixture owns rather than from a silo, so they run in
/// milliseconds and stay honest about what they pin: the <b>listening behaviour</b>.
/// The separate assumption - that Orleans still publishes an instrument by this name -
/// is pinned against a real silo by
/// <see cref="RepoContextActivationCensusInstrumentNameTests"/>, because a fixture
/// that supplies its own instrument name cannot possibly detect Orleans renaming
/// theirs.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextActivationCensusTests
{
    private static string ProbeName() => "probe-activation-working-set-" + Guid.NewGuid().ToString("N");

    [Test]
    public void The_instrument_it_watches_by_default_is_the_orleans_catalog_working_set()
    {
        // Named as a constant so the coupling to another project's instrument is
        // stated once and is greppable, rather than buried in a listener predicate.
        Assert.That(
            RepoContextActivationCensus.InstrumentName,
            Is.EqualTo("orleans-catalog-activation-working-set"));
    }

    [Test]
    public void A_published_reading_is_sampled()
    {
        var probe = ProbeName();
        using var meter = new Meter("probe." + Guid.NewGuid().ToString("N"));
        meter.CreateObservableGauge(probe, () => 4321L);

        using var census = new RepoContextActivationCensus(probe);

        Assert.That(census.TrySample(), Is.EqualTo(4321));
    }

    [Test]
    public void An_int_valued_gauge_is_sampled_too_because_the_instrument_type_is_not_ours_to_fix()
    {
        var probe = ProbeName();
        using var meter = new Meter("probe." + Guid.NewGuid().ToString("N"));
        meter.CreateObservableGauge(probe, () => 17);

        using var census = new RepoContextActivationCensus(probe);

        Assert.That(census.TrySample(), Is.EqualTo(17));
    }

    [Test]
    public void Each_sample_re_reads_the_instrument_rather_than_returning_the_first_answer_forever()
    {
        // The projection is only worth publishing if it tracks a growing resident
        // set. A census that latched its first reading would report a container as
        // comfortably inside its budget for as long as it kept growing past it.
        var probe = ProbeName();
        var value = 10L;
        using var meter = new Meter("probe." + Guid.NewGuid().ToString("N"));
        meter.CreateObservableGauge(probe, () => value);

        using var census = new RepoContextActivationCensus(probe);

        Assert.That(census.TrySample(), Is.EqualTo(10));
        value = 20_000;
        Assert.That(census.TrySample(), Is.EqualTo(20_000));
    }

    [Test]
    public void An_absent_instrument_samples_as_unavailable_rather_than_as_an_empty_silo()
    {
        // The distinction the whole design rests on. Orleans owns this instrument
        // name and may withdraw it; reporting that as a residency of zero would turn
        // a lost signal into a confident wrong one, and a projected drain of zero
        // would read as a container in perfect health.
        using var census = new RepoContextActivationCensus(ProbeName());

        Assert.That(census.TrySample(), Is.Null);
    }

    [Test]
    public void A_faulting_callback_samples_as_unavailable_rather_than_throwing_into_the_drain_path()
    {
        // This is sampled at the start of a drain. A throw here would cost the stop
        // sequence to save a diagnostic number about the stop sequence.
        var probe = ProbeName();
        using var meter = new Meter("probe." + Guid.NewGuid().ToString("N"));
        var faulting = new Func<long>(() => throw new InvalidOperationException("probe fault"));
        meter.CreateObservableGauge(probe, faulting);

        using var census = new RepoContextActivationCensus(probe);

        Assert.That(census.TrySample(), Is.Null);
    }

    [Test]
    public void A_negative_reading_is_refused_because_a_resident_set_cannot_be_negative()
    {
        var probe = ProbeName();
        using var meter = new Meter("probe." + Guid.NewGuid().ToString("N"));
        meter.CreateObservableGauge(probe, () => -5L);

        using var census = new RepoContextActivationCensus(probe);

        Assert.That(census.TrySample(), Is.Null);
    }

    [Test]
    public void A_disposed_census_samples_as_unavailable_rather_than_touching_a_disposed_listener()
    {
        var probe = ProbeName();
        using var meter = new Meter("probe." + Guid.NewGuid().ToString("N"));
        meter.CreateObservableGauge(probe, () => 7L);

        var census = new RepoContextActivationCensus(probe);
        Assert.That(census.TrySample(), Is.EqualTo(7));

        census.Dispose();

        Assert.That(census.TrySample(), Is.Null);
    }

    [Test]
    public void Disposing_twice_is_safe_because_the_host_disposes_it_on_a_shutdown_path()
    {
        var census = new RepoContextActivationCensus(ProbeName());
        census.Dispose();

        Assert.That(census.Dispose, Throws.Nothing);
    }

    [Test]
    public void A_census_ignores_an_instrument_of_a_different_name_on_the_same_meter()
    {
        var probe = ProbeName();
        using var meter = new Meter("probe." + Guid.NewGuid().ToString("N"));
        meter.CreateObservableGauge(probe + "-other", () => 99L);

        using var census = new RepoContextActivationCensus(probe);

        Assert.That(census.TrySample(), Is.Null);
    }
}
