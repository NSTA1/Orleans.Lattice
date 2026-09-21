using System.Diagnostics.Metrics;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Pins the shared host meter name under the collector's subscribed prefix.
/// </summary>
/// <remarks>
/// Prometheus series here are named from the <em>instrument</em>, not from the
/// meter, so renaming the meter to anything still under the prefix changes nothing
/// an operator can see. Moving it out from under the prefix ends the collector's
/// subscription and every host series disappears from <c>/metrics</c> at once, with
/// no error and with the exposition still looking complete. Only that second case
/// is harmful, and it is the case these tests fail on.
/// </remarks>
[TestFixture]
public sealed class RepoContextHostMeterTests
{
    [Test]
    public void The_host_meter_name_sits_under_the_collectors_subscribed_prefix()
        => Assert.That(
            RepoContextHostMeter.Name.StartsWith(
                RepoContextMetricsCollector.MeterNamePrefix, StringComparison.OrdinalIgnoreCase),
            Is.True,
            $"'{RepoContextHostMeter.Name}' no longer starts with "
            + $"'{RepoContextMetricsCollector.MeterNamePrefix}'. The collector subscribes by meter-name "
            + "prefix, so every host instrument published on this meter - the drain forecast, the garbage "
            + "collector and the backup surface - would vanish from /metrics simultaneously and silently.");

    [Test]
    public void Every_host_component_publishes_on_the_one_shared_meter_name()
        => Assert.That(
            RepoContextDrainForecastService.MeterName,
            Is.EqualTo(RepoContextHostMeter.Name),
            "RepoContextDrainForecastService.MeterName is retained only as an alias of the shared "
            + "constant. If they diverge, host series split across two meters and a reader has no single "
            + "place to learn which meter a host instrument is on.");

    [Test]
    public void An_instrument_on_the_shared_meter_is_visible_to_a_prefix_subscriber()
    {
        // Demonstrates the subscription the two assertions above protect, rather
        // than restating the prefix rule a third time.
        using var meter = new Meter(RepoContextHostMeter.Name);
        var observed = false;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name.StartsWith(
                    RepoContextMetricsCollector.MeterNamePrefix, StringComparison.OrdinalIgnoreCase)
                && instrument.Name == "lattice_repocontext_hostmeter_probe")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<int>((_, value, _, _) => observed = value == 7);
        listener.Start();

        meter.CreateObservableGauge("lattice_repocontext_hostmeter_probe", () => 7);
        listener.RecordObservableInstruments();

        Assert.That(
            observed,
            Is.True,
            "An instrument published on the shared host meter was not delivered to a listener "
            + "subscribing by the collector's prefix.");
    }
}
