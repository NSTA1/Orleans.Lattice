using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.UI.Areas.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Telemetry;

/// <summary>Finite readings must remain drawable throughout the double range.</summary>
[TestFixture]
public sealed class TelemetryChartGeometryTests
{
    [TestCase(1e308, 1.7e308)]
    [TestCase(-1.7e308, 1.7e308)]
    [TestCase(-1.7e308, -1.7e308)]
    [TestCase(double.Epsilon, double.Epsilon)]
    public void Finite_extreme_readings_have_finite_axes_and_coordinates(double first, double last)
    {
        var response = TelemetryTestData.Response(new TelemetryQueryRequest { QueryId = "extreme" }, default,
            TelemetryTestData.Series("tree", TelemetryTestData.Now, first, last));
        var geometry = TelemetryChartGeometry.Build(response, null, TelemetryMeasurementSemantic.Level);

        Assert.Multiple(() =>
        {
            Assert.That(double.IsFinite(geometry.Minimum), Is.True);
            Assert.That(double.IsFinite(geometry.Maximum), Is.True);
            Assert.That(geometry.Maximum, Is.GreaterThan(geometry.Minimum));
            Assert.That(geometry.Y(first), Is.InRange(TelemetryChartGeometry.Top, TelemetryChartGeometry.PlotBottom));
            Assert.That(geometry.Y(last), Is.InRange(TelemetryChartGeometry.Top, TelemetryChartGeometry.PlotBottom));
            Assert.That(geometry.Lines.Single().Path, Does.Not.Contain("NaN").And.Not.Contain("Infinity"));
            Assert.That(geometry.Ticks.All(tick => double.IsFinite(tick.Y)), Is.True);
        });
    }
}
