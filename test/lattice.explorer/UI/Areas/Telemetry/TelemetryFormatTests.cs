using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.UI.Areas.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Telemetry;

/// <summary>Localized readings, UCUM annotations and UTC timestamp precision.</summary>
[TestFixture]
[SetCulture("en-US")]
public sealed class TelemetryFormatTests
{
    [TestCase(double.NaN)]
    [TestCase(double.PositiveInfinity)]
    [TestCase(double.NegativeInfinity)]
    public void Value_nonfinite_readings_are_not_displayed_as_measurements(double value) =>
        Assert.That(TelemetryFormat.Value(value, "By", TelemetryMeasurementSemantic.Level), Is.EqualTo("no reading"));

    [TestCase(0d, "0 B")]
    [TestCase(-1536d, "-1.5 KiB")]
    [TestCase(1_099_511_627_776d, "1 TiB")]
    [TestCase(1_125_899_906_842_624d, "1 PiB")]
    [TestCase(1_152_921_504_606_846_976d, "1024 PiB")]
    public void Value_bytes_use_binary_units_and_stop_at_the_unit_ceiling(double value, string expected) =>
        Assert.That(TelemetryFormat.Value(value, "By", TelemetryMeasurementSemantic.Level), Is.EqualTo(expected));

    [TestCase(0d, "0")]
    [TestCase(0.125d, "0.13")]
    [TestCase(-0.125d, "-0.13")]
    [TestCase(9.99d, "9.99")]
    [TestCase(10d, "10")]
    [TestCase(12.34d, "12.3")]
    [TestCase(99.94d, "99.9")]
    [TestCase(100d, "100")]
    [TestCase(-1234d, "-1,234")]
    public void Value_precision_uses_the_absolute_magnitude(double value, string expected) =>
        Assert.That(TelemetryFormat.Value(value, null, TelemetryMeasurementSemantic.Level), Is.EqualTo(expected));

    [TestCase(null, "")]
    [TestCase("", "")]
    [TestCase(" \t", "")]
    [TestCase("1", "")]
    [TestCase("By", "bytes")]
    [TestCase("{op}/s", "op/s")]
    [TestCase("{a}/{b}", "a/b")]
    [TestCase("ms", "ms")]
    [TestCase("by", "by")]
    public void Unit_optional_UCUM_annotations_become_plain_words(string? unit, string expected) =>
        Assert.That(TelemetryFormat.Unit(unit), Is.EqualTo(expected));

    [TestCase(false, false, "23:02")]
    [TestCase(true, false, "2026-10-29 23:02")]
    [TestCase(false, true, "23:02:03.125")]
    [TestCase(true, true, "2026-10-29 23:02:03.125")]
    public void Time_date_and_seconds_flags_preserve_UTC_precision(bool withDate, bool withSeconds, string expected)
    {
        var instant = new DateTimeOffset(2026, 10, 30, 1, 2, 3, TimeSpan.FromHours(2)).AddMilliseconds(125);

        Assert.That(TelemetryFormat.Time(instant, withDate, withSeconds), Is.EqualTo(expected));
    }

    [Test]
    public void Time_whole_seconds_omit_the_fractional_separator() =>
        Assert.That(TelemetryFormat.Time(new DateTimeOffset(2026, 10, 30, 1, 2, 3, TimeSpan.Zero), false, true), Is.EqualTo("01:02:03"));

    [Test]
    [SetCulture("fr-FR")]
    public void Value_localized_numbers_preserve_UCUM_units_and_ratio_semantics()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryFormat.Value(1536, "By", TelemetryMeasurementSemantic.Level), Is.EqualTo("1,5 KiB"));
            Assert.That(TelemetryFormat.Value(0.931, "1", TelemetryMeasurementSemantic.Ratio), Is.EqualTo("93,1 %"));
            Assert.That(TelemetryFormat.Value(0.931, "1", TelemetryMeasurementSemantic.Level), Is.EqualTo("0,93"));
            Assert.That(TelemetryFormat.Value(12.34, "ms", TelemetryMeasurementSemantic.Duration), Is.EqualTo("12,3 ms"));
        });
    }
}
