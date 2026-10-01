using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The text forms of the time primitives: ISO 8601 UTC to the second, read
/// culture-invariant; offsets named from UTC; durations in their largest whole units.
/// </summary>
[TestFixture]
public sealed class LtTimeTextTests
{
    [Test]
    public void An_instant_is_written_in_utc_to_the_second()
    {
        var instant = new DateTimeOffset(2026, 9, 28, 16, 5, 7, 999, TimeSpan.FromHours(2));

        Assert.Multiple(() =>
        {
            Assert.That(LtTimeText.Iso(instant), Is.EqualTo("2026-09-28T14:05:07Z"));
            Assert.That(LtTimeText.Readable(instant), Is.EqualTo("2026-09-28 14:05:07 UTC"));
            Assert.That(LtTimeText.ToSecond(instant), Is.EqualTo(new DateTimeOffset(2026, 9, 28, 14, 5, 7, TimeSpan.Zero)));
        });
    }

    [TestCase("2026-09-28T14:05:07Z", "2026-09-28T14:05:07Z")]
    [TestCase("2026-09-28 14:05:07", "2026-09-28T14:05:07Z")]
    [TestCase("2026-09-28T16:05:07+02:00", "2026-09-28T14:05:07Z")]
    [TestCase(" 2026-09-28T14:05:07.75Z ", "2026-09-28T14:05:07Z")]
    [TestCase("2026-09-28", "2026-09-28T00:00:00Z")]
    public void A_typed_instant_is_read_in_utc(string text, string expected)
    {
        Assert.Multiple(() =>
        {
            Assert.That(LtTimeText.TryParseInstant(text, out var instant), Is.True);
            Assert.That(LtTimeText.Iso(instant), Is.EqualTo(expected));
            Assert.That(instant.Offset, Is.EqualTo(TimeSpan.Zero));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("yesterday")]
    [TestCase("2026-13-01T00:00:00Z")]
    public void Text_that_names_no_instant_is_refused(string? text) =>
        Assert.That(LtTimeText.TryParseInstant(text, out _), Is.False);

    [Test]
    public void An_offset_is_named_from_utc()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LtTimeText.Offset(TimeSpan.FromHours(1)), Is.EqualTo("UTC+01:00"));
            Assert.That(LtTimeText.Offset(TimeSpan.FromMinutes(-330)), Is.EqualTo("UTC-05:30"));
            Assert.That(LtTimeText.Offset(TimeSpan.Zero), Is.EqualTo("UTC+00:00"));
        });
    }

    [Test]
    public void An_instant_in_a_zone_names_the_zone_and_its_offset()
    {
        var zone = TimeZoneInfo.CreateCustomTimeZone("UTC+05:30", TimeSpan.FromMinutes(330), "UTC+05:30", "UTC+05:30");

        Assert.That(LtTimeText.InZone(new DateTimeOffset(2026, 9, 28, 14, 0, 0, TimeSpan.Zero), zone), Is.EqualTo("2026-09-28 19:30:00 UTC+05:30 (UTC+05:30)"));
    }

    [Test]
    public void A_duration_is_named_in_its_largest_whole_units()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LtTimeText.Duration(new TimeSpan(1, 2, 3, 4)), Is.EqualTo("1 d 2 h 3 min 4 s"));
            Assert.That(LtTimeText.Duration(TimeSpan.FromMinutes(90)), Is.EqualTo("1 h 30 min"));
            Assert.That(LtTimeText.Duration(TimeSpan.FromDays(400)), Is.EqualTo("400 d"));
            Assert.That(LtTimeText.Duration(TimeSpan.Zero), Is.EqualTo("0 s"));
            Assert.That(LtTimeText.Duration(TimeSpan.FromMilliseconds(500)), Is.EqualTo("0 s"));
        });
    }
}
