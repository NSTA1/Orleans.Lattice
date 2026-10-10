using Bunit;
using Microsoft.AspNetCore.Components.Web;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

public sealed partial class LtDateTimeInputTests
{
    [TestCase(1, 1, 1, "Previous month")]
    [TestCase(9999, 12, 31, "Next month")]
    public void The_calendar_opens_at_the_date_limits_and_disables_navigation_beyond_them(
        int year, int month, int day, string blocked)
    {
        var cut = RenderField(p => p.Add(x => x.Value, new DateTimeOffset(year, month, day, 0, 0, 0, TimeSpan.Zero)));

        cut.Find(".lt-datetime__toggle").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find($".lt-datetime__nav[aria-label=\"{blocked}\"]").HasAttribute("disabled"), Is.True);
            Assert.That(cut.FindAll(".lt-datetime__day[tabindex=\"0\"]").Single().TextContent, Is.EqualTo(day.ToString()));
            Assert.That(cut.FindAll(".lt-datetime__day").Count, Is.InRange(31, 42));
        });
    }

    [TestCase(1, 1, 1, "ArrowLeft", false)]
    [TestCase(1, 1, 1, "ArrowUp", false)]
    [TestCase(1, 1, 1, "PageUp", false)]
    [TestCase(1, 1, 1, "PageUp", true)]
    [TestCase(9999, 12, 31, "ArrowRight", false)]
    [TestCase(9999, 12, 31, "ArrowDown", false)]
    [TestCase(9999, 12, 31, "PageDown", false)]
    [TestCase(9999, 12, 31, "PageDown", true)]
    [TestCase(9999, 12, 31, "End", false)]
    public void Keyboard_navigation_stops_at_the_date_limits(int year, int month, int day, string key, bool shift)
    {
        var cut = RenderField(p => p.Add(x => x.Value, new DateTimeOffset(year, month, day, 0, 0, 0, TimeSpan.Zero)));
        cut.Find(".lt-datetime__toggle").Click();
        var label = cut.Find(".lt-datetime__day[tabindex=\"0\"]").GetAttribute("aria-label");

        cut.Find(".lt-datetime__day[tabindex=\"0\"]").KeyDown(new KeyboardEventArgs { Key = key, ShiftKey = shift });

        Assert.That(cut.Find(".lt-datetime__day[tabindex=\"0\"]").GetAttribute("aria-label"), Is.EqualTo(label));
    }
}
