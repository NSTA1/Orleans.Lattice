using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The date and time field's picker: a Monday-first month grid that works from the keyboard,
/// a time to the second in UTC, quick picks inside the range, and the empty state.
/// </summary>
public sealed partial class LtDateTimeInputTests
{
    [Test]
    public void The_picker_opens_on_the_chosen_month_with_the_chosen_day_selected_and_focused()
    {
        var cut = RenderField(p => p.Add(x => x.Value, new DateTimeOffset(2026, 9, 10, 8, 0, 0, TimeSpan.Zero)));

        cut.Find(".lt-datetime__toggle").Click();

        var picker = cut.Find(".lt-datetime__picker");
        var selected = cut.FindAll(".lt-datetime__cell[aria-selected=\"true\"] button");
        var weekdays = cut.FindAll(".lt-datetime__weekday").Select(header => header.GetAttribute("abbr")).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-datetime__toggle").GetAttribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(cut.Find(".lt-datetime__toggle").GetAttribute("aria-controls"), Is.EqualTo(picker.Id));
            Assert.That(cut.Find(".lt-datetime__month").TextContent, Is.EqualTo("September 2026"));
            Assert.That(cut.Find("[role=grid]").GetAttribute("aria-labelledby"), Is.EqualTo(cut.Find(".lt-datetime__month").Id));
            Assert.That(weekdays, Is.EqualTo(new[] { "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday" }));
            Assert.That(selected.Select(day => day.GetAttribute("aria-label")), Is.EqualTo(new[] { "Thursday 10 September 2026" }));
            Assert.That(cut.FindAll(".lt-datetime__day[tabindex=\"0\"]").Select(day => day.TextContent), Is.EqualTo(new[] { "10" }), "one roving tab stop, on the chosen day");
            Assert.That(cut.Find(".lt-datetime__day[aria-current=\"date\"]").GetAttribute("aria-label"), Is.EqualTo("Monday 28 September 2026"));
            Assert.That(cut.FindAll(".lt-datetime__day").First().GetAttribute("aria-label"), Is.EqualTo("Monday 31 August 2026"), "the grid starts on a Monday");
        });

        var focused = (ElementReference)JSInterop.VerifyFocusAsyncInvoke().Arguments[0]!;
        Assert.Multiple(() =>
        {
            Assert.That(focused.Id, Is.EqualTo(cut.Find(".lt-datetime__day[tabindex=\"0\"]").GetAttribute("blazor:elementreference")));
            Assert.That(focused.Id, Is.EqualTo(cut.Instance.ActiveDayReference.Id));
        });
    }

    [Test]
    public void An_empty_field_opens_on_today()
    {
        var cut = RenderField();

        cut.Find(".lt-datetime__toggle").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-datetime__day[tabindex=\"0\"]").GetAttribute("aria-label"), Is.EqualTo("Monday 28 September 2026"));
            Assert.That(cut.FindAll(".lt-datetime__cell[aria-selected=\"true\"]"), Is.Empty);
            Assert.That(cut.Find(".lt-datetime__time input").GetAttribute("value"), Is.Empty);
        });
    }

    [Test]
    public void Choosing_a_day_keeps_the_time_of_day_and_choosing_a_time_keeps_the_day()
    {
        var raised = new List<DateTimeOffset?>();
        var cut = RenderField(p => p
            .Add(x => x.Value, new DateTimeOffset(2026, 9, 10, 8, 15, 30, TimeSpan.Zero))
            .Add(x => x.ValueChanged, (DateTimeOffset? value) => raised.Add(value)));
        cut.Find(".lt-datetime__toggle").Click();

        Day(cut, "Tuesday 15 September 2026").Click();
        cut.Find(".lt-datetime__time input").Change("21:05:09");

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.EqualTo(new DateTimeOffset?[]
            {
                new DateTimeOffset(2026, 9, 15, 8, 15, 30, TimeSpan.Zero),
                new DateTimeOffset(2026, 9, 15, 21, 5, 9, TimeSpan.Zero),
            }));
            Assert.That(cut.Find("input.lt-datetime__input").GetAttribute("value"), Is.EqualTo("2026-09-15T21:05:09Z"));
            Assert.That(cut.Find(".lt-datetime__time-label").TextContent, Is.EqualTo("Time (UTC)"), "the time is in the zone the field is in");
            Assert.That(cut.Find(".lt-datetime__time input").GetAttribute("step"), Is.EqualTo("1"), "to the second");
        });
    }

    [Test]
    public void A_quick_pick_sets_the_time_closes_the_picker_and_returns_focus_to_its_button()
    {
        DateTimeOffset? raised = null;
        var cut = RenderField(p => p.Add(x => x.ValueChanged, (DateTimeOffset? value) => raised = value));
        cut.Find(".lt-datetime__toggle").Click();

        Pick(cut, "24 hours ago").Click();

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.EqualTo(Now.AddHours(-24)));
            Assert.That(cut.FindAll(".lt-datetime__picker"), Is.Empty);
            Assert.That(cut.Find(".lt-datetime__toggle").GetAttribute("aria-expanded"), Is.EqualTo("false"));
        });
        var focused = (ElementReference)JSInterop.Invocations["Blazor._internal.domWrapper.focus"].Last().Arguments[0]!;
        Assert.That(focused.Id, Is.EqualTo(cut.Instance.ToggleReference.Id));
    }

    [Test]
    public void The_quick_picks_are_now_an_hour_a_day_and_a_week_ago_and_those_outside_the_range_are_disabled()
    {
        var cut = RenderField(p => p.Add(x => x.Min, Now.AddDays(-2)));
        cut.Find(".lt-datetime__toggle").Click();

        var picks = cut.FindAll(".lt-datetime__pick");
        Assert.Multiple(() =>
        {
            Assert.That(picks.Select(pick => pick.TextContent), Is.EqualTo(new[] { "Now", "1 hour ago", "24 hours ago", "7 days ago" }));
            Assert.That(picks.Select(pick => pick.HasAttribute("disabled")), Is.EqualTo(new[] { false, false, false, true }));
        });
    }

    [Test]
    public void Quick_picks_can_be_left_out()
    {
        var cut = RenderField(p => p.Add(x => x.QuickPicks, false));
        cut.Find(".lt-datetime__toggle").Click();

        Assert.That(cut.FindAll(".lt-datetime__pick"), Is.Empty);
    }

    [Test]
    public void Days_outside_the_range_cannot_be_chosen_but_can_still_be_reached()
    {
        var raised = 0;
        var cut = RenderField(p => p
            .Add(x => x.AllowFuture, false)
            .Add(x => x.ValueChanged, (DateTimeOffset? _) => raised++));
        cut.Find(".lt-datetime__toggle").Click();

        var tomorrow = Day(cut, "Tuesday 29 September 2026");
        Assert.That(tomorrow.GetAttribute("aria-disabled"), Is.EqualTo("true"));
        Assert.That(tomorrow.HasAttribute("disabled"), Is.False, "a disabled button would drop keyboard focus");
        Assert.That(Day(cut, "Monday 28 September 2026").HasAttribute("aria-disabled"), Is.False, "today is partly in range");

        Day(cut, "Tuesday 29 September 2026").Click();

        Assert.That(raised, Is.Zero);
    }

    [Test]
    public void Choosing_a_day_clamps_its_time_into_the_range()
    {
        DateTimeOffset? raised = null;
        var cut = RenderField(p => p
            .Add(x => x.Value, new DateTimeOffset(2026, 9, 20, 23, 0, 0, TimeSpan.Zero))
            .Add(x => x.AllowFuture, false)
            .Add(x => x.ValueChanged, (DateTimeOffset? value) => raised = value));
        cut.Find(".lt-datetime__toggle").Click();

        Day(cut, "Monday 28 September 2026").Click();

        Assert.That(raised, Is.EqualTo(Now), "23:00 today is in the future, so it is held at now");
    }

    [TestCase("ArrowRight", false, "Friday 11 September 2026")]
    [TestCase("ArrowLeft", false, "Wednesday 9 September 2026")]
    [TestCase("ArrowDown", false, "Thursday 17 September 2026")]
    [TestCase("ArrowUp", false, "Thursday 3 September 2026")]
    [TestCase("PageDown", false, "Saturday 10 October 2026")]
    [TestCase("PageUp", false, "Monday 10 August 2026")]
    [TestCase("PageDown", true, "Friday 10 September 2027")]
    [TestCase("Home", false, "Monday 7 September 2026")]
    [TestCase("End", false, "Sunday 13 September 2026")]
    public void The_keyboard_moves_the_focused_day(string key, bool shift, string expected)
    {
        var cut = RenderField(p => p.Add(x => x.Value, new DateTimeOffset(2026, 9, 10, 8, 0, 0, TimeSpan.Zero)));
        cut.Find(".lt-datetime__toggle").Click();

        cut.Find(".lt-datetime__day[tabindex=\"0\"]").KeyDown(new KeyboardEventArgs { Key = key, ShiftKey = shift });

        var active = cut.Find(".lt-datetime__day[tabindex=\"0\"]");
        Assert.That(active.GetAttribute("aria-label"), Is.EqualTo(expected));
        var focused = (ElementReference)JSInterop.Invocations["Blazor._internal.domWrapper.focus"].Last().Arguments[0]!;
        Assert.That(focused.Id, Is.EqualTo(cut.Instance.ActiveDayReference.Id), "focus follows the active day");
    }

    [Test]
    public void The_month_buttons_turn_the_page_without_choosing_anything()
    {
        var raised = 0;
        var cut = RenderField(p => p.Add(x => x.ValueChanged, (DateTimeOffset? _) => raised++));
        cut.Find(".lt-datetime__toggle").Click();

        cut.Find(".lt-datetime__nav[aria-label=\"Next month\"]").Click();
        Assert.That(cut.Find(".lt-datetime__month").TextContent, Is.EqualTo("October 2026"));
        cut.Find(".lt-datetime__nav[aria-label=\"Previous month\"]").Click();
        cut.Find(".lt-datetime__nav[aria-label=\"Previous month\"]").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-datetime__month").TextContent, Is.EqualTo("August 2026"));
            Assert.That(cut.Find(".lt-datetime__month").GetAttribute("aria-live"), Is.EqualTo("polite"));
            Assert.That(raised, Is.Zero);
        });
    }

    [Test]
    public void Escape_closes_the_picker_and_returns_focus_to_its_button()
    {
        var cut = RenderField();
        cut.Find(".lt-datetime__toggle").Click();

        cut.Find(".lt-datetime__picker").KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.That(cut.FindAll(".lt-datetime__picker"), Is.Empty);
        var focused = (ElementReference)JSInterop.Invocations["Blazor._internal.domWrapper.focus"].Last().Arguments[0]!;
        Assert.That(focused.Id, Is.EqualTo(cut.Instance.ToggleReference.Id));
    }

    [Test]
    public void Clear_empties_a_field_that_may_be_empty_and_says_what_that_means()
    {
        DateTimeOffset? raised = Now;
        var cut = RenderField(p => p
            .Add(x => x.Value, Now)
            .Add(x => x.EmptyText, "Latest")
            .Add(x => x.ValueChanged, (DateTimeOffset? value) => raised = value));
        cut.Find(".lt-datetime__toggle").Click();

        cut.FindAll(".lt-datetime__actions button").Single(button => button.TextContent == "Clear (Latest)").Click();

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.Null);
            Assert.That(cut.Find("input.lt-datetime__input").GetAttribute("value"), Is.Empty);
            Assert.That(cut.FindAll(".lt-datetime__picker"), Is.Empty);
        });
    }

    [Test]
    public void A_field_that_needs_a_value_offers_no_clear()
    {
        var cut = RenderField();
        cut.Find(".lt-datetime__toggle").Click();

        Assert.That(cut.FindAll(".lt-datetime__actions button").Select(button => button.TextContent), Is.EqualTo(new[] { "Done" }));
    }

    [Test]
    public void On_a_phone_the_picker_opens_in_the_flow_of_the_page()
    {
        var cut = Render<CascadingValue<LtBreakpoint?>>(p => p
            .Add(x => x.Name, LtBreakpointCascade.Name)
            .Add(x => x.Value, LtBreakpoint.Compact)
            .AddChildContent<LtDateTimeInput>(field => field.Add(x => x.Label, "As of")));

        cut.Find(".lt-datetime__toggle").Click();

        Assert.That(cut.Find(".lt-datetime__picker").ClassList, Does.Contain("lt-datetime__picker--inline"));
    }

    [Test]
    public void On_a_wide_screen_the_picker_floats_below_the_field()
    {
        var cut = RenderField();
        cut.Find(".lt-datetime__toggle").Click();

        Assert.That(cut.Find(".lt-datetime__picker").ClassList, Does.Not.Contain("lt-datetime__picker--inline"));
    }

    private static AngleSharp.Dom.IElement Day(IRenderedComponent<LtDateTimeInput> cut, string name) =>
        cut.FindAll(".lt-datetime__day").Single(day => day.GetAttribute("aria-label") == name);

    private static AngleSharp.Dom.IElement Pick(IRenderedComponent<LtDateTimeInput> cut, string text) =>
        cut.FindAll(".lt-datetime__pick").Single(pick => pick.TextContent == text);
}
