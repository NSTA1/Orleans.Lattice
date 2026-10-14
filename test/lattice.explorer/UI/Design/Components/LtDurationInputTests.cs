using Bunit;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// Issue #4148: the duration field. A whole-number box per unit inside one control box,
/// each named by its unit; a value split into the units offered; inline validation against
/// a minimum and a maximum; and an optional empty state.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtDurationInputTests : ShellDesignTestContext
{
    [Test]
    public void Each_unit_has_its_own_box_named_by_the_field_and_its_unit()
    {
        var cut = RenderField();
        var inputs = cut.FindAll("input");
        var label = cut.Find("label.lt-field__label");
        var group = cut.Find(".lt-duration__control");

        Assert.Multiple(() =>
        {
            Assert.That(label.GetAttribute("for"), Is.EqualTo(inputs[0].Id));
            Assert.That(inputs[0].Id, Is.EqualTo(cut.Instance.InputId));
            Assert.That(group.GetAttribute("role"), Is.EqualTo("group"));
            Assert.That(group.GetAttribute("aria-labelledby"), Is.EqualTo(label.Id));
            Assert.That(inputs.Select(input => input.GetAttribute("aria-label")), Is.EqualTo(new[] { "Every, hours", "Every, minutes" }));
            Assert.That(inputs.Select(input => input.GetAttribute("inputmode")), Is.All.EqualTo("numeric"));
            Assert.That(cut.FindAll(".lt-duration__unit").Select(unit => unit.TextContent), Is.EqualTo(new[] { "h", "min" }));
        });
    }

    [Test]
    public void A_value_is_split_into_the_units_offered_the_largest_taking_the_rest()
    {
        var hoursAndMinutes = RenderField(p => p.Add(x => x.Value, TimeSpan.FromHours(36) + TimeSpan.FromMinutes(5)));
        var everyUnit = RenderField(p => p
            .Add(x => x.Units, LtDurationUnits.Days | LtDurationUnits.Hours | LtDurationUnits.Minutes | LtDurationUnits.Seconds)
            .Add(x => x.Value, new TimeSpan(1, 2, 3, 4)));

        Assert.Multiple(() =>
        {
            Assert.That(Values(hoursAndMinutes), Is.EqualTo(new[] { "36", "5" }));
            Assert.That(Values(everyUnit), Is.EqualTo(new[] { "1", "2", "3", "4" }));
            Assert.That(everyUnit.FindAll(".lt-duration__unit").Select(unit => unit.TextContent), Is.EqualTo(new[] { "d", "h", "min", "s" }));
        });
    }

    [Test]
    public void An_empty_field_shows_empty_boxes()
    {
        var cut = RenderField(p => p.Add(x => x.Optional, true));

        Assert.That(Values(cut), Is.All.Empty);
    }

    [Test]
    public void Typing_raises_the_total_with_a_blank_box_read_as_zero()
    {
        var raised = new List<TimeSpan?>();
        var cut = RenderField(p => p.Add(x => x.ValueChanged, (TimeSpan? value) => raised.Add(value)));

        cut.FindAll("input")[1].Input("45");
        cut.FindAll("input")[0].Input("2");

        Assert.That(raised, Is.EqualTo(new TimeSpan?[] { TimeSpan.FromMinutes(45), TimeSpan.FromMinutes(165) }));
    }

    [Test]
    public async Task A_box_that_is_not_a_whole_number_is_refused_inline_and_raises_nothing()
    {
        var raised = 0;
        var cut = RenderField(p => p.Add(x => x.ValueChanged, (TimeSpan? _) => raised++));

        cut.FindAll("input")[0].Input("1.5");
        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        var error = cut.Find(".lt-field__error");
        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(raised, Is.Zero);
            Assert.That(error.TextContent, Does.Contain("Give a whole number of hours and minutes."));
            Assert.That(cut.FindAll("input").Select(input => input.GetAttribute("aria-invalid")), Is.All.EqualTo("true"));
            Assert.That(cut.FindAll("input")[0].GetAttribute("aria-describedby"), Is.EqualTo(error.Id));
            Assert.That(cut.Find(".lt-duration__control").ClassList, Does.Contain("lt-duration__control--invalid"));
        });
    }

    [TestCase("-1")]
    [TestCase("x")]
    [TestCase("99999999999999999999")]
    public void A_negative_a_word_or_an_overflow_is_refused_on_change(string text)
    {
        var cut = RenderField();

        cut.FindAll("input")[0].Change(text);

        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Give a whole number of hours and minutes."));
    }

    [Test]
    public async Task A_duration_under_the_minimum_is_refused()
    {
        var cut = RenderField(p => p.Add(x => x.Min, TimeSpan.FromMinutes(1)).Add(x => x.Value, TimeSpan.FromHours(1)));

        cut.FindAll("input")[0].Input("0");
        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Give at least 1 min."));
        });
    }

    [Test]
    public async Task A_duration_over_the_maximum_is_refused()
    {
        var cut = RenderField(p => p.Add(x => x.Max, TimeSpan.FromDays(1)));

        cut.FindAll("input")[0].Input("25");
        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Give at most 1 d."));
        });
    }

    [Test]
    public async Task An_optional_field_left_empty_raises_none()
    {
        TimeSpan? raised = TimeSpan.FromHours(1);
        var cut = RenderField(p => p
            .Add(x => x.Optional, true)
            .Add(x => x.Value, TimeSpan.FromHours(1))
            .Add(x => x.ValueChanged, (TimeSpan? value) => raised = value));

        cut.FindAll("input")[0].Input(string.Empty);
        cut.FindAll("input")[1].Input(string.Empty);
        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.True);
            Assert.That(raised, Is.Null);
        });
    }

    [Test]
    public async Task A_required_field_left_empty_says_so()
    {
        var cut = RenderField();

        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Give a duration in hours and minutes."));
        });
    }

    [Test]
    public void A_pages_own_message_and_hint_are_announced_with_every_box()
    {
        var cut = RenderField(p => p.Add(x => x.Hint, "How often.").Add(x => x.Error, "Not now."));

        var described = cut.Find(".lt-field__hint").Id + " " + cut.Find(".lt-field__error").Id;
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("input").Select(input => input.GetAttribute("aria-describedby")), Is.All.EqualTo(described));
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Not now."));
        });
    }

    [Test]
    public void A_new_value_from_the_page_replaces_what_is_typed()
    {
        var cut = RenderField();
        cut.FindAll("input")[0].Change("x");

        cut.Render(p => p.Add(x => x.Value, TimeSpan.FromMinutes(90)));

        Assert.Multiple(() =>
        {
            Assert.That(Values(cut), Is.EqualTo(new[] { "1", "30" }));
            Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
        });
    }

    [Test]
    public void Disabled_reaches_every_box()
    {
        var cut = RenderField(p => p.Add(x => x.Disabled, true));

        Assert.That(cut.FindAll("input").Select(input => input.HasAttribute("disabled")), Is.All.True);
    }

    [Test]
    public void A_field_with_no_unit_is_a_mistake_that_says_so()
    {
        Assert.That(
            () => RenderField(p => p.Add(x => x.Units, LtDurationUnits.None)),
            Throws.InvalidOperationException.With.Message.Contains("offers no unit"));
    }

    [Test]
    public void Change_raises_a_valid_duration_and_repeated_changes_do_not_raise_it_again()
    {
        var raised = new List<TimeSpan?>();
        var cut = RenderField(p => p.Add(x => x.ValueChanged, (TimeSpan? value) => raised.Add(value)));

        cut.FindAll("input")[0].Change("2");
        cut.FindAll("input")[0].Change("2");
        cut.FindAll("input")[1].Change("30");

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.EqualTo(new TimeSpan?[] { TimeSpan.FromHours(2), TimeSpan.FromMinutes(150) }));
            Assert.That(cut.Instance.Value, Is.EqualTo(TimeSpan.FromMinutes(150)));
            Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
        });
    }

    [Test]
    public void A_single_unit_names_its_validation_message_and_a_hint_is_announced_without_an_error()
    {
        var cut = RenderField(p => p.Add(x => x.Units, LtDurationUnits.Seconds).Add(x => x.Hint, "How long."));
        Assert.That(cut.Find("input").GetAttribute("aria-describedby"), Is.EqualTo(cut.Find(".lt-field__hint").Id));

        cut.Find("input").Change("x");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.EndWith("Give a whole number of seconds."));
            Assert.That(cut.Find("input").GetAttribute("aria-describedby"),
                Is.EqualTo(cut.Find(".lt-field__hint").Id + " " + cut.Find(".lt-field__error").Id));
        });
    }

    [Test]
    public void A_count_that_fits_in_a_long_but_overflows_its_unit_is_refused_without_a_callback()
    {
        var raised = new List<TimeSpan?>();
        var cut = RenderField(p => p.Add(x => x.Units, LtDurationUnits.Days)
            .Add(x => x.ValueChanged, (TimeSpan? value) => raised.Add(value)));
        var tooManyDays = TimeSpan.MaxValue.Ticks / TimeSpan.TicksPerDay + 1;

        cut.Find("input").Change(tooManyDays.ToString(System.Globalization.CultureInfo.InvariantCulture));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.EndWith("Give a whole number of days."));
            Assert.That(raised, Is.Empty);
        });
    }

    [Test]
    public void Individually_valid_counts_whose_sum_overflows_are_refused()
    {
        var raised = new List<TimeSpan?>();
        var cut = RenderField(p => p.Add(x => x.Units, LtDurationUnits.Minutes | LtDurationUnits.Seconds)
            .Add(x => x.ValueChanged, (TimeSpan? value) => raised.Add(value)));
        var minutes = TimeSpan.MaxValue.Ticks / TimeSpan.TicksPerMinute;
        cut.FindAll("input")[0].Change(minutes.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Assert.That(raised, Has.Count.EqualTo(1), "the first count alone is representable");
        raised.Clear();

        cut.FindAll("input")[1].Change("59");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.EndWith("Give a whole number of minutes and seconds."));
            Assert.That(raised, Is.Empty);
            Assert.That(cut.Instance.Value, Is.EqualTo(TimeSpan.FromTicks(minutes * TimeSpan.TicksPerMinute)));
        });
    }

    [TestCase("1")]
    [TestCase("2")]
    public async Task Exact_range_boundaries_are_accepted_and_trimmed_input_is_not_rewritten(string hours)
    {
        var raised = new List<TimeSpan?>();
        var cut = RenderField(p => p.Add(x => x.Min, TimeSpan.FromHours(1)).Add(x => x.Max, TimeSpan.FromHours(2))
            .Add(x => x.ValueChanged, (TimeSpan? value) => raised.Add(value)));

        cut.FindAll("input")[0].Change(" " + hours + " ");
        var accepted = await cut.InvokeAsync(() => cut.Instance.ConfirmAsync());
        cut.Render();

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.True);
            Assert.That(raised, Is.EqualTo(new TimeSpan?[] { TimeSpan.FromHours(int.Parse(hours, System.Globalization.CultureInfo.InvariantCulture)) }));
            Assert.That(Values(cut)[0], Is.EqualTo(" " + hours + " "));
            Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
        });
    }

    [Test]
    public void Changing_units_resplits_the_same_value_and_clears_a_previous_error()
    {
        var cut = RenderField(p => p.Add(x => x.Value, TimeSpan.FromMinutes(90)));
        cut.FindAll("input")[0].Change("bad");

        cut.Render(p => p.Add(x => x.Units, LtDurationUnits.Minutes));

        Assert.Multiple(() =>
        {
            Assert.That(Values(cut), Is.EqualTo(new[] { "90" }));
            Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
            Assert.That(cut.Find("input").GetAttribute("aria-label"), Is.EqualTo("Every, minutes"));
        });
    }

    [Test]
    public void Negative_values_are_displayed_as_zero_and_subunit_remainders_are_not_displayed()
    {
        var negative = RenderField(p => p.Add(x => x.Value, TimeSpan.FromHours(-1)));
        var remainder = RenderField(p => p.Add(x => x.Value, TimeSpan.FromHours(1) + TimeSpan.FromSeconds(59)));

        Assert.Multiple(() =>
        {
            Assert.That(Values(negative), Is.EqualTo(new[] { "0", "0" }));
            Assert.That(Values(remainder), Is.EqualTo(new[] { "1", "0" }));
        });
    }

    private IRenderedComponent<LtDurationInput> RenderField(Action<ComponentParameterCollectionBuilder<LtDurationInput>>? more = null) =>
        Render<LtDurationInput>(p =>
        {
            p.Add(x => x.Label, "Every");
            more?.Invoke(p);
        });

    private static string[] Values(IRenderedComponent<LtDurationInput> cut) =>
        [.. cut.FindAll("input").Select(input => input.GetAttribute("value") ?? string.Empty)];
}
