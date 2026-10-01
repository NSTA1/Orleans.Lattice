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

    private IRenderedComponent<LtDurationInput> RenderField(Action<ComponentParameterCollectionBuilder<LtDurationInput>>? more = null) =>
        Render<LtDurationInput>(p =>
        {
            p.Add(x => x.Label, "Every");
            more?.Invoke(p);
        });

    private static string[] Values(IRenderedComponent<LtDurationInput> cut) =>
        [.. cut.FindAll("input").Select(input => input.GetAttribute("value") ?? string.Empty)];
}
