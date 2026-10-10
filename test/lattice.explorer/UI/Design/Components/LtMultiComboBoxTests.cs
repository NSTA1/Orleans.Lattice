using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The multi-value combobox: chosen values as removable chips, added by choosing
/// a suggestion, by Enter or by a comma; a value the source does not list is
/// refused in pick-existing mode, and free text is accepted when the source
/// cannot list.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtMultiComboBoxTests : ShellDesignTestContext
{
    private static readonly string[] Regions = ["eu-west", "eu-north", "us-east"];

    [Test]
    public void The_chosen_values_are_chips_each_with_a_named_remove_control()
    {
        var cut = RenderBox(new FakeSuggestionSource(Regions), ["eu-west", "us-east"]);

        var chips = cut.FindAll(".lt-combobox__chip");
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-combobox__chips").GetAttribute("aria-label"), Is.EqualTo("Allowed regions, chosen"));
            Assert.That(chips.Select(chip => chip.QuerySelector(".lt-combobox__chip-value")!.TextContent), Is.EqualTo(new[] { "eu-west", "us-east" }));
            Assert.That(chips[0].QuerySelector("button")!.GetAttribute("aria-label"), Is.EqualTo("Remove eu-west"));
            Assert.That(cut.Find("input").GetAttribute("data-lt-enter"), Is.EqualTo("add"));
        });
    }

    [Test]
    public void No_chip_list_is_drawn_while_nothing_is_chosen()
    {
        var cut = RenderBox(new FakeSuggestionSource(Regions), []);

        Assert.That(cut.FindAll(".lt-combobox__chips"), Is.Empty);
    }

    [Test]
    public void The_chips_and_the_input_are_one_control_in_one_frame()
    {
        // Issue #3986: chips drawn as boxes above an empty input read as two controls.
        var cut = RenderBox(new FakeSuggestionSource(Regions), ["eu-west", "us-east"]);

        var frame = cut.Find(".lt-combobox__control");
        var children = frame.Children.Select(child => child.LocalName).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(frame.ClassList, Does.Contain("lt-combobox__control--tokens"));
            Assert.That(children.Take(2), Is.EqualTo(new[] { "ul", "input" }), "the chosen values come first, then the input, inside the one frame");
            Assert.That(frame.QuerySelector(".lt-combobox__chips"), Is.Not.Null);
            Assert.That(cut.Find(".lt-combobox").Children.Count(child => child.LocalName == "ul"), Is.Zero, "no chip list sits outside the frame");
        });
    }

    [Test]
    public void The_frame_is_drawn_before_anything_is_chosen_so_the_field_never_changes_shape()
    {
        var cut = RenderBox(new FakeSuggestionSource(Regions), []);

        Assert.That(cut.Find(".lt-combobox__control").ClassList, Does.Contain("lt-combobox__control--tokens"));
    }

    [Test]
    public void An_error_marks_the_frame_not_the_borderless_input()
    {
        var cut = RenderBox(new FakeSuggestionSource(Regions), ["eu-west"]);

        cut.Find("input").Input("mars");
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Enter" });

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-combobox__control").ClassList, Does.Contain("lt-combobox__control--invalid"));
            Assert.That(cut.Find("input").GetAttribute("aria-invalid"), Is.EqualTo("true"));
        });
    }

    [Test]
    public void Choosing_a_suggestion_adds_it_and_clears_the_input()
    {
        IReadOnlyList<string>? values = null;
        var cut = RenderBox(new FakeSuggestionSource(Regions), ["eu-west"], v => values = v);

        cut.Find("input").Input("us");
        cut.FindAll("[role=option]")[0].Click();

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.EqualTo(new[] { "eu-west", "us-east" }));
            Assert.That(cut.Find("input").GetAttribute("value"), Is.Empty);
            Assert.That(cut.FindAll(".lt-combobox__chip"), Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void Enter_adds_a_typed_existing_value()
    {
        IReadOnlyList<string>? values = null;
        var cut = RenderBox(new FakeSuggestionSource(Regions), [], v => values = v);

        cut.Find("input").Input("eu-north");
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Enter" });

        Assert.That(values, Is.EqualTo(new[] { "eu-north" }));
    }

    [Test]
    public void A_comma_adds_every_value_before_it()
    {
        IReadOnlyList<string>? values = null;
        var cut = RenderBox(new FakeSuggestionSource(Regions), [], v => values = v);

        cut.Find("input").Input("eu-west, us-east,");

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.EqualTo(new[] { "eu-west", "us-east" }));
            Assert.That(cut.Find("input").GetAttribute("value"), Is.Empty);
        });
    }

    [Test]
    public void Pick_existing_refuses_a_value_the_source_does_not_list_and_keeps_it_to_correct()
    {
        IReadOnlyList<string>? values = null;
        var cut = RenderBox(new FakeSuggestionSource(Regions), [], v => values = v);

        cut.Find("input").Input("eu-west, mars-1,");

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.EqualTo(new[] { "eu-west" }));
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("No region is named mars-1."));
            Assert.That(cut.Find("input").GetAttribute("value"), Is.EqualTo("mars-1"));
        });
    }

    [Test]
    public void A_refused_pasted_value_keeps_the_unfinished_value_after_the_last_separator()
    {
        IReadOnlyList<string>? values = null;
        var cut = RenderBox(new FakeSuggestionSource(Regions), [], v => values = v);

        cut.Find("input").Input("eu-west, mars-1, us");

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.EqualTo(new[] { "eu-west" }));
            Assert.That(cut.Find("input").GetAttribute("value"), Is.EqualTo("mars-1, us"));
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("No region is named mars-1."));
        });
    }

    [Test]
    public void Duplicates_are_ignored()
    {
        IReadOnlyList<string>? values = null;
        var cut = RenderBox(new FakeSuggestionSource(Regions), ["eu-west"], v => values = v);

        cut.Find("input").Input("eu-west,");

        Assert.That(values, Is.Null, "nothing changed, so nothing is raised");
    }

    [Test]
    public void A_source_that_cannot_list_accepts_what_is_typed()
    {
        IReadOnlyList<string>? values = null;
        var source = new FakeSuggestionSource(Regions) { Unavailable = "The cluster's regions could not be listed." };
        var cut = RenderBox(source, [], v => values = v);

        cut.Find("input").Input("anywhere,");

        Assert.That(values, Is.EqualTo(new[] { "anywhere" }));
    }

    [Test]
    public void Removing_a_chip_raises_the_remaining_values_and_returns_focus_to_the_input()
    {
        IReadOnlyList<string>? values = null;
        var cut = RenderBox(new FakeSuggestionSource(Regions), ["eu-west", "us-east"], v => values = v);

        cut.FindAll(".lt-combobox__remove")[0].Click();

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.EqualTo(new[] { "us-east" }));
            Assert.That(JSInterop.VerifyFocusAsyncInvoke().Arguments[0], Is.InstanceOf<Microsoft.AspNetCore.Components.ElementReference>(), "focus goes back to the input, not to the body");

        });
    }

    [Test]
    public async Task ConfirmAsync_adds_a_value_still_typed_and_reports_a_refusal()
    {
        IReadOnlyList<string>? values = null;
        var cut = RenderBox(new FakeSuggestionSource(Regions), [], v => values = v);

        cut.Find("input").Input("us-east");
        var accepted = await cut.InvokeAsync(cut.Instance.ConfirmAsync);
        cut.Find("input").Input("pluto");
        var refused = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.True);
            Assert.That(values, Is.EqualTo(new[] { "us-east" }));
            Assert.That(refused, Is.False);
        });
    }

    [Test]
    public async Task ConfirmAsync_with_nothing_typed_accepts()
    {
        var cut = RenderBox(new FakeSuggestionSource(Regions), []);

        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
    }

    [Test]
    public void Suggest_mode_adds_new_values()
    {
        IReadOnlyList<string>? values = null;
        var cut = RenderBox(new FakeSuggestionSource(Regions), [], v => values = v, LtComboBoxMode.Suggest);

        cut.Find("input").Input("ap-south,");

        Assert.That(values, Is.EqualTo(new[] { "ap-south" }));
    }

    [Test]
    public void The_page_error_and_disabled_state_reach_the_field()
    {
        var cut = Render<LtMultiComboBox>(p => p
            .Add(x => x.Label, "Allowed regions")
            .Add(x => x.Values, new[] { "eu-west" })
            .Add(x => x.Error, "Revoke residency first.")
            .Add(x => x.Disabled, true)
            .Add(x => x.Hint, "Every allowed region."));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Revoke residency first."));
            Assert.That(cut.Find("input").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find(".lt-combobox__remove").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find(".lt-field__hint").TextContent, Is.EqualTo("Every allowed region."));
        });
    }

    private IRenderedComponent<LtMultiComboBox> RenderBox(
        ILtSuggestionSource source,
        string[] values,
        Action<IReadOnlyList<string>>? changed = null,
        LtComboBoxMode mode = LtComboBoxMode.PickExisting) =>
        Render<LtMultiComboBox>(p =>
        {
            p.Add(x => x.Label, "Allowed regions")
                .Add(x => x.Noun, "region")
                .Add(x => x.Source, source)
                .Add(x => x.Mode, mode)
                .Add(x => x.Values, values);
            if (changed is not null)
            {
                p.Add(x => x.ValuesChanged, changed);
            }
        });
}
