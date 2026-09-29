using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The two modes: pick-existing refuses a value that names nothing with an inline
/// error; suggest accepts any text and flags a value that exists. A source that
/// cannot list, or fails, turns either into free text with a note - never a crash.
/// </summary>
public sealed partial class LtComboBoxTests
{
    [Test]
    public void Pick_existing_refuses_a_typed_value_that_names_nothing_when_the_field_is_left()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));
        cut.Find("input").Input("crm/order");

        cut.Find("input").Blur();

        var input = cut.Find("input");
        Assert.Multiple(() =>
        {
            Assert.That(input.GetAttribute("aria-invalid"), Is.EqualTo("true"));
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("No tree is named crm/order. Choose one from the list."));
            Assert.That(input.GetAttribute("aria-describedby"), Does.Contain(cut.Find(".lt-field__error").Id));
        });
    }

    [Test]
    public async Task Pick_existing_confirms_only_a_listed_value()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));

        cut.Find("input").Input("crm/order");
        var refused = await cut.InvokeAsync(cut.Instance.ConfirmAsync);
        cut.Find("input").Input("crm/orders");
        var accepted = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(refused, Is.False);
            Assert.That(accepted, Is.True);
            Assert.That(cut.FindAll(".lt-field__error"), Is.Empty, "the accepted value clears the error");
        });
    }

    [Test]
    public async Task An_empty_value_is_the_pages_to_require()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));

        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
    }

    [Test]
    public async Task Pick_existing_confirms_an_exact_match_even_beyond_the_limit()
    {
        // The source puts an exact match first, so a value that is also the prefix
        // of many others is still found in a bounded answer.
        var values = Enumerable.Range(0, 20).Select(i => $"crm/orders-{i:00}").Append("crm/orders").ToArray();
        var cut = RenderBox(new FakeSuggestionSource(values), p => p.Add(x => x.Limit, 3));

        cut.Find("input").Input("crm/orders");

        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
    }

    [Test]
    public void Suggest_mode_accepts_new_text_and_flags_a_value_that_exists()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p.Add(x => x.Mode, LtComboBoxMode.Suggest));

        cut.Find("input").Input("crm/new");
        Assert.That(cut.FindAll(".lt-combobox__flag"), Is.Empty);

        cut.Find("input").Input("crm/orders");
        var flag = cut.Find(".lt-combobox__flag");
        Assert.Multiple(() =>
        {
            Assert.That(flag.TextContent, Is.EqualTo("A tree named crm/orders already exists."));
            Assert.That(cut.Find("input").GetAttribute("aria-describedby"), Does.Contain(flag.Id));
            Assert.That(cut.Find("input").HasAttribute("aria-invalid"), Is.False, "a flag is not an error");
        });
    }

    [Test]
    public void Suggest_mode_uses_the_pages_own_flag_sentence()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p
            .Add(x => x.Mode, LtComboBoxMode.Suggest)
            .Add(x => x.ExistingMessage, "Restoring replaces this tree's contents."));

        cut.Find("input").Input("crm/orders");

        Assert.That(cut.Find(".lt-combobox__flag").TextContent, Is.EqualTo("Restoring replaces this tree's contents."));
    }

    [Test]
    public async Task Suggest_mode_can_refuse_a_value_that_must_be_new()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p
            .Add(x => x.Mode, LtComboBoxMode.Suggest)
            .Add(x => x.RejectExisting, true));

        cut.Find("input").Input("crm/orders");
        var refused = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(refused, Is.False);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("A tree named crm/orders already exists."));
        });

        cut.Find("input").Input("crm/brand-new");
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
    }

    [Test]
    public async Task A_source_that_cannot_list_turns_the_field_into_free_text_and_says_why()
    {
        var source = new FakeSuggestionSource(Trees) { Unavailable = "No identity directory is configured." };
        var cut = RenderBox(source);

        cut.Find("input").Input("anyone");

        var note = cut.Find(".lt-combobox__note");
        Assert.Multiple(() =>
        {
            Assert.That(note.TextContent, Is.EqualTo("No identity directory is configured."));
            Assert.That(cut.Find("input").GetAttribute("aria-describedby"), Does.Contain(note.Id));
            Assert.That(cut.FindAll("[role=listbox]"), Is.Empty);
        });
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True, "a pick-existing field falls back to suggest mode");
    }

    [Test]
    public async Task A_source_that_throws_is_a_note_never_a_crash()
    {
        var source = new FakeSuggestionSource(Trees) { Throws = new InvalidOperationException("boom") };
        var cut = RenderBox(source);

        cut.Find("input").Input("crm");
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-combobox__note").TextContent, Is.EqualTo("Suggestions could not be loaded, so the value is used as typed."));
            Assert.That(cut.Markup, Does.Not.Contain("boom"));
        });
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
    }

    [Test]
    public void A_query_that_matches_nothing_says_so()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));

        cut.Find("input").Input("zzz");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=listbox]"), Is.Empty);
            Assert.That(Status(cut), Is.EqualTo("No tree matches zzz."));
        });
    }

    [Test]
    public void Choosing_a_value_clears_the_fields_own_error()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));
        cut.Find("input").Input("crm/order");
        cut.Find("input").Blur();
        Assert.That(cut.FindAll(".lt-field__error"), Has.Count.EqualTo(1));

        cut.Find("input").Input("crm/orders");
        cut.FindAll("[role=option]")[0].Click();

        Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
    }

    [Test]
    public void The_pages_error_replaces_the_fields_own()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p.Add(x => x.Error, "The tree is being deleted."));
        cut.Find("input").Input("crm/order");
        cut.Find("input").Blur();

        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("The tree is being deleted.").And.Not.Contain("No tree"));
    }
}
