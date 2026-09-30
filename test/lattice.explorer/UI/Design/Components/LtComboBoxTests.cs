using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The combobox as an ARIA 1.2 combobox with a listbox popup: a labelled input
/// that owns its listbox, keyboard-complete with no focus trap, the highlighted
/// option announced through <c>aria-activedescendant</c> and the result count
/// through a polite status region.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed partial class LtComboBoxTests : ShellDesignTestContext
{
    private static readonly string[] Trees = ["crm/orders", "crm/customers", "billing/invoices", "crm/orders-archive"];

    [Test]
    public void The_input_is_a_labelled_combobox_whose_list_starts_closed()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));
        var input = cut.Find("input");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("label").GetAttribute("for"), Is.EqualTo(input.Id));
            Assert.That(input.Id, Is.EqualTo(cut.Instance.InputId));
            Assert.That(input.GetAttribute("role"), Is.EqualTo("combobox"));
            Assert.That(input.GetAttribute("aria-autocomplete"), Is.EqualTo("list"));
            Assert.That(input.GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(input.HasAttribute("aria-activedescendant"), Is.False);
            Assert.That(input.ClassList, Does.Contain("lt-input--mono"));
            Assert.That(cut.FindAll("[role=listbox]"), Is.Empty);
        });
    }

    [Test]
    public void Typing_lists_the_matching_existing_values_and_announces_how_many()
    {
        var source = new FakeSuggestionSource(Trees);
        var cut = RenderBox(source);

        cut.Find("input").Input("crm/");

        var input = cut.Find("input");
        var listbox = cut.Find("[role=listbox]");
        Assert.Multiple(() =>
        {
            Assert.That(source.Queries.Select(query => query.Text), Is.EqualTo(new[] { "crm/" }));
            Assert.That(input.GetAttribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(input.GetAttribute("aria-controls"), Is.EqualTo(listbox.Id));
            Assert.That(Options(cut), Is.EqualTo(new[] { "crm/orders", "crm/customers", "crm/orders-archive" }));
            Assert.That(cut.FindAll("[role=option]").All(option => option.GetAttribute("aria-selected") == "false"), Is.True);
            Assert.That(Status(cut), Is.EqualTo("3 suggestions."));
        });
    }

    [Test]
    public void The_query_is_bounded_by_the_limit()
    {
        var source = new FakeSuggestionSource(Trees);
        var cut = RenderBox(source, p => p.Add(x => x.Limit, 2));

        cut.Find("input").Input("crm");

        Assert.Multiple(() =>
        {
            Assert.That(source.Queries.Single().Limit, Is.EqualTo(2));
            Assert.That(Options(cut), Has.Length.EqualTo(2));
            Assert.That(Status(cut), Does.StartWith("More than 2 suggestions"));
        });
    }

    [Test]
    public void Arrow_keys_move_the_highlighted_option_and_wrap()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));
        cut.Find("input").Input("crm/");
        var options = cut.FindAll("[role=option]");

        Key(cut, "ArrowDown");
        Assert.That(cut.Find("input").GetAttribute("aria-activedescendant"), Is.EqualTo(options[0].Id));
        Assert.That(cut.FindAll("[role=option]")[0].GetAttribute("aria-selected"), Is.EqualTo("true"));
        Assert.That(Status(cut), Is.EqualTo("crm/orders, Tree"));

        Key(cut, "ArrowUp");
        Assert.That(cut.Find("input").GetAttribute("aria-activedescendant"), Is.EqualTo(cut.FindAll("[role=option]")[2].Id), "Up from the first wraps to the last");

        Key(cut, "ArrowDown");
        Assert.That(cut.Find("input").GetAttribute("aria-activedescendant"), Is.EqualTo(cut.FindAll("[role=option]")[0].Id), "Down from the last wraps to the first");
    }

    [Test]
    public void Down_on_a_closed_field_opens_the_list_on_the_first_option()
    {
        var source = new FakeSuggestionSource(Trees);
        var cut = RenderBox(source);

        Key(cut, "ArrowDown");

        Assert.Multiple(() =>
        {
            Assert.That(source.Queries.Single().Text, Is.Empty, "an empty field lists the first values");
            Assert.That(cut.Find("input").GetAttribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(cut.Find("input").GetAttribute("aria-activedescendant"), Is.EqualTo(cut.FindAll("[role=option]")[0].Id));
        });
    }

    [Test]
    public void Up_on_a_closed_field_opens_the_list_on_the_last_option()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));

        Key(cut, "ArrowUp");

        var options = cut.FindAll("[role=option]");
        Assert.That(cut.Find("input").GetAttribute("aria-activedescendant"), Is.EqualTo(options[^1].Id));
    }

    [Test]
    public void Enter_chooses_the_highlighted_option_and_closes_the_list()
    {
        string? value = null;
        string? committed = null;
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p
            .Add(x => x.ValueChanged, (string v) => value = v)
            .Add(x => x.OnCommit, (string v) => committed = v));
        cut.Find("input").Input("crm/");
        Key(cut, "ArrowDown");
        Key(cut, "ArrowDown");

        Key(cut, "Enter");

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo("crm/customers"));
            Assert.That(committed, Is.EqualTo("crm/customers"));
            Assert.That(cut.Find("input").GetAttribute("value"), Is.EqualTo("crm/customers"));
            Assert.That(cut.Find("input").GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(Status(cut), Is.EqualTo("crm/customers chosen."));
        });
    }

    [Test]
    public void Clicking_an_option_chooses_it()
    {
        string? value = null;
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p.Add(x => x.ValueChanged, (string v) => value = v));
        cut.Find("input").Input("bill");

        cut.FindAll("[role=option]")[0].Click();

        Assert.That(value, Is.EqualTo("billing/invoices"));
    }

    [Test]
    public void Choosing_reports_the_suggestion_itself_and_typing_does_not()
    {
        var chosen = new List<LtSuggestion>();
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p.Add(x => x.OnChoose, (LtSuggestion s) => chosen.Add(s)));

        cut.Find("input").Input("bill");
        Assert.That(chosen, Is.Empty);
        cut.FindAll("[role=option]")[0].Click();

        Assert.That(chosen, Is.EqualTo(new[] { new LtSuggestion("billing/invoices", "Tree") }));
    }
    [Test]
    public void Escape_closes_the_list_and_keeps_the_text()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));
        cut.Find("input").Input("crm/");
        Key(cut, "ArrowDown");

        Key(cut, "Escape");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=listbox]"), Is.Empty);
            Assert.That(cut.Find("input").GetAttribute("value"), Is.EqualTo("crm/"));
            Assert.That(cut.Find("input").HasAttribute("aria-activedescendant"), Is.False);
        });
    }

    [TestCase("Home")]
    [TestCase("End")]
    public void Home_and_End_return_to_editing_the_text(string key)
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));
        cut.Find("input").Input("crm/");
        Key(cut, "ArrowDown");

        Key(cut, key);

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("input").HasAttribute("aria-activedescendant"), Is.False);
            Assert.That(cut.FindAll("[role=option]"), Has.Count.EqualTo(3), "the list stays open");
        });
    }

    [Test]
    public void Tab_closes_the_list_without_choosing()
    {
        string? value = null;
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p.Add(x => x.ValueChanged, (string v) => value = v));
        cut.Find("input").Input("crm/");
        Key(cut, "ArrowDown");

        Key(cut, "Tab");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=listbox]"), Is.Empty);
            Assert.That(value, Is.EqualTo("crm/"), "no focus trap and no silent choice");
        });
    }

    [Test]
    public void Clicking_the_field_opens_the_list_for_touch()
    {
        var source = new FakeSuggestionSource(Trees);
        var cut = RenderBox(source);

        cut.Find("input").Click();

        Assert.That(Options(cut), Has.Length.EqualTo(4));
    }

    [Test]
    public void A_read_only_or_disabled_field_never_asks_its_source()
    {
        var source = new FakeSuggestionSource(Trees);
        var cut = RenderBox(source, p => p.Add(x => x.ReadOnly, true));

        cut.Find("input").Click();
        Key(cut, "ArrowDown");

        Assert.Multiple(() =>
        {
            Assert.That(source.Queries, Is.Empty);
            Assert.That(cut.Find("input").HasAttribute("readonly"), Is.True);
        });
    }

    [Test]
    public void A_field_without_a_source_is_a_plain_text_input()
    {
        string? value = null;
        var cut = Render<LtComboBox>(p => p.Add(x => x.Label, "Tree").Add(x => x.ValueChanged, (string v) => value = v));

        cut.Find("input").Input("anything");

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo("anything"));
            Assert.That(cut.FindAll("[role=listbox]"), Is.Empty);
        });
    }

    [Test]
    public void The_hint_and_the_pages_error_are_announced_with_the_input()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p.Add(x => x.Hint, "A tree id.").Add(x => x.Error, "Required."));
        var input = cut.Find("input");

        Assert.Multiple(() =>
        {
            Assert.That(input.GetAttribute("aria-invalid"), Is.EqualTo("true"));
            Assert.That(input.GetAttribute("aria-describedby"), Is.EqualTo(cut.Find(".lt-field__hint").Id + " " + cut.Find(".lt-field__error").Id));
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Required."));
        });
    }

    [Test]
    public void A_new_value_from_the_page_replaces_the_text()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p.Add(x => x.Value, "crm/orders"));

        cut.Render(p => p.Add(x => x.Value, string.Empty));

        Assert.That(cut.Find("input").GetAttribute("value"), Is.Empty);
    }

    [Test]
    public void The_proportional_variant_is_spell_checked_as_prose()
    {
        var input = RenderBox(new FakeSuggestionSource(Trees), p => p.Add(x => x.Mono, false)).Find("input");

        Assert.Multiple(() =>
        {
            Assert.That(input.ClassList, Does.Not.Contain("lt-input--mono"));
            Assert.That(input.HasAttribute("spellcheck"), Is.False);
        });
    }

    [Test]
    public void A_committing_field_asks_script_to_keep_Enter_from_submitting_the_form()
    {
        var plain = RenderBox(new FakeSuggestionSource(Trees)).Find("input");
        var committing = RenderBox(new FakeSuggestionSource(Trees), p => p.Add(x => x.OnCommit, (string _) => { })).Find("input");

        Assert.Multiple(() =>
        {
            Assert.That(plain.HasAttribute("data-lt-enter"), Is.False);
            Assert.That(committing.GetAttribute("data-lt-enter"), Is.EqualTo("commit"));
        });
    }

    [Test]
    public void The_script_module_is_imported_and_attached_to_the_input()
    {
        var module = JSInterop.SetupModule(Orleans.Lattice.Explorer.UI.Design.ShellDesignAssets.ComboBoxModuleSpecifier);
        module.Mode = JSRuntimeMode.Loose;

        var cut = RenderBox(new FakeSuggestionSource(Trees));

        var attach = module.VerifyInvoke("attach");
        Assert.That(((ElementReference)attach.Arguments[0]!).Id, Is.EqualTo(cut.Find("input").GetAttribute("blazor:elementreference")));
    }

    [Test]
    public async Task FocusAsync_moves_focus_to_the_input()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));

        await cut.InvokeAsync(() => cut.Instance.FocusAsync().AsTask());

        var invocation = JSInterop.VerifyFocusAsyncInvoke();
        Assert.That(((ElementReference)invocation.Arguments[0]!).Id, Is.EqualTo(cut.Find("input").GetAttribute("blazor:elementreference")));
    }

    private IRenderedComponent<LtComboBox> RenderBox(ILtSuggestionSource source, Action<ComponentParameterCollectionBuilder<LtComboBox>>? more = null) =>
        Render<LtComboBox>(p =>
        {
            p.Add(x => x.Label, "Tree").Add(x => x.Noun, "tree").Add(x => x.Source, source);
            more?.Invoke(p);
        });

    private static void Key(IRenderedComponent<LtComboBox> cut, string key) =>
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = key });

    private static string[] Options(IRenderedComponent<LtComboBox> cut) =>
        [.. cut.FindAll("[role=option] .lt-combobox__value").Select(element => element.TextContent)];

    private static string Status(IRenderedComponent<LtComboBox> cut) => cut.Find("[role=status]").TextContent;
}
