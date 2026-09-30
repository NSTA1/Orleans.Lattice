using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// Issue #3986: a picker looks like a picker. While idle a field that can list
/// values carries a select's chevron, which turns while the list is open; a
/// field can list its values as soon as it has focus, as a dropdown does; and
/// the value already in force is drawn with the "you are here" node.
/// </summary>
public sealed partial class LtComboBoxTests
{
    [Test]
    public void An_idle_field_that_can_list_values_carries_a_chevron()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));

        var control = cut.Find(".lt-combobox__control");
        var chevron = cut.Find(".lt-combobox__control > svg.lt-combobox__chevron");
        Assert.Multiple(() =>
        {
            Assert.That(control.ClassList, Does.Contain("lt-combobox__control--picker"));
            Assert.That(chevron.GetAttribute("aria-hidden"), Is.EqualTo("true"), "the chevron is decoration; the combobox role says what the field is");
            Assert.That(chevron.GetAttribute("focusable"), Is.EqualTo("false"));
            Assert.That(control.HasAttribute("data-lt-open"), Is.False);
        });
    }

    [TestCase("none")]
    [TestCase("disabled")]
    [TestCase("readonly")]
    public void A_field_that_cannot_list_values_draws_no_chevron(string why)
    {
        var cut = Render<LtComboBox>(p =>
        {
            p.Add(x => x.Label, "Tree");
            if (why != "none")
            {
                p.Add(x => x.Source, new FakeSuggestionSource(Trees));
            }

            p.Add(x => x.Disabled, why == "disabled").Add(x => x.ReadOnly, why == "readonly");
        });

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-combobox__chevron"), Is.Empty);
            Assert.That(cut.Find(".lt-combobox__control").ClassList, Does.Not.Contain("lt-combobox__control--picker"));
        });
    }

    [Test]
    public void The_control_is_marked_open_while_the_list_shows()
    {
        var cut = RenderBox(new FakeSuggestionSource(Trees));

        cut.Find("input").Input("crm/");
        var open = cut.Find(".lt-combobox__control").GetAttribute("data-lt-open");
        Key(cut, "Escape");

        Assert.Multiple(() =>
        {
            Assert.That(open, Is.EqualTo("true"));
            Assert.That(cut.Find(".lt-combobox__control").HasAttribute("data-lt-open"), Is.False);
        });
    }

    [Test]
    public void OpenOnFocus_lists_every_value_as_soon_as_the_field_has_focus()
    {
        var source = new FakeSuggestionSource(Trees);
        var cut = RenderBox(source, p => p.Add(x => x.OpenOnFocus, true));

        cut.Find("input").Focus();

        Assert.Multiple(() =>
        {
            Assert.That(source.Queries.Select(query => query.Text), Is.EqualTo(new[] { string.Empty }));
            Assert.That(cut.Find("input").GetAttribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(Options(cut), Is.EqualTo(Trees));
            Assert.That(cut.Find("input").HasAttribute("aria-activedescendant"), Is.False, "nothing is highlighted until the keyboard moves");
        });
    }

    [Test]
    public void Without_OpenOnFocus_focus_alone_lists_nothing()
    {
        var source = new FakeSuggestionSource(Trees);
        var cut = RenderBox(source);

        cut.Find("input").Focus();

        Assert.Multiple(() =>
        {
            Assert.That(source.Queries, Is.Empty);
            Assert.That(cut.FindAll("[role=listbox]"), Is.Empty);
        });
    }

    [Test]
    public void The_current_value_is_drawn_with_the_you_are_here_node()
    {
        var source = new ListedSource(
            new LtSuggestion("acme", "Active tenant") { Current = true },
            new LtSuggestion("globex"));
        var cut = RenderBox(source, p => p.Add(x => x.OpenOnFocus, true));

        cut.Find("input").Focus();

        var options = cut.FindAll("[role=option]");
        Assert.Multiple(() =>
        {
            Assert.That(options[0].QuerySelector(".lt-node")!.ClassList, Does.Contain("lt-node--join"));
            Assert.That(options[0].QuerySelector(".lt-combobox__detail")!.TextContent, Is.EqualTo("Active tenant"), "the mark is never the node alone");
            Assert.That(options[1].QuerySelector(".lt-node")!.ClassList, Does.Contain("lt-node--hollow"));
        });
    }

    [Test]
    public void Escape_with_the_list_already_closed_asks_the_host_to_dismiss()
    {
        // A host panel's own Escape handler can be starved by a stale stop-propagation
        // flag when keys come quickly; the field decides on the server instead.
        var dismissed = 0;
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p.Add(x => x.OnDismiss, () => dismissed++));
        cut.Find("input").Input("crm/");

        Key(cut, "Escape");
        var afterFirst = dismissed;
        Key(cut, "Escape");

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.Zero, "the first Escape only closes the list");
            Assert.That(dismissed, Is.EqualTo(1), "the next Escape asks the host to close");
        });
    }

    [Test]
    public void With_OpenOnFocus_one_Escape_closes_the_list_and_asks_the_host_to_dismiss()
    {
        // The list is the field's dropdown, so Escape is not spent closing it first.
        var dismissed = 0;
        var cut = RenderBox(new FakeSuggestionSource(Trees), p => p.Add(x => x.OpenOnFocus, true).Add(x => x.OnDismiss, () => dismissed++));
        cut.Find("input").Focus();
        Assert.That(cut.Find("input").GetAttribute("aria-expanded"), Is.EqualTo("true"));

        Key(cut, "Escape");

        Assert.Multiple(() =>
        {
            Assert.That(dismissed, Is.EqualTo(1));
            Assert.That(cut.Find("input").GetAttribute("aria-expanded"), Is.EqualTo("false"));
        });
    }

    [Test]
    public void Inside_a_dialog_Escape_on_a_closed_list_closes_the_dialog_exactly_once()
    {
        // The field keeps its keys from its ancestors, so the dialog is closed by the
        // field alone: never twice, and never starved by a stale propagation flag.
        var closes = 0;
        var cut = RenderInDialog(() => closes++, dismissOnEscape: true);
        var input = cut.Find("input");

        input.Input("crm/");
        input.KeyDown(new KeyboardEventArgs { Key = "Escape" });
        var afterListEscape = closes;
        input.KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.Multiple(() =>
        {
            Assert.That(afterListEscape, Is.Zero, "the first Escape closes only the list");
            Assert.That(closes, Is.EqualTo(1), "the next one closes the dialog, once");
        });
    }

    [Test]
    public void Inside_a_dialog_that_ignores_Escape_the_field_does_not_close_it()
    {
        var closes = 0;
        var cut = RenderInDialog(() => closes++, dismissOnEscape: false);

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.That(closes, Is.Zero);
    }

    private IRenderedComponent<LtDialog> RenderInDialog(Action closed, bool dismissOnEscape) =>
        Render<LtDialog>(p => p
            .Add(x => x.Open, true)
            .Add(x => x.Title, "New tenant")
            .Add(x => x.DismissOnEscape, dismissOnEscape)
            .Add(x => x.OpenChanged, (bool open) => { if (!open) { closed(); } })
            .AddChildContent<LtComboBox>(box => box
                .Add(x => x.Label, "Tree")
                .Add(x => x.Noun, "tree")
                .Add(x => x.Source, new FakeSuggestionSource(Trees))));

    [Test]
    public void A_suggestion_is_not_current_unless_it_says_so()
    {
        var plain = new LtSuggestion("acme");
        var current = plain with { Current = true };

        Assert.Multiple(() =>
        {
            Assert.That(plain.Current, Is.False);
            Assert.That(current.Current, Is.True);
            Assert.That(current, Is.Not.EqualTo(plain), "being current is part of the suggestion's value");
        });
    }

    private sealed class ListedSource(params LtSuggestion[] items) : ILtSuggestionSource
    {
        public ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken) =>
            ValueTask.FromResult(LtSuggestionSet.Of(items, truncated: false));
    }
}
