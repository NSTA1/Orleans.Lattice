namespace Orleans.Lattice.Explorer.Tests.UI.Design;

/// <summary>
/// Issue #3986: the design system draws each kind of control one way, wherever it
/// is used, measured in the stylesheets that ship. An anchor drawn as a button is
/// the button; a picker looks like a picker; a multi-value field is one control;
/// an id in a table is never broken mid-token; and the tenant switcher's list
/// stays inside the panel that holds it.
/// </summary>
[TestFixture]
public sealed class ShellDesignConsistencyTests
{
    private const string Chrome = ShellStylesheets.WebRoot + "/shell/lattice-chrome.css";

    [Test]
    public void An_anchor_drawn_as_a_button_is_drawn_exactly_as_a_native_button()
    {
        // A link's underline inside button chrome, and a content-box border that
        // makes an <a class="lt-btn"> 2px taller than its <button> neighbours.
        var button = Body(ShellStylesheets.Primitives, ".lt-btn");

        Assert.Multiple(() =>
        {
            Assert.That(button, Does.Contain("text-decoration: none;"));
            Assert.That(button, Does.Contain("box-sizing: border-box;"));
        });
    }

    [Test]
    public void An_idle_picker_draws_the_selects_arrow_in_the_text_colour()
    {
        var chevron = Body(ShellStylesheets.Primitives, ".lt-combobox__chevron");
        var select = Body(ShellStylesheets.Primitives, ".lt-select");

        Assert.Multiple(() =>
        {
            Assert.That(chevron, Does.Contain("fill: currentColor;"), "drawn in the text colour, so forced colours keep it");
            Assert.That(chevron, Does.Contain("color: var(--lt-ink-2);"));
            Assert.That(select, Does.Contain("var(--lt-ink-2)"), "the same ink as the select's arrow");
            Assert.That(chevron, Does.Contain("inset-inline-end: 0.7rem;").And.Contain("width: 0.6rem;").And.Contain("height: 0.3rem;"),
                "the same size and place as the select's arrow, which ends 0.7rem from the edge and is two 0.3rem halves wide");
            Assert.That(chevron, Does.Contain("pointer-events: none;"), "a press on it lands on the input, which opens the list");
            Assert.That(Body(ShellStylesheets.Primitives, ".lt-combobox__control--picker > .lt-input"), Does.Contain("padding-inline-end: 2rem;"),
                "typed text never runs under the chevron");
            Assert.That(Body(ShellStylesheets.Primitives, ".lt-combobox__control[data-lt-open] > .lt-combobox__chevron"), Does.Contain("rotate(180deg)"));
        });
    }

    [Test]
    public void A_multi_value_field_is_one_frame_around_its_chips_and_its_input()
    {
        var frame = Body(ShellStylesheets.Primitives, ".lt-combobox__control--tokens");
        var input = Body(ShellStylesheets.Primitives, ".lt-combobox__control--tokens > .lt-input");
        var chip = Body(ShellStylesheets.Primitives, ".lt-combobox__chip");

        Assert.Multiple(() =>
        {
            Assert.That(frame, Does.Contain("border: 1px solid var(--lt-op-control-border);"));
            Assert.That(frame, Does.Contain("background: var(--lt-surface-sunken);"));
            Assert.That(input, Does.Contain("border: 0;"), "the input draws no second box inside the frame");
            Assert.That(chip, Does.Not.Contain("border-radius"), "a chip is set off by a hairline, not boxed as a second control");
            Assert.That(chip, Does.Contain("min-height: var(--lt-op-control-height);"), "each chip's remove control stays a full touch target");
            Assert.That(Body(ShellStylesheets.Primitives, ".lt-combobox__control--invalid"), Does.Contain("border-color: var(--lt-danger);"));
        });
    }

    [Test]
    public void A_mono_table_cell_never_breaks_inside_an_id()
    {
        var cell = Body(ShellStylesheets.Primitives, ".lt-table__cell--mono");

        Assert.Multiple(() =>
        {
            Assert.That(cell, Does.Contain("white-space: nowrap;"));
            Assert.That(cell, Does.Contain("text-overflow: ellipsis;"));
            Assert.That(cell, Does.Contain("overflow: hidden;"));
            Assert.That(cell, Does.Match(@"max-inline-size:\s*\d+(?:\.\d+)?rem;"), "a long id is cut at a bound, not allowed to widen the table without limit");
            Assert.That(cell, Does.Not.Contain("overflow-wrap"));
        });
    }

    [Test]
    public void An_empty_table_draws_no_booktabs_rules()
    {
        var empty = Body(ShellStylesheets.Primitives, ".lt-table__empty");

        Assert.That(empty, Does.Not.Contain("border"));
    }

    [Test]
    public void The_tenant_switchers_list_opens_in_flow_inside_its_panel()
    {
        Assert.That(Body(Chrome, ".lt-shell-tenant .lt-combobox__list"), Does.Contain("position: static;"),
            "a floating list overflowed the panel's hairline border");
    }

    private static string Body(string stylesheet, string selector)
    {
        var matches = ShellStylesheets.Rules(stylesheet).Where(rule => rule.AtRule.Length == 0 && rule.Selector == selector).ToArray();
        Assert.That(matches, Has.Length.EqualTo(1), $"{stylesheet} must declare exactly one top-level '{selector}' rule");
        return matches[0].Body;
    }
}
