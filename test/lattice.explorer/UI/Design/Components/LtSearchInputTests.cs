using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The search input: always labelled, Enter submits, Escape clears, and a key
/// hint is shown and announced through <c>aria-keyshortcuts</c>.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtSearchInputTests : ShellDesignTestContext
{
    [Test]
    public void The_label_is_present_for_assistive_technology_and_bound_to_the_input()
    {
        var cut = Render<LtSearchInput>(p => p.Add(x => x.Label, "Filter trees"));
        var label = cut.Find("label");
        var input = cut.Find("input");

        Assert.Multiple(() =>
        {
            Assert.That(label.TextContent, Is.EqualTo("Filter trees"));
            Assert.That(label.ClassList, Does.Contain("lt-visually-hidden"));
            Assert.That(label.GetAttribute("for"), Is.EqualTo(input.Id));
            Assert.That(input.GetAttribute("type"), Is.EqualTo("search"));
        });
    }

    [Test]
    public void Enter_submits_the_current_query()
    {
        string? submitted = null;
        var cut = Render<LtSearchInput>(p => p
            .Add(x => x.Label, "Filter")
            .Add(x => x.Value, "orders")
            .Add(x => x.OnSubmit, (string query) => submitted = query));

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Enter" });

        Assert.That(submitted, Is.EqualTo("orders"));
    }

    [Test]
    public void Escape_clears_a_query()
    {
        var observed = new List<string>();
        var cut = Render<LtSearchInput>(p => p
            .Add(x => x.Label, "Filter")
            .Add(x => x.Value, "orders")
            .Add(x => x.ValueChanged, (string value) => observed.Add(value)));

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.That(observed, Is.EqualTo(new[] { string.Empty }));
    }

    [Test]
    public void Escape_on_an_empty_query_changes_nothing()
    {
        var observed = new List<string>();
        var cut = Render<LtSearchInput>(p => p
            .Add(x => x.Label, "Filter")
            .Add(x => x.ValueChanged, (string value) => observed.Add(value)));

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.That(observed, Is.Empty);
    }

    [Test]
    public void Typing_raises_ValueChanged()
    {
        string? observed = null;
        var cut = Render<LtSearchInput>(p => p.Add(x => x.Label, "Filter").Add(x => x.ValueChanged, (string value) => observed = value));

        cut.Find("input").Input("crm");

        Assert.That(observed, Is.EqualTo("crm"));
    }

    [Test]
    public void A_key_shortcut_is_shown_as_a_hint_and_announced()
    {
        var cut = Render<LtSearchInput>(p => p.Add(x => x.Label, "Search").Add(x => x.KeyShortcut, "/"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("input").GetAttribute("aria-keyshortcuts"), Is.EqualTo("/"));
            Assert.That(cut.Find("kbd").TextContent, Is.EqualTo("/"));
            Assert.That(cut.Find("kbd").GetAttribute("aria-hidden"), Is.EqualTo("true"),
                "the hint is visual; aria-keyshortcuts is what announces it");
        });
    }

    [Test]
    public void Without_a_shortcut_there_is_no_hint()
    {
        var cut = Render<LtSearchInput>(p => p.Add(x => x.Label, "Filter"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("kbd"), Is.Empty);
            Assert.That(cut.Find("input").HasAttribute("aria-keyshortcuts"), Is.False);
        });
    }

    [Test]
    [TestCase(true, "search")]
    [TestCase(false, null)]
    public void Only_the_page_search_is_a_search_landmark(bool landmark, string? expectedRole)
    {
        var cut = Render<LtSearchInput>(p => p.Add(x => x.Label, "Search").Add(x => x.Landmark, landmark));

        Assert.That(cut.Find(".lt-search").GetAttribute("role"), Is.EqualTo(expectedRole));
    }
}
