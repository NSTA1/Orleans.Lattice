using Bunit;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Areas.Access;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Access;

/// <summary>
/// The rule table on its own: its caption and empty sentence, and an app-owned
/// rule whose id carries no readable slug, which is still never editable.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessRuleTableTests : AccessTestContext
{
    [Test]
    public void The_caption_and_empty_sentence_are_the_callers()
    {
        var cut = Render<AccessRuleTable>(parameters => parameters
            .Add(table => table.Rules, [])
            .Add(table => table.Caption, "Matched rules")
            .Add(table => table.EmptyText, "Nothing matched."));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("caption").TextContent, Is.EqualTo("Matched rules"));
            Assert.That(cut.Find("caption").ClassList, Does.Not.Contain("lt-visually-hidden"));
            Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Is.EqualTo("Nothing matched."));
        });
    }

    [Test]
    public void An_app_owned_rule_without_a_readable_slug_is_attributed_to_an_app_without_a_link()
    {
        var rule = new LatticeAuthorizationRule("app::viewer:1", LatticeSubjectSelector.Group("g"), LatticeScope.Tree("t"), LatticeOperation.Read, LatticeEffect.Allow);

        var cut = Render<AccessRuleTable>(parameters => parameters
            .Add(table => table.Rules, [rule])
            .Add(table => table.CaptionHidden, true));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("tbody td:last-child").TextContent.Trim(), Is.EqualTo("an app"));
            Assert.That(cut.FindAll("tbody td:last-child a"), Is.Empty);
            Assert.That(cut.Find("caption").ClassList, Does.Contain("lt-visually-hidden"));
        });
    }

    [Test]
    public void Columns_sort_by_rule_id()
    {
        var cut = Render<AccessRuleTable>(parameters => parameters.Add(table => table.Rules, [Rule("b"), Rule("a")]));

        cut.Find("th button.lt-table__sort").Click();

        Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent), Is.EqualTo(new[] { "a", "b" }));
    }
}
