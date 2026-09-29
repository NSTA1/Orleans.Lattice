using Bunit;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The placeholder states: an empty state is a hollow node, a heading at the
/// page's level and the action that would change it; a skeleton is announced
/// once as a busy status.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtPlaceholderTests : ShellDesignTestContext
{
    [Test]
    public void An_empty_state_is_a_hollow_node_a_heading_an_explanation_and_an_action()
    {
        var cut = Render<LtEmptyState>(p => p
            .Add(x => x.Title, "No apps installed")
            .AddChildContent("Browse a source to install one.")
            .Add(x => x.Actions, "<button>Browse</button>"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h2").TextContent, Is.EqualTo("No apps installed"));
            Assert.That(cut.Find(".lt-empty__body").TextContent, Is.EqualTo("Browse a source to install one."));
            Assert.That(cut.Find(".lt-empty__actions button").TextContent, Is.EqualTo("Browse"));
            Assert.That(cut.Find(".lt-node").ClassName, Is.EqualTo("lt-node lt-node--hollow lt-node--large"));
        });
    }

    [Test]
    [TestCase(3, "h3")]
    [TestCase(4, "h4")]
    public void The_heading_follows_the_page_outline(int level, string element)
    {
        var cut = Render<LtEmptyState>(p => p.Add(x => x.Title, "No keys").Add(x => x.HeadingLevel, level));

        Assert.That(cut.Find(element).TextContent, Is.EqualTo("No keys"));
    }

    [Test]
    [TestCase(1)]
    [TestCase(5)]
    public void A_heading_level_outside_two_to_four_is_rejected(int level)
    {
        Assert.That(
            () => Render<LtEmptyState>(p => p.Add(x => x.Title, "No keys").Add(x => x.HeadingLevel, level)),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Without_content_or_actions_only_the_heading_is_rendered()
    {
        var cut = Render<LtEmptyState>(p => p.Add(x => x.Title, "Nothing here"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-empty__body"), Is.Empty);
            Assert.That(cut.FindAll(".lt-empty__actions"), Is.Empty);
        });
    }

    [Test]
    public void A_skeleton_is_one_busy_status_with_hidden_lines()
    {
        var cut = Render<LtSkeleton>(p => p.Add(x => x.Lines, 4).Add(x => x.Label, "Loading trees"));
        var status = cut.Find("[role=status]");

        Assert.Multiple(() =>
        {
            Assert.That(status.GetAttribute("aria-busy"), Is.EqualTo("true"));
            Assert.That(status.QuerySelector(".lt-visually-hidden")?.TextContent, Is.EqualTo("Loading trees"));
            Assert.That(cut.FindAll(".lt-skeleton__line"), Has.Count.EqualTo(4));
            Assert.That(cut.FindAll(".lt-skeleton__line").All(line => line.GetAttribute("aria-hidden") == "true"), Is.True);
        });
    }

    [Test]
    public void A_skeleton_defaults_to_three_lines_announced_as_loading()
    {
        var cut = Render<LtSkeleton>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-skeleton__line"), Has.Count.EqualTo(3));
            Assert.That(cut.Find(".lt-visually-hidden").TextContent, Is.EqualTo("Loading"));
        });
    }

    [Test]
    [TestCase(0)]
    [TestCase(LtSkeleton.MaximumLines + 1)]
    public void A_skeleton_line_count_outside_its_bounds_is_rejected(int lines)
    {
        Assert.That(() => Render<LtSkeleton>(p => p.Add(x => x.Lines, lines)), Throws.TypeOf<ArgumentOutOfRangeException>());
    }
}
