using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

[TestFixture]
public sealed class AppMcpToolNameTests
{
    [Test]
    public void Compose_prefixes_the_slug_and_separator()
        => Assert.That(AppMcpToolName.Compose(AppSlug.Parse("notes"), "search"), Is.EqualTo("notes_search"));

    [Test]
    public void Compose_rejects_an_uninitialised_slug_or_empty_tool_name()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentException>(() => AppMcpToolName.Compose(default, "search"));
            Assert.Throws<ArgumentException>(() => AppMcpToolName.Compose(AppSlug.Parse("notes"), ""));
            Assert.Throws<ArgumentNullException>(() => AppMcpToolName.Compose(AppSlug.Parse("notes"), null!));
        });
    }

    [Test]
    public void TryParse_round_trips_a_composed_name_even_when_the_local_name_contains_the_separator()
    {
        var name = AppMcpToolName.Compose(AppSlug.Parse("notes"), "search_all");

        Assert.Multiple(() =>
        {
            Assert.That(AppMcpToolName.TryParse(name, out var slug, out var tool), Is.True);
            Assert.That(slug, Is.EqualTo(AppSlug.Parse("notes")));
            Assert.That(tool, Is.EqualTo("search_all"));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("notes")]
    [TestCase("_search")]
    [TestCase("notes_")]
    [TestCase("Notes_search")]
    [TestCase("n_search")]
    public void TryParse_rejects_a_name_that_is_not_slug_separator_tool(string? name)
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppMcpToolName.TryParse(name, out var slug, out var tool), Is.False);
            Assert.That(slug, Is.EqualTo(default(AppSlug)));
            Assert.That(tool, Is.Null);
        });
    }

    [Test]
    public void The_same_local_name_in_two_apps_composes_to_distinct_names()
        => Assert.That(
            AppMcpToolName.Compose(AppSlug.Parse("notes"), "search"),
            Is.Not.EqualTo(AppMcpToolName.Compose(AppSlug.Parse("tasks"), "search")));

    [Test]
    public void Separator_is_an_underscore()
        => Assert.That(AppMcpToolName.Separator, Is.EqualTo('_'));
}
