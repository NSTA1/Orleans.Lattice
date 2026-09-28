using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public class AppSourcePageTests
{
    private static AppSourceEntry Entry(string slug) =>
        AppSourceEntry.Unavailable(AppSlug.Parse(slug), [new("json", "$", "Bad.")]);

    [Test]
    public void Empty_is_a_shared_final_page()
    {
        Assert.That(AppSourcePage.Empty.Entries, Is.Empty);
        Assert.That(AppSourcePage.Empty.Continuation, Is.Null);
        Assert.That(AppSourcePage.Empty.HasMore, Is.False);
        Assert.That(AppSourcePage.Create([], null), Is.SameAs(AppSourcePage.Empty));
    }

    [Test]
    public void Create_copies_entries_and_carries_the_continuation()
    {
        var entries = new List<AppSourceEntry> { Entry("alpha"), Entry("beta") };

        var page = AppSourcePage.Create(entries, "next");
        entries.Clear();

        Assert.That(page.Entries.Select(e => e.Slug.Value), Is.EqualTo(new[] { "alpha", "beta" }));
        Assert.That(page.Continuation, Is.EqualTo("next"));
        Assert.That(page.HasMore, Is.True);
    }

    [Test]
    public void Create_allows_an_empty_page_that_still_has_more()
    {
        var page = AppSourcePage.Create([], "next");

        Assert.That(page, Is.Not.SameAs(AppSourcePage.Empty));
        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.HasMore, Is.True);
    }

    [Test]
    public void Create_rejects_null_entries_or_an_empty_continuation()
    {
        Assert.Throws<ArgumentNullException>(() => AppSourcePage.Create(null!, null));
        Assert.Throws<ArgumentException>(() => AppSourcePage.Create([null!], null));
        Assert.Throws<ArgumentException>(() => AppSourcePage.Create([Entry("alpha")], string.Empty));
    }

    [Test]
    public void Wrap_uses_the_array_without_copying()
    {
        var entries = new[] { Entry("alpha") };

        var page = AppSourcePage.Wrap(entries, null);

        Assert.That(page.Entries, Is.SameAs(entries));
        Assert.That(AppSourcePage.Wrap([], null), Is.SameAs(AppSourcePage.Empty));
    }
}
