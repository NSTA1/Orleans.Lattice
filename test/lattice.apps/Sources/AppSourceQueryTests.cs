using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public class AppSourceQueryTests
{
    [Test]
    public void Default_is_the_first_page_at_the_default_size_without_text()
    {
        var query = AppSourceQuery.Default;

        Assert.That(query.Text, Is.Null);
        Assert.That(query.Continuation, Is.Null);
        Assert.That(query.PageSize, Is.EqualTo(AppSourceQuery.DefaultPageSize));
        Assert.That(new AppSourceQuery().PageSize, Is.EqualTo(50));
        Assert.That(AppSourceQuery.Default, Is.SameAs(AppSourceQuery.Default));
    }

    [TestCase(int.MinValue, 1)]
    [TestCase(-1, 1)]
    [TestCase(0, 1)]
    [TestCase(1, 1)]
    [TestCase(37, 37)]
    [TestCase(200, 200)]
    [TestCase(201, 200)]
    [TestCase(int.MaxValue, 200)]
    public void PageSize_is_clamped_to_the_allowed_range(int requested, int expected)
    {
        Assert.That(new AppSourceQuery { PageSize = requested }.PageSize, Is.EqualTo(expected));
    }

    [Test]
    public void Bounds_are_one_and_two_hundred()
    {
        Assert.That(AppSourceQuery.MinPageSize, Is.EqualTo(1));
        Assert.That(AppSourceQuery.MaxPageSize, Is.EqualTo(200));
    }

    [Test]
    public void Text_and_continuation_are_carried_and_compare_by_value()
    {
        var query = new AppSourceQuery { Text = "notes", Continuation = "token", PageSize = 5 };

        Assert.That(query.Text, Is.EqualTo("notes"));
        Assert.That(query.Continuation, Is.EqualTo("token"));
        Assert.That(query, Is.EqualTo(new AppSourceQuery { Text = "notes", Continuation = "token", PageSize = 5 }));
        Assert.That((query with { PageSize = 1000 }).PageSize, Is.EqualTo(200));
    }
}
